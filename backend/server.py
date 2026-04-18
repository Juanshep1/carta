"""
CARTA — A Personal Codex
FastAPI backend: Wikipedia fetcher, image pipeline, SQLite store, Ollama-powered CARTA.

Run:  ./start.sh
Env:  OLLAMA_URL, OLLAMA_MODEL, CARTA_PORT
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Optional, Any
from urllib.parse import unquote, urlparse

import httpx
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# ---------- paths & config ----------
BASE = Path(__file__).parent
DB_PATH = BASE / "carta.db"
IMG_DIR = BASE / "static" / "images"
IMG_DIR.mkdir(parents=True, exist_ok=True)
# Frontend location (used for local dev; on Netlify the site is served separately)
FRONTEND_HTML = BASE.parent / "frontend" / "index.html"

OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://127.0.0.1:11434").rstrip("/")
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "llama3.2:1b")
USER_AGENT = os.environ.get(
    "CARTA_USER_AGENT",
    "CARTA/1.0 (personal-wiki; https://github.com/; contact: zebrosyeah@yahoo.com) python-httpx"
)
# Shared-secret bearer token. Empty = no auth (local dev). Set via env for public access.
CARTA_API_KEY = os.environ.get("CARTA_API_KEY", "").strip()

WIKI_REST = "https://en.wikipedia.org/api/rest_v1"
WIKI_API = "https://en.wikipedia.org/w/api.php"

log = logging.getLogger("carta")
logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(name)s  %(levelname)s  %(message)s")

PROVINCES_SEED = [
    ("Natural Sciences",            "⚛", "Physics, biology, chemistry — the world beneath the skin of things."),
    ("Computation & Cryptography",  "⌘", "The logic of machines and the secrets kept by mathematics."),
    ("Philosophy & Ethics",         "☙", "How to think; how to live."),
    ("History of Ideas",            "§", "What was once thought and why it matters still."),
    ("Mathematics",                 "∫", "The pure architecture."),
    ("Languages",                   "℞", "What humans do when they make sound carry meaning."),
    ("Music & Art",                 "♪", "Pattern, proportion, and that which cannot be said."),
    ("Earth & Cosmos",              "✶", "From plate tectonics to the Oort cloud."),
    ("Hacking & Security",          "⚷", "Offensive and defensive arts of the networked world."),
    ("Economics",                   "₿", "Incentives, scarcity, and the study of what people do."),
    ("Medicine & Body",             "☤", "The organism and its repair."),
    ("Technology",                  "⚙", "Tools, engineering, and the systems we build."),
    ("Curiosities",                 "❦", "Things too strange for any other shelf."),
    ("Miscellaneous",               "⁂", "Everything else that didn't fit elsewhere."),
]

# Canonical province set. The capture endpoint classifies into one of these
# and never creates new provinces. Keep in sync with PROVINCES_SEED names.
PROVINCE_NAMES = [p[0] for p in PROVINCES_SEED]

# Keyword heuristics used when the LLM is unavailable or returns something
# unrecognizable. Order matters — first match wins, so more specific categories
# (Hacking & Security, Computation) come before broader ones (Natural Sciences).
PROVINCE_KEYWORDS: list[tuple[str, list[str]]] = [
    ("Hacking & Security",          ["hacking", "cybersecurity", "malware", "exploit", "penetration testing",
                                      "vulnerability", "phishing", "ransomware", "cryptanalysis"]),
    ("Computation & Cryptography",  ["algorithm", "computer science", "programming", "software", "compiler",
                                      "operating system", "data structure", "cryptography", "encryption",
                                      "cipher", "hash function", "turing", "computation"]),
    ("Technology",                  ["technology", "engineering", "invention", "device", "machine",
                                      "gadget", "hardware", "electronics", "manufacturing", "robotics",
                                      "automobile", "aviation", "spacecraft", "internet", "smartphone"]),
    ("Mathematics",                 ["theorem", "mathematics", "geometry", "algebra", "calculus",
                                      "topology", "number theory", "equation", "proof", "combinatorics"]),
    ("Natural Sciences",            ["physics", "chemistry", "biology", "zoology", "botany", "genetics",
                                      "ecology", "species", "organism", "molecule", "atom", "quantum",
                                      "evolution", "ecosystem"]),
    ("Earth & Cosmos",              ["astronomy", "planet", "galaxy", "star", "solar system", "geology",
                                      "plate tectonics", "meteorology", "climate", "oceanography",
                                      "volcano", "earthquake", "cosmos", "nebula", "black hole"]),
    ("Medicine & Body",             ["medicine", "disease", "anatomy", "physiology", "surgery", "pharmacology",
                                      "neuroscience", "virus", "bacteria", "vaccine", "cancer", "immune",
                                      "pathology", "health"]),
    ("Philosophy & Ethics",         ["philosophy", "ethics", "metaphysics", "epistemology", "phenomenology",
                                      "stoicism", "existentialism", "moral", "virtue"]),
    ("History of Ideas",            ["history", "historical", "enlightenment", "renaissance", "civilization",
                                      "ancient", "medieval", "empire", "revolution", "dynasty", "century"]),
    ("Languages",                   ["language", "linguistics", "grammar", "phonetics", "syntax", "etymology",
                                      "dialect", "alphabet", "script", "translation"]),
    ("Music & Art",                 ["music", "composer", "symphony", "opera", "painting", "sculpture",
                                      "painter", "artist", "architecture", "film", "cinema", "poetry",
                                      "literature", "novel", "theatre", "dance"]),
    ("Economics",                   ["economics", "economy", "finance", "market", "inflation", "currency",
                                      "trade", "capital", "bank", "stock", "bond", "monetary"]),
]

# ---------- helpers ----------
def slugify(text: str) -> str:
    text = (text or "").lower().replace("&", "and")
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s_-]+", "-", text).strip("-")
    return text or "untitled"


@contextmanager
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        yield conn
    finally:
        conn.close()


def init_db() -> None:
    with get_db() as db:
        with open(BASE / "schema.sql") as f:
            db.executescript(f.read())

        # Upsert the canonical seed so new provinces (e.g. Technology) are added
        # even if the table already exists from a prior deploy.
        for name, icon, desc in PROVINCES_SEED:
            db.execute("""
                INSERT INTO provinces (name, slug, icon, description)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(slug) DO UPDATE SET
                    name = excluded.name, icon = excluded.icon, description = excluded.description
            """, (name, slugify(name), icon, desc))

        # Migration: any province not in the canonical set is a legacy entry
        # (e.g. "New Orleans, Louisiana" created by the old capture code). Move
        # its articles to Curiosities and delete the province. Provinces are
        # static from here on — capture classifies into PROVINCE_NAMES only.
        canonical_slugs = [slugify(n) for n in PROVINCE_NAMES]
        placeholders = ",".join("?" * len(canonical_slugs))
        curios = db.execute(
            "SELECT id FROM provinces WHERE slug = 'curiosities'"
        ).fetchone()
        if curios:
            stray = db.execute(
                f"SELECT id, name FROM provinces WHERE slug NOT IN ({placeholders})",
                canonical_slugs,
            ).fetchall()
            for row in stray:
                db.execute(
                    "UPDATE articles SET province_id = ? WHERE province_id = ?",
                    (curios["id"], row["id"]),
                )
                db.execute("DELETE FROM provinces WHERE id = ?", (row["id"],))
                log.info("migration: moved articles out of legacy province '%s' to Curiosities", row["name"])

            # One-off: ensure the Hoodoo disambiguation lives in Curiosities.
            db.execute("""
                UPDATE articles SET province_id = ?
                 WHERE slug = 'hoodoo' OR lower(title) = 'hoodoo'
            """, (curios["id"],))

        db.commit()
        log.info("provinces canonicalized (%d)", len(PROVINCES_SEED))


# ---------- Wikipedia ----------
async def wiki_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        timeout=httpx.Timeout(30, connect=10),
        headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
        follow_redirects=True,
    )


async def resolve_title(query: str) -> Optional[str]:
    """Given a user topic, resolve to an exact Wikipedia article title."""
    async with await wiki_client() as client:
        r = await client.get(WIKI_API, params={
            "action": "opensearch", "search": query, "limit": 1,
            "namespace": 0, "format": "json",
        })
        r.raise_for_status()
        data = r.json()
        if data and len(data) > 1 and data[1]:
            return data[1][0]
    return None


async def fetch_summary(title: str) -> dict:
    async with await wiki_client() as client:
        r = await client.get(f"{WIKI_REST}/page/summary/{_enc(title)}")
        if r.status_code == 404:
            raise HTTPException(404, f"Wikipedia has no article '{title}'")
        r.raise_for_status()
        return r.json()


async def fetch_article_html(title: str) -> str:
    """Parsoid HTML — clean, structured, one <section> per heading."""
    async with await wiki_client() as client:
        r = await client.get(f"{WIKI_REST}/page/html/{_enc(title)}", headers={"Accept": "text/html"})
        if r.status_code == 404:
            # fall back to action=parse
            p = await client.get(WIKI_API, params={
                "action": "parse", "page": title, "prop": "text", "format": "json",
            })
            p.raise_for_status()
            return p.json().get("parse", {}).get("text", {}).get("*", "")
        r.raise_for_status()
        return r.text


def _enc(s: str) -> str:
    # Wikipedia prefers underscores + URL-encoded segments, but httpx handles it fine
    return s.replace(" ", "_")


CTYPE_EXT = {
    "image/jpeg": ".jpg", "image/pjpeg": ".jpg",
    "image/png": ".png",
    "image/gif": ".gif",
    "image/svg+xml": ".svg", "image/svg": ".svg",
    "image/webp": ".webp",
}

def _sniff_ext(body: bytes) -> Optional[str]:
    """Detect image type by magic number."""
    if len(body) < 4:
        return None
    if body[:4] == b"\x89PNG":
        return ".png"
    if body[:2] == b"\xff\xd8":
        return ".jpg"
    if body[:4] == b"GIF8":
        return ".gif"
    if body[:4] == b"RIFF" and len(body) >= 12 and body[8:12] == b"WEBP":
        return ".webp"
    head = body[:512].lstrip()
    if head.startswith(b"<?xml") or head.startswith(b"<svg"):
        return ".svg"
    return None


async def download_image(url: str, client: httpx.AsyncClient) -> Optional[tuple[str, int]]:
    """Download an image. Returns (filename, byte_size) or None on failure.
    Extension picked from content-type → URL suffix → magic-number sniff."""
    try:
        if url.startswith("//"):
            url = "https:" + url
        if not url.startswith("http"):
            return None
        r = await client.get(url, timeout=45)
        if r.status_code != 200 or not r.content or len(r.content) < 80:
            return None

        # 1) content-type is authoritative
        ctype = r.headers.get("content-type", "").split(";")[0].strip().lower()
        ext = CTYPE_EXT.get(ctype)

        # 2) URL suffix
        if not ext:
            path = urlparse(url).path
            url_ext = (Path(unquote(path)).suffix or "").lower()
            if url_ext == ".jpeg":
                ext = ".jpg"
            elif url_ext in {".jpg", ".png", ".gif", ".svg", ".webp"}:
                ext = url_ext

        # 3) magic-number sniff
        if not ext:
            ext = _sniff_ext(r.content)

        if not ext:
            log.info("skipping image with unknown type: %s (ctype=%s)", url, ctype)
            return None

        h = hashlib.sha256(url.encode()).hexdigest()[:24]
        filename = f"{h}{ext}"
        out = IMG_DIR / filename
        if not out.exists():
            out.write_bytes(r.content)
        return filename, len(r.content)
    except Exception as e:
        log.warning("image download failed %s: %s", url, e)
        return None


REMOVE_SELECTORS = [
    ".mw-editsection", ".hatnote", ".navigation-not-searchable",
    ".metadata", ".ambox", ".sistersitebox", ".mbox-small",
    ".reference", ".mw-references-wrap", ".reflist", ".refbegin", ".references",
    ".navbox", ".vertical-navbox", ".infobox", ".shortdescription",
    ".thumb .magnify", ".mw-empty-elt", ".mw-headline-anchor",
    ".error", "style", "link", "script", "noscript",
    ".noprint", ".mw-kartographer-container", ".portal", ".sidebar",
    "[role=note]", "[role=navigation]",
    ".IPA", ".plainlinks", ".external.text",
    ".Inline-Template", ".ombox",
]

# Tags to strip attributes from (keep structure, drop mw-* clutter)
ATTR_KEEP = {"src", "alt", "href", "title", "colspan", "rowspan", "target", "rel", "data-wiki"}


async def process_article_html(raw_html: str) -> tuple[str, list[dict]]:
    """Clean Parsoid/action-parse HTML; download images; rewrite links."""
    soup = BeautifulSoup(raw_html, "lxml")

    # Remove the Wikipedia base <head>, keep body/section content
    for tag in soup.find_all(["head", "meta", "link", "style", "script"]):
        tag.decompose()

    for sel in REMOVE_SELECTORS:
        for el in soup.select(sel):
            el.decompose()

    # Remove empty <sup> (citation markers) and <span class=mw-ref>
    for sup in soup.find_all("sup"):
        if sup.get("class") and any(c.startswith("reference") or c == "noprint" for c in sup.get("class")):
            sup.decompose()
        elif not sup.get_text(strip=True):
            sup.decompose()

    # Collect image tasks
    img_jobs: list[tuple[Any, str]] = []
    for img in soup.find_all("img"):
        src = img.get("src") or img.get("data-src") or ""
        if not src:
            img.decompose()
            continue
        if src.startswith("//"):
            src = "https:" + src
        elif src.startswith("/"):
            src = "https://en.wikipedia.org" + src
        img_jobs.append((img, src))

    images: list[dict] = []
    if img_jobs:
        async with httpx.AsyncClient(
            timeout=60, headers={"User-Agent": USER_AGENT}, follow_redirects=True
        ) as client:
            sem = asyncio.Semaphore(6)

            async def _dl(img_tag, url):
                async with sem:
                    return img_tag, url, await download_image(url, client)

            results = await asyncio.gather(*[_dl(i, u) for i, u in img_jobs])

        for img, url, got in results:
            if not got:
                img.decompose()
                continue
            fname, size = got
            img["src"] = f"/static/images/{fname}"
            for a in ("srcset", "data-src", "data-file-width", "data-file-height",
                      "resource", "data-mw"):
                if img.has_attr(a):
                    del img[a]
            # Lazy-load in browser
            img["loading"] = "lazy"
            img["decoding"] = "async"
            images.append({
                "filename": fname,
                "wiki_url": url,
                "alt": img.get("alt", ""),
                "size": size,
            })

    # Rewrite internal wiki links
    for a in soup.find_all("a"):
        href = a.get("href", "")
        if not href:
            continue
        if href.startswith("./"):
            target = unquote(href[2:].split("#", 1)[0])
            a["href"] = f"#/article/{slugify(target)}"
            a["data-wiki"] = target
        elif href.startswith("/wiki/"):
            target = unquote(href[6:].split("#", 1)[0])
            a["href"] = f"#/article/{slugify(target)}"
            a["data-wiki"] = target
        elif href.startswith("#"):
            pass  # in-page anchor
        elif href.startswith(("http://", "https://")):
            a["target"] = "_blank"
            a["rel"] = "noopener noreferrer"
        # drop action/edit/template links
        for attr in list(a.attrs):
            if attr not in ATTR_KEEP and attr != "class":
                del a[attr]

    # Strip mw-* classes but keep layout-useful ones
    for el in soup.find_all(True):
        if el.has_attr("class"):
            keep = [c for c in el["class"] if not c.startswith("mw-") and not c.startswith("cite")]
            if keep:
                el["class"] = keep
            else:
                del el["class"]
        for attr in ("id", "typeof", "about", "data-mw", "rel"):
            if el.has_attr(attr) and attr not in ATTR_KEEP:
                # keep ids on headings for TOC anchoring
                if attr == "id" and el.name in {"h2", "h3", "h4", "section"}:
                    continue
                del el[attr]

    # Extract body contents
    body = soup.find("body")
    content = body.decode_contents() if body else str(soup)

    # Drop the final "References" / "External links" / "See also" sections if present
    # (quick heuristic; Parsoid wraps each heading in <section>)
    content_soup = BeautifulSoup(content, "lxml")
    KILL_HEADINGS = {"references", "external links", "see also", "further reading",
                     "notes", "bibliography", "citations", "sources"}
    for section in content_soup.find_all("section"):
        h = section.find(["h2", "h3"])
        if h and h.get_text(strip=True).lower() in KILL_HEADINGS:
            section.decompose()
    # also handle flat structure
    for h in content_soup.find_all(["h2", "h3"]):
        if h.get_text(strip=True).lower() in KILL_HEADINGS:
            # remove this heading and everything after it until the next same-or-higher heading
            to_remove = [h]
            for sib in h.find_next_siblings():
                if sib.name in {"h2"} and h.name == "h2":
                    break
                to_remove.append(sib)
            for el in to_remove:
                el.decompose()

    content = str(content_soup.body.decode_contents()) if content_soup.body else str(content_soup)
    return content, images


def extract_text(html: str) -> str:
    return BeautifulSoup(html, "lxml").get_text(" ", strip=True)


# ---------- Ollama ----------
async def ollama_chat(messages: list[dict], *, temperature: float = 0.7,
                      format_json: bool = False, max_tokens: int = 400) -> str:
    payload: dict[str, Any] = {
        "model": OLLAMA_MODEL,
        "messages": messages,
        "stream": False,
        "options": {"temperature": temperature, "num_predict": max_tokens},
    }
    if format_json:
        payload["format"] = "json"
    async with httpx.AsyncClient(timeout=180) as client:
        r = await client.post(f"{OLLAMA_URL}/api/chat", json=payload)
        r.raise_for_status()
        data = r.json()
        reply = data.get("message", {}).get("content", "")
        # strip <think> blocks some models emit
        return re.sub(r"<think>.*?</think>", "", reply, flags=re.DOTALL).strip()


# ---------- FastAPI ----------
app = FastAPI(title="CARTA", version="1.1")

# CORS — allow Netlify deploys, localhost dev, and tailnet IPs
app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=(
        r"https://([a-z0-9-]+\.)*(netlify\.app|netlify\.com|ts\.net)"
        r"|https?://(localhost|127\.0\.0\.1)(:[0-9]+)?"
        r"|https?://10\.\d+\.\d+\.\d+(:[0-9]+)?"
        r"|https?://192\.168\.\d+\.\d+(:[0-9]+)?"
        r"|https?://100\.\d+\.\d+\.\d+(:[0-9]+)?"
    ),
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)
app.add_middleware(GZipMiddleware, minimum_size=900)


PUBLIC_PATHS = {"/", "/healthz"}


@app.middleware("http")
async def auth_and_perf(request: Request, call_next):
    path = request.url.path
    # Auth gate. OPTIONS preflight, healthz, root, and static images are always public.
    # Static images are fetched by <img> tags which can't set headers, so we let them be
    # public once downloaded — they are content-addressed (sha256), so knowing the URL
    # already implies access.
    needs_auth = (
        CARTA_API_KEY
        and request.method != "OPTIONS"
        and path not in PUBLIC_PATHS
        and not path.startswith("/static/")
    )
    if needs_auth:
        auth = request.headers.get("authorization", "")
        key = auth[7:].strip() if auth.startswith("Bearer ") else request.query_params.get("key", "")
        if key != CARTA_API_KEY:
            return JSONResponse({"error": "unauthorized"}, status_code=401)

    response = await call_next(request)
    if path.startswith("/static/images/"):
        response.headers["Cache-Control"] = "public, max-age=31536000, immutable"
    elif path == "/" or path.endswith(".html"):
        response.headers["Cache-Control"] = "no-store, must-revalidate"
        response.headers["Pragma"] = "no-cache"
    return response


app.mount("/static", StaticFiles(directory=BASE / "static"), name="static")


@app.on_event("startup")
def _startup():
    init_db()
    auth_state = "ENABLED" if CARTA_API_KEY else "DISABLED (local-only)"
    log.info("CARTA ready. DB=%s  Ollama=%s  Model=%s  Auth=%s",
             DB_PATH, OLLAMA_URL, OLLAMA_MODEL, auth_state)


@app.get("/")
def root():
    if FRONTEND_HTML.exists():
        return FileResponse(FRONTEND_HTML)
    return JSONResponse({
        "service": "CARTA",
        "version": "1.1",
        "note": "API only. Frontend is served separately (e.g. Netlify).",
    })


@app.get("/healthz")
async def health():
    ok_ollama = False
    model_present = False
    try:
        async with httpx.AsyncClient(timeout=3) as client:
            r = await client.get(f"{OLLAMA_URL}/api/tags")
            if r.status_code == 200:
                ok_ollama = True
                models = [m.get("name", "") for m in r.json().get("models", [])]
                model_present = OLLAMA_MODEL in models
    except Exception:
        pass
    return {"ok": True, "ollama": ok_ollama, "model": OLLAMA_MODEL, "model_present": model_present}


# ---------- /api/atlas ----------
@app.get("/api/atlas")
def api_atlas():
    with get_db() as db:
        art_count = db.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
        marg_count = db.execute("SELECT COUNT(*) FROM marginalia").fetchone()[0]
        prov_total = db.execute("SELECT COUNT(*) FROM provinces").fetchone()[0]

        provinces = [dict(r) for r in db.execute("""
            SELECT p.id, p.name, p.slug, p.icon, p.description,
                   COUNT(a.id) AS article_count
              FROM provinces p
              LEFT JOIN articles a ON a.province_id = p.id
             GROUP BY p.id
             ORDER BY article_count DESC, p.name
        """).fetchall()]
        for p in provinces:
            p["recent"] = [r["title"] for r in db.execute(
                "SELECT title FROM articles WHERE province_id = ? ORDER BY captured_at DESC LIMIT 3",
                (p["id"],)
            ).fetchall()]

        recent = [dict(r) for r in db.execute("""
            SELECT a.slug, a.title, a.deck, a.summary, a.read_minutes, a.captured_at,
                   p.name AS province_name
              FROM articles a LEFT JOIN provinces p ON p.id = a.province_id
             ORDER BY a.captured_at DESC LIMIT 12
        """).fetchall()]

        featured = None
        row = db.execute("""
            SELECT a.*, p.name AS province_name
              FROM articles a LEFT JOIN provinces p ON p.id = a.province_id
             ORDER BY RANDOM() LIMIT 1
        """).fetchone()
        if row:
            featured = dict(row)
            featured["marginalia_count"] = db.execute(
                "SELECT COUNT(*) FROM marginalia WHERE article_id = ?", (featured["id"],)
            ).fetchone()[0]
            # trim heavy payload
            featured.pop("content_html", None)

        return {
            "stats": {
                "articles": art_count,
                "provinces": prov_total,
                "marginalia": marg_count,
            },
            "provinces": provinces,
            "recent": recent,
            "featured": featured,
            "most_recent": recent[0] if recent else None,
        }


# ---------- /api/provinces ----------
@app.get("/api/provinces/{slug}")
def api_province(slug: str):
    with get_db() as db:
        p = db.execute("SELECT * FROM provinces WHERE slug = ?", (slug,)).fetchone()
        if not p:
            raise HTTPException(404)
        articles = [dict(r) for r in db.execute("""
            SELECT slug, title, deck, summary, read_minutes, captured_at, lead_image
              FROM articles WHERE province_id = ?
             ORDER BY captured_at DESC
        """, (p["id"],)).fetchall()]
        return {"province": dict(p), "articles": articles}


# ---------- /api/articles ----------
@app.get("/api/articles/{slug}")
def api_article(slug: str):
    with get_db() as db:
        row = db.execute("""
            SELECT a.*, p.name AS province_name, p.slug AS province_slug
              FROM articles a LEFT JOIN provinces p ON p.id = a.province_id
             WHERE a.slug = ?
        """, (slug,)).fetchone()
        if not row:
            raise HTTPException(404)
        art = dict(row)
        art["marginalia"] = [dict(r) for r in db.execute(
            "SELECT id, content, source, created_at FROM marginalia WHERE article_id = ? ORDER BY created_at DESC",
            (art["id"],)
        ).fetchall()]
        art["cross_refs"] = [dict(r) for r in db.execute("""
            SELECT cr.target_title, cr.target_slug, cr.context,
                   a.slug AS existing_slug, p.name AS existing_province
              FROM cross_refs cr
              LEFT JOIN articles a ON a.id = cr.target_article_id
              LEFT JOIN provinces p ON p.id = a.province_id
             WHERE cr.article_id = ?
        """, (art["id"],)).fetchall()]
        # TOC from content headings
        soup = BeautifulSoup(art.get("content_html") or "", "lxml")
        toc = []
        n = 0
        for h in soup.find_all(["h2"]):
            n += 1
            text = h.get_text(" ", strip=True)
            aid = h.get("id") or f"s{n}"
            h["id"] = aid
            toc.append({"n": n, "text": text, "id": aid})
        # write back with ids
        art["content_html"] = str(soup.body.decode_contents() if soup.body else soup)
        art["toc"] = toc
        return art


@app.delete("/api/articles/{slug}")
def api_delete_article(slug: str):
    with get_db() as db:
        row = db.execute("SELECT id FROM articles WHERE slug = ?", (slug,)).fetchone()
        if not row:
            raise HTTPException(404)
        db.execute("DELETE FROM articles WHERE id = ?", (row["id"],))
        db.commit()
    return {"ok": True}


class MoveReq(BaseModel):
    province: str


@app.patch("/api/articles/{slug}")
def api_move_article(slug: str, req: MoveReq):
    """Reassign an article to a different canonical province. Rejects unknown
    provinces so the static-provinces invariant holds."""
    with get_db() as db:
        art = db.execute("SELECT id FROM articles WHERE slug = ?", (slug,)).fetchone()
        if not art:
            raise HTTPException(404, f"No article '{slug}'")
        prov = db.execute(
            "SELECT id, name, slug FROM provinces WHERE slug = ? OR lower(name) = lower(?)",
            (slugify(req.province), req.province),
        ).fetchone()
        if not prov:
            raise HTTPException(
                400,
                f"'{req.province}' is not a province. Allowed: {', '.join(PROVINCE_NAMES)}",
            )
        db.execute(
            "UPDATE articles SET province_id = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (prov["id"], art["id"]),
        )
        db.commit()
    return {"slug": slug, "province_name": prov["name"], "province_slug": prov["slug"]}


@app.post("/api/articles/{slug}/refresh")
async def api_refresh(slug: str):
    """Re-fetch from Wikipedia. Preserves user marginalia."""
    with get_db() as db:
        row = db.execute("""
            SELECT a.id, a.title, a.wiki_title, a.subheading, p.name AS province_name
              FROM articles a LEFT JOIN provinces p ON p.id = a.province_id
             WHERE a.slug = ?
        """, (slug,)).fetchone()
        if not row:
            raise HTTPException(404)
        saved_margs = [dict(m) for m in db.execute(
            "SELECT content, source, created_at FROM marginalia WHERE article_id = ?",
            (row["id"],),
        ).fetchall()]
        db.execute("DELETE FROM articles WHERE id = ?", (row["id"],))
        db.commit()

    res = await api_capture(CaptureReq(
        topic=row["title"],
        wikipedia_title=row["wiki_title"],
        province=row["province_name"],
        subheading=row["subheading"],
    ))

    with get_db() as db:
        new_aid = db.execute(
            "SELECT id FROM articles WHERE slug = ?", (res["slug"],)
        ).fetchone()["id"]
        # restore only user marginalia (skip carta auto-drafts to avoid dupes)
        for m in saved_margs:
            db.execute(
                "INSERT INTO marginalia (article_id, content, source, created_at) VALUES (?,?,?,?)",
                (new_aid, m["content"], m["source"], m["created_at"]),
            )
        db.commit()

    return res


# ---------- province classification ----------
def classify_by_keyword(*texts: str) -> Optional[str]:
    """First-match keyword classifier over the canonical PROVINCE_NAMES."""
    blob = " ".join(t for t in texts if t).lower()
    if not blob:
        return None
    for name, keywords in PROVINCE_KEYWORDS:
        for kw in keywords:
            if kw in blob:
                return name
    return None


async def classify_by_llm(title: str, summary: str) -> Optional[str]:
    """Ask Ollama to pick one of PROVINCE_NAMES. Returns None on any error."""
    options = ", ".join(PROVINCE_NAMES)
    system = (
        "Classify a Wikipedia topic into EXACTLY ONE of these provinces. "
        "Respond with ONLY the province name, no punctuation, no prose.\n"
        f"Provinces: {options}"
    )
    user = f"Title: {title}\nSummary: {(summary or '')[:600]}"
    try:
        raw = await ollama_chat(
            [{"role": "system", "content": system}, {"role": "user", "content": user}],
            temperature=0.0, max_tokens=20,
        )
    except Exception as e:
        log.info("classify_by_llm unavailable: %s", e)
        return None
    pick = (raw or "").strip().strip('"').strip("'").splitlines()[0].strip()
    # Allow case-insensitive exact match against canonical names.
    for name in PROVINCE_NAMES:
        if pick.lower() == name.lower():
            return name
    # Loose match: pick the first canonical name that appears in the response.
    low = pick.lower()
    for name in PROVINCE_NAMES:
        if name.lower() in low:
            return name
    return None


async def resolve_province(
    db: sqlite3.Connection,
    requested: Optional[str],
    title: str,
    summary: str,
) -> Optional[int]:
    """Return the province_id for a capture, classifying if needed. Never
    creates new provinces — unknown requests fall through to classification."""
    # 1. Honor explicit request if it matches an existing canonical province.
    if requested:
        row = db.execute(
            "SELECT id FROM provinces WHERE slug = ? OR lower(name) = lower(?)",
            (slugify(requested), requested),
        ).fetchone()
        if row:
            return row["id"]

    # 2. Keyword heuristic on the article text.
    pick = classify_by_keyword(title, summary)

    # 3. Ask the local LLM if heuristics were silent.
    if not pick:
        pick = await classify_by_llm(title, summary)

    # 4. Last resort: Miscellaneous (pure uncategorized catch-all;
    # Curiosities is reserved for the semantically "strange" — only reached
    # via explicit request or LLM pick).
    if not pick:
        pick = "Miscellaneous"

    row = db.execute(
        "SELECT id FROM provinces WHERE lower(name) = lower(?)", (pick,)
    ).fetchone()
    return row["id"] if row else None


# ---------- /api/capture ----------
class CaptureReq(BaseModel):
    topic: str
    province: Optional[str] = None
    wikipedia_title: Optional[str] = None
    opening_marginalia: Optional[str] = None
    subheading: Optional[str] = None


@app.post("/api/capture")
async def api_capture(req: CaptureReq):
    t0 = time.time()
    title = (req.wikipedia_title or "").strip()
    if not title:
        title = await resolve_title(req.topic)
    if not title:
        raise HTTPException(404, f"No Wikipedia article matches '{req.topic}'")

    log.info("capture: resolving '%s' -> '%s'", req.topic, title)
    summary = await fetch_summary(title)
    resolved_title = summary.get("title", title)

    # Reject if already stored
    base_slug = slugify(resolved_title)
    with get_db() as db:
        if db.execute("SELECT 1 FROM articles WHERE slug = ?", (base_slug,)).fetchone():
            raise HTTPException(409, f"'{resolved_title}' is already in the codex")

    raw_html = await fetch_article_html(title)
    content_html, images = await process_article_html(raw_html)
    text = extract_text(content_html)
    words = len(text.split())
    read_minutes = max(1, words // 220)
    sha = hashlib.sha256(content_html.encode("utf-8")).hexdigest()

    # Lead image: prefer summary thumbnail
    lead_filename: Optional[str] = None
    thumb = (summary.get("originalimage") or {}).get("source") or \
            (summary.get("thumbnail") or {}).get("source")
    if thumb:
        async with httpx.AsyncClient(timeout=30, headers={"User-Agent": USER_AGENT},
                                      follow_redirects=True) as client:
            got = await download_image(thumb, client)
            if got:
                lead_filename = got[0]
    if not lead_filename and images:
        lead_filename = images[0]["filename"]

    # Province — always classified into the canonical static set.
    # Explicit req.province is honored iff it matches an existing province;
    # otherwise we classify by keyword, then LLM, then Curiosities.
    with get_db() as db:
        province_id = await resolve_province(
            db, req.province, resolved_title, summary.get("extract", "")
        )

        cur = db.execute("""
            INSERT INTO articles (slug, title, deck, province_id, subheading,
                wiki_title, wiki_url, wiki_revid, summary, content_html,
                lead_image, word_count, read_minutes, source_sha256)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            base_slug, resolved_title, summary.get("description", ""),
            province_id, req.subheading,
            title,
            (summary.get("content_urls") or {}).get("desktop", {}).get("page"),
            summary.get("revision"),
            summary.get("extract", ""),
            content_html, lead_filename, words, read_minutes, sha,
        ))
        article_id = cur.lastrowid

        for i, img in enumerate(images):
            db.execute("""
                INSERT INTO images (article_id, local_filename, wiki_url, position, is_lead)
                VALUES (?, ?, ?, ?, ?)
            """, (article_id, img["filename"], img["wiki_url"], i,
                  1 if img["filename"] == lead_filename else 0))

        if req.opening_marginalia:
            db.execute(
                "INSERT INTO marginalia (article_id, content, source) VALUES (?, ?, 'user')",
                (article_id, req.opening_marginalia),
            )

        db.commit()

    log.info("captured '%s' · %d words · %d images · %.1fs",
             resolved_title, words, len(images), time.time() - t0)
    return {
        "slug": base_slug,
        "title": resolved_title,
        "image_count": len(images),
        "word_count": words,
        "elapsed_s": round(time.time() - t0, 2),
    }


# ---------- /api/search ----------
@app.get("/api/search")
def api_search(q: str = ""):
    q = (q or "").strip()
    if len(q) < 2:
        return {"articles": [], "marginalia": [], "provinces": []}
    like = f"%{q}%"
    with get_db() as db:
        articles = [dict(r) for r in db.execute("""
            SELECT a.slug, a.title, a.deck, p.name AS province_name
              FROM articles a LEFT JOIN provinces p ON p.id = a.province_id
             WHERE a.title LIKE ? OR a.deck LIKE ? OR a.summary LIKE ?
             ORDER BY a.captured_at DESC LIMIT 20
        """, (like, like, like)).fetchall()]
        margins = [dict(r) for r in db.execute("""
            SELECT m.content, m.source, a.slug, a.title
              FROM marginalia m JOIN articles a ON a.id = m.article_id
             WHERE m.content LIKE ? ORDER BY m.created_at DESC LIMIT 10
        """, (like,)).fetchall()]
        provinces = [dict(r) for r in db.execute(
            "SELECT slug, name FROM provinces WHERE name LIKE ? LIMIT 5", (like,)
        ).fetchall()]
    return {"articles": articles, "marginalia": margins, "provinces": provinces}


# ---------- /api/marginalia ----------
class MargReq(BaseModel):
    article_slug: str
    content: str
    source: str = "user"


@app.post("/api/marginalia")
def api_marginalia(req: MargReq):
    with get_db() as db:
        row = db.execute("SELECT id FROM articles WHERE slug = ?", (req.article_slug,)).fetchone()
        if not row:
            raise HTTPException(404)
        db.execute(
            "INSERT INTO marginalia (article_id, content, source) VALUES (?, ?, ?)",
            (row["id"], req.content, req.source),
        )
        db.commit()
    return {"ok": True}


# ---------- /api/carta/chat ----------
class CartaChatReq(BaseModel):
    message: str
    history: list[dict] = []


@app.post("/api/carta/chat")
async def api_carta_chat(req: CartaChatReq):
    with get_db() as db:
        n_articles = db.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
        n_provinces = db.execute("SELECT COUNT(*) FROM provinces").fetchone()[0]
        titles = [r["title"] for r in db.execute(
            "SELECT title FROM articles ORDER BY captured_at DESC LIMIT 60"
        ).fetchall()]

    system = (
        "You are CARTA, Juan's friendly assistant for his personal wiki on his Raspberry Pi. "
        "Talk like a smart, warm friend — NOT a librarian or a professor. Be curious, a "
        "little enthusiastic, casual. Use contractions (it's, you're, I'll, that's). "
        "Keep it conversational and short: 2–3 sentences, rarely four. "
        "Italicize key terms with *asterisks* occasionally — not every sentence. "
        "Never use emoji. Never say things like 'perchance', 'verily', 'indeed', 'quite so', "
        "or other archaic filler. "
        "When Juan tells you about something he learned, offer to add it to his wiki and "
        "mention the Wikipedia article you'd pull from. "
        f"The wiki currently has {n_articles} {'entry' if n_articles == 1 else 'entries'}. "
    )
    if titles:
        system += (
            "\n\nIMPORTANT: Juan's wiki already contains the articles listed below. "
            "Whenever you mention ANY of these titles in your reply, wrap it in "
            "DOUBLE SQUARE BRACKETS so he can click to open it. Example: "
            "[[Quantum entanglement]]. Do NOT use brackets for topics that aren't on "
            "this list.\n\nExisting articles:\n- " + "\n- ".join(titles) + "\n"
        )
    system += "\nKeep responses to 2–3 short sentences."

    messages = [{"role": "system", "content": system}]
    for m in (req.history or [])[-8:]:
        role = m.get("role", "user")
        if role in ("user", "assistant"):
            messages.append({"role": role, "content": str(m.get("content", ""))})
    messages.append({"role": "user", "content": req.message})

    try:
        reply = await ollama_chat(messages, temperature=0.75, max_tokens=260)
    except Exception as e:
        log.error("ollama chat failed: %s", e)
        return {"reply": f"*CARTA is elsewhere — the llama process cannot be reached.*  \n`{e}`",
                "error": True}

    with get_db() as db:
        db.execute("INSERT INTO carta_log (role, content) VALUES ('user', ?)", (req.message,))
        db.execute("INSERT INTO carta_log (role, content) VALUES ('carta', ?)", (reply,))
        db.commit()
    return {"reply": reply}


# ---------- /api/carta/draft ----------
class CartaDraftReq(BaseModel):
    description: str


@app.post("/api/carta/draft")
async def api_carta_draft(req: CartaDraftReq):
    with get_db() as db:
        provinces = [r["name"] for r in db.execute(
            "SELECT name FROM provinces ORDER BY name"
        ).fetchall()]

    system = (
        "You extract metadata for Juan's personal wiki. "
        "Given a description of something he learned, respond with ONLY a JSON object, "
        "no prose before or after. Shape:\n"
        '{\n'
        '  "topic": "short canonical name of the subject",\n'
        '  "wikipedia_title": "exact Wikipedia article title",\n'
        '  "province": "one of the provinces below",\n'
        '  "subheading": "optional, or null",\n'
        '  "confirmation": "one friendly, casual sentence, like texting a friend — e.g. '
        '\\"Cool, I\\u2019ll pull the Wikipedia article and file it under Natural Sciences.\\""\n'
        '}\n'
        f"Provinces (pick one, exactly as spelled): {', '.join(provinces)}. "
        "These are the only allowed categories — never invent a new one. "
        "If nothing fits, pick 'Miscellaneous'."
    )
    messages = [
        {"role": "system", "content": system},
        {"role": "user", "content": req.description},
    ]
    try:
        raw = await ollama_chat(messages, temperature=0.2, format_json=True, max_tokens=400)
    except Exception as e:
        raise HTTPException(502, f"Ollama unavailable: {e}")

    # Try to parse JSON from the response (some models wrap in prose despite format=json)
    raw = raw.strip()
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        m = re.search(r"\{.*\}", raw, re.DOTALL)
        if not m:
            raise HTTPException(502, f"Could not parse CARTA draft: {raw[:200]}")
        try:
            data = json.loads(m.group(0))
        except json.JSONDecodeError as e:
            raise HTTPException(502, f"Invalid JSON from CARTA: {e}")

    # Normalize (coerce null / empty to sensible defaults)
    data["topic"] = (data.get("topic") or req.description[:60]).strip()
    data["wikipedia_title"] = (data.get("wikipedia_title") or data["topic"]).strip()
    suggested = (data.get("province") or "Miscellaneous").strip()
    # Clamp to a canonical province; never leak a non-existent one to the client.
    canonical = next((n for n in PROVINCE_NAMES if n.lower() == suggested.lower()), None)
    if not canonical:
        canonical = next((n for n in PROVINCE_NAMES if n.lower() in suggested.lower()), None)
    data["province"] = canonical or "Miscellaneous"
    data["subheading"] = data.get("subheading") or None
    data["confirmation"] = (data.get("confirmation") or
                            "Very well — I shall draft the entry.").strip()

    # Try to resolve the wikipedia title now so user sees a real target
    resolved = await resolve_title(data["wikipedia_title"] or data["topic"])
    if resolved:
        data["wikipedia_title"] = resolved
    return data


# ---------- /api/carta/quiz ----------
class QuizReq(BaseModel):
    slug: Optional[str] = None
    count: Optional[int] = 5


@app.post("/api/carta/quiz")
async def api_carta_quiz(req: QuizReq):
    """Generate a multiple-choice quiz from an article in the codex.
    If no slug is given, picks a recent article at random. Returns
    {article: {slug, title}, questions: [{question, options[4], answer_index, explanation}]}."""
    count = max(1, min(int(req.count or 5), 10))

    with get_db() as db:
        if req.slug:
            row = db.execute(
                "SELECT slug, title, summary, content_html FROM articles WHERE slug = ?",
                (req.slug,),
            ).fetchone()
            if not row:
                raise HTTPException(404, f"No article '{req.slug}'")
        else:
            row = db.execute("""
                SELECT slug, title, summary, content_html FROM articles
                  WHERE content_html IS NOT NULL AND length(content_html) > 400
                ORDER BY RANDOM() LIMIT 1
            """).fetchone()
            if not row:
                raise HTTPException(
                    404,
                    "No articles with enough content to quiz on yet. Capture something first.",
                )

    # Trim the source text so we stay under the small local model's context.
    body_text = extract_text(row["content_html"] or "")
    source = (row["summary"] or "") + "\n\n" + body_text
    source = source.strip()[:4500]

    system = (
        "You write quiz questions. Output ONLY a JSON object, no prose. Shape:\n"
        '{ "questions": [\n'
        '    { "question": "...", "options": ["A", "B", "C", "D"],\n'
        '      "answer_index": 0, "explanation": "one short sentence" }\n'
        "  ]\n"
        "}\n"
        f"Write exactly {count} questions drawn STRICTLY from the passage below. "
        "Each question: 4 plausible options, exactly one correct, answer_index in [0..3]. "
        "Make distractors reasonable — not obviously wrong. No duplicate answers. "
        "Keep each question under 180 characters."
    )
    user = f"Title: {row['title']}\n\nPassage:\n{source}"

    try:
        raw = await ollama_chat(
            [{"role": "system", "content": system}, {"role": "user", "content": user}],
            temperature=0.4, format_json=True, max_tokens=1400,
        )
    except Exception as e:
        raise HTTPException(502, f"Ollama unavailable: {e}")

    raw = (raw or "").strip()
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        m = re.search(r"\{.*\}", raw, re.DOTALL)
        if not m:
            raise HTTPException(502, f"Could not parse quiz: {raw[:200]}")
        try:
            data = json.loads(m.group(0))
        except json.JSONDecodeError as e:
            raise HTTPException(502, f"Invalid JSON from quiz: {e}")

    questions: list[dict] = []
    for q in (data.get("questions") or [])[:count]:
        question = str(q.get("question") or "").strip()
        options = [str(o).strip() for o in (q.get("options") or []) if str(o).strip()]
        try:
            answer_index = int(q.get("answer_index", 0))
        except (TypeError, ValueError):
            answer_index = 0
        explanation = str(q.get("explanation") or "").strip()
        if question and len(options) == 4 and 0 <= answer_index < 4:
            questions.append({
                "question": question,
                "options": options,
                "answer_index": answer_index,
                "explanation": explanation,
            })

    if not questions:
        raise HTTPException(502, "Model returned no valid questions; try again.")

    return {
        "article": {"slug": row["slug"], "title": row["title"]},
        "questions": questions,
    }
