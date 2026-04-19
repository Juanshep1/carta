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
from fastapi.responses import FileResponse, JSONResponse, Response
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
# YouTube Data API v3 — set YOUTUBE_API_KEY to light up YouTube as a Watch
# source. Free tier is 10k units/day; each search costs 100 units (~100
# searches/day). Enable at https://console.cloud.google.com/apis →
# YouTube Data API v3 → Credentials → API key. Leave blank to skip.
YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY", "").strip()

# Ollama Cloud (hosted): https://ollama.com — set OLLAMA_CLOUD_KEY to enable.
OLLAMA_CLOUD_URL = os.environ.get("OLLAMA_CLOUD_URL", "https://ollama.com").rstrip("/")
OLLAMA_CLOUD_KEY = os.environ.get("OLLAMA_CLOUD_KEY", "").strip()
# Static fallback if the live Ollama Cloud /api/tags call fails. The live list
# is authoritative and used by default — this only kicks in when the cloud is
# unreachable. Keep a broad, recognizable selection.
OLLAMA_CLOUD_FALLBACK = [
    "gpt-oss:20b",
    "gpt-oss:120b",
    "gemma3:4b",
    "gemma3:12b",
    "gemma3:27b",
    "gemma4:31b",
    "qwen3-coder:480b",
    "qwen3-next:80b",
    "qwen3-vl:235b",
    "deepseek-v3.1:671b",
    "deepseek-v3.2",
    "kimi-k2:1t",
    "kimi-k2-thinking",
    "kimi-k2.5",
    "glm-4.6",
    "glm-4.7",
    "glm-5",
    "minimax-m2",
    "mistral-large-3:675b",
    "nemotron-3-super",
]
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
def resolve_provider(provider: Optional[str], model: Optional[str]) -> tuple[str, str, dict]:
    """Pick (base_url, model, headers) based on a client-supplied provider/model.
    Falls back to local Ollama when cloud is requested without a key."""
    want_cloud = (provider or "").lower() == "cloud"
    if want_cloud and OLLAMA_CLOUD_KEY:
        base = OLLAMA_CLOUD_URL
        headers = {"Authorization": f"Bearer {OLLAMA_CLOUD_KEY}"}
        chosen = model or (OLLAMA_CLOUD_MODELS[0] if OLLAMA_CLOUD_MODELS else OLLAMA_MODEL)
        return base, chosen, headers
    return OLLAMA_URL, (model or OLLAMA_MODEL), {}


class OllamaError(Exception):
    """Structured upstream error: carries the HTTP status so callers can
    re-emit it cleanly instead of wrapping every case as a generic 502."""
    def __init__(self, status: int, message: str, *, model: Optional[str] = None):
        self.status = status
        self.message = message
        self.model = model
        super().__init__(f"{status}: {message}")


# Cloud fallback: on 403 (subscription required / capacity) or 5xx on the
# chosen model, retry once with this model. Verified working on free tier:
# gpt-oss:20b responds in ~0.4s with the provided key.
CLOUD_FALLBACK_MODEL = "gpt-oss:20b"


async def ollama_chat(messages: list[dict], *, temperature: float = 0.7,
                      format_json: bool = False, max_tokens: int = 400,
                      timeout: float = 180,
                      provider: Optional[str] = None,
                      model: Optional[str] = None,
                      _allow_fallback: bool = True) -> str:
    base_url, chosen_model, headers = resolve_provider(provider, model)
    payload: dict[str, Any] = {
        "model": chosen_model,
        "messages": messages,
        "stream": False,
        "options": {"temperature": temperature, "num_predict": max_tokens},
    }
    if format_json:
        payload["format"] = "json"
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            r = await client.post(f"{base_url}/api/chat", json=payload, headers=headers)
    except httpx.TimeoutException as e:
        raise OllamaError(504, f"upstream timed out after {timeout}s", model=chosen_model) from e
    except httpx.RequestError as e:
        raise OllamaError(502, f"cannot reach {base_url}: {e}", model=chosen_model) from e

    if r.status_code >= 400:
        # If the user's cloud pick is gated (403 "subscription required") or
        # flaking (5xx), transparently retry once with a known-working cloud
        # model so the feature doesn't just fail in their face.
        want_cloud = (provider or "").lower() == "cloud" and OLLAMA_CLOUD_KEY
        is_fallbackable = r.status_code == 403 or 500 <= r.status_code < 600
        if (_allow_fallback and want_cloud and is_fallbackable
                and chosen_model != CLOUD_FALLBACK_MODEL):
            log.info("ollama cloud: '%s' returned %d; falling back to '%s'",
                     chosen_model, r.status_code, CLOUD_FALLBACK_MODEL)
            return await ollama_chat(
                messages, temperature=temperature, format_json=format_json,
                max_tokens=max_tokens, timeout=timeout,
                provider="cloud", model=CLOUD_FALLBACK_MODEL,
                _allow_fallback=False,
            )
        # Surface the upstream's own error so the UI can show something
        # actionable like "subscription required" or "model is cold".
        try:
            payload_err = r.json()
            upstream_msg = (payload_err.get("error")
                            or payload_err.get("message")
                            or r.text)
        except Exception:
            upstream_msg = r.text or r.reason_phrase
        msg = f"{chosen_model}: {str(upstream_msg)[:400].strip()}"
        raise OllamaError(r.status_code, msg, model=chosen_model)

    try:
        data = r.json()
    except Exception as e:
        raise OllamaError(502, f"{chosen_model}: invalid JSON response — {e}", model=chosen_model) from e

    reply = data.get("message", {}).get("content", "")
    # strip <think> blocks some models emit
    reply = re.sub(r"<think>.*?</think>", "", reply, flags=re.DOTALL).strip()

    # Some cloud SKUs (notably gpt-oss:20b on certain JSON-schema prompts)
    # happily return HTTP 200 with an empty content body — there's no error
    # to surface, but downstream JSON parsing will explode. Treat that as a
    # retryable failure and hop to the fallback model, mirroring the 5xx path.
    want_cloud = (provider or "").lower() == "cloud" and OLLAMA_CLOUD_KEY
    if (_allow_fallback and want_cloud and format_json
            and not reply
            and chosen_model != CLOUD_FALLBACK_MODEL):
        log.info("ollama cloud: '%s' returned 200 with empty JSON body; "
                 "falling back to '%s'", chosen_model, CLOUD_FALLBACK_MODEL)
        return await ollama_chat(
            messages, temperature=temperature, format_json=format_json,
            max_tokens=max_tokens, timeout=timeout,
            provider="cloud", model=CLOUD_FALLBACK_MODEL,
            _allow_fallback=False,
        )
    return reply


# ---------- FastAPI ----------
app = FastAPI(title="CARTA", version="1.1")

# CORS — allow Netlify deploys, localhost dev, and tailnet IPs
app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=(
        r"https://([a-z0-9-]+\.)*(netlify\.app|netlify\.com|ts\.net|github\.io)"
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


# ---------- /api/provinces/{slug}/export.epub ----------
# EPUB 3.0 is just a ZIP of a handful of XML files + HTML chapters. We build
# it with stdlib `zipfile` so no new Python dep is needed.
import io as _io
import uuid as _uuid
import zipfile as _zipfile
from xml.sax.saxutils import escape as _xml_escape


def _epub_from_articles(province: dict, articles: list[dict]) -> bytes:
    prov_title = _xml_escape(province["name"])
    book_id = f"urn:uuid:{_uuid.uuid4()}"

    container_xml = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<container version="1.0" xmlns="urn:oasis:names:tc:opendocument:xmlns:container">\n'
        '  <rootfiles>\n'
        '    <rootfile full-path="OEBPS/content.opf" media-type="application/oebps-package+xml"/>\n'
        '  </rootfiles>\n'
        '</container>\n'
    )
    stylesheet = (
        "@page { margin: 6% 6%; }\n"
        "body { font-family: 'EB Garamond', Georgia, serif; line-height: 1.62; color: #1a1410; }\n"
        "h1 { font-family: 'Fraunces', Georgia, serif; font-weight: 500; "
        "     margin: 0 0 0.2em 0; font-size: 1.8em; color: #1a1410; }\n"
        "h2 { font-family: 'Fraunces', Georgia, serif; font-size: 1.3em; "
        "     border-bottom: 1px solid rgba(26,20,16,0.2); padding-bottom: 0.15em; }\n"
        ".kicker { color: #7a1f1f; font-family: 'IBM Plex Mono', monospace; "
        "          letter-spacing: 0.18em; text-transform: uppercase; font-size: 0.7em; margin-bottom: 1em; }\n"
        ".deck { font-style: italic; color: #3a2f27; margin-bottom: 1.4em; }\n"
        "p { margin: 0.7em 0; }\n"
        "a { color: #7a1f1f; text-decoration: none; }\n"
        "blockquote { border-left: 2px solid #7a1f1f; padding: 0.2em 0.9em; "
        "             font-style: italic; color: #3a2f27; margin: 1em 0; }\n"
        "img { max-width: 100%; height: auto; display: block; margin: 1em auto; }\n"
    )

    manifest_items = [
        '<item id="style" href="style.css" media-type="text/css"/>',
        '<item id="nav" href="nav.xhtml" media-type="application/xhtml+xml" properties="nav"/>',
        '<item id="titlepage" href="titlepage.xhtml" media-type="application/xhtml+xml"/>',
    ]
    spine_items = ['<itemref idref="titlepage"/>']
    nav_items = []
    chapter_files: list[tuple[str, str]] = []

    for i, a in enumerate(articles, start=1):
        cid = f"ch{i:03d}"
        fname = f"{cid}.xhtml"
        title = _xml_escape(a.get("title") or f"Chapter {i}")
        deck = _xml_escape(a.get("deck") or a.get("summary") or "")
        body_html = a.get("content_html") or f"<p>{_xml_escape(a.get('summary') or '')}</p>"
        # Best-effort stripping of tags/attrs EPUB readers don't like.
        # We trust the server's own process_article_html sanitizer; just drop
        # <script> and inline event handlers defensively.
        body_html = re.sub(r"<script[\s\S]*?</script>", "", body_html, flags=re.I)
        body_html = re.sub(r"\son[a-z]+=\"[^\"]*\"", "", body_html, flags=re.I)
        chapter_xhtml = (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            '<!DOCTYPE html>\n'
            '<html xmlns="http://www.w3.org/1999/xhtml" lang="en">\n'
            f'<head><title>{title}</title>'
            '<link rel="stylesheet" href="style.css"/></head>\n'
            f'<body><section>\n'
            f'  <p class="kicker">{prov_title}</p>\n'
            f'  <h1>{title}</h1>\n'
            + (f'  <p class="deck">{deck}</p>\n' if deck else "")
            + f'  {body_html}\n'
            f'</section></body></html>\n'
        )
        chapter_files.append((fname, chapter_xhtml))
        manifest_items.append(
            f'<item id="{cid}" href="{fname}" media-type="application/xhtml+xml"/>'
        )
        spine_items.append(f'<itemref idref="{cid}"/>')
        nav_items.append(f'<li><a href="{fname}">{title}</a></li>')

    titlepage_xhtml = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<!DOCTYPE html>\n'
        '<html xmlns="http://www.w3.org/1999/xhtml" lang="en"><head>'
        f'<title>{prov_title}</title>'
        '<link rel="stylesheet" href="style.css"/></head>'
        '<body style="text-align:center; margin-top: 30%;">'
        f'<h1 style="font-size:2.4em;">{prov_title}</h1>'
        f'<p class="deck">A compiled province of CARTA — {len(articles)} entries.</p>'
        '<p style="color:#7a1f1f; letter-spacing: 0.24em; font-size: 0.8em;">§</p>'
        '</body></html>\n'
    )
    nav_xhtml = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<!DOCTYPE html>\n'
        '<html xmlns="http://www.w3.org/1999/xhtml" '
        'xmlns:epub="http://www.idpf.org/2007/ops" lang="en">'
        f'<head><title>{prov_title} — Contents</title>'
        '<link rel="stylesheet" href="style.css"/></head>'
        '<body><nav epub:type="toc" id="toc"><h1>Contents</h1><ol>'
        + "".join(nav_items)
        + '</ol></nav></body></html>\n'
    )
    content_opf = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<package xmlns="http://www.idpf.org/2007/opf" version="3.0" '
        'unique-identifier="book-id" xml:lang="en">\n'
        '  <metadata xmlns:dc="http://purl.org/dc/elements/1.1/">\n'
        f'    <dc:identifier id="book-id">{book_id}</dc:identifier>\n'
        f'    <dc:title>{prov_title}</dc:title>\n'
        '    <dc:language>en</dc:language>\n'
        '    <dc:creator>CARTA — Personal Codex</dc:creator>\n'
        f'    <meta property="dcterms:modified">{time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}</meta>\n'
        '  </metadata>\n'
        '  <manifest>\n    '
        + "\n    ".join(manifest_items) +
        '\n  </manifest>\n'
        '  <spine>\n    '
        + "\n    ".join(spine_items) +
        '\n  </spine>\n'
        '</package>\n'
    )

    buf = _io.BytesIO()
    with _zipfile.ZipFile(buf, "w") as zf:
        # mimetype MUST be the first entry and uncompressed.
        zf.writestr(_zipfile.ZipInfo("mimetype"),
                    "application/epub+zip", compress_type=_zipfile.ZIP_STORED)
        zf.writestr("META-INF/container.xml", container_xml)
        zf.writestr("OEBPS/content.opf", content_opf)
        zf.writestr("OEBPS/nav.xhtml", nav_xhtml)
        zf.writestr("OEBPS/titlepage.xhtml", titlepage_xhtml)
        zf.writestr("OEBPS/style.css", stylesheet)
        for fname, content in chapter_files:
            zf.writestr(f"OEBPS/{fname}", content)
    return buf.getvalue()


@app.get("/api/provinces/{slug}/export.epub")
def api_export_province_epub(slug: str):
    with get_db() as db:
        p = db.execute("SELECT * FROM provinces WHERE slug = ?", (slug,)).fetchone()
        if not p:
            raise HTTPException(404, "Unknown province")
        rows = db.execute("""
            SELECT slug, title, deck, summary, content_html, captured_at
              FROM articles WHERE province_id = ?
             ORDER BY captured_at ASC
        """, (p["id"],)).fetchall()
        if not rows:
            raise HTTPException(404, "No articles in this province to compile")
        articles = [dict(r) for r in rows]
        data = _epub_from_articles(dict(p), articles)

    filename = f"{slugify(p['name'])}.epub"
    return Response(
        content=data,
        media_type="application/epub+zip",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


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
        # Fallback: a requested slug often comes from a Wikipedia link whose
        # title redirects to a canonical article stored under a different slug
        # (e.g. link target "Hoodoo_(spirituality)" → requested slug
        # "hoodoo-spirituality", but the actual stored slug is "hoodoo"
        # because that's the resolved Wikipedia title). Try matching by
        # slugified wiki_title / title so links to redirect targets open the
        # real article instead of 404ing and triggering a duplicate capture.
        if not row:
            candidates = db.execute("""
                SELECT a.*, p.name AS province_name, p.slug AS province_slug
                  FROM articles a LEFT JOIN provinces p ON p.id = a.province_id
            """).fetchall()
            for c in candidates:
                if (slugify(c["wiki_title"] or "") == slug
                        or slugify(c["title"] or "") == slug):
                    row = c
                    break
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


# ---------- /api/articles/{slug}/videos ----------
# Companion-video surface. Free sources only:
#   - Internet Archive (advancedsearch.php, no auth)
#   - Wikimedia Commons (filetype:video, no auth)
# Both return CC-licensed or public-domain material, which fits CARTA's
# archival ethos better than YouTube and avoids any API-key/billing mess.

VIDEO_CACHE_TTL_SECONDS = 60 * 60 * 24 * 7   # 7 days — videos rarely churn
VIDEO_QUERY_TIMEOUT = 10


async def _search_internet_archive(title: str) -> list[dict]:
    """Returns up to 3 Internet Archive movie items matching `title`.
    Applies a quality filter to sidestep the fan-upload noise in
    `opensource_movies`: prefer items with real download counts and avoid
    the hand-wave collections that tend to host shouty self-help uploads."""
    # Over-fetch so we have candidates after filtering.
    params = {
        "q": f'title:("{title}") AND mediatype:movies',
        "fl[]": ["identifier", "title", "description", "creator", "year",
                 "downloads", "collection"],
        "rows": 12,
        "page": 1,
        "sort[]": "downloads desc",
        "output": "json",
    }
    # httpx flattens repeated params when passed a list under a bracket key.
    flat: list[tuple[str, str]] = []
    for k, v in params.items():
        if isinstance(v, list):
            for item in v:
                flat.append((k, str(item)))
        else:
            flat.append((k, str(v)))
    try:
        async with httpx.AsyncClient(
            timeout=VIDEO_QUERY_TIMEOUT,
            headers={"User-Agent": USER_AGENT},
        ) as client:
            r = await client.get("https://archive.org/advancedsearch.php", params=flat)
            r.raise_for_status()
            data = r.json()
    except Exception as e:
        log.info("videos: IA search failed for '%s': %s", title, e)
        return []

    # IA quality heuristics. We don't filter on downloads alone — the shouty
    # fan uploads sometimes crack 300, and the Prelinger / newsandpublicaffairs
    # items sometimes sit at 200. Instead:
    #   1. Reject when EVERY collection is a known-noisy bucket (generic
    #      user-upload pile, YouTube scrape, user favorites, etc.).
    #   2. Rank survivors by downloads * curated_weight. An item with at
    #      least one curated collection gets the full weight; an item with
    #      only noisy collections (but not *all* — e.g. mixed) gets 0.1x.
    # This lets the City-of-Longmont-at-210-downloads beat the shouty-fan-
    # upload-at-276 when both survive the reject filter.
    NOISY_SOLE_COLLECTIONS = {
        "opensource_movies", "opensource_audio",
        "community_video", "community_media",
        "youtube",
        # IA's YouTube/social scrape mirrors — where the shouty self-help
        # Stoicism video lives.
        "mirrortube", "social-media-video", "additional_collections_video",
        # Fringe / deemphasize buckets IA uses for low-quality material.
        "altcensored", "fringe", "deemphasize",
    }
    # User-favorites collections are named `fav-<username>` — not curation.
    def _is_noisy(c: str) -> bool:
        c = c.lower()
        return c in NOISY_SOLE_COLLECTIONS or c.startswith("fav-")

    def _as_list(x):
        if x is None: return []
        return x if isinstance(x, list) else [x]

    def _score_doc(doc: dict) -> float | None:
        ident = doc.get("identifier")
        if not ident:
            return None
        try:
            downloads = int(doc.get("downloads") or 0)
        except (TypeError, ValueError):
            downloads = 0
        collections = [str(c) for c in _as_list(doc.get("collection"))]
        if collections and all(_is_noisy(c) for c in collections):
            return None   # every collection is noisy → drop entirely
        has_curated = any(not _is_noisy(c) for c in collections)
        has_meta = bool(doc.get("year")) or bool(doc.get("creator"))
        # Still require some signal at the low end — a 10-download upload
        # with a filled-in year is probably still noise.
        if downloads < 50 and not has_meta:
            return None
        # Curated items get the full weight; half-noisy items get demoted
        # so a curated 210-download lecture beats a noisy 276-download rant.
        weight = 1.0 if has_curated else 0.1
        return downloads * weight

    out: list[dict] = []
    scored = []
    for doc in (data.get("response", {}).get("docs") or []):
        s = _score_doc(doc)
        if s is None:
            continue
        scored.append((s, doc))
    scored.sort(key=lambda sd: sd[0], reverse=True)

    for _score, doc in scored[:3]:
        ident = doc["identifier"]
        raw_desc = doc.get("description")
        if isinstance(raw_desc, list):
            raw_desc = raw_desc[0] if raw_desc else ""
        desc = re.sub(r"<[^>]+>", "", str(raw_desc or ""))[:280].strip()
        creator_raw = doc.get("creator")
        creator = (creator_raw if isinstance(creator_raw, str)
                   else (creator_raw[0] if isinstance(creator_raw, list) and creator_raw else ""))
        out.append({
            "source": "internet_archive",
            "id": ident,
            "title": str(doc.get("title") or ident),
            "description": desc,
            "creator": creator or "",
            "year": str(doc.get("year") or ""),
            "embed_type": "iframe",
            "embed_url": f"https://archive.org/embed/{ident}",
            "page_url": f"https://archive.org/details/{ident}",
            "thumb_url": f"https://archive.org/services/img/{ident}",
            "attribution": "Internet Archive",
        })
    return out


async def _search_commons_videos(title: str) -> list[dict]:
    """Returns up to 2 Wikimedia Commons video files matching `title`."""
    try:
        async with httpx.AsyncClient(
            timeout=VIDEO_QUERY_TIMEOUT,
            headers={"User-Agent": USER_AGENT},
        ) as client:
            # 1. Search Commons File namespace for videos matching the title.
            r = await client.get(
                "https://commons.wikimedia.org/w/api.php",
                params={
                    "action": "query",
                    "format": "json",
                    "list": "search",
                    "srsearch": f'"{title}" filetype:video',
                    "srnamespace": 6,   # File:
                    "srlimit": 2,
                },
            )
            r.raise_for_status()
            results = r.json().get("query", {}).get("search", [])
            if not results:
                return []

            file_titles = [r["title"] for r in results if r.get("title", "").startswith("File:")]
            if not file_titles:
                return []

            # 2. Resolve URLs + thumbs for those files.
            r2 = await client.get(
                "https://commons.wikimedia.org/w/api.php",
                params={
                    "action": "query",
                    "format": "json",
                    "prop": "imageinfo",
                    "iiprop": "url|mediatype|size|user",
                    "iiurlwidth": 480,
                    "titles": "|".join(file_titles),
                },
            )
            r2.raise_for_status()
            pages = r2.json().get("query", {}).get("pages", {}) or {}
    except Exception as e:
        log.info("videos: Commons search failed for '%s': %s", title, e)
        return []

    out: list[dict] = []
    for _pid, page in pages.items():
        info = (page.get("imageinfo") or [{}])[0]
        url = info.get("url")
        if not url:
            continue
        thumb = info.get("thumburl") or url
        raw_title = (page.get("title") or "").removeprefix("File:")
        out.append({
            "source": "wikimedia_commons",
            "id": page.get("title"),
            "title": raw_title,
            "description": "",
            "creator": info.get("user") or "",
            "year": "",
            "embed_type": "video",
            "embed_url": url,                       # direct .webm/.ogv/.mp4
            "page_url": f"https://commons.wikimedia.org/wiki/{(page.get('title') or '').replace(' ', '_')}",
            "thumb_url": thumb,
            "attribution": "Wikimedia Commons",
        })
    return out


async def _search_youtube(title: str) -> list[dict]:
    """Returns up to 3 YouTube results for `title`. No-ops if
    YOUTUBE_API_KEY isn't set (users without the key keep IA+Commons+LoC).
    Restricted to embeddable videos so the iframe player always works."""
    if not YOUTUBE_API_KEY:
        return []
    params = {
        "part": "snippet",
        "maxResults": 3,
        "q": title,
        "type": "video",
        "videoEmbeddable": "true",
        "safeSearch": "strict",
        "relevanceLanguage": "en",
        "key": YOUTUBE_API_KEY,
    }
    try:
        async with httpx.AsyncClient(
            timeout=VIDEO_QUERY_TIMEOUT,
            headers={"User-Agent": USER_AGENT},
        ) as client:
            r = await client.get(
                "https://www.googleapis.com/youtube/v3/search", params=params
            )
            if r.status_code != 200:
                log.info("videos: YouTube search returned %d: %s",
                         r.status_code, r.text[:200])
                return []
            items = r.json().get("items", [])
    except Exception as e:
        log.info("videos: YouTube search failed for '%s': %s", title, e)
        return []

    out: list[dict] = []
    for it in items:
        vid = (it.get("id") or {}).get("videoId")
        sn = it.get("snippet") or {}
        if not vid:
            continue
        thumbs = sn.get("thumbnails") or {}
        thumb = ((thumbs.get("medium") or thumbs.get("high")
                  or thumbs.get("default") or {}).get("url")) or ""
        published = (sn.get("publishedAt") or "")[:4]
        desc = re.sub(r"\s+", " ", sn.get("description") or "")[:280].strip()
        out.append({
            "source": "youtube",
            "id": vid,
            "title": sn.get("title") or vid,
            "description": desc,
            "creator": sn.get("channelTitle") or "",
            "year": published,
            "embed_type": "iframe",
            # rel=0 hides "related videos from the rest of YouTube" at the end
            # so the player doesn't bait users out of the codex.
            "embed_url": f"https://www.youtube.com/embed/{vid}?rel=0&modestbranding=1",
            "page_url": f"https://www.youtube.com/watch?v={vid}",
            "thumb_url": thumb,
            "attribution": "YouTube",
        })
    return out


async def _search_library_of_congress(title: str) -> list[dict]:
    """Returns up to 3 Library of Congress items with a playable video URL.
    LoC is 100% free, no key, and its digitized-film catalog is unmatched
    for archival material (WPA, early 20th-century newsreels, etc.)."""
    params = {
        "q": title,
        "fo": "json",
        "fa": "online-format:video",
        "c": 10,
        "sp": "1",
    }
    try:
        async with httpx.AsyncClient(
            timeout=VIDEO_QUERY_TIMEOUT,
            headers={"User-Agent": USER_AGENT},
        ) as client:
            r = await client.get("https://www.loc.gov/search/", params=params)
            if r.status_code != 200:
                return []
            data = r.json()
    except Exception as e:
        log.info("videos: LoC search failed for '%s': %s", title, e)
        return []

    def _pick_video_url(resources) -> Optional[str]:
        """Walk LoC's nested `resources` structure looking for the first
        direct .mp4 / .m3u8 URL. Schema is inconsistent across collections."""
        if not isinstance(resources, list):
            return None
        for res in resources:
            if not isinstance(res, dict):
                continue
            for key in ("video", "mp4", "m3u8"):
                v = res.get(key)
                if isinstance(v, str) and v.startswith("http"):
                    return v
            # Nested files[] of dicts with mimetype/url
            for f_group in (res.get("files") or []):
                if isinstance(f_group, list):
                    for f in f_group:
                        if isinstance(f, dict):
                            mime = (f.get("mimetype") or "").lower()
                            url = f.get("url") or ""
                            if url.startswith("http") and (
                                mime.startswith("video/")
                                or url.lower().endswith((".mp4", ".m3u8", ".webm"))
                            ):
                                return url
        return None

    out: list[dict] = []
    for item in data.get("results") or []:
        video_url = _pick_video_url(item.get("resources"))
        if not video_url:
            continue
        thumbs = item.get("image_url") or []
        thumb = thumbs[0] if isinstance(thumbs, list) and thumbs else ""
        if isinstance(thumb, str) and thumb.startswith("//"):
            thumb = "https:" + thumb
        raw_desc = item.get("description")
        if isinstance(raw_desc, list):
            raw_desc = raw_desc[0] if raw_desc else ""
        desc = re.sub(r"<[^>]+>", "", str(raw_desc or ""))[:280].strip()
        year = ""
        for key in ("date", "dates"):
            d = item.get(key)
            if isinstance(d, list) and d:
                year = str(d[0])[:4]; break
            if isinstance(d, str) and d:
                year = d[:4]; break
        out.append({
            "source": "library_of_congress",
            "id": item.get("id") or video_url,
            "title": item.get("title") or "Library of Congress item",
            "description": desc,
            "creator": "Library of Congress",
            "year": year,
            "embed_type": "video",
            "embed_url": video_url,
            "page_url": item.get("id") or "",
            "thumb_url": thumb,
            "attribution": "Library of Congress",
        })
        if len(out) >= 3:
            break
    return out


async def _fetch_article_videos(article: dict) -> list[dict]:
    """Merge results from all configured sources, interleaved so no single
    source dominates. Runs searches in parallel — the whole fetch is bounded
    by the slowest upstream, not the sum."""
    title = article.get("title") or ""
    ia, commons, yt, loc = await asyncio.gather(
        _search_internet_archive(title),
        _search_commons_videos(title),
        _search_youtube(title),
        _search_library_of_congress(title),
        return_exceptions=True,
    )
    def _ok(r) -> list[dict]:
        return r if isinstance(r, list) else []
    # Ordering inside the interleave determines tie-break when all sources
    # have results: IA's curated archives first, then YouTube (usually the
    # most populous), then Commons and LoC.
    pools = [list(_ok(ia)), list(_ok(yt)), list(_ok(commons)), list(_ok(loc))]
    merged: list[dict] = []
    CAP = 6
    while any(pools) and len(merged) < CAP:
        for pool in pools:
            if pool and len(merged) < CAP:
                merged.append(pool.pop(0))
    return merged


@app.get("/api/articles/{slug}/videos")
async def api_article_videos(slug: str, refresh: bool = False):
    """Companion videos for an article. Results cached per-article in the
    `article_videos` table with a 7-day TTL; pass ?refresh=true to force."""
    with get_db() as db:
        row = db.execute(
            "SELECT id, title FROM articles WHERE slug = ?", (slug,)
        ).fetchone()
        if not row:
            raise HTTPException(404, f"No article '{slug}'")
        aid = row["id"]

        if not refresh:
            cached = db.execute(
                "SELECT payload, fetched_at FROM article_videos WHERE article_id = ?",
                (aid,),
            ).fetchone()
            if cached:
                age = db.execute(
                    "SELECT (strftime('%s','now') - strftime('%s', ?))",
                    (cached["fetched_at"],),
                ).fetchone()[0]
                try:
                    if int(age) < VIDEO_CACHE_TTL_SECONDS:
                        return {"slug": slug, "videos": json.loads(cached["payload"])}
                except (TypeError, ValueError):
                    pass

    videos = await _fetch_article_videos(dict(row))
    with get_db() as db:
        db.execute("""
            INSERT INTO article_videos (article_id, payload, fetched_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(article_id) DO UPDATE SET
              payload = excluded.payload, fetched_at = CURRENT_TIMESTAMP
        """, (aid, json.dumps(videos)))
        db.commit()

    return {"slug": slug, "videos": videos}


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


# ---------- /api/carta/models ----------
@app.get("/api/carta/models")
async def api_carta_models():
    """List model options for the iOS/web settings picker.
    - local: live from the Pi's /api/tags (empty if Ollama is down).
    - cloud: live from Ollama Cloud's /api/tags when OLLAMA_CLOUD_KEY is set;
      falls back to OLLAMA_CLOUD_FALLBACK if that call fails."""
    result: dict[str, Any] = {
        "local": [],
        "cloud": [],
        "default_provider": "local",
        "default_model": OLLAMA_MODEL,
        "cloud_enabled": bool(OLLAMA_CLOUD_KEY),
    }

    try:
        async with httpx.AsyncClient(timeout=5) as client:
            r = await client.get(f"{OLLAMA_URL}/api/tags")
            if r.status_code == 200:
                result["local"] = [m.get("name", "") for m in (r.json().get("models") or [])
                                   if m.get("name")]
    except Exception as e:
        log.info("models: local /api/tags unreachable: %s", e)

    if OLLAMA_CLOUD_KEY:
        try:
            async with httpx.AsyncClient(timeout=8) as client:
                r = await client.get(
                    f"{OLLAMA_CLOUD_URL}/api/tags",
                    headers={"Authorization": f"Bearer {OLLAMA_CLOUD_KEY}"},
                )
                if r.status_code == 200:
                    names = sorted({m.get("name", "") for m in (r.json().get("models") or [])
                                    if m.get("name")})
                    result["cloud"] = names
        except Exception as e:
            log.info("models: cloud /api/tags unreachable: %s", e)
        if not result["cloud"]:
            result["cloud"] = OLLAMA_CLOUD_FALLBACK

    return result


# ---------- /api/carta/chat ----------
class CartaChatReq(BaseModel):
    message: str
    history: list[dict] = []
    provider: Optional[str] = None  # "local" | "cloud"
    model: Optional[str] = None


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
    system += (
        "\n\nMANDATORY MARKUP RULE:\n"
        "Any specific topic, concept, person, or place you mention MUST be "
        "wrapped in [[ double square brackets ]]. No exceptions. This turns "
        "them into tappable links on Juan's phone.\n\n"
        "EXAMPLES of correct output:\n"
        "  User: I'm learning about black holes.\n"
        "  CARTA: Oh, [[Black hole]]! Those tie into [[General relativity]] — "
        "want me to capture it?\n\n"
        "  User: Tell me about octopuses.\n"
        "  CARTA: [[Octopus]] cognition is wild — they're related to [[Cephalopod]]s "
        "and show real [[Problem solving]] behavior.\n\n"
        "  User: Just read about Stoicism.\n"
        "  CARTA: Nice, [[Stoicism]]! If you haven't, [[Marcus Aurelius]]' "
        "*Meditations* is the classic entry point.\n\n"
        "If you forget to bracket a topic, Juan can't tap it. Bracket generously.\n"
    )
    if titles:
        system += (
            "\nTopics ALREADY in his wiki (bracketing these opens them):\n- "
            + "\n- ".join(titles) + "\n"
            "All OTHER bracketed topics become '+ capture' chips.\n"
        )
    system += (
        "\nKeep responses to 2–3 short sentences. "
        "Remember: bracket every topic. Every single one."
    )

    messages = [{"role": "system", "content": system}]
    for m in (req.history or [])[-8:]:
        role = m.get("role", "user")
        if role in ("user", "assistant"):
            messages.append({"role": role, "content": str(m.get("content", ""))})
    messages.append({"role": "user", "content": req.message})

    try:
        reply = await ollama_chat(
            messages, temperature=0.75, max_tokens=260,
            provider=req.provider, model=req.model,
        )
    except OllamaError as e:
        log.error("ollama chat failed (%d): %s", e.status, e.message)
        hint = ""
        if e.status == 403:
            hint = " This model likely needs a paid Ollama subscription — try `gpt-oss:20b` or switch back to Local."
        return {"reply": f"*CARTA is elsewhere — {e.message}.*{hint}", "error": True}
    except Exception as e:
        log.error("ollama chat failed: %s", e)
        return {"reply": f"*CARTA is elsewhere — {e}.*", "error": True}

    with get_db() as db:
        db.execute("INSERT INTO carta_log (role, content) VALUES ('user', ?)", (req.message,))
        db.execute("INSERT INTO carta_log (role, content) VALUES ('carta', ?)", (reply,))
        db.commit()
    return {"reply": reply}


# ---------- /api/carta/draft ----------
class CartaDraftReq(BaseModel):
    description: str
    provider: Optional[str] = None
    model: Optional[str] = None


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
        raw = await ollama_chat(
            messages, temperature=0.2, format_json=True, max_tokens=400,
            provider=req.provider, model=req.model,
        )
    except OllamaError as e:
        raise HTTPException(e.status, e.message)
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
    provider: Optional[str] = None
    model: Optional[str] = None


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

    # Trim the source text to stay within the local model's comfortable context.
    # 2500 chars ≈ 600 tokens of source — leaves room for 3B models to think fast.
    body_text = extract_text(row["content_html"] or "")
    source = (row["summary"] or "") + "\n\n" + body_text
    source = source.strip()[:2500]

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

    # Quizzes are heavier — give the model up to 10 minutes and only as many
    # tokens as count warrants (~180 per question is plenty).
    try:
        raw = await ollama_chat(
            [{"role": "system", "content": system}, {"role": "user", "content": user}],
            temperature=0.4, format_json=True,
            max_tokens=min(1500, 200 + 250 * count),
            timeout=600,
            provider=req.provider, model=req.model,
        )
    except OllamaError as e:
        raise HTTPException(e.status, e.message)
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
        log.warning("quiz: model produced no valid questions. raw payload (first 800): %s", raw[:800])
        raise HTTPException(502, "Model returned no valid questions; try again.")

    return {
        "article": {"slug": row["slug"], "title": row["title"]},
        "questions": questions,
    }


# ---------- /api/carta/quest ----------
class QuestReq(BaseModel):
    topic: str
    provider: Optional[str] = None
    model: Optional[str] = None


@app.post("/api/carta/quest")
async def api_carta_quest(req: QuestReq):
    """Build a 7-day reading curriculum around `topic` out of already-captured
    articles. Returns {topic, days: [{day, article_slugs, quiz_count,
    reflection}]}. If the local corpus is too thin, fewer articles are assigned
    per day rather than failing — the plan shape is always 7 days."""
    topic = (req.topic or "").strip()
    if not topic:
        raise HTTPException(400, "topic is required")

    with get_db() as db:
        rows = db.execute("""
            SELECT a.slug, a.title, a.summary, a.deck, p.name AS province_name
              FROM articles a LEFT JOIN provinces p ON p.id = a.province_id
        """).fetchall()
    corpus = [dict(r) for r in rows]
    if not corpus:
        raise HTTPException(409, "No articles in the codex yet. Capture something first.")

    # Rank candidates by a cheap keyword/substring match first so the prompt
    # stays short (we hand the model only ~30 titles, not the whole corpus).
    # Zero-score articles are filtered out — we'd rather have short days than
    # pad the plan with irrelevant picks.
    needles = [w for w in re.findall(r"\w{3,}", topic.lower()) if w]
    def score(a: dict) -> int:
        blob = " ".join([
            (a.get("title") or "").lower(),
            (a.get("summary") or "").lower(),
            (a.get("deck") or "").lower(),
            (a.get("province_name") or "").lower(),
        ])
        return sum(1 for n in needles if n in blob)
    scored = [(score(a), a) for a in corpus]
    relevant = [a for s, a in scored if s > 0]
    relevant.sort(key=lambda a: (-score(a), a.get("title") or ""))
    shortlist = relevant[:30]
    relevant_slugs = {a["slug"] for a in shortlist}
    menu = ("\n".join(f"- {a['title']} [{a['slug']}] — {a.get('province_name') or '—'}"
                      for a in shortlist)
            if shortlist else "(no matching articles captured yet)")

    system = (
        "You design short, focused reading quests from Juan's personal wiki. "
        "Output ONLY JSON, no prose. Shape:\n"
        '{ "days": [\n'
        '    { "day": 1, "article_slugs": ["slug-one", "slug-two"],\n'
        '      "quiz_count": 2, "reflection": "one-sentence prompt" }\n'
        "  ]\n"
        "}\n"
        "Exactly 7 days. Each day 0–3 article_slugs drawn ONLY from the list "
        "below (use the bracketed slug, never invent). Never include a slug "
        "that isn't in the list. If fewer than 7 days of reading fit the "
        "topic, return some days with an empty article_slugs array and a "
        "short reflection prompt — DO NOT pad with unrelated articles. "
        "quiz_count in [0..3]. Order days from introduction → deeper → synthesis."
    )
    user = (
        f"Topic: {topic}\n"
        f"Relevant articles available: {len(shortlist)} "
        f"(out of {len(corpus)} total in the codex)\n\n"
        f"Available articles:\n{menu}"
    )

    # Skip the model entirely if nothing in the codex matches the topic.
    # Burning tokens to get back 7 empty days is worse UX than saying so.
    if not shortlist:
        return {
            "topic": topic,
            "days": [
                {
                    "day": n,
                    "article_slugs": [],
                    "articles": [],
                    "quiz_count": 0,
                    "reflection": (f"No captured articles touch '{topic}' yet. "
                                   "Consider capturing one before starting this quest.")
                                   if n == 1 else "",
                }
                for n in range(1, 8)
            ],
        }

    try:
        raw = await ollama_chat(
            [{"role": "system", "content": system}, {"role": "user", "content": user}],
            temperature=0.35, format_json=True, max_tokens=900, timeout=300,
            provider=req.provider, model=req.model,
        )
    except OllamaError as e:
        raise HTTPException(e.status, e.message)
    except Exception as e:
        raise HTTPException(502, f"Ollama unavailable: {e}")

    raw = (raw or "").strip()
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        m = re.search(r"\{.*\}", raw, re.DOTALL)
        if not m:
            raise HTTPException(502, f"Could not parse quest: {raw[:200]}")
        try:
            data = json.loads(m.group(0))
        except json.JSONDecodeError as e:
            raise HTTPException(502, f"Invalid JSON from quest: {e}")

    title_by_slug = {a["slug"]: a["title"] for a in corpus}
    province_by_slug = {a["slug"]: a.get("province_name") for a in corpus}

    days_out: list[dict] = []
    for idx, d in enumerate((data.get("days") or [])[:7], start=1):
        raw_slugs = [str(s).strip() for s in (d.get("article_slugs") or [])]
        # Keep only topic-relevant slugs. If the corpus has no relevant
        # matches at all (relevant_slugs empty), we end up with empty days
        # and a reflection prompt — honest about the thin corpus.
        clean_slugs = [s for s in raw_slugs if s in relevant_slugs][:3]
        try:
            qc = max(0, min(int(d.get("quiz_count", 1)), 3))
        except (TypeError, ValueError):
            qc = 1
        reflection = str(d.get("reflection") or "").strip()
        days_out.append({
            "day": idx,
            "article_slugs": clean_slugs,
            "articles": [
                {"slug": s, "title": title_by_slug.get(s, s),
                 "province_name": province_by_slug.get(s)}
                for s in clean_slugs
            ],
            "quiz_count": qc,
            "reflection": reflection,
        })

    # Pad to 7 days if the model returned fewer; keep the shape stable.
    while len(days_out) < 7:
        days_out.append({
            "day": len(days_out) + 1,
            "article_slugs": [],
            "articles": [],
            "quiz_count": 0,
            "reflection": "",
        })

    return {"topic": topic, "days": days_out}
