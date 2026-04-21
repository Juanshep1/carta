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
from fastapi.responses import FileResponse, JSONResponse, Response, StreamingResponse
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
    ("Philosophy & Ethics",         ["philosophy", "ethics", "metaphysics", "metaphysical",
                                      "epistemology", "phenomenology", "stoicism", "existentialism",
                                      "moral", "virtue", "consciousness", "paranormal", "psychic",
                                      "parapsychology", "occult", "esoteric", "mysticism", "spirituality"]),
    ("History of Ideas",            ["history of", "enlightenment", "renaissance", "civilization",
                                      "ancient greece", "medieval europe", "empire", "revolution", "dynasty",
                                      "intellectual history"]),
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


def _coerce_llm_json(raw: str) -> Optional[Any]:
    """Parse LLM output as JSON, tolerating common malformations.

    LLMs reliably break JSON in a handful of recurring ways even when
    instructed to output strict JSON and the API is asked for
    `format: json`:
      1. Markdown fences: ```json ... ```
      2. Prose prefix or suffix around the JSON
      3. Smart quotes (“ ”) instead of ASCII double quotes
      4. Trailing commas inside objects/arrays
      5. Unescaped double quotes inside string values — the classic
         "Expecting ',' delimiter" error users see
      6. Truncated output (ran out of tokens mid-object)

    Returns the parsed object or None if every repair attempt fails.
    Never raises — the caller can decide whether to retry or error."""
    if not raw:
        return None
    s = raw.strip()

    # 1. Strip ```json ... ``` / ``` ... ``` fences
    if s.startswith("```"):
        first = s.find("\n")
        if first != -1:
            s = s[first + 1:]
        if s.endswith("```"):
            s = s[:-3]
        s = s.strip()

    # 2. Easy win
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        pass

    # 3. Extract the outermost { ... } balanced span
    def _extract_balanced(text: str) -> Optional[str]:
        start = text.find("{")
        if start < 0:
            return None
        depth = 0
        in_str = False
        escape = False
        for i in range(start, len(text)):
            ch = text[i]
            if escape:
                escape = False
                continue
            if ch == "\\":
                escape = True
                continue
            if ch == '"' and not escape:
                in_str = not in_str
                continue
            if in_str:
                continue
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    return text[start:i + 1]
        return None

    balanced = _extract_balanced(s)
    if balanced:
        try:
            return json.loads(balanced)
        except json.JSONDecodeError:
            s = balanced

    # 4. Normalize smart quotes + zero-width junk
    repairs = (s
        .replace("\u201c", '"').replace("\u201d", '"')
        .replace("\u2018", "'").replace("\u2019", "'")
        .replace("\ufeff", "").replace("\u200b", ""))

    # 5. Drop trailing commas before } or ]
    repairs = re.sub(r",\s*([}\]])", r"\1", repairs)

    try:
        return json.loads(repairs)
    except json.JSONDecodeError:
        pass

    # 6. Try closing an unterminated structure (truncated response)
    opens_obj = repairs.count("{") - repairs.count("}")
    opens_arr = repairs.count("[") - repairs.count("]")
    if opens_obj > 0 or opens_arr > 0:
        # If the last character is inside an open string, try to close it.
        quote_count = 0
        escape = False
        for ch in repairs:
            if escape:
                escape = False
                continue
            if ch == "\\":
                escape = True
                continue
            if ch == '"':
                quote_count += 1
        padded = repairs
        if quote_count % 2 == 1:
            padded += '"'
        padded += "]" * max(0, opens_arr) + "}" * max(0, opens_obj)
        try:
            return json.loads(padded)
        except json.JSONDecodeError:
            pass

    # 7. Last resort: aggressive fix for the "Expecting ',' delimiter"
    # case where a string value contains an unescaped quote. Rewrite
    # naked quotes inside suspected string values by walking the
    # structure character by character.
    fixed = _repair_unescaped_quotes(repairs)
    if fixed is not None:
        try:
            return json.loads(fixed)
        except json.JSONDecodeError:
            pass

    return None


def _repair_unescaped_quotes(s: str) -> Optional[str]:
    """Walk the string and escape double quotes that appear inside
    what looks like a JSON string value. Heuristic: once we're inside
    a string, a `"` is an end-quote only if the next non-whitespace
    character is one of , } ] : — otherwise it's an unescaped
    interior quote and we add a backslash before it."""
    if not s:
        return None
    out: list[str] = []
    in_str = False
    escape = False
    i = 0
    while i < len(s):
        ch = s[i]
        if escape:
            out.append(ch)
            escape = False
            i += 1
            continue
        if ch == "\\":
            out.append(ch)
            escape = True
            i += 1
            continue
        if ch == '"':
            if not in_str:
                in_str = True
                out.append(ch)
            else:
                # Look ahead for the first non-whitespace character.
                j = i + 1
                while j < len(s) and s[j] in " \t\n\r":
                    j += 1
                terminator = s[j] if j < len(s) else ""
                if terminator in (",", "}", "]", ":", ""):
                    in_str = False
                    out.append(ch)
                else:
                    # Interior quote inside a string value — escape it.
                    out.append("\\")
                    out.append(ch)
            i += 1
            continue
        out.append(ch)
        i += 1
    return "".join(out)


async def ollama_chat(messages: list[dict], *, temperature: float = 0.7,
                      format_json: bool = False, max_tokens: int = 400,
                      timeout: float = 180,
                      provider: Optional[str] = None,
                      model: Optional[str] = None,
                      _allow_fallback: bool = True,
                      _length_retries: int = 2) -> str:
    base_url, chosen_model, headers = resolve_provider(provider, model)
    payload: dict[str, Any] = {
        "model": chosen_model,
        "messages": messages,
        "stream": False,
        # `think: false` tells reasoning-capable models (gpt-oss, deepseek,
        # qwen3, etc.) to skip the <thinking> phase and go straight to the
        # answer. Without this, gpt-oss:20b happily burns all num_predict
        # tokens on planning and returns content="". Models that don't know
        # the flag ignore it.
        "think": False,
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

    msg = data.get("message", {}) or {}
    reply = msg.get("content", "") or ""
    # strip <think> blocks some models emit
    reply = re.sub(r"<think>.*?</think>", "", reply, flags=re.DOTALL).strip()

    # Reasoning models (gpt-oss, deepseek, kimi) sometimes burn the entire
    # num_predict budget on a `"thinking"` field and emit content="". When
    # done_reason is "length" and content is empty, retry with double the
    # token budget. Independent of _allow_fallback so the retry still fires
    # even when we're inside a cloud-fallback call (the original reason we
    # got here).
    done_reason = data.get("done_reason") or ""
    has_thinking = bool((msg.get("thinking") or "").strip())
    if (_length_retries > 0 and not reply and has_thinking
            and done_reason == "length"
            and max_tokens < 6000):
        doubled = min(max_tokens * 2, 6000)
        log.info("ollama: '%s' ran out of tokens on thinking phase "
                 "(done_reason=length); retrying at num_predict=%d",
                 chosen_model, doubled)
        return await ollama_chat(
            messages, temperature=temperature, format_json=format_json,
            max_tokens=doubled, timeout=timeout,
            provider=provider, model=model,
            _allow_fallback=_allow_fallback,
            _length_retries=_length_retries - 1,
        )

    # Some cloud SKUs (notably gpt-oss:20b on certain prompts) happily
    # return HTTP 200 with an empty content body — there's no error to
    # surface, but the client gets back a silent empty bubble. Treat that
    # as a retryable failure and hop to the fallback model, mirroring the
    # 5xx path. Applies to both JSON and free-form prompts: chat hits this
    # as often as quiz does.
    want_cloud = (provider or "").lower() == "cloud" and OLLAMA_CLOUD_KEY
    if (_allow_fallback and want_cloud
            and not reply
            and chosen_model != CLOUD_FALLBACK_MODEL):
        log.info("ollama cloud: '%s' returned 200 with empty body; "
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


# Map of file extension to EPUB manifest media-type. EPUB 3 readers are
# strict about this — a wrong media-type on a JPEG and the reader shows
# the broken-image glyph even though the bytes are there.
_EXT_MEDIA_TYPE = {
    ".jpg":  "image/jpeg",
    ".jpeg": "image/jpeg",
    ".png":  "image/png",
    ".gif":  "image/gif",
    ".webp": "image/webp",
    ".svg":  "image/svg+xml",
}

# Matches the src paths produced by process_article_html when captured
# content was baked into the DB. Examples:
#   <img src="/static/images/abc123.jpg" ...>
#   src="/static/images/ab.png"
_IMG_SRC_RE = re.compile(
    r'''(<img[^>]*\s)(src|data-src)\s*=\s*(['"])(/static/images/([^'"]+))\3''',
    flags=re.IGNORECASE,
)


def _rewrite_and_collect_images(html: str,
                                collected: dict[str, bytes]) -> str:
    """Rewrite every /static/images/<file> src in `html` to `images/<file>`
    and load each referenced file's bytes into `collected` (keyed by the
    bare filename). Missing files are left untouched — the reader will
    show the broken-image glyph for those, same as before, but every
    file that *does* exist on disk is now guaranteed to embed.

    We operate on the serialized HTML (not BeautifulSoup) because the
    chapter body is already finalized XHTML — reparsing would risk
    trimming whitespace or attribute quoting EPUB readers are picky
    about."""
    if not html:
        return html

    def _sub(m: re.Match) -> str:
        prefix = m.group(1)  # `<img ... ` up through the attribute name
        attr_name = m.group(2)
        quote = m.group(3)
        full_path = m.group(4)  # /static/images/<filename>
        filename = m.group(5)
        disk_path = IMG_DIR / filename
        if filename not in collected:
            try:
                collected[filename] = disk_path.read_bytes()
            except (FileNotFoundError, OSError):
                # Leave the original src so we fail visibly rather than
                # silently pointing at a file that isn't in the zip.
                return m.group(0)
        # Always normalize to src (drop data-src) since the HTML is being
        # read offline by an EPUB reader that doesn't know about lazy loading.
        return f'{prefix}src={quote}images/{filename}{quote}'

    return _IMG_SRC_RE.sub(_sub, html)


def _build_image_manifest_entries(image_files: dict[str, bytes],
                                  start_id: int = 1) -> list[str]:
    """Turn a {filename: bytes} map into EPUB manifest <item> lines.
    Skips files with unknown extensions (EPUB readers reject those)."""
    items: list[str] = []
    for idx, filename in enumerate(sorted(image_files.keys()), start=start_id):
        ext = Path(filename).suffix.lower()
        media = _EXT_MEDIA_TYPE.get(ext)
        if not media:
            continue
        # Manifest ids must be XML NCNames — safe subset here is a letter prefix + idx.
        items.append(
            f'<item id="img{idx:04d}" href="images/{filename}" media-type="{media}"/>'
        )
    return items


def _inline_images_as_data_uris(html: str) -> str:
    """For the print-ready HTML (which the user saves as PDF from the
    browser), replace every /static/images/<file> src with a base64
    data URI. This makes the page self-contained: images survive
    download-to-disk and offline printing without a running server."""
    if not html:
        return html
    import base64 as _b64

    def _sub(m: re.Match) -> str:
        prefix = m.group(1)
        quote = m.group(3)
        filename = m.group(5)
        ext = Path(filename).suffix.lower()
        media = _EXT_MEDIA_TYPE.get(ext)
        if not media:
            return m.group(0)
        try:
            data = (IMG_DIR / filename).read_bytes()
        except (FileNotFoundError, OSError):
            return m.group(0)
        b64 = _b64.b64encode(data).decode("ascii")
        return f'{prefix}src={quote}data:{media};base64,{b64}{quote}'

    return _IMG_SRC_RE.sub(_sub, html)


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
    image_files: dict[str, bytes] = {}   # filename -> bytes, deduped across chapters

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
        # Pull /static/images/<file> paths in-line. Each unique file is
        # added to `image_files` once; the chapter HTML's src gets
        # rewritten to the packaged path.
        body_html = _rewrite_and_collect_images(body_html, image_files)
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
    manifest_items.extend(_build_image_manifest_entries(image_files))

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
        for filename, data in image_files.items():
            if Path(filename).suffix.lower() in _EXT_MEDIA_TYPE:
                zf.writestr(f"OEBPS/images/{filename}", data)
    return buf.getvalue()


# ---------- /api/compile ----------
# Topic → learning book. Ranks the existing corpus by keyword relevance,
# optionally auto-captures more Wikipedia articles to hit a target size, and
# stitches the result into an EPUB (same builder as province export) or a
# print-ready HTML page the user can save as PDF from their browser.

async def _wiki_related_titles(topic: str, limit: int = 12) -> list[str]:
    """Fetch relevance-ranked Wikipedia titles for `topic`. No auth required."""
    try:
        async with httpx.AsyncClient(
            timeout=15, headers={"User-Agent": USER_AGENT}
        ) as client:
            r = await client.get(
                "https://en.wikipedia.org/w/api.php",
                params={
                    "action": "query",
                    "list": "search",
                    "srsearch": topic,
                    "srlimit": limit,
                    "srwhat": "text",
                    "format": "json",
                },
            )
            if r.status_code != 200:
                return []
            return [hit["title"] for hit in (r.json().get("query") or {}).get("search", [])]
    except Exception as e:
        log.info("compile: wiki related titles failed for '%s': %s", topic, e)
        return []


def _compile_html(topic: str, articles: list[dict]) -> str:
    """Print-ready single HTML page that renders well on paper AND can be
    saved as PDF via the browser's print dialog. No print-server dependency."""
    css = """
    @page { size: letter; margin: 0.8in; }
    html, body { margin: 0; padding: 0; background: #f1e8d3;
                 font-family: "EB Garamond", Georgia, serif;
                 color: #1a1410; line-height: 1.55; }
    .hero { text-align: center; margin: 2em 0 3em; }
    .hero .kicker { font-family: "IBM Plex Mono", monospace;
                    letter-spacing: 0.28em; font-size: 11px;
                    color: #7a1f1f; text-transform: uppercase; }
    .hero h1 { font-family: "Fraunces", Georgia, serif; font-weight: 400;
               font-size: 44px; margin: 14px 0 6px; }
    .hero h1 em { font-style: italic; color: #7a1f1f; }
    .hero .count { font-style: italic; color: #6d5e47; }
    .toc { margin: 2em 0 3em; padding: 1em 0; border-top: 1px solid #c4a35a;
           border-bottom: 1px solid #c4a35a; }
    .toc h2 { font-family: "Fraunces", serif; font-weight: 400; font-size: 18px;
              letter-spacing: 0.24em; text-transform: uppercase;
              color: #7a1f1f; margin: 0 0 14px; }
    .toc ol { list-style: none; counter-reset: ch; padding: 0; }
    .toc li { counter-increment: ch; padding: 4px 0;
              display: flex; justify-content: space-between; gap: 10px; }
    .toc li::before { content: counter(ch, upper-roman) "."; color: #9b7a2e;
                      font-family: "Fraunces", serif; font-style: italic;
                      min-width: 40px; }
    .chapter { page-break-before: always; padding-top: 1em; }
    .chapter .kicker { font-family: "IBM Plex Mono", monospace;
                       font-size: 10px; letter-spacing: 0.22em;
                       color: #7a1f1f; text-transform: uppercase; }
    .chapter h1 { font-family: "Fraunces", serif; font-weight: 500;
                  font-size: 30px; margin: 6px 0 4px; }
    .chapter .deck { font-style: italic; color: #3a2f27; margin-bottom: 1.4em; }
    .chapter h2 { font-family: "Fraunces", serif; font-size: 20px;
                  font-weight: 500; border-bottom: 1px solid rgba(26,20,16,0.18);
                  padding-bottom: 4px; margin-top: 1.8em; }
    .chapter p { margin: 0.7em 0; }
    .chapter a { color: #7a1f1f; text-decoration: none; }
    .chapter blockquote { border-left: 2px solid #7a1f1f;
                          padding: 0.4em 1em; margin: 1em 0;
                          font-style: italic; color: #3a2f27; }
    img { max-width: 100%; height: auto; }
    .print-button { position: fixed; top: 16px; right: 16px;
                    padding: 10px 18px; background: #7a1f1f; color: #f1e8d3;
                    font-family: "IBM Plex Mono", monospace; font-size: 11px;
                    letter-spacing: 0.22em; text-transform: uppercase;
                    border: 0; cursor: pointer; z-index: 100; }
    @media print { .print-button { display: none; } }
    """
    esc = lambda s: (str(s or "").replace("&", "&amp;")
                     .replace("<", "&lt;").replace(">", "&gt;"))
    toc_items = "".join(
        f'<li><span>{esc(a.get("title") or "—")}</span>'
        f'<span style="color:#6d5e47">{esc(a.get("province_name") or "")}</span></li>'
        for a in articles
    )
    chapter_html = ""
    for i, a in enumerate(articles, 1):
        body = a.get("content_html") or f"<p>{esc(a.get('summary') or '')}</p>"
        body = re.sub(r"<script[\s\S]*?</script>", "", body, flags=re.I)
        body = re.sub(r"\son[a-z]+=\"[^\"]*\"", "", body, flags=re.I)
        kicker = esc(a.get("province_name") or "")
        chapter_html += f"""
        <section class="chapter">
          <div class="kicker">Chapter {i}{' · ' + kicker if kicker else ''}</div>
          <h1>{esc(a.get('title') or '')}</h1>
          {f'<div class="deck">{esc(a.get("deck") or a.get("summary") or "")}</div>'
           if (a.get('deck') or a.get('summary')) else ''}
          {body}
        </section>
        """
    return f"""<!doctype html><html><head><meta charset="utf-8">
    <title>CARTA · {esc(topic)} — Learning Book</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link href="https://fonts.googleapis.com/css2?family=Fraunces:ital,wght@0,400..700;1,400..700&family=EB+Garamond:ital,wght@0,400..700;1,400..700&family=IBM+Plex+Mono:wght@400;500&display=swap" rel="stylesheet">
    <style>{css}</style></head><body>
    <button class="print-button" onclick="window.print()">Save as PDF</button>
    <div class="hero">
      <div class="kicker">A CARTA Learning Book</div>
      <h1><em>{esc(topic)}</em></h1>
      <div class="count">{len(articles)} chapters · compiled from your codex</div>
    </div>
    <section class="toc"><h2>Contents</h2><ol>{toc_items}</ol></section>
    {chapter_html}
    </body></html>"""


class CompileReq(BaseModel):
    topic: str
    format: str = "epub"             # "epub" | "html"
    target_articles: int = 8
    auto_expand: bool = True
    provider: Optional[str] = None
    model: Optional[str] = None
    # Free-text angle — "historical origins", "modern conspiracy culture",
    # "pharmacology only", etc. When given, the LLM curriculum prompt
    # weaves it in and the ranker treats its words as secondary needles.
    angle: Optional[str] = None
    # Kinds of Wikipedia entries to reject out of hand. Free-text words;
    # matched against the article's Wikipedia `description` field and
    # title parenthetical. Typical values: "song, film, tv, video game,
    # episode, album, band". When the user types "Illuminati" and the
    # corpus drifts into "Illuminati (song by Madonna)", this is what
    # drops it — the short description reads "song by..." which matches.
    excludes: Optional[list[str]] = None


# English stopwords we strip from topic needles so "the french revolution"
# actually ranks by "french" + "revolution", not by how many articles happen
# to contain the word "the".
_COMPILE_STOPWORDS = {
    "the", "a", "an", "and", "or", "of", "in", "on", "to", "for", "with",
    "is", "was", "are", "were", "be", "been", "as", "at", "by", "from",
    "how", "what", "why", "this", "that", "these", "those", "it", "its",
    "into", "about", "over", "under", "up", "down", "between",
}


def _compile_needles(topic: str) -> list[str]:
    return [w for w in re.findall(r"\w+", topic.lower())
            if len(w) >= 3 and w not in _COMPILE_STOPWORDS]


def _compile_rank(topic: str, corpus: list[dict]) -> list[tuple[float, dict]]:
    """Weighted scorer that strongly prefers title matches over summary
    fuzz. Returns list of (score, article) sorted descending. Articles with
    zero matches never appear — no more padding with unrelated entries."""
    needles = _compile_needles(topic)
    if not needles:
        return []

    def score(a: dict) -> float:
        title = (a.get("title") or "").lower()
        deck = (a.get("deck") or "").lower()
        summary = (a.get("summary") or "").lower()
        province = (a.get("province_name") or "").lower()
        wiki_title = (a.get("wiki_title") or "").lower()
        hits = 0.0
        for n in needles:
            if n in title or n in wiki_title:
                hits += 3.0
            if n in deck:
                hits += 1.5
            if n in summary:
                hits += 1.0
            if n in province:
                hits += 0.8
        # Multi-needle bonus: rewards articles that hit several aspects
        # of a compound topic ("roman empire" → both "roman" + "empire").
        matched = sum(1 for n in needles if n in (title + " " + deck + " "
                                                  + summary + " " + province
                                                  + " " + wiki_title))
        if len(needles) >= 2 and matched >= 2:
            hits *= 1.4
        return hits

    scored = [(score(a), a) for a in corpus]
    scored = [(s, a) for s, a in scored if s >= 2.0]   # strict threshold
    scored.sort(key=lambda sa: -sa[0])
    return scored


# Pop-culture / media disambiguation markers. Anything the user's topic
# is obviously NOT (e.g. a song / film / episode) belongs here. Extended
# vocabulary so "Illuminati (song by Madonna)" ranks far below the
# actual Illuminati article.
_DRIFT_MEDIA_KINDS: tuple[str, ...] = (
    # Music
    "band", "song", "album", "ep", "single", "mixtape", "soundtrack",
    "rapper", "musician", "singer", "producer", "dj", "record label",
    "discography",
    # Film / TV
    "film", "movie", "documentary", "short film", "tv series", "tv show",
    "tv episode", "episode", "season", "miniseries", "web series",
    "anime", "cartoon", "franchise",
    # Print / interactive
    "novel", "book", "comic", "manga", "graphic novel",
    "video game", "mobile game", "board game",
    "magazine", "newspaper",
    # Fictional entities
    "character", "fictional", "superhero", "supervillain",
    # Events / venues
    "concert", "festival", "tournament", "convention", "pageant",
    "wrestler", "athlete", "actress", "actor",
)


def _candidate_drift_score(title: str, topic: str) -> int:
    """Deprioritize Wikipedia titles that clearly mean something other
    than the topic. Typical case: a user compiles on "Illuminati" and
    the search pool surfaces "Illuminati (song by Madonna)" or
    "Illuminati (Rapsody album)". 0 = probably the primary topic;
    higher = more likely a detour. Tied 0-scores preserve Wikipedia's
    own relevance order."""
    low = title.lower()
    topic_low = topic.lower()
    drift = 0
    if "(" in low and ")" in low:
        inner = low[low.index("(") + 1 : low.index(")")]
        # Any media-kind hit is a strong drop signal.
        if any(kw in inner for kw in _DRIFT_MEDIA_KINDS):
            drift += 20
        # "Smith (disambiguation)" hub page — never the primary topic.
        if "disambiguation" in inner:
            drift += 12
        # Year-based disambiguation often indicates a specific film /
        # event rather than the general subject.
        if re.search(r"\b(19|20)\d{2}\b", inner):
            drift += 6
    # Exact title match with the topic is a strong primary-topic signal.
    bare = re.sub(r"\s*\([^)]*\)", "", low).strip()
    if bare == topic_low:
        drift -= 4
    return drift


def _description_suggests_media(description: str) -> Optional[str]:
    """Return the media-kind keyword if the Wikipedia `description` field
    ("song by Madonna", "2019 American film", "Japanese television
    drama series") implies the article is a specific media object
    rather than the general subject. Empty / missing descriptions
    return None."""
    if not description:
        return None
    d = description.lower()
    # Sorted longest-first so "tv series" beats "tv".
    for kw in sorted(_DRIFT_MEDIA_KINDS, key=len, reverse=True):
        if kw in d:
            return kw
    return None


def _topic_implies_media(topic: str) -> bool:
    """When the user's topic itself contains a media keyword
    ("electric light orchestra song", "breaking bad episode"), the
    media-description filter above should NOT reject matches — the
    user wants media-specific results. Cheap word-level check."""
    if not topic:
        return False
    t = topic.lower()
    return any(kw in t for kw in _DRIFT_MEDIA_KINDS)


def _excludes_hit(description: str, title: str, excludes: list[str]) -> Optional[str]:
    """Return the first exclude keyword that appears in the article's
    description or parenthetical title. Used to honor the compile form's
    explicit "exclude" field ("songs, films, tv")."""
    if not excludes:
        return None
    d = (description or "").lower()
    t = (title or "").lower()
    inner = ""
    if "(" in t and ")" in t:
        inner = t[t.index("(") + 1 : t.index(")")]
    for raw in excludes:
        kw = raw.strip().lower()
        if len(kw) < 2:
            continue
        if kw in d or kw in inner:
            return kw
    return None


_TOPIC_SYNONYMS: dict[str, tuple[str, ...]] = {
    "telekinesis": ("psychokinesis",),
    "psychokinesis": ("telekinesis",),
    "telepathy": ("mind reading", "thought transference"),
    "clairvoyance": ("remote viewing", "second sight"),
    "esp": ("extrasensory perception", "psi"),
    "ufo": ("unidentified flying object", "uap"),
}


async def _article_matches_topic(slug: str, topic: str) -> bool:
    """Verify an auto-captured article actually covers the topic.

    The drift-score ranker is the primary defense against parenthetical
    disambiguations like 'Telekinesis (band)'. This function is a
    post-capture backstop catching articles that reference the topic
    only incidentally.

    Strategy, cheapest first:
      1. Meta blob (title/deck/summary) contains the topic phrase, any
         stopword-filtered needle, or a known technical synonym → keep.
      2. Body contains the topic/needle with real coverage: for
         single-word topics, ≥3 absolute occurrences OR density
         ≥1.0 hits per 1k words. For multi-word topics the phrase
         itself or two distinct needles in the body is enough.

    Calibration points (topic='telekinesis'):
      Parapsychology   22 hits / 11k words = 2.00/1k → KEEP (via psychokinesis synonym)
      Zapped (1982)     4 hits / 1.8k     = 2.19/1k → KEEP (absolute)
      Betsy Braddock   13 hits / 14k     = 0.94/1k → KEEP (absolute)
      Omega-level       1 hit  / 1.2k     = 0.83/1k → DROP
      List of Boys chars 1 hit / 36k     = 0.03/1k → DROP"""
    needles = _compile_needles(topic)
    if not needles:
        return True
    synonyms = _TOPIC_SYNONYMS.get(topic.lower().strip(), ())
    expanded = list(needles) + list(synonyms)
    with get_db() as db:
        row = db.execute(
            "SELECT title, deck, summary, content_html FROM articles WHERE slug = ?",
            (slug,),
        ).fetchone()
    if not row:
        return False

    topic_low = topic.lower()
    meta_blob = " ".join([
        (row["title"] or "").lower(),
        (row["deck"] or "").lower(),
        (row["summary"] or "").lower(),
    ])
    if topic_low in meta_blob:
        return True
    if any(syn in meta_blob for syn in synonyms):
        return True
    meta_hits = sum(1 for n in needles if n in meta_blob)
    if meta_hits >= 1 and len(needles) >= 2:
        return True

    raw_html = (row["content_html"] or "").lower()
    body = re.sub(r"<[^>]+>", " ", raw_html)
    body = re.sub(r"\s+", " ", body)
    word_count = max(len(body.split()), 1)

    if len(needles) >= 2:
        if topic_low in body:
            return True
        return sum(1 for n in needles if n in body) >= 2

    primary = needles[0]
    total = body.count(primary) + sum(body.count(s) for s in synonyms)
    density_per_1k = total / word_count * 1000
    return total >= 3 or density_per_1k >= 1.0


async def _parallel_auto_expand(topic: str,
                                existing_slugs: set[str],
                                existing_titles_lower: set[str],
                                needed: int,
                                max_concurrent: int = 10,
                                excludes: Optional[list[str]] = None
                                ) -> list[str]:
    """Run Wikipedia captures in parallel batches until we've acquired
    `needed` fresh slugs or exhausted the candidate pool.

    Multiple batches cover the case where the first round under-fills
    because some titles 404 or resolve to articles already in the codex.
    Each batch fires up to `max_concurrent` requests concurrently to stay
    inside Tailscale Funnel's ~60s request budget.

    Pool ordering puts primary-topic titles first by applying a drift
    score to Wikipedia's raw search output — so "Telekinesis" ranks
    above "Telekinesis (band)" for a "telekinesis" topic. After each
    capture we verify the article's summary actually covers the topic
    before accepting it."""
    if needed <= 0:
        return []

    # Pull a generous pool so a second pass has something to try.
    candidates = await _wiki_related_titles(topic, limit=max(needed * 5, 30))
    # Drop already-captured, then drop anything whose title parenthetical
    # matches an excluded media kind up-front — cheapest filter, no
    # network round-trip per candidate. Then stable-sort by drift score
    # so primary matches come first.
    normalized_excludes = [e.strip().lower() for e in (excludes or []) if e.strip()]
    indexed: list[tuple[int, str]] = []
    for i, t in enumerate(candidates):
        if t.lower() in existing_titles_lower:
            continue
        if _excludes_hit("", t, normalized_excludes):
            log.info("compile: pre-filter drop '%s' (excluded keyword in title)", t)
            continue
        indexed.append((i, t))
    indexed.sort(key=lambda it: (_candidate_drift_score(it[1], topic), it[0]))
    pool = [t for _i, t in indexed]

    acquired: list[str] = []
    attempted_titles: set[str] = set()
    topic_is_media_spec = _topic_implies_media(topic)

    async def _one(title: str) -> Optional[str]:
        # Peek at the Wikipedia summary's `description` field BEFORE
        # committing to a full capture. If it says "song by X" / "2019
        # American film" / "TV episode" and the user's topic isn't
        # itself a media-specific phrase, skip without pulling the
        # article body at all. Saves a lot of bandwidth and DB churn
        # on drift-heavy topics like "Illuminati".
        try:
            summary = await fetch_summary(title)
        except Exception as e:
            log.info("compile: summary peek failed for '%s': %s", title, e)
            summary = {}
        description = (summary.get("description") or "").strip()

        if description and not topic_is_media_spec:
            kind = _description_suggests_media(description)
            if kind:
                log.info(
                    "compile: pre-capture drop '%s' — description='%s' (kind=%s)",
                    title, description, kind,
                )
                return None

        hit = _excludes_hit(description, title, normalized_excludes)
        if hit:
            log.info(
                "compile: pre-capture drop '%s' — matched exclude '%s' in description='%s'",
                title, hit, description,
            )
            return None

        try:
            r = await api_capture(CaptureReq(
                topic=title,
                wikipedia_title=title,
                topic_context=topic,
            ))
        except HTTPException as e:
            log.info("compile: capture '%s' skipped (%d): %s",
                     title, e.status_code, e.detail)
            return None
        except Exception as e:
            log.info("compile: capture '%s' errored: %s", title, e)
            return None
        slug = r.get("slug")
        if not slug or slug in existing_slugs or slug in acquired:
            return None
        # Verify the captured article actually covers the topic — belt-
        # and-braces after the pre-filter.
        if not await _article_matches_topic(slug, topic):
            log.info("compile: captured '%s' (from '%s') dropped as off-topic for '%s'",
                     slug, title, topic)
            return None
        return slug

    # Up to 3 batches so we don't loop forever on pathological topics.
    for _batch_idx in range(3):
        remaining = needed - len(acquired)
        if remaining <= 0 or not pool:
            break
        batch_size = min(max_concurrent, remaining, len(pool))
        # Over-fetch slightly within the batch to absorb 404s without
        # needing another full round-trip for typical cases.
        batch_size = min(len(pool), batch_size + max(2, remaining // 2))
        batch = pool[:batch_size]
        pool = pool[batch_size:]
        attempted_titles.update(batch)
        results = await asyncio.gather(*(_one(t) for t in batch))
        for slug in results:
            if slug and slug not in acquired:
                acquired.append(slug)
                if len(acquired) >= needed:
                    break

    return acquired[:needed]


async def _generate_curriculum(topic: str,
                               articles: list[dict],
                               provider: Optional[str],
                               model: Optional[str],
                               angle: Optional[str] = None) -> dict:
    """One LLM call that returns the entire course structure — book title,
    intro, conclusion, and per-chapter metadata. Returning None on failure
    tells the caller to fall back to plain bookshelf mode. `angle` (if
    given) biases every section toward the user's specific interest
    — "historical origins", "pharmacological mechanism", etc."""
    menu = "\n".join(
        f"{i+1}. [{a.get('slug')}] {a.get('title')} — "
        f"{(a.get('deck') or a.get('summary') or '')[:240]}"
        for i, a in enumerate(articles)
    )
    n = len(articles)
    angle_clause = ""
    if angle:
        angle_clause = (
            f"\n\nThe reader's specific angle on this topic is: \u201C{angle}\u201D. "
            "Frame the introduction, chapter titles, objectives, and takeaways "
            "through that lens. If a source article covers material outside the "
            "angle, note in its chapter that the reader should skim it or skip ahead."
        )
    system = (
        "You design short, cohesive learning books for a personal wiki. "
        "Given a topic and exactly N source articles, return ONLY JSON — "
        "no prose before or after. Shape:\n"
        "{\n"
        '  "book_title": "A Short Course in ...",\n'
        '  "subtitle": "short italicized line",\n'
        '  "introduction": "1-2 paragraphs framing why this topic matters "\n'
        '    "and how the chapters progress. ~140-200 words.",\n'
        '  "conclusion": "1-2 paragraphs synthesizing the chapters and "\n'
        '    "suggesting what to read next. ~120-180 words.",\n'
        '  "chapters": [\n'
        '    { "source_slug": "<from list>",\n'
        '      "chapter_title": "question or statement framing the idea",\n'
        '      "learning_objectives": ["3 bullets"],\n'
        '      "key_takeaways": ["3 bullets"],\n'
        '      "discussion_questions": ["2 open-ended questions"] }\n'
        "  ]\n"
        "}\n"
        f"Exactly {n} chapters, ONE per source article, in an order that "
        "reads as a curriculum (overview → deeper → synthesis). Chapter "
        "titles should not simply repeat the Wikipedia article title — "
        "reframe them around the concept the reader is learning. Objectives "
        "and takeaways should be concrete and specific to this topic."
        + angle_clause
    )
    user_angle = f"\n\nAngle: {angle}" if angle else ""
    user = f"Topic: {topic}{user_angle}\n\nSources:\n{menu}"

    try:
        raw = await ollama_chat(
            [{"role": "system", "content": system},
             {"role": "user", "content": user}],
            temperature=0.35, format_json=True, max_tokens=3000,
            timeout=120,
            provider=provider, model=model,
        )
    except OllamaError as e:
        log.info("compile: curriculum LLM failed (%d): %s", e.status, e.message)
        return None
    except Exception as e:
        log.info("compile: curriculum LLM errored: %s", e)
        return None

    raw = (raw or "").strip()
    if not raw:
        log.info("compile: curriculum LLM returned empty reply")
        return None
    try:
        data = json.loads(raw)
    except Exception:
        m = re.search(r"\{.*\}", raw, re.DOTALL)
        if not m:
            log.info("compile: curriculum raw not parseable: %s", raw[:400])
            return None
        try:
            data = json.loads(m.group(0))
        except Exception:
            log.info("compile: curriculum extracted JSON invalid: %s", m.group(0)[:400])
            return None

    if not isinstance(data, dict) or not isinstance(data.get("chapters"), list):
        return None

    # Index source articles so we can map chapter → article and also keep
    # order-of-output. Fall back to positional order if slugs mismatch.
    by_slug = {a.get("slug"): a for a in articles}
    used: set[str] = set()
    ordered: list[dict] = []
    for ch in data["chapters"]:
        if not isinstance(ch, dict):
            continue
        slug = ch.get("source_slug")
        src = by_slug.get(slug) if slug else None
        if not src:
            # find the next unused article as a positional fallback
            for a in articles:
                if a.get("slug") not in used:
                    src = a; break
        if not src or src.get("slug") in used:
            continue
        used.add(src.get("slug"))
        ordered.append({
            "source": src,
            "chapter_title": str(ch.get("chapter_title") or src.get("title") or ""),
            "learning_objectives": [str(x) for x in (ch.get("learning_objectives") or [])][:5],
            "key_takeaways": [str(x) for x in (ch.get("key_takeaways") or [])][:5],
            "discussion_questions": [str(x) for x in (ch.get("discussion_questions") or [])][:4],
        })

    # Any sources the LLM skipped still get appended as bare-chapter entries
    # so the chapter count honors the user's setting.
    for a in articles:
        if a.get("slug") in used:
            continue
        ordered.append({
            "source": a,
            "chapter_title": a.get("title") or "",
            "learning_objectives": [],
            "key_takeaways": [],
            "discussion_questions": [],
        })

    return {
        "book_title": str(data.get("book_title") or f"A Short Course in {topic}"),
        "subtitle": str(data.get("subtitle") or ""),
        "introduction": str(data.get("introduction") or ""),
        "conclusion": str(data.get("conclusion") or ""),
        "chapters": ordered,
    }


def _xhtml_esc(s: str) -> str:
    return (str(s or "")
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;"))


def _chapter_body_html(source: dict,
                       objectives: list[str],
                       takeaways: list[str],
                       questions: list[str]) -> str:
    """Wraps an article's content_html with curriculum shell: objectives
    up top, then the source material, then takeaways + questions."""
    raw = source.get("content_html") or f"<p>{_xhtml_esc(source.get('summary') or '')}</p>"
    raw = re.sub(r"<script[\s\S]*?</script>", "", raw, flags=re.I)
    raw = re.sub(r"\son[a-z]+=\"[^\"]*\"", "", raw, flags=re.I)

    def _ul(items: list[str]) -> str:
        if not items: return ""
        return "<ul>" + "".join(f"<li>{_xhtml_esc(x)}</li>" for x in items) + "</ul>"

    pieces = []
    if objectives:
        pieces.append(
            f'<section class="objectives"><h3>Learning Objectives</h3>{_ul(objectives)}</section>'
        )
    pieces.append(f'<section class="reading"><h3>Reading</h3>{raw}</section>')
    if takeaways:
        pieces.append(
            f'<section class="takeaways"><h3>Key Takeaways</h3>{_ul(takeaways)}</section>'
        )
    if questions:
        pieces.append(
            f'<section class="questions"><h3>Discussion Questions</h3>'
            f'<ol>{"".join(f"<li>{_xhtml_esc(q)}</li>" for q in questions)}</ol></section>'
        )
    return "\n".join(pieces)


def _epub_from_curriculum(topic: str, course: dict) -> bytes:
    """Like _epub_from_articles but emits a cohesive curriculum: titlepage,
    introduction, chapter-per-source with learning shell, conclusion."""
    book_title = _xhtml_esc(course.get("book_title") or f"A Short Course in {topic}")
    subtitle = _xhtml_esc(course.get("subtitle") or "")
    intro = course.get("introduction") or ""
    concl = course.get("conclusion") or ""
    chapters = course.get("chapters") or []

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
        "     margin: 0 0 0.2em 0; font-size: 1.9em; color: #1a1410; }\n"
        "h2 { font-family: 'Fraunces', Georgia, serif; font-size: 1.4em; "
        "     border-bottom: 1px solid rgba(26,20,16,0.2); padding-bottom: 0.15em; "
        "     margin-top: 1.6em; }\n"
        "h3 { font-family: 'Fraunces', Georgia, serif; font-size: 1.05em; "
        "     text-transform: uppercase; letter-spacing: 0.16em; color: #7a1f1f; "
        "     margin: 1.4em 0 0.4em; font-weight: 500; }\n"
        ".kicker { color: #7a1f1f; font-family: 'IBM Plex Mono', monospace; "
        "          letter-spacing: 0.18em; text-transform: uppercase; "
        "          font-size: 0.7em; margin-bottom: 0.6em; }\n"
        ".subtitle { font-style: italic; color: #3a2f27; margin-bottom: 1.4em; }\n"
        "ul, ol { margin: 0.4em 0 1em 1.3em; }\n"
        "li { margin: 0.3em 0; }\n"
        "p { margin: 0.7em 0; }\n"
        "a { color: #7a1f1f; text-decoration: none; }\n"
        "blockquote { border-left: 2px solid #7a1f1f; padding: 0.2em 0.9em; "
        "             font-style: italic; color: #3a2f27; margin: 1em 0; }\n"
        "img { max-width: 100%; height: auto; display: block; margin: 1em auto; }\n"
        ".objectives, .takeaways, .questions {\n"
        "  background: rgba(155,122,46,0.06); padding: 0.8em 1.1em; margin: 1em 0;\n"
        "  border-left: 2px solid rgba(155,122,46,0.5); }\n"
        ".reading h3 { border-bottom: 1px solid rgba(26,20,16,0.15); "
        "              padding-bottom: 0.3em; }\n"
    )

    manifest_items = [
        '<item id="style" href="style.css" media-type="text/css"/>',
        '<item id="nav" href="nav.xhtml" media-type="application/xhtml+xml" properties="nav"/>',
        '<item id="titlepage" href="titlepage.xhtml" media-type="application/xhtml+xml"/>',
        '<item id="intro" href="intro.xhtml" media-type="application/xhtml+xml"/>',
        '<item id="concl" href="conclusion.xhtml" media-type="application/xhtml+xml"/>',
    ]
    spine_items = [
        '<itemref idref="titlepage"/>',
        '<itemref idref="intro"/>',
    ]
    nav_items = [
        '<li><a href="intro.xhtml">Introduction</a></li>',
    ]
    files: list[tuple[str, str]] = []
    image_files: dict[str, bytes] = {}

    titlepage_xhtml = (
        '<?xml version="1.0" encoding="UTF-8"?>\n<!DOCTYPE html>\n'
        '<html xmlns="http://www.w3.org/1999/xhtml" lang="en">'
        f'<head><title>{book_title}</title>'
        '<link rel="stylesheet" href="style.css"/></head>'
        '<body style="text-align:center; margin-top: 28%;">'
        f'<p class="kicker">A CARTA Learning Book</p>'
        f'<h1 style="font-size:2.6em;">{book_title}</h1>'
        + (f'<p class="subtitle">{subtitle}</p>' if subtitle else '')
        + f'<p style="color:#6d5e47;">{len(chapters)} chapters · compiled from your codex</p>'
        '<p style="color:#7a1f1f; letter-spacing: 0.24em; font-size: 0.9em;">§</p>'
        '</body></html>\n'
    )
    files.append(("titlepage.xhtml", titlepage_xhtml))

    intro_xhtml = (
        '<?xml version="1.0" encoding="UTF-8"?>\n<!DOCTYPE html>\n'
        '<html xmlns="http://www.w3.org/1999/xhtml" lang="en">'
        '<head><title>Introduction</title>'
        '<link rel="stylesheet" href="style.css"/></head>'
        '<body><section>'
        '<p class="kicker">Introduction</p>'
        f'<h1>{book_title}</h1>'
        + "".join(f"<p>{_xhtml_esc(p)}</p>" for p in intro.split("\n\n") if p.strip())
        + '</section></body></html>\n'
    )
    files.append(("intro.xhtml", intro_xhtml))

    for i, ch in enumerate(chapters, start=1):
        src = ch["source"]
        cid = f"ch{i:03d}"
        fname = f"{cid}.xhtml"
        chapter_title = _xhtml_esc(ch.get("chapter_title") or src.get("title") or "")
        roman = _to_roman(i)
        chapter_xhtml = (
            '<?xml version="1.0" encoding="UTF-8"?>\n<!DOCTYPE html>\n'
            '<html xmlns="http://www.w3.org/1999/xhtml" lang="en">'
            f'<head><title>{chapter_title}</title>'
            '<link rel="stylesheet" href="style.css"/></head>'
            '<body><section>'
            f'<p class="kicker">Chapter {roman}</p>'
            f'<h1>{chapter_title}</h1>'
            + (f'<p class="subtitle">Based on: {_xhtml_esc(src.get("title") or "")}</p>'
               if src.get("title") else "")
            + _chapter_body_html(src,
                                 ch.get("learning_objectives") or [],
                                 ch.get("key_takeaways") or [],
                                 ch.get("discussion_questions") or [])
            + '</section></body></html>\n'
        )
        chapter_xhtml = _rewrite_and_collect_images(chapter_xhtml, image_files)
        files.append((fname, chapter_xhtml))
        manifest_items.append(
            f'<item id="{cid}" href="{fname}" media-type="application/xhtml+xml"/>'
        )
        spine_items.append(f'<itemref idref="{cid}"/>')
        nav_items.append(f'<li><a href="{fname}">Chapter {roman}: {chapter_title}</a></li>')

    concl_xhtml = (
        '<?xml version="1.0" encoding="UTF-8"?>\n<!DOCTYPE html>\n'
        '<html xmlns="http://www.w3.org/1999/xhtml" lang="en">'
        '<head><title>Conclusion</title>'
        '<link rel="stylesheet" href="style.css"/></head>'
        '<body><section>'
        '<p class="kicker">Conclusion</p>'
        '<h1>Where to go from here</h1>'
        + "".join(f"<p>{_xhtml_esc(p)}</p>" for p in concl.split("\n\n") if p.strip())
        + '</section></body></html>\n'
    )
    files.append(("conclusion.xhtml", concl_xhtml))
    spine_items.append('<itemref idref="concl"/>')
    nav_items.append('<li><a href="conclusion.xhtml">Conclusion</a></li>')

    nav_xhtml = (
        '<?xml version="1.0" encoding="UTF-8"?>\n<!DOCTYPE html>\n'
        '<html xmlns="http://www.w3.org/1999/xhtml" '
        'xmlns:epub="http://www.idpf.org/2007/ops" lang="en">'
        f'<head><title>{book_title} — Contents</title>'
        '<link rel="stylesheet" href="style.css"/></head>'
        '<body><nav epub:type="toc" id="toc"><h1>Contents</h1><ol>'
        + "".join(nav_items)
        + '</ol></nav></body></html>\n'
    )
    manifest_items.extend(_build_image_manifest_entries(image_files))
    content_opf = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<package xmlns="http://www.idpf.org/2007/opf" version="3.0" '
        'unique-identifier="book-id" xml:lang="en">\n'
        '  <metadata xmlns:dc="http://purl.org/dc/elements/1.1/">\n'
        f'    <dc:identifier id="book-id">{book_id}</dc:identifier>\n'
        f'    <dc:title>{book_title}</dc:title>\n'
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
        zf.writestr(_zipfile.ZipInfo("mimetype"),
                    "application/epub+zip", compress_type=_zipfile.ZIP_STORED)
        zf.writestr("META-INF/container.xml", container_xml)
        zf.writestr("OEBPS/content.opf", content_opf)
        zf.writestr("OEBPS/nav.xhtml", nav_xhtml)
        zf.writestr("OEBPS/style.css", stylesheet)
        for fname, content in files:
            zf.writestr(f"OEBPS/{fname}", content)
        for filename, data in image_files.items():
            if Path(filename).suffix.lower() in _EXT_MEDIA_TYPE:
                zf.writestr(f"OEBPS/images/{filename}", data)
    return buf.getvalue()


def _to_roman(n: int) -> str:
    numerals = [("M",1000),("CM",900),("D",500),("CD",400),
                ("C",100),("XC",90),("L",50),("XL",40),
                ("X",10),("IX",9),("V",5),("IV",4),("I",1)]
    out, v = "", n
    for sym, val in numerals:
        while v >= val:
            out += sym; v -= val
    return out


def _html_from_curriculum(topic: str, course: dict) -> str:
    """Print-ready single HTML page with the same curriculum structure."""
    book_title = _xhtml_esc(course.get("book_title") or f"A Short Course in {topic}")
    subtitle = _xhtml_esc(course.get("subtitle") or "")
    intro = course.get("introduction") or ""
    concl = course.get("conclusion") or ""
    chapters = course.get("chapters") or []

    css = """
    @page { size: letter; margin: 0.8in; }
    html, body { margin: 0; padding: 0; background: #f1e8d3;
                 font-family: "EB Garamond", Georgia, serif;
                 color: #1a1410; line-height: 1.62; }
    .hero { text-align: center; margin: 2.4em 0 3em; }
    .hero .kicker { font-family: "IBM Plex Mono", monospace;
                    letter-spacing: 0.28em; font-size: 11px;
                    color: #7a1f1f; text-transform: uppercase; }
    .hero h1 { font-family: "Fraunces", serif; font-weight: 400;
               font-size: 46px; margin: 14px 0 6px; }
    .hero .subtitle { font-style: italic; color: #3a2f27; }
    .toc { margin: 2em 0 3em; padding: 1em 0;
           border-top: 1px solid #c4a35a; border-bottom: 1px solid #c4a35a; }
    .toc h2 { font-family: "Fraunces", serif; font-weight: 400; font-size: 18px;
              letter-spacing: 0.24em; text-transform: uppercase;
              color: #7a1f1f; margin: 0 0 14px; }
    .toc ol { list-style: none; counter-reset: ch; padding: 0; }
    .toc li { counter-increment: ch; padding: 4px 0;
              display: flex; justify-content: space-between; gap: 10px; }
    .toc li::before { content: counter(ch, upper-roman) "."; color: #9b7a2e;
                      font-family: "Fraunces", serif; font-style: italic;
                      min-width: 40px; }
    .intro, .chapter, .conclusion { page-break-before: always; padding-top: 1em; }
    .intro .kicker, .chapter .kicker, .conclusion .kicker {
      font-family: "IBM Plex Mono", monospace; font-size: 10px;
      letter-spacing: 0.22em; color: #7a1f1f; text-transform: uppercase; }
    .chapter h1, .intro h1, .conclusion h1 {
      font-family: "Fraunces", serif; font-weight: 500;
      font-size: 32px; margin: 6px 0 4px; }
    .chapter .subtitle { font-style: italic; color: #3a2f27; margin-bottom: 1.4em; }
    .chapter h2, .intro h2 { font-family: "Fraunces", serif; font-size: 22px;
                  font-weight: 500; border-bottom: 1px solid rgba(26,20,16,0.18);
                  padding-bottom: 4px; margin-top: 1.8em; }
    .chapter h3 { font-family: "Fraunces", serif; font-size: 14px;
                  font-weight: 500; letter-spacing: 0.18em;
                  text-transform: uppercase; color: #7a1f1f;
                  margin: 1.4em 0 0.4em; }
    .chapter p { margin: 0.7em 0; }
    .chapter a { color: #7a1f1f; text-decoration: none; }
    .objectives, .takeaways, .questions {
      background: rgba(155,122,46,0.08); padding: 0.9em 1.2em; margin: 1em 0;
      border-left: 2px solid rgba(155,122,46,0.5); }
    img { max-width: 100%; height: auto; }
    .print-button { position: fixed; top: 16px; right: 16px;
                    padding: 10px 18px; background: #7a1f1f; color: #f1e8d3;
                    font-family: "IBM Plex Mono", monospace; font-size: 11px;
                    letter-spacing: 0.22em; text-transform: uppercase;
                    border: 0; cursor: pointer; z-index: 100; }
    @media print { .print-button { display: none; } }
    """
    toc = "".join(
        f'<li><span>{_xhtml_esc(ch.get("chapter_title") or "")}</span>'
        f'<span style="color:#6d5e47">{_xhtml_esc((ch.get("source") or {}).get("title") or "")}</span></li>'
        for ch in chapters
    )
    intro_html = "".join(f"<p>{_xhtml_esc(p)}</p>" for p in intro.split("\n\n") if p.strip())
    concl_html = "".join(f"<p>{_xhtml_esc(p)}</p>" for p in concl.split("\n\n") if p.strip())

    chap_html = ""
    for i, ch in enumerate(chapters, start=1):
        src = ch["source"]
        body = _chapter_body_html(src,
                                  ch.get("learning_objectives") or [],
                                  ch.get("key_takeaways") or [],
                                  ch.get("discussion_questions") or [])
        # Inline /static/images/<file> src as base64 data URIs so the
        # printed PDF (or saved-then-opened HTML) keeps its figures
        # instead of showing broken-image glyphs.
        body = _inline_images_as_data_uris(body)
        chap_html += f"""
        <section class="chapter">
          <div class="kicker">Chapter {_to_roman(i)}</div>
          <h1>{_xhtml_esc(ch.get("chapter_title") or "")}</h1>
          {f'<div class="subtitle">Based on: {_xhtml_esc(src.get("title") or "")}</div>' if src.get("title") else ""}
          {body}
        </section>
        """

    return f"""<!doctype html><html><head><meta charset="utf-8">
    <title>{book_title}</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link href="https://fonts.googleapis.com/css2?family=Fraunces:ital,wght@0,400..700;1,400..700&family=EB+Garamond:ital,wght@0,400..700;1,400..700&family=IBM+Plex+Mono:wght@400;500&display=swap" rel="stylesheet">
    <style>{css}</style></head><body>
    <button class="print-button" onclick="window.print()">Save as PDF</button>
    <div class="hero">
      <div class="kicker">A CARTA Learning Book</div>
      <h1>{book_title}</h1>
      {f'<div class="subtitle">{subtitle}</div>' if subtitle else ''}
      <div style="color:#6d5e47;font-style:italic;margin-top:6px">
        {len(chapters)} chapters · compiled from your codex
      </div>
    </div>
    <section class="toc"><h2>Contents</h2><ol>{toc}</ol></section>
    <section class="intro">
      <div class="kicker">Introduction</div>
      <h1>{book_title}</h1>
      {intro_html}
    </section>
    {chap_html}
    <section class="conclusion">
      <div class="kicker">Conclusion</div>
      <h1>Where to go from here</h1>
      {concl_html}
    </section>
    </body></html>"""


@app.post("/api/compile")
async def api_compile(req: CompileReq):
    topic = (req.topic or "").strip()
    if not topic:
        raise HTTPException(400, "topic is required")
    target = max(3, min(int(req.target_articles or 8), 12))  # LLM budget caps this

    # Strict relevance ranker — weighted, stopword-filtered, requires ≥2.0 score.
    with get_db() as db:
        rows = db.execute("""
            SELECT a.id, a.slug, a.title, a.deck, a.summary, a.content_html,
                   a.wiki_title, a.captured_at,
                   p.name AS province_name, p.slug AS province_slug
              FROM articles a LEFT JOIN provinces p ON p.id = a.province_id
        """).fetchall()
    corpus = [dict(r) for r in rows]
    # If the user gave an angle, treat its words as secondary needles —
    # that way "illuminati historical origins" ranks origin-flavored
    # captures higher than ones that just mention the secret society.
    ranking_key = f"{topic} {req.angle}".strip() if req.angle else topic
    scored = _compile_rank(ranking_key, corpus)
    relevant = [a for _s, a in scored][:target]

    # Drop existing-corpus entries whose title parenthetical matches an
    # excluded kind. Defensive — mostly catches old captures the user
    # made before introducing excludes.
    if req.excludes:
        normalized_ex = [e.strip().lower() for e in req.excludes if e.strip()]
        filtered: list[dict] = []
        for a in relevant:
            if _excludes_hit((a.get("deck") or a.get("summary") or ""),
                             (a.get("title") or ""), normalized_ex):
                log.info("compile: existing '%s' dropped by exclude filter", a.get("title"))
                continue
            filtered.append(a)
        relevant = filtered

    # Auto-expand — parallel captures up to `target`-fill. We TRUST the
    # Wikipedia search ranker for newly captured articles: if we re-applied
    # the strict keyword filter here, auto-captures for fuzzy topics would
    # get silently dropped and the user would end up with fewer chapters
    # than the slider said. The existing (strict-scored) corpus stays at
    # the top; Wikipedia finds fill the rest in search-relevance order.
    added: list[str] = []
    if req.auto_expand and len(relevant) < target:
        needed = target - len(relevant)
        existing_slugs = {a["slug"] for a in corpus}
        existing_titles_lower = {(a.get("title") or "").lower() for a in corpus}
        added = await _parallel_auto_expand(
            topic, existing_slugs, existing_titles_lower,
            needed=needed, max_concurrent=10,
            excludes=req.excludes,
        )
        if added:
            with get_db() as db:
                placeholders = ",".join("?" * len(added))
                new_rows = db.execute(f"""
                    SELECT a.id, a.slug, a.title, a.deck, a.summary, a.content_html,
                           a.wiki_title, a.captured_at,
                           p.name AS province_name, p.slug AS province_slug
                      FROM articles a LEFT JOIN provinces p ON p.id = a.province_id
                     WHERE a.slug IN ({placeholders})
                """, added).fetchall()
            # Preserve Wikipedia's relevance order (matches `added` list order,
            # which itself follows `picks` from _parallel_auto_expand).
            by_slug = {r["slug"]: dict(r) for r in new_rows}
            ordered_new = [by_slug[s] for s in added if s in by_slug]
            # Append to the strict-filtered list, dedupe by slug, cap at target.
            existing_slug_set = {a["slug"] for a in relevant}
            for na in ordered_new:
                if na["slug"] not in existing_slug_set:
                    relevant.append(na)
                    existing_slug_set.add(na["slug"])
            relevant = relevant[:target]

    if not relevant:
        raise HTTPException(
            409,
            f"Nothing in the codex matches '{topic}' and auto-expand couldn't "
            "find enough Wikipedia articles either. Try a different topic.",
        )

    # One LLM call → curriculum structure (book title, intro, conclusion,
    # per-chapter objectives/takeaways/questions). Fallback to plain bookshelf
    # mode if the LLM is unavailable or returns unparseable JSON.
    course = await _generate_curriculum(
        topic, relevant,
        provider=req.provider, model=req.model,
        angle=req.angle,
    )
    fallback = course is None
    if fallback:
        # Minimal shell so the EPUB/HTML paths can still render.
        course = {
            "book_title": f"A Short Course in {topic}",
            "subtitle": "compiled from your codex",
            "introduction": (
                f"This book gathers {len(relevant)} articles relevant to '{topic}' "
                "into a single reading. The curriculum layer couldn't be generated — "
                "treat each chapter as a standalone reading rather than a graded lesson."
            ),
            "conclusion": (
                "To go deeper, open each source article from the Atlas and let "
                "CARTA suggest related captures from the cross-references at the bottom."
            ),
            "chapters": [
                {"source": a,
                 "chapter_title": a.get("title") or "",
                 "learning_objectives": [],
                 "key_takeaways": [],
                 "discussion_questions": []}
                for a in relevant
            ],
        }

    filename_slug = slugify(topic) or "learning"
    compiled_count = len(course["chapters"])

    if (req.format or "epub").lower() == "html":
        html = _html_from_curriculum(topic, course)
        return Response(
            content=html,
            media_type="text/html; charset=utf-8",
            headers={
                "Content-Disposition": f'inline; filename="{filename_slug}-course.html"',
                "X-Compiled-Count": str(compiled_count),
                "X-Auto-Added": str(len(added)),
                "X-Curriculum": "plain" if fallback else "llm",
            },
        )

    data = _epub_from_curriculum(topic, course)
    return Response(
        content=data,
        media_type="application/epub+zip",
        headers={
            "Content-Disposition": f'attachment; filename="{filename_slug}-course.epub"',
            "X-Compiled-Count": str(compiled_count),
            "X-Auto-Added": str(len(added)),
            "X-Curriculum": "plain" if fallback else "llm",
        },
    )


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


# ---------- /api/quote — daily positive quote for the hero section ----------
# Rotates deterministically on day-of-year so every client gets the same quote
# on the same day without needing a server-side scheduler. Curated to be
# literary, non-saccharine, and CARTA-flavored (knowledge + curiosity leaning).
DAILY_QUOTES: list[tuple[str, str]] = [
    ("The larger the island of knowledge, the longer the shoreline of wonder.", "Ralph W. Sockman"),
    ("I have no special talent. I am only passionately curious.", "Albert Einstein"),
    ("The real voyage of discovery consists not in seeking new landscapes, but in having new eyes.", "Marcel Proust"),
    ("We are what we repeatedly do. Excellence, then, is not an act, but a habit.", "Aristotle"),
    ("The more I read, the more I acquire, the more certain I am that I know nothing.", "Voltaire"),
    ("An investment in knowledge pays the best interest.", "Benjamin Franklin"),
    ("Learn from yesterday, live for today, hope for tomorrow.", "Albert Einstein"),
    ("The function of education is to teach one to think intensively and to think critically.", "Martin Luther King Jr."),
    ("Anyone who stops learning is old, whether at twenty or eighty.", "Henry Ford"),
    ("The beautiful thing about learning is that no one can take it away from you.", "B. B. King"),
    ("Curiosity is the wick in the candle of learning.", "William Arthur Ward"),
    ("The more that you read, the more things you will know.", "Dr. Seuss"),
    ("Tell me and I forget. Teach me and I remember. Involve me and I learn.", "Benjamin Franklin"),
    ("Knowledge is a treasure, but practice is the key to it.", "Lao Tzu"),
    ("Wisdom begins in wonder.", "Socrates"),
    ("Education is not the filling of a pail, but the lighting of a fire.", "W. B. Yeats"),
    ("The only true wisdom is in knowing you know nothing.", "Socrates"),
    ("Live as if you were to die tomorrow. Learn as if you were to live forever.", "Mahatma Gandhi"),
    ("The art and science of asking questions is the source of all knowledge.", "Thomas Berger"),
    ("Change is the end result of all true learning.", "Leo Buscaglia"),
    ("Study the past if you would define the future.", "Confucius"),
    ("The expert in anything was once a beginner.", "Helen Hayes"),
    ("A book is a dream that you hold in your hand.", "Neil Gaiman"),
    ("Reading is to the mind what exercise is to the body.", "Joseph Addison"),
    ("The journey of a thousand miles begins with a single step.", "Lao Tzu"),
    ("The only person you are destined to become is the person you decide to be.", "Ralph Waldo Emerson"),
    ("In the middle of difficulty lies opportunity.", "Albert Einstein"),
    ("It is during our darkest moments that we must focus to see the light.", "Aristotle"),
    ("The mind is not a vessel to be filled, but a fire to be kindled.", "Plutarch"),
    ("Nothing in life is to be feared, it is only to be understood.", "Marie Curie"),
    ("Think before you speak. Read before you think.", "Fran Lebowitz"),
    ("A room without books is like a body without a soul.", "Cicero"),
    ("The beginning is the most important part of the work.", "Plato"),
    ("To know, is to know that you know nothing. That is the meaning of true knowledge.", "Socrates"),
    ("The only limit to our realization of tomorrow is our doubts of today.", "Franklin D. Roosevelt"),
    ("A person who never made a mistake never tried anything new.", "Albert Einstein"),
    ("Patience is bitter, but its fruit is sweet.", "Aristotle"),
    ("Knowing yourself is the beginning of all wisdom.", "Aristotle"),
    ("Do not go where the path may lead, go instead where there is no path.", "Ralph Waldo Emerson"),
    ("The unexamined life is not worth living.", "Socrates"),
    ("There is no friend as loyal as a book.", "Ernest Hemingway"),
    ("One cannot think well, love well, sleep well, if one has not dined well.", "Virginia Woolf"),
    ("I cannot live without books.", "Thomas Jefferson"),
    ("The secret of getting ahead is getting started.", "Mark Twain"),
    ("Whatever you are, be a good one.", "Abraham Lincoln"),
    ("The only way to do great work is to love what you do.", "Steve Jobs"),
    ("We cannot solve our problems with the same thinking we used when we created them.", "Albert Einstein"),
    ("What we think, we become.", "Buddha"),
    ("The future belongs to those who believe in the beauty of their dreams.", "Eleanor Roosevelt"),
    ("Reading furnishes the mind only with materials of knowledge; it is thinking that makes what we read ours.", "John Locke"),
    ("Without music, life would be a mistake.", "Friedrich Nietzsche"),
    ("What is a teacher? I'll tell you: it isn't someone who teaches something, but someone who inspires the student.", "Paulo Coelho"),
    ("All truths are easy to understand once they are discovered; the point is to discover them.", "Galileo Galilei"),
    ("Doubt is the origin of wisdom.", "René Descartes"),
    ("Well begun is half done.", "Aristotle"),
    ("Knowledge has to be improved, challenged, and increased constantly, or it vanishes.", "Peter Drucker"),
    ("History is the version of past events that people have decided to agree upon.", "Napoleon Bonaparte"),
    ("The measure of intelligence is the ability to change.", "Albert Einstein"),
    ("Everything you can imagine is real.", "Pablo Picasso"),
    ("The roots of education are bitter, but the fruit is sweet.", "Aristotle"),
    ("What you seek is seeking you.", "Rumi"),
    ("Every artist was first an amateur.", "Ralph Waldo Emerson"),
    ("The pen is mightier than the sword.", "Edward Bulwer-Lytton"),
    ("Literature is the most agreeable way of ignoring life.", "Fernando Pessoa"),
    ("I am still learning.", "Michelangelo"),
]


@app.get("/api/quote")
def api_daily_quote():
    """Return today's deterministic quote from the curated list.
    Rotates on day-of-year so every client gets the same quote on the
    same day; wraps around every ~365 days."""
    day = time.gmtime().tm_yday
    q, a = DAILY_QUOTES[day % len(DAILY_QUOTES)]
    return {"text": q, "author": a, "day_of_year": day}


# ---------- /api/articles ----------
@app.get("/api/articles")
def api_articles():
    """Lightweight list of every captured article for picker UIs
    (quiz, compile, move-to). Returns slug + title + province only —
    no content_html, no summary — so the payload stays small even for
    codexes with thousands of entries."""
    with get_db() as db:
        rows = db.execute("""
            SELECT a.slug, a.title, a.deck, a.captured_at,
                   p.name AS province_name
              FROM articles a LEFT JOIN provinces p ON p.id = a.province_id
             ORDER BY a.title COLLATE NOCASE ASC
        """).fetchall()
    return {"articles": [dict(r) for r in rows], "total": len(rows)}


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


def _videos_cache_sig() -> str:
    """Signature for the current set of active video sources. When the set
    changes (e.g. YOUTUBE_API_KEY is set/unset, or we add a new source),
    the signature flips and previously-cached payloads are treated as
    stale so YouTube/LoC results appear without a 7-day wait."""
    sources = ["ia", "commons", "loc"]          # always on
    if YOUTUBE_API_KEY:
        sources.append("yt")
    return "v3:" + "+".join(sorted(sources))


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
    """Returns up to 6 YouTube results for `title`. No-ops if
    YOUTUBE_API_KEY isn't set (users without the key keep IA+Commons+LoC).
    Restricted to embeddable videos so the iframe player always works."""
    if not YOUTUBE_API_KEY:
        return []
    params = {
        "part": "snippet",
        "maxResults": 6,
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
    # YouTube gets two slots per round (appears twice in the pool list) so
    # it contributes more generously — users typically want a richer YT
    # presence than Commons/LoC which are thin. Overall cap is 10 per
    # article; fewer if some sources returned empty.
    yt_pool = list(_ok(yt))
    ia_pool = list(_ok(ia))
    co_pool = list(_ok(commons))
    lo_pool = list(_ok(loc))
    pools = [ia_pool, yt_pool, co_pool, yt_pool, lo_pool]
    merged: list[dict] = []
    seen_ids: set[tuple[str, str]] = set()
    CAP = 10
    while any(pools) and len(merged) < CAP:
        for pool in pools:
            if pool and len(merged) < CAP:
                item = pool.pop(0)
                key = (item.get("source", ""), item.get("id", ""))
                if key in seen_ids:
                    continue
                seen_ids.add(key)
                merged.append(item)
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
                        parsed = json.loads(cached["payload"])
                        # New payload shape: {"sig": "...", "videos": [...]}.
                        # Old bare-list payloads are treated as stale so the
                        # YouTube/LoC expansion can backfill without waiting
                        # 7 days per article.
                        if (isinstance(parsed, dict)
                                and parsed.get("sig") == _videos_cache_sig()):
                            return {"slug": slug, "videos": parsed.get("videos", [])}
                except (TypeError, ValueError):
                    pass

    videos = await _fetch_article_videos(dict(row))
    payload = json.dumps({"sig": _videos_cache_sig(), "videos": videos})
    with get_db() as db:
        db.execute("""
            INSERT INTO article_videos (article_id, payload, fetched_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(article_id) DO UPDATE SET
              payload = excluded.payload, fetched_at = CURRENT_TIMESTAMP
        """, (aid, payload))
        db.commit()

    return {"slug": slug, "videos": videos}


# ---------- /api/watch/warm ----------
# One-shot backfill that re-fetches every article whose cached payload is
# either missing, stale (old sig), or empty. Useful right after adding a
# new source (YouTube, LoC, whatever's next) so the aggregated Watch feed
# has fresh material across the whole corpus without waiting for every
# article to be opened manually.
@app.post("/api/watch/warm")
async def api_watch_warm(max: int = 40):
    target_sig = _videos_cache_sig()
    with get_db() as db:
        article_rows = db.execute(
            "SELECT id, slug, title FROM articles ORDER BY captured_at DESC"
        ).fetchall()
        stale: list[dict] = []
        for r in article_rows:
            cached = db.execute(
                "SELECT payload FROM article_videos WHERE article_id = ?",
                (r["id"],),
            ).fetchone()
            needs = True
            if cached:
                try:
                    parsed = json.loads(cached["payload"])
                    if (isinstance(parsed, dict)
                            and parsed.get("sig") == target_sig):
                        needs = False
                except Exception:
                    pass
            if needs:
                stale.append(dict(r))
            if len(stale) >= max:
                break

    refreshed = 0
    for r in stale:
        videos = await _fetch_article_videos(r)
        payload = json.dumps({"sig": target_sig, "videos": videos})
        with get_db() as db:
            db.execute("""
                INSERT INTO article_videos (article_id, payload, fetched_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(article_id) DO UPDATE SET
                  payload = excluded.payload, fetched_at = CURRENT_TIMESTAMP
            """, (r["id"], payload))
            db.commit()
        refreshed += 1

    return {
        "target_sig": target_sig,
        "total_articles": len(article_rows),
        "stale_found": len(stale),
        "refreshed": refreshed,
    }


# ---------- /api/watch ----------
# Aggregated video feed across every captured article. Pure read from the
# article_videos cache — no upstream fetches here so the page loads in ~100ms
# even with a corpus of hundreds.

SOURCE_META = {
    "internet_archive": ("Internet Archive", "curated archives"),
    "youtube":          ("YouTube",          "educational + lectures"),
    "wikimedia_commons":("Wikimedia Commons","free-licensed clips"),
    "library_of_congress": ("Library of Congress", "digitized film catalog"),
}


def _load_all_cached_videos() -> list[dict]:
    """Flatten every cached payload into a list of video dicts annotated with
    article_slug / article_title / province_name / captured_at. Stale-sig
    payloads and bare-list payloads both yield their videos — we'd rather
    show something than nothing on the /watch aggregate view."""
    with get_db() as db:
        rows = db.execute("""
            SELECT av.payload, av.fetched_at,
                   a.slug AS article_slug, a.title AS article_title,
                   a.captured_at, p.name AS province_name, p.slug AS province_slug
              FROM article_videos av
              JOIN articles a ON a.id = av.article_id
              LEFT JOIN provinces p ON p.id = a.province_id
        """).fetchall()

    out: list[dict] = []
    for r in rows:
        try:
            parsed = json.loads(r["payload"])
        except Exception:
            continue
        if isinstance(parsed, dict):
            videos = parsed.get("videos") or []
        elif isinstance(parsed, list):
            videos = parsed
        else:
            continue
        for v in videos:
            if not isinstance(v, dict) or not v.get("embed_url"):
                continue
            vv = dict(v)
            vv["article_slug"] = r["article_slug"]
            vv["article_title"] = r["article_title"]
            vv["province_name"] = r["province_name"]
            vv["province_slug"] = r["province_slug"]
            vv["captured_at"] = r["captured_at"]
            out.append(vv)
    return out


def _pick_featured(videos: list[dict]) -> Optional[dict]:
    """Pick a hero video: prefer Internet Archive (usually high-signal
    archival), then LoC, then Commons, then YouTube — scanned from the
    most-recently-captured article downward so the hero rotates as Juan
    captures things."""
    by_article: list[dict] = sorted(
        videos, key=lambda v: v.get("captured_at") or "", reverse=True
    )
    priority = {"internet_archive": 0, "library_of_congress": 1,
                "wikimedia_commons": 2, "youtube": 3}
    for v in by_article:
        if v.get("source") in priority:
            return v
    return by_article[0] if by_article else None


@app.get("/api/watch")
def api_watch():
    """Aggregated Watch feed: featured hero + sections grouped by recency,
    source, and province. Used by the dedicated Watch tab on iOS/web."""
    videos = _load_all_cached_videos()
    if not videos:
        return {"featured": None, "sections": []}

    # ---- Featured ---- (most-recent-capture's best source)
    featured = _pick_featured(videos)

    # ---- Section: Newly added ---- (videos from 8 most-recently-captured articles)
    by_recent_article: dict[str, list[dict]] = {}
    order: list[str] = []
    for v in sorted(videos, key=lambda v: v.get("captured_at") or "", reverse=True):
        slug = v.get("article_slug") or ""
        if slug not in by_recent_article:
            if len(order) >= 8:
                continue
            order.append(slug)
            by_recent_article[slug] = []
        by_recent_article[slug].append(v)
    newly_added: list[dict] = []
    # One per article first (diversity), then fill with seconds if short.
    for slug in order:
        newly_added.append(by_recent_article[slug][0])
    for slug in order:
        for v in by_recent_article[slug][1:]:
            if len(newly_added) >= 12:
                break
            newly_added.append(v)
        if len(newly_added) >= 12:
            break

    # ---- Source sections ----
    by_source: dict[str, list[dict]] = {}
    for v in videos:
        by_source.setdefault(v.get("source") or "", []).append(v)
    source_sections = []
    for src_key in ("internet_archive", "youtube",
                    "wikimedia_commons", "library_of_congress"):
        items = by_source.get(src_key) or []
        if not items:
            continue
        title, subtitle = SOURCE_META.get(src_key, (src_key, ""))
        source_sections.append({
            "kind": "source",
            "source": src_key,
            "title": title,
            "subtitle": subtitle,
            "videos": items[:16],
        })

    # ---- Province sections ---- (one rail per province that has >= 2 videos)
    by_province: dict[str, dict] = {}
    for v in videos:
        name = v.get("province_name")
        slug = v.get("province_slug")
        if not name:
            continue
        key = slug or name
        bucket = by_province.setdefault(key, {"name": name, "slug": slug, "videos": []})
        bucket["videos"].append(v)
    province_sections = []
    for bucket in sorted(by_province.values(),
                         key=lambda b: len(b["videos"]), reverse=True):
        if len(bucket["videos"]) < 2:
            continue
        province_sections.append({
            "kind": "province",
            "title": bucket["name"],
            "subtitle": f"{len(bucket['videos'])} clips",
            "province_slug": bucket["slug"],
            "videos": bucket["videos"][:12],
        })

    sections = []
    if newly_added:
        sections.append({
            "kind": "recent",
            "title": "Newly Added",
            "subtitle": "from your latest captures",
            "videos": newly_added,
        })
    sections.extend(source_sections)
    sections.extend(province_sections)

    return {"featured": featured, "sections": sections}


# ---------- province classification ----------
def classify_by_keyword(*texts: str) -> Optional[str]:
    """Legacy first-match classifier, kept for callers that don't have
    body text to pass. New code should call `classify_by_content`."""
    blob = " ".join(t for t in texts if t).lower()
    if not blob:
        return None
    for name, keywords in PROVINCE_KEYWORDS:
        for kw in keywords:
            if kw in blob:
                return name
    return None


def classify_by_content(
    title: str,
    summary: str,
    body_text: str = "",
    topic_context: str = "",
) -> Optional[tuple[str, float]]:
    """Weighted-scoring classifier. Each province's keywords contribute:
      - title match:          5 points (article is *about* this keyword)
      - summary match:        3 points (Wikipedia's own framing)
      - topic-context match:  2 points (compile's driving topic — e.g.
                              "telekinesis" should push Parapsychology
                              into the parapsychology-adjacent province)
      - body occurrence:      0.5 each, capped at 3 per keyword
                              (so a List of Characters that happens to
                              say "telekinesis" 50 times across 200
                              characters doesn't dominate).

    Unlike the old first-match classifier, this gives every province a
    real score and returns the winner *plus* its score so the caller can
    decide whether the signal is strong enough to commit. Weak signals
    (< 3.0) return None and let the LLM classifier weigh in."""
    title_low = (title or "").lower()
    summary_low = (summary or "").lower()
    context_low = (topic_context or "").lower()
    # Cap body length to keep classification cheap on long articles.
    body_low = (body_text or "").lower()[:18000]

    scores: dict[str, float] = {}
    for name, keywords in PROVINCE_KEYWORDS:
        score = 0.0
        for kw in keywords:
            if kw in title_low:     score += 5.0
            if kw in summary_low:   score += 3.0
            if kw in context_low:   score += 2.0
            hits = body_low.count(kw)
            if hits:
                score += min(hits * 0.5, 3.0)
        scores[name] = score

    if not scores:
        return None
    best = max(scores.items(), key=lambda x: x[1])
    if best[1] < 3.0:
        return None
    return best


async def classify_by_llm(
    title: str,
    summary: str,
    body_text: str = "",
    topic_context: str = "",
) -> Optional[str]:
    """Ask Ollama to pick one of PROVINCE_NAMES with whatever context we
    have. Returns None on any error. Kept permissive for the UI's
    standalone captures (no body, no context) while giving compile a
    richer payload to reason over."""
    options = ", ".join(PROVINCE_NAMES)
    system = (
        "Classify a Wikipedia topic into EXACTLY ONE of these provinces, "
        "based on what the article is primarily about. Ignore tangential "
        "mentions — focus on the subject's core domain.\n"
        "Respond with ONLY the province name, no punctuation, no prose.\n"
        f"Provinces: {options}"
    )
    parts = [f"Title: {title}"]
    if topic_context:
        parts.append(f"Context: this article was captured as part of a study on '{topic_context}'.")
    if summary:
        parts.append(f"Summary: {summary[:600]}")
    if body_text:
        # First ~1.2k chars of the body is usually the lead + first
        # section: enough to reveal the article's actual domain.
        parts.append(f"Opening: {body_text[:1200]}")
    user = "\n".join(parts)
    try:
        raw = await ollama_chat(
            [{"role": "system", "content": system}, {"role": "user", "content": user}],
            temperature=0.0, max_tokens=20,
        )
    except Exception as e:
        log.info("classify_by_llm unavailable: %s", e)
        return None
    pick = (raw or "").strip().strip('"').strip("'").splitlines()[0].strip()
    for name in PROVINCE_NAMES:
        if pick.lower() == name.lower():
            return name
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
    body_text: str = "",
    topic_context: str = "",
) -> Optional[int]:
    """Return the province_id for a capture.

    Order of operations:
      1. Honor an explicit `requested` province when it matches a
         canonical one (user knows best).
      2. Weighted content-score classifier using title + summary +
         body + optional compile-topic context. If the best province
         has a confident score (≥3.0), commit.
      3. Ask the LLM with the same rich context. Useful when keyword
         heuristics are silent on novel domains (e.g. "Parapsychology"
         doesn't trip any keyword in Natural Sciences, but the LLM
         will happily put it there or in Philosophy).
      4. Last resort: Miscellaneous — never Curiosities, which is
         reserved for explicit "strange" picks."""
    if requested:
        row = db.execute(
            "SELECT id FROM provinces WHERE slug = ? OR lower(name) = lower(?)",
            (slugify(requested), requested),
        ).fetchone()
        if row:
            return row["id"]

    scored = classify_by_content(title, summary, body_text, topic_context)
    pick = scored[0] if scored else None
    if pick:
        log.info("classify: '%s' -> %s (score=%.1f, context='%s')",
                 title, pick, scored[1], topic_context or "—")

    if not pick:
        pick = await classify_by_llm(title, summary, body_text, topic_context)
        if pick:
            log.info("classify: '%s' -> %s (llm, context='%s')",
                     title, pick, topic_context or "—")

    if not pick:
        pick = "Miscellaneous"
        log.info("classify: '%s' -> Miscellaneous (no signal)", title)

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
    # Optional context that helps classify the article. Compile passes
    # its own topic here so auto-captured chapters land in the province
    # most relevant to the *study*, not whatever the article in
    # isolation would suggest.
    topic_context: Optional[str] = None


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
    # otherwise we run the weighted content classifier (title + summary
    # + first ~3k words of the body + optional compile topic) then the
    # LLM, then Miscellaneous.
    with get_db() as db:
        province_id = await resolve_province(
            db,
            req.province,
            resolved_title,
            summary.get("extract", ""),
            body_text=text,
            topic_context=(req.topic_context or "").strip(),
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


# ---------- /api/tts ----------
# Microsoft Edge's Natural voices (Aria, Jenny, Guy, Ryan, Sonia, Libby
# and family) via the `edge-tts` library. Free, unauthenticated, genuinely
# pleasant. Gives the browser a consistent high-quality voice that works
# on any device, not just Apple hardware with the right download.
#
# Endpoint returns MP3 directly so <audio src="..."> can stream-play it.
# A curated subset of voices is exposed by /api/tts/voices; the full
# catalogue is ~400 voices across 100+ locales, which is overkill for a
# personal wiki. Curation biased toward English neural voices that sound
# good for long-form reading.

# A curated list of Edge neural voices that read long-form text well.
# Name → (voice_id, accent, character). Kept short so the picker doesn't
# drown the user; the full catalogue (hundreds of voices) is still
# available by passing an exact voice id.
EDGE_TTS_VOICES: list[dict] = [
    {"id": "en-US-AriaNeural",      "name": "Aria",      "accent": "American", "hint": "warm, articulate"},
    {"id": "en-US-JennyNeural",     "name": "Jenny",     "accent": "American", "hint": "natural narrator"},
    {"id": "en-US-GuyNeural",       "name": "Guy",       "accent": "American", "hint": "measured baritone"},
    {"id": "en-US-DavisNeural",     "name": "Davis",     "accent": "American", "hint": "calm, thoughtful"},
    {"id": "en-US-AmberNeural",     "name": "Amber",     "accent": "American", "hint": "clear and bright"},
    {"id": "en-US-TonyNeural",      "name": "Tony",      "accent": "American", "hint": "confident"},
    {"id": "en-GB-SoniaNeural",     "name": "Sonia",     "accent": "British",  "hint": "gentle RP"},
    {"id": "en-GB-RyanNeural",      "name": "Ryan",      "accent": "British",  "hint": "crisp RP"},
    {"id": "en-GB-LibbyNeural",     "name": "Libby",     "accent": "British",  "hint": "warm London"},
    {"id": "en-IE-EmilyNeural",     "name": "Emily",     "accent": "Irish",    "hint": "lilting"},
    {"id": "en-AU-NatashaNeural",   "name": "Natasha",   "accent": "Australian", "hint": "conversational"},
    {"id": "en-AU-WilliamNeural",   "name": "William",   "accent": "Australian", "hint": "measured"},
    {"id": "en-CA-ClaraNeural",     "name": "Clara",     "accent": "Canadian", "hint": "clean narrator"},
    {"id": "en-IN-NeerjaNeural",    "name": "Neerja",    "accent": "Indian",   "hint": "articulate, neutral"},
]


@app.get("/api/tts/voices")
def api_tts_voices():
    """List the curated CARTA Premium voices the client can pick from."""
    return {"voices": EDGE_TTS_VOICES, "default": "en-US-AriaNeural"}


class TTSReq(BaseModel):
    text: str
    voice: Optional[str] = None
    rate: Optional[str] = None      # e.g. "-10%" or "+15%"
    pitch: Optional[str] = None     # e.g. "-5Hz" or "+2Hz"


@app.post("/api/tts")
async def api_tts(req: TTSReq):
    """Synthesize `text` as MP3 via Microsoft Edge's Natural voice engine.
    Streams the audio chunks back as the synthesizer produces them so
    `<audio>` can start playing before the full file is ready."""
    try:
        import edge_tts  # local import — dep is optional at runtime
    except ImportError:
        raise HTTPException(
            503,
            "The 'edge-tts' package isn't installed on the backend. "
            "Add it to requirements and restart."
        )

    text = (req.text or "").strip()
    if not text:
        raise HTTPException(400, "text is required")
    if len(text) > 20000:
        # Guard: Edge's service has per-request caps; also keep Pi memory sane.
        text = text[:20000]

    voice = req.voice or "en-US-AriaNeural"
    # Validate voice against the curated list OR accept the raw ID if it
    # looks like a valid locale-VoiceName-Neural shape.
    known_ids = {v["id"] for v in EDGE_TTS_VOICES}
    if voice not in known_ids and not re.match(r"^[a-z]{2}-[A-Z]{2}-\w+Neural$", voice):
        raise HTTPException(400, f"Unknown voice: {voice}")

    communicate = edge_tts.Communicate(
        text=text,
        voice=voice,
        rate=req.rate or "+0%",
        pitch=req.pitch or "+0Hz",
    )

    async def audio_stream():
        try:
            async for chunk in communicate.stream():
                if chunk.get("type") == "audio":
                    yield chunk["data"]
        except Exception as e:
            log.info("tts: edge-tts stream failed: %s", e)

    return StreamingResponse(
        audio_stream(),
        media_type="audio/mpeg",
        headers={
            "Cache-Control": "public, max-age=3600",
            "X-Voice": voice,
        },
    )


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
            messages, temperature=0.75, max_tokens=600,
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

    if not reply.strip():
        # Both the requested model AND the fallback returned empty. Don't
        # show an empty bubble — explain what's going on so the user knows
        # to pick a different model.
        log.warning("ollama chat returned empty reply (model=%s, provider=%s)",
                    req.model, req.provider)
        return {
            "reply": "*The model returned an empty reply. Try a different model in Settings — some cloud SKUs go silent on free-tier quotas.*",
            "error": True,
        }

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

    data = _coerce_llm_json(raw)
    if data is None:
        # One retry with a tougher prompt — LLMs sometimes recover when
        # told their previous output was malformed and to emit only the
        # JSON with no prose or markdown fences.
        log.info("quiz: first pass unparseable (len=%d), retrying", len(raw or ""))
        try:
            raw2 = await ollama_chat(
                [
                    {"role": "system", "content": system
                        + "\n\nCRITICAL: respond with ONLY the JSON object. "
                        + "No prose, no markdown code fences, no trailing comma. "
                        + "Escape every double quote inside a string value with a backslash."},
                    {"role": "user", "content": user},
                ],
                temperature=0.2, format_json=True,
                max_tokens=min(1800, 200 + 280 * count),
                timeout=600,
                provider=req.provider, model=req.model,
            )
        except Exception as e:
            raise HTTPException(502, f"Ollama unavailable: {e}")
        data = _coerce_llm_json(raw2)
        if data is None:
            log.warning("quiz: second pass still unparseable. raw first 400 of pass 1: %s", (raw or "")[:400])
            raise HTTPException(502, "Model returned malformed JSON twice; try another article.")

    questions: list[dict] = []
    # Tolerate two model-output shapes: the canonical dict-per-question
    # and the lazy "array of strings" where the model dropped structure.
    raw_qs = data.get("questions") if isinstance(data, dict) else data
    if not isinstance(raw_qs, list):
        raw_qs = []
    for q in raw_qs[:count]:
        if not isinstance(q, dict):
            continue  # skip string entries — we can't reconstruct options
        question = str(q.get("question") or q.get("q") or "").strip()
        raw_opts = q.get("options") or q.get("choices") or []
        if not isinstance(raw_opts, list):
            continue
        options = [str(o).strip() for o in raw_opts if str(o).strip()]
        try:
            answer_index = int(q.get("answer_index",
                                     q.get("answer", q.get("correct", 0))))
        except (TypeError, ValueError):
            answer_index = 0
        explanation = str(q.get("explanation") or q.get("rationale") or "").strip()
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
