"""
Fragrantica scraping pipeline (SQLite-backed, resumable, zero-duplicates).

Constraints implemented:
- SQLite is the source of truth (`scrape.db` by default).
- Unique primary key is `perfume_id` parsed from URL pattern "-<id>.html".
- Canonical URLs: https, no query/fragment, no trailing slash; only canonical stored.
- Resume/restart safe: does not re-scrape `done` unless `--force`.
- Multi-worker safe: simple row claiming with `BEGIN IMMEDIATE` + conditional UPDATE.
- Output structure is ID-based and stable:
    data/fragrantica/<perfume_id>/
      meta.json
      html/page.html (optional)
      images/hero_og.jpg
      images/hero_twitter.jpg (optional)
      images/gallery/*.jpg (optional, capped)

Usage examples:

  # Discover perfumes from designer pages into the queue
  python3 scrape_fragrantica.py discover --designer-url "https://www.fragrantica.com/designers/Prada.html"

  # Run a worker loop that claims jobs and scrapes them (safe to restart; safe for multiple workers)
  python3 scrape_fragrantica.py work --max 1000 --out data/fragrantica

  # Scrape a single perfume URL (also inserts/updates DB)
  python3 scrape_fragrantica.py scrape-one "https://www.fragrantica.com/perfume/Prada/Paradigme-110661.html"

  # Check queue health and recent errors
  python3 scrape_fragrantica.py stats
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import hashlib
import io
import json
import os
import random
import re
import sqlite3
import time
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/121.0.0.0 Safari/537.36"
)

DB_PATH_DEFAULT = "scrape.db"
MAX_TRIES_DEFAULT = 5


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def write_json(path: Path, data: Any) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()


def canonicalize_url(url: str) -> str:
    """
    Canonicalize URLs:
    - enforce https
    - remove query params and fragments
    - remove trailing slashes
    """
    u = (url or "").strip()
    p = urlparse(u)
    scheme = "https"
    netloc = p.netloc
    path = (p.path or "").rstrip("/")

    # Handle URLs missing scheme (urlparse puts it in path)
    if not netloc and path.startswith("www."):
        parts = path.split("/", 1)
        netloc = parts[0]
        path = "/" + (parts[1] if len(parts) > 1 else "")

    return urlunparse((scheme, netloc, path, "", "", ""))


def perfume_id_from_url(url: str) -> str:
    """
    Unique primary key: trailing "-<id>.html" (e.g. "...-110661.html").
    """
    path = urlparse(url).path
    m = re.search(r"-(\d+)\.html?$", path)
    return m.group(1) if m else ""


# ---------------------------
# SQLite (source of truth)
# ---------------------------


def connect_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=30, isolation_level=None)  # autocommit
    conn.row_factory = sqlite3.Row
    # Concurrency-friendly pragmas
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA busy_timeout=5000;")
    return conn


def init_db(db_path: str) -> sqlite3.Connection:
    conn = connect_db(db_path)
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS frontier (
          perfume_id TEXT PRIMARY KEY,
          url TEXT UNIQUE NOT NULL,
          status TEXT NOT NULL DEFAULT 'queued',   -- queued|in_progress|done|failed|blocked
          discovered_at TEXT,
          updated_at TEXT,
          source TEXT,
          tries INTEGER NOT NULL DEFAULT 0,
          last_error TEXT
        );

        CREATE TABLE IF NOT EXISTS perfumes (
          perfume_id TEXT PRIMARY KEY,
          url TEXT UNIQUE NOT NULL,
          brand TEXT,
          name TEXT,
          rating REAL,
          votes INTEGER,
          meta_path TEXT,
          html_path TEXT,
          fetched_with TEXT,
          scraped_at TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_frontier_status_discovered ON frontier(status, discovered_at);
        CREATE INDEX IF NOT EXISTS idx_frontier_updated ON frontier(updated_at);
        """
    )
    return conn


def upsert_frontier(conn: sqlite3.Connection, perfume_id: str, url: str, source: str) -> bool:
    """
    INSERT OR IGNORE into frontier. Returns True if inserted, False if ignored.
    """
    now = utc_now_iso()
    cur = conn.execute(
        """
        INSERT OR IGNORE INTO frontier(perfume_id, url, status, discovered_at, updated_at, source)
        VALUES (?, ?, 'queued', ?, ?, ?)
        """,
        (perfume_id, url, now, now, source),
    )
    return cur.rowcount == 1


def claim_next_job(conn: sqlite3.Connection) -> Optional[sqlite3.Row]:
    """
    Claim one queued job without two workers claiming the same row.
    """
    conn.execute("BEGIN IMMEDIATE;")
    try:
        row = conn.execute(
            """
            SELECT perfume_id, url, status, tries, discovered_at
            FROM frontier
            WHERE status='queued'
            ORDER BY discovered_at
            LIMIT 1
            """
        ).fetchone()
        if row is None:
            conn.execute("COMMIT;")
            return None

        now = utc_now_iso()
        updated = conn.execute(
            """
            UPDATE frontier
            SET status='in_progress', updated_at=?
            WHERE perfume_id=? AND status='queued'
            """,
            (now, row["perfume_id"]),
        )
        conn.execute("COMMIT;")
        if updated.rowcount == 1:
            return row
        return None
    except Exception:
        conn.execute("ROLLBACK;")
        raise


def requeue_one_failed(conn: sqlite3.Connection, max_tries: int) -> bool:
    """
    Enable retries while keeping `claim_next_job()` strictly "queued-only" (per spec).
    Promotes one eligible failed row back to queued.
    Returns True if a row was re-queued.
    """
    now = utc_now_iso()
    cur = conn.execute(
        """
        UPDATE frontier
        SET status='queued', updated_at=?
        WHERE perfume_id = (
          SELECT perfume_id
          FROM frontier
          WHERE status='failed' AND tries < ?
          ORDER BY updated_at
          LIMIT 1
        )
        """,
        (now, max_tries),
    )
    return cur.rowcount == 1


def update_job_status_done(
    conn: sqlite3.Connection,
    perfume_id: str,
    url: str,
    meta_path: Path,
    html_path: Optional[Path],
    fetched_with: str,
    scraped_at: str,
    brand: Optional[str],
    name: Optional[str],
    rating: Optional[float],
    votes: Optional[int],
) -> None:
    now = utc_now_iso()
    conn.execute(
        "UPDATE frontier SET status='done', updated_at=?, last_error=NULL WHERE perfume_id=?",
        (now, perfume_id),
    )
    conn.execute(
        """
        INSERT OR REPLACE INTO perfumes(
          perfume_id, url, brand, name, rating, votes, meta_path, html_path, fetched_with, scraped_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            perfume_id,
            url,
            brand,
            name,
            rating,
            votes,
            str(meta_path),
            str(html_path) if html_path else None,
            fetched_with,
            scraped_at,
        ),
    )


def update_job_status_failed(conn: sqlite3.Connection, perfume_id: str, err: str, max_tries: int) -> None:
    now = utc_now_iso()
    row = conn.execute("SELECT tries FROM frontier WHERE perfume_id=?", (perfume_id,)).fetchone()
    tries = int(row["tries"]) if row else 0
    tries += 1
    status = "blocked" if tries >= max_tries else "failed"
    conn.execute(
        """
        UPDATE frontier
        SET status=?, updated_at=?, tries=?, last_error=?
        WHERE perfume_id=?
        """,
        (status, now, tries, err[:2000], perfume_id),
    )


def should_skip_done(conn: sqlite3.Connection, perfume_id: str, force: bool) -> bool:
    if force:
        return False
    row = conn.execute("SELECT status FROM frontier WHERE perfume_id=?", (perfume_id,)).fetchone()
    return bool(row and row["status"] == "done")


# ---------------------------
# Fetching (requests + optional Playwright)
# ---------------------------


@dataclasses.dataclass(frozen=True)
class FetchResult:
    url: str
    final_url: str
    html: str
    used_playwright: bool
    extracted: Optional[dict[str, Any]] = None


class BlockedError(RuntimeError):
    """Raised when the fetched page is a bot-check / interstitial that cannot be scraped."""


def is_probable_interstitial(html: str) -> bool:
    h = html.lower()
    return any(
        s in h
        for s in (
            "checking your browser",
            "just a moment",
            "cf-browser-verification",
            "cloudflare",
            "attention required",
            "verify you are human",
            "cf-turnstile-response",
            "challenge-platform",
        )
    )


def fetch_html_requests(url: str, timeout_s: int = 30) -> FetchResult:
    sess = requests.Session()
    resp = sess.get(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        },
        timeout=timeout_s,
    )
    resp.raise_for_status()
    return FetchResult(url=url, final_url=str(resp.url), html=resp.text, used_playwright=False, extracted=None)


def fetch_html_playwright(url: str, timeout_s: int = 45) -> FetchResult:
    # Lazy import: Playwright is optional.
    from playwright.sync_api import sync_playwright  # type: ignore

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(user_agent=USER_AGENT, viewport={"width": 1280, "height": 900})
        page = ctx.new_page()
        page.goto(url, wait_until="domcontentloaded", timeout=timeout_s * 1000)
        page.wait_for_timeout(1500)

        extracted = page.evaluate(
            """
            () => {
              const norm = (s) => (s || "").replace(/\\s+/g, " ").trim();
              const findLabelEl = (label) => {
                const target = label.toLowerCase();
                const els = Array.from(document.querySelectorAll("span,div,li,strong,em,p,a,td,th,label,h1,h2,h3"));
                for (const el of els) {
                  const t = norm(el?.innerText).toLowerCase();
                  if (t === target) return el;
                }
                return null;
              };
              const percentFromStyle = (el) => {
                const w = (el && el.style && el.style.width) ? el.style.width : "";
                const m = /([0-9]{1,3})\\s*%/.exec(w);
                if (!m) return null;
                const v = parseInt(m[1], 10);
                return (isNaN(v) || v < 0 || v > 100) ? null : v;
              };
              const percentFromGeometry = (bar, container) => {
                try {
                  const b = bar.getBoundingClientRect();
                  const c = container.getBoundingClientRect();
                  if (!c.width || c.width <= 0) return null;
                  const v = Math.round((b.width / c.width) * 100);
                  return (v < 0 || v > 100) ? null : v;
                } catch (e) { return null; }
              };
              const findBarNear = (labelEl) => {
                if (!labelEl) return null;
                let scope = labelEl.parentElement;
                for (let up = 0; up < 4 && scope; up++) {
                  const divs = Array.from(scope.querySelectorAll("div,span"));
                  let best = null;
                  let bestWidth = 0;
                  for (const d of divs) {
                    const r = d.getBoundingClientRect();
                    if (r.width > bestWidth && r.height >= 3 && r.height <= 40) {
                      best = d;
                      bestWidth = r.width;
                    }
                  }
                  if (best) return best;
                  scope = scope.parentElement;
                }
                return null;
              };
              const getPct = (label) => {
                const labelEl = findLabelEl(label);
                if (!labelEl) return null;
                const bar = findBarNear(labelEl);
                if (!bar) return null;
                const fromStyle = percentFromStyle(bar);
                if (fromStyle !== null) return fromStyle;
                const container = bar.parentElement || labelEl.parentElement;
                return container ? percentFromGeometry(bar, container) : null;
              };
              return {
                h1: norm(document.querySelector("h1")?.innerText),
                seasonality: {
                  spring: getPct("Spring"),
                  summer: getPct("Summer"),
                  fall: getPct("Fall") ?? getPct("Autumn"),
                  winter: getPct("Winter"),
                },
                daynight: { day: getPct("Day"), night: getPct("Night") },
              };
            }
            """
        )

        html = page.content()
        final_url = page.url
        ctx.close()
        browser.close()
    if is_probable_interstitial(html):
        raise BlockedError("Blocked by interstitial / Turnstile challenge (Playwright).")

    return FetchResult(
        url=url,
        final_url=final_url,
        html=html,
        used_playwright=True,
        extracted=extracted if isinstance(extracted, dict) else None,
    )


def fetch_html(url: str, allow_playwright_fallback: bool = True) -> FetchResult:
    try:
        r = fetch_html_requests(url)
    except requests.exceptions.HTTPError as e:
        status = getattr(getattr(e, "response", None), "status_code", None)
        # Fragrantica sometimes returns 403/429/503 to non-browser clients.
        if allow_playwright_fallback and status in (403, 429, 503):
            return fetch_html_playwright(url)
        raise

    if allow_playwright_fallback and is_probable_interstitial(r.html):
        return fetch_html_playwright(url)
    if is_probable_interstitial(r.html):
        raise BlockedError("Blocked by interstitial / Turnstile challenge (requests).")
    return r


# ---------------------------
# Parsing (keep existing logic where possible)
# ---------------------------


def parse_jsonld_products(soup: BeautifulSoup) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for tag in soup.select('script[type="application/ld+json"]'):
        raw = (tag.string or "").strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue

        def walk(v: Any) -> None:
            if isinstance(v, dict):
                if v.get("@type") in ("Product", "IndividualProduct"):
                    out.append(v)
                for vv in v.values():
                    walk(vv)
            elif isinstance(v, list):
                for vv in v:
                    walk(vv)

        walk(data)
    return out


def pick_first_str(*candidates: Optional[str]) -> Optional[str]:
    for c in candidates:
        if c is None:
            continue
        s = c.strip()
        if s:
            return s
    return None


def text_of(el: Any) -> Optional[str]:
    if el is None:
        return None
    try:
        return el.get_text(" ", strip=True)
    except Exception:
        return None


def normalize_votes(v: str) -> Optional[int]:
    v = (v or "").strip().replace(",", "")
    m = re.search(r"(\d+)", v)
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None


def normalize_rating(v: str) -> Optional[float]:
    m = re.search(r"(\d+(?:\.\d+)?)", (v or "").strip())
    if not m:
        return None
    try:
        return float(m.group(1))
    except Exception:
        return None


def extract_rating_votes_from_text(page_text: str) -> tuple[Optional[float], Optional[int]]:
    rating = None
    votes = None
    m = re.search(r"(\d+(?:\.\d+)?)\s*/\s*5", page_text)
    if m:
        rating = normalize_rating(m.group(1))
    m2 = re.search(r"(\d[\d,]*)\s+votes", page_text, flags=re.IGNORECASE)
    if m2:
        votes = normalize_votes(m2.group(1))
    return rating, votes


def extract_longevity_sillage_scores(page_text: str) -> tuple[Optional[float], Optional[float]]:
    longevity = None
    sillage = None
    m = re.search(r"Longevity\s+(\d+(?:\.\d+)?)\s+out\s+of\s+5", page_text, flags=re.I)
    if m:
        longevity = normalize_rating(m.group(1))
    m2 = re.search(r"Sillage\s+(\d+(?:\.\d+)?)\s+out\s+of\s+4", page_text, flags=re.I)
    if m2:
        sillage = normalize_rating(m2.group(1))
    return longevity, sillage


def extract_season_daynight_percent_fallback(html: str) -> dict[str, dict[str, Optional[int]]]:
    h = html

    def find_pct(label: str) -> Optional[int]:
        patterns = [
            rf"{label}\D{{0,40}}(\d{{1,3}})\s*%",
            rf'"{label}"\s*:\s*(\d{{1,3}})',
            rf"{label}\s*:\s*(\d{{1,3}})",
        ]
        for pat in patterns:
            m = re.search(pat, h, flags=re.I)
            if m:
                try:
                    v = int(m.group(1))
                except Exception:
                    continue
                if 0 <= v <= 100:
                    return v
        return None

    return {
        "seasonality": {
            "spring": find_pct("Spring"),
            "summer": find_pct("Summer"),
            "fall": find_pct("Fall|Autumn"),
            "winter": find_pct("Winter"),
        },
        "daynight": {"day": find_pct("Day"), "night": find_pct("Night")},
    }


def clean_name_from_heading(h1_text: str, brand: Optional[str]) -> tuple[Optional[str], dict[str, Any]]:
    if not h1_text:
        return None, {}
    t = re.sub(r"\s+", " ", h1_text).strip()
    extras: dict[str, Any] = {}

    # Remove SEO tail (e.g. "- a new fragrance for ...")
    t = re.split(r"\s+-\s+", t, maxsplit=1)[0].strip()

    # Gender
    m_gender = re.search(r"\bfor\s+(men|women|men and women|unisex)\b", t, flags=re.I)
    if m_gender:
        g = m_gender.group(1).lower()
        extras["gender"] = "unisex" if "men and women" in g else g
        t = re.sub(r"\s*\bfor\s+(men|women|men and women|unisex)\b\s*", " ", t, flags=re.I).strip()

    # Year
    m_year = re.search(r"\b(19\d{2}|20\d{2})\b", t)
    if m_year:
        try:
            extras["year"] = int(m_year.group(1))
            t = re.sub(r"\b(19\d{2}|20\d{2})\b", "", t).strip()
        except Exception:
            pass

    # Concentration
    m_conc = re.search(
        r"\b(parfum|extrait|edp|eau de parfum|edt|eau de toilette|cologne|eau de cologne)\b",
        t,
        flags=re.I,
    )
    if m_conc:
        conc = m_conc.group(1).lower()
        conc = {"edp": "eau de parfum", "edt": "eau de toilette"}.get(conc, conc)
        extras["concentration"] = conc
        t = re.sub(
            r"\b(parfum|extrait|edp|eau de parfum|edt|eau de toilette|cologne|eau de cologne)\b",
            "",
            t,
            flags=re.I,
        ).strip()

    # If brand appears in H1, treat left side as name
    if brand:
        b = re.sub(r"\s+", " ", brand).strip()
        idx = t.lower().find(b.lower())
        if idx > 0:
            left = t[:idx].strip()
            if left:
                return left, extras

    # Otherwise: take first chunk
    t2 = re.split(r"\s+\bby\b\s+|\s+\(|\s+–\s+|\s+-\s+", t, maxsplit=1)[0].strip()
    return (t2 or None), extras


def extract_main_accords_dom(soup: BeautifulSoup) -> list[str]:
    def is_valid_accord_label(label: str) -> bool:
        l = re.sub(r"\s+", " ", (label or "")).strip()
        if not l or len(l) > 30:
            return False
        bad = {
            "i have it",
            "i had it",
            "i want it",
            "sponsored",
            "love",
            "like",
            "ok",
            "dislike",
            "hate",
            "winter",
            "spring",
            "summer",
            "fall",
            "autumn",
            "day",
            "night",
        }
        if l.lower() in bad:
            return False
        if re.search(r"\b(votes?|rating|reviews?)\b", l, flags=re.I):
            return False
        return bool(re.fullmatch(r"[A-Za-z][A-Za-z '\-]{0,29}", l))

    out: list[str] = []
    for el in soup.find_all(True, class_=re.compile(r"accord", flags=re.I)):
        style = (el.get("style") or "").lower()
        if not re.search(r"width\s*:\s*\d{1,3}\s*%", style):
            continue
        label = el.get_text(" ", strip=True)
        if is_valid_accord_label(label):
            out.append(re.sub(r"\s+", " ", label).strip())

    seen: set[str] = set()
    dedup: list[str] = []
    for a in out:
        k = a.lower()
        if k in seen:
            continue
        seen.add(k)
        dedup.append(a)
    return dedup[:15]


def extract_notes(soup: BeautifulSoup) -> dict[str, list[str]]:
    txt = soup.get_text("\n", strip=True)
    lines = [ln.strip() for ln in txt.splitlines() if ln.strip()]
    buckets = {"top": [], "middle": [], "base": [], "other": []}

    def harvest_after(anchor: str, key: str) -> None:
        for i, ln in enumerate(lines):
            if ln.lower() == anchor:
                for ln2 in lines[i + 1 : i + 30]:
                    if ln2.lower() in ("top notes", "middle notes", "base notes", "main accords"):
                        break
                    if len(ln2) > 40:
                        continue
                    if re.search(r"\d", ln2):
                        continue
                    buckets[key].append(ln2)
                return

    harvest_after("top notes", "top")
    harvest_after("middle notes", "middle")
    harvest_after("base notes", "base")

    for k in list(buckets.keys()):
        seen: set[str] = set()
        dedup: list[str] = []
        for n in buckets[k]:
            kk = n.lower()
            if kk in seen:
                continue
            seen.add(kk)
            dedup.append(n)
        buckets[k] = dedup

    return buckets


# ---------------------------
# Images (strict filtering + storage reduction)
# ---------------------------


def extract_meta_image_url(soup: BeautifulSoup, key: str) -> Optional[str]:
    if key.startswith("og:"):
        el = soup.select_one(f'meta[property="{key}"]')
        v = el.get("content") if el else None
        return v.strip() if isinstance(v, str) and v.strip() else None
    el = soup.select_one(f'meta[name="{key}"]') or soup.select_one(f'meta[property="{key}"]')
    v = el.get("content") if el else None
    return v.strip() if isinstance(v, str) and v.strip() else None


def filter_image_urls_for_perfume(urls: list[str], perfume_id: str, include_photogram: bool) -> list[str]:
    pid = perfume_id.strip()

    def is_noise(u: str) -> bool:
        return any(
            s in u
            for s in (
                "/mdimg/news/",
                "/mdimg/sastojci/",
                "/mdimg/perfume-thumbs/s.",
            )
        )

    def allowed(u: str) -> bool:
        if is_noise(u):
            return False
        if include_photogram and "fimgs.net/photogram/" in u:
            return True
        return bool(pid and pid in u)

    def priority(u: str) -> tuple[int, int]:
        if re.search(rf"/mdimg/perfume-thumbs/375x500\.{re.escape(pid)}\.", u):
            return (0, 0)
        if re.search(rf"/mdimg/perfume/m\.{re.escape(pid)}\.", u):
            return (1, 0)
        if "/mdimg/perfume-social-cards/" in u:
            return (2, 0)
        if "fimgs.net/photogram/" in u:
            return (3, 0)
        return (4, 0)

    out: list[str] = []
    seen: set[str] = set()
    for u in urls:
        if u in seen:
            continue
        seen.add(u)
        if allowed(u):
            out.append(u)
    out.sort(key=priority)
    return out


def gather_image_urls(
    soup: BeautifulSoup,
    base_url: str,
    perfume_id: str,
    include_photogram: bool,
) -> dict[str, Any]:
    hero_og = extract_meta_image_url(soup, "og:image")
    hero_tw = extract_meta_image_url(soup, "twitter:image") or extract_meta_image_url(soup, "twitter:image:src")

    candidates: list[str] = []
    for img in soup.select("img"):
        src = img.get("src") or img.get("data-src") or img.get("data-lazy-src")
        if not isinstance(src, str) or not src.strip():
            continue
        s = src.strip()
        # Keep candidates that match perfume_id or photogram (if enabled)
        if perfume_id and perfume_id in s:
            candidates.append(s)
        elif include_photogram and "fimgs.net/photogram/" in s:
            candidates.append(s)

    all_urls: list[str] = []
    for u in [hero_og, hero_tw, *candidates]:
        if not u:
            continue
        all_urls.append(urljoin(base_url, u))

    gallery = filter_image_urls_for_perfume(all_urls, perfume_id=perfume_id, include_photogram=include_photogram)
    return {
        "hero_og_url": urljoin(base_url, hero_og) if hero_og else None,
        "hero_twitter_url": urljoin(base_url, hero_tw) if hero_tw else None,
        "gallery_urls": gallery,
    }


def _try_convert_to_jpg(content: bytes) -> Optional[bytes]:
    """
    Convert images to JPEG to reduce storage if Pillow is installed.
    Returns JPEG bytes, or None if conversion isn't possible.
    """
    try:
        from PIL import Image  # type: ignore

        im = Image.open(io.BytesIO(content))
        if im.mode not in ("RGB", "L"):
            im = im.convert("RGB")
        out = io.BytesIO()
        im.save(out, format="JPEG", quality=85, optimize=True)
        return out.getvalue()
    except Exception:
        return None


def download_bytes(session: requests.Session, url: str, timeout_s: int = 30) -> Optional[bytes]:
    try:
        resp = session.get(url, timeout=timeout_s)
        resp.raise_for_status()
        return resp.content
    except Exception:
        return None


# ---------------------------
# Scrape + write outputs (ID-based folders)
# ---------------------------


def scrape_and_write(
    perfume_url: str,
    out_root: Path,
    allow_playwright: bool,
    save_html: bool,
    max_images: int,
    include_photogram: bool,
) -> tuple[dict[str, Any], Path, Optional[Path]]:
    url = canonicalize_url(perfume_url)
    pid = perfume_id_from_url(url)
    if not pid:
        raise ValueError(f"Could not parse perfume_id from URL: {perfume_url}")

    fetch = fetch_html(url, allow_playwright_fallback=allow_playwright)
    soup = BeautifulSoup(fetch.html, "lxml")

    # Brand: JSON-LD first
    prods = parse_jsonld_products(soup)
    prod0 = prods[0] if prods else {}
    brand = None
    b = prod0.get("brand") if isinstance(prod0, dict) else None
    if isinstance(b, dict) and isinstance(b.get("name"), str):
        brand = b["name"].strip()
    elif isinstance(b, str):
        brand = b.strip()

    # Name: H1-based cleaning
    h1 = text_of(soup.select_one("h1")) or ""
    name, name_extras = clean_name_from_heading(h1, brand=brand)

    # Description: JSON-LD or og:description
    og_desc = soup.select_one('meta[property="og:description"]')
    desc = pick_first_str(
        (prod0.get("description") if isinstance(prod0, dict) else None),
        (og_desc.get("content") if og_desc else None),
    )

    page_text = soup.get_text(" ", strip=True)
    rating, votes = extract_rating_votes_from_text(page_text)
    longevity_score, sillage_score = extract_longevity_sillage_scores(page_text)
    accords = extract_main_accords_dom(soup)
    notes = extract_notes(soup)

    sliders = extract_season_daynight_percent_fallback(fetch.html)
    if fetch.extracted and isinstance(fetch.extracted, dict):
        seas = fetch.extracted.get("seasonality")
        dn = fetch.extracted.get("daynight")
        if isinstance(seas, dict):
            sliders["seasonality"] = {
                "spring": seas.get("spring"),
                "summer": seas.get("summer"),
                "fall": seas.get("fall"),
                "winter": seas.get("winter"),
            }
        if isinstance(dn, dict):
            sliders["daynight"] = {"day": dn.get("day"), "night": dn.get("night")}

    root = out_root / pid
    ensure_dir(root)

    html_path: Optional[Path] = None
    if save_html:
        html_dir = root / "html"
        ensure_dir(html_dir)
        html_path = html_dir / "page.html"
        html_path.write_text(fetch.html, encoding="utf-8", errors="ignore")

    images_dir = root / "images"
    ensure_dir(images_dir)
    gallery_dir = images_dir / "gallery"
    ensure_dir(gallery_dir)

    sess = requests.Session()
    sess.headers.update({"User-Agent": USER_AGENT, "Accept": "image/*,*/*;q=0.8"})

    images_info = gather_image_urls(soup, base_url=fetch.final_url, perfume_id=pid, include_photogram=include_photogram)

    images_saved: dict[str, Any] = {"hero_og": None, "hero_twitter": None, "gallery": []}

    def save_named_hero(url0: Optional[str], filename: str) -> Optional[str]:
        if not url0:
            return None
        content = download_bytes(sess, url0)
        if not content:
            return None
        jpg = _try_convert_to_jpg(content)
        out_path = images_dir / filename
        out_path.write_bytes(jpg if jpg is not None else content)
        return str(out_path.relative_to(root).as_posix())

    images_saved["hero_og"] = save_named_hero(images_info.get("hero_og_url"), "hero_og.jpg")
    images_saved["hero_twitter"] = save_named_hero(images_info.get("hero_twitter_url"), "hero_twitter.jpg")

    # Gallery (strictly filtered); cap includes heroes within total max_images budget.
    gallery_urls: list[str] = list(images_info.get("gallery_urls") or [])
    hero_set = set([u for u in [images_info.get("hero_og_url"), images_info.get("hero_twitter_url")] if u])
    gallery_urls = [u for u in gallery_urls if u not in hero_set]

    photogram_count = 0
    gallery_budget = max(0, max_images - 2)
    for u in gallery_urls:
        if len(images_saved["gallery"]) >= gallery_budget:
            break
        if "fimgs.net/photogram/" in u:
            if photogram_count >= 10:
                continue
            photogram_count += 1

        content = download_bytes(sess, u)
        if not content:
            continue
        jpg = _try_convert_to_jpg(content) or content
        digest = hashlib.sha256(jpg).hexdigest()[:16]
        out_path = gallery_dir / f"{digest}.jpg"
        if not out_path.exists():
            out_path.write_bytes(jpg)
        images_saved["gallery"].append(str(out_path.relative_to(root).as_posix()))
        time.sleep(0.15)

    fetched_with = "playwright" if fetch.used_playwright else "requests"
    final_url = canonicalize_url(fetch.final_url)
    scraped_at = utc_now_iso()

    meta: dict[str, Any] = {
        "perfume_id": pid,
        "source": {
            "site": "fragrantica",
            "url": url,
            "final_url": final_url,
            "scraped_at": scraped_at,
            "fetched_with": fetched_with,
            "html_sha256": sha256_text(fetch.html),
        },
        "brand": brand,
        "name": name,
        "name_extras": name_extras,
        "description": desc,
        "rating": {"value": rating, "votes": votes},
        "main_accords": accords,
        "notes": notes,
        "performance": {
            "longevity_score": longevity_score,
            "sillage_score": sillage_score,
            **sliders,
        },
        "images": images_saved,
    }

    meta_path = root / "meta.json"
    write_json(meta_path, meta)
    return meta, meta_path, html_path


# ---------------------------
# Discovery
# ---------------------------


def collect_perfume_urls_from_designer(designer_url: str, html: str) -> list[str]:
    """
    Discover perfume links from a designer page.
    """
    soup = BeautifulSoup(html, "lxml")
    links: list[str] = []
    for a in soup.select("a[href]"):
        href = a.get("href")
        if not isinstance(href, str) or not href:
            continue
        if "/perfume/" not in href:
            continue
        if not href.endswith(".html") and ".html?" not in href:
            continue
        links.append(urljoin(designer_url, href))

    seen: set[str] = set()
    out: list[str] = []
    for u in links:
        cu = canonicalize_url(u)
        if cu in seen:
            continue
        seen.add(cu)
        out.append(cu)
    return out


# ---------------------------
# CLI
# ---------------------------


def cmd_discover(args: argparse.Namespace) -> None:
    conn = init_db(args.db)
    designer_urls: list[str] = list(args.designer_url or [])
    if args.designer_file:
        p = Path(args.designer_file).expanduser()
        for line in p.read_text(encoding="utf-8").splitlines():
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            designer_urls.append(s)

    discovered = 0
    inserted = 0
    for durl_raw in designer_urls:
        durl = canonicalize_url(durl_raw)
        try:
            fetch = fetch_html(durl, allow_playwright_fallback=not args.no_playwright)
        except BlockedError as e:
            # Designer discovery is best-effort; keep going.
            print(json.dumps({"designer_url": durl, "blocked": True, "error": str(e)}, indent=2))
            continue

        links = collect_perfume_urls_from_designer(fetch.final_url, fetch.html)
        discovered += len(links)
        for u in links:
            pid = perfume_id_from_url(u)
            if not pid:
                continue
            if upsert_frontier(conn, pid, canonicalize_url(u), source=f"designer:{durl}"):
                inserted += 1

    print(json.dumps({"discovered": discovered, "inserted": inserted}, indent=2))


def cmd_scrape_one(args: argparse.Namespace) -> None:
    conn = init_db(args.db)
    url = canonicalize_url(args.url)
    pid = perfume_id_from_url(url)
    if not pid:
        raise SystemExit(f"Could not parse perfume_id from URL: {args.url}")

    # Ensure frontier row exists
    upsert_frontier(conn, pid, url, source="manual:scrape-one")

    if should_skip_done(conn, pid, force=args.force):
        print(json.dumps({"skipped": True, "perfume_id": pid, "reason": "already_done"}, indent=2))
        return

    try:
        meta, meta_path, html_path = scrape_and_write(
            perfume_url=url,
            out_root=Path(args.out).expanduser().resolve(),
            allow_playwright=not args.no_playwright,
            save_html=not args.no_html,
            max_images=args.max_images,
            include_photogram=not args.no_photogram,
        )
        update_job_status_done(
            conn=conn,
            perfume_id=pid,
            url=url,
            meta_path=meta_path,
            html_path=html_path,
            fetched_with=(meta.get("source") or {}).get("fetched_with") or "",
            scraped_at=(meta.get("source") or {}).get("scraped_at") or "",
            brand=meta.get("brand"),
            name=meta.get("name"),
            rating=(meta.get("rating") or {}).get("value"),
            votes=(meta.get("rating") or {}).get("votes"),
        )
        print(json.dumps({"ok": True, "perfume_id": pid, "out": str(meta_path.parent)}, indent=2))
    except Exception as e:
        update_job_status_failed(conn, pid, repr(e), max_tries=args.max_tries)
        raise


def cmd_work(args: argparse.Namespace) -> None:
    conn = init_db(args.db)
    out_root = Path(args.out).expanduser().resolve()
    ensure_dir(out_root)

    processed = 0
    while True:
        if args.max and processed >= args.max:
            return

        job = claim_next_job(conn)
        if job is None:
            # No queued jobs; if there are eligible failures, re-queue one for retry.
            if requeue_one_failed(conn, max_tries=args.max_tries):
                continue
            time.sleep(3.0)
            continue

        pid = job["perfume_id"]
        url = job["url"]

        # Safety: don't re-scrape done rows unless forced
        if should_skip_done(conn, pid, force=args.force):
            conn.execute(
                "UPDATE frontier SET status='done', updated_at=? WHERE perfume_id=?",
                (utc_now_iso(), pid),
            )
            processed += 1
            continue

        # Enforce max tries unless forced (blocked rows won't be queued, but keep safe)
        tries_row = conn.execute("SELECT tries FROM frontier WHERE perfume_id=?", (pid,)).fetchone()
        tries = int(tries_row["tries"]) if tries_row else 0
        if tries >= args.max_tries and not args.force:
            conn.execute(
                "UPDATE frontier SET status='blocked', updated_at=? WHERE perfume_id=?",
                (utc_now_iso(), pid),
            )
            processed += 1
            continue

        try:
            meta, meta_path, html_path = scrape_and_write(
                perfume_url=url,
                out_root=out_root,
                allow_playwright=not args.no_playwright,
                save_html=not args.no_html,
                max_images=args.max_images,
                include_photogram=not args.no_photogram,
            )
            update_job_status_done(
                conn=conn,
                perfume_id=pid,
                url=url,
                meta_path=meta_path,
                html_path=html_path,
                fetched_with=(meta.get("source") or {}).get("fetched_with") or "",
                scraped_at=(meta.get("source") or {}).get("scraped_at") or "",
                brand=meta.get("brand"),
                name=meta.get("name"),
                rating=(meta.get("rating") or {}).get("value"),
                votes=(meta.get("rating") or {}).get("votes"),
            )
        except Exception as e:
            update_job_status_failed(conn, pid, repr(e), max_tries=args.max_tries)
        finally:
            processed += 1
            time.sleep(random.uniform(args.sleep_min, args.sleep_max))


def cmd_stats(args: argparse.Namespace) -> None:
    conn = init_db(args.db)
    rows = conn.execute(
        """
        SELECT status, COUNT(*) as n
        FROM frontier
        GROUP BY status
        ORDER BY status
        """
    ).fetchall()
    counts = {r["status"]: r["n"] for r in rows}

    recent_errors = conn.execute(
        """
        SELECT perfume_id, status, tries, updated_at, last_error
        FROM frontier
        WHERE status IN ('failed','blocked') AND last_error IS NOT NULL
        ORDER BY updated_at DESC
        LIMIT 10
        """
    ).fetchall()

    out = {
        "counts": counts,
        "recent_errors": [
            {
                "perfume_id": r["perfume_id"],
                "status": r["status"],
                "tries": r["tries"],
                "updated_at": r["updated_at"],
                "last_error": r["last_error"],
            }
            for r in recent_errors
        ],
    }
    print(json.dumps(out, indent=2))


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Fragrantica harvester (SQLite-backed)")
    p.add_argument("--db", default=DB_PATH_DEFAULT, help="SQLite db path (default: scrape.db)")
    sub = p.add_subparsers(dest="cmd", required=True)

    p_dis = sub.add_parser("discover", help="Discover perfume URLs from designer pages into the frontier")
    p_dis.add_argument("--designer-url", action="append", default=[], help="Designer URL (repeatable)")
    p_dis.add_argument("--designer-file", default="", help="Text file with designer URLs (one per line)")
    p_dis.add_argument("--no-playwright", action="store_true", help="Disable Playwright fallback for discovery")
    p_dis.set_defaults(func=cmd_discover)

    p_work = sub.add_parser("work", help="Worker loop: claim queued jobs and scrape them")
    p_work.add_argument("--max", type=int, default=0, help="Max jobs to process (0 = infinite)")
    p_work.add_argument("--sleep-min", type=float, default=1.5, help="Min sleep between jobs (seconds)")
    p_work.add_argument("--sleep-max", type=float, default=3.0, help="Max sleep between jobs (seconds)")
    p_work.add_argument("--out", default="data/fragrantica", help="Output root folder")
    p_work.add_argument("--max-tries", type=int, default=MAX_TRIES_DEFAULT, help="Retries before blocked")
    p_work.add_argument("--max-images", type=int, default=15, help="Max images (heroes + gallery) (default: 15)")
    p_work.add_argument("--no-playwright", action="store_true", help="Disable Playwright fallback")
    p_work.add_argument("--no-html", action="store_true", help="Do not save html/page.html snapshots")
    p_work.add_argument("--no-photogram", action="store_true", help="Disable photogram images in gallery")
    p_work.add_argument("--force", action="store_true", help="Re-scrape even if already done")
    p_work.set_defaults(func=cmd_work)

    p_one = sub.add_parser("scrape-one", help="Scrape a single perfume URL and update DB/output")
    p_one.add_argument("url", help="Perfume URL")
    p_one.add_argument("--out", default="data/fragrantica", help="Output root folder")
    p_one.add_argument("--max-tries", type=int, default=MAX_TRIES_DEFAULT, help="Retries before blocked")
    p_one.add_argument("--max-images", type=int, default=15, help="Max images (heroes + gallery) (default: 15)")
    p_one.add_argument("--no-playwright", action="store_true", help="Disable Playwright fallback")
    p_one.add_argument("--no-html", action="store_true", help="Do not save html/page.html snapshots")
    p_one.add_argument("--no-photogram", action="store_true", help="Disable photogram images in gallery")
    p_one.add_argument("--force", action="store_true", help="Re-scrape even if already done")
    p_one.set_defaults(func=cmd_scrape_one)

    p_stats = sub.add_parser("stats", help="Show totals queued/in_progress/done/failed and recent errors")
    p_stats.set_defaults(func=cmd_stats)

    return p


def main() -> None:
    args = build_parser().parse_args()
    args.func(args)


if __name__ == "__main__":
    main()

