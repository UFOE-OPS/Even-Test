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
      images/main.<ext>              (required: main product/bottle photo)
      images/hero_og.jpg             (optional; only with --download-all-images)
      images/hero_twitter.jpg        (optional; only with --download-all-images)
      images/gallery/*.jpg           (optional, capped; only with --download-all-images)

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
import sys
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


def _warn(msg: str, **kv: Any) -> None:
    payload = {"level": "warning", "msg": msg}
    if kv:
        payload.update(kv)
    try:
        print(json.dumps(payload, ensure_ascii=False), file=sys.stderr)
    except Exception:
        print(f"WARNING: {msg}", file=sys.stderr)


def extract_clean_name(soup: BeautifulSoup, fallback_name: str) -> str:
    """
    Extract a stable, short perfume name from the page DOM.

    Fragrantica sometimes includes SEO-ish tails like:
      "X cologne - a new fragrance for men ..."
    """

    def norm(s: str) -> str:
        return re.sub(r"\s+", " ", (s or "").replace("\xa0", " ")).strip()

    # Preferred: H1 title
    raw = norm(text_of(soup.select_one("h1")) or "")
    if not raw:
        # Fallback: og:title is sometimes present and cleaner than body text.
        ogt = soup.select_one('meta[property="og:title"]')
        raw = norm((ogt.get("content") if ogt else "") or "")
    if not raw:
        raw = norm(fallback_name)

    # Remove common SEO tail patterns.
    # Examples observed: "- a new fragrance for women and men", "- a new fragrance for men 2024"
    raw = re.sub(r"\s*-\s*a new fragrance.*$", "", raw, flags=re.I).strip()
    raw = re.sub(
        r"\b(cologne|perfume|parfum)\s*-\s*a new fragrance.*$",
        "",
        raw,
        flags=re.I,
    ).strip()
    # Last resort: split on " - " and keep the left side.
    raw = re.split(r"\s+-\s+", raw, maxsplit=1)[0].strip()
    return raw or norm(fallback_name) or "Unknown"


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


def _looks_like_bottle_image(url: str, perfume_id: str) -> bool:
    u = (url or "").lower()
    pid = (perfume_id or "").strip()
    if pid and pid in u:
        return True
    if "/mdimg/perfume/" in u or "/mdimg/perfume-thumbs/" in u:
        if "/mdimg/perfume-social-cards/" in u:
            return False
        return True
    return False


def _probe_url_exists(url: str, timeout_s: float = 6.0) -> bool:
    """
    Lightweight existence probe for image URLs.
    Prefer HEAD, fallback to GET(stream=True). Keep timeouts short.
    """
    if not url:
        return False
    headers = {"User-Agent": USER_AGENT, "Accept": "image/*,*/*;q=0.8"}
    try:
        r = requests.head(url, headers=headers, allow_redirects=True, timeout=timeout_s)
        if r.status_code and r.status_code < 400:
            return True
    except Exception:
        pass

    try:
        r2 = requests.get(url, headers=headers, allow_redirects=True, timeout=timeout_s, stream=True)
        ok = bool(r2.status_code and r2.status_code < 400)
        try:
            r2.close()
        except Exception:
            pass
        return ok
    except Exception:
        return False


def _ext_from_url(url: str) -> str:
    path = urlparse(url).path.lower()
    m = re.search(r"\.(jpg|jpeg|png|webp|gif)$", path)
    if m:
        ext = m.group(1)
        return ".jpg" if ext == "jpeg" else f".{ext}"
    return ".jpg"


def _parse_px(v: Any) -> Optional[int]:
    if v is None:
        return None
    if isinstance(v, (int, float)) and v >= 0:
        return int(v)
    s = str(v).strip().lower()
    m = re.search(r"(\d+)", s)
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None


def _img_dims_from_tag(img: Any) -> tuple[Optional[int], Optional[int]]:
    """
    Try to infer width/height in px from <img> attributes or inline styles.
    """
    if img is None:
        return None, None

    w = _parse_px(img.get("width"))
    h = _parse_px(img.get("height"))
    if w is not None or h is not None:
        return w, h

    style = (img.get("style") or "") if hasattr(img, "get") else ""
    if isinstance(style, str) and style:
        mw = re.search(r"width\s*:\s*(\d+)\s*px", style, flags=re.I)
        mh = re.search(r"height\s*:\s*(\d+)\s*px", style, flags=re.I)
        w2 = int(mw.group(1)) if mw else None
        h2 = int(mh.group(1)) if mh else None
        if w2 is not None or h2 is not None:
            return w2, h2

    # Sometimes the size is on the parent container.
    parent = getattr(img, "parent", None)
    if parent is not None and hasattr(parent, "get"):
        pstyle = parent.get("style") or ""
        if isinstance(pstyle, str) and pstyle:
            mw = re.search(r"width\s*:\s*(\d+)\s*px", pstyle, flags=re.I)
            mh = re.search(r"height\s*:\s*(\d+)\s*px", pstyle, flags=re.I)
            w3 = int(mw.group(1)) if mw else None
            h3 = int(mh.group(1)) if mh else None
            if w3 is not None or h3 is not None:
                return w3, h3

    return None, None


def get_main_product_image(perfume_id: str, soup: BeautifulSoup, base_url: str) -> Optional[str]:
    """
    Return the best “bottle/product” image URL for a fragrance.

    Priority:
    (a) og:image if it contains perfume_id OR looks like a bottle image
    (b) deterministic fimgs.net patterns based on perfume_id (probe existence)
    (c) fallback: scan <img> tags for likely bottle image and choose biggest by pixel area
    """
    pid = (perfume_id or "").strip()
    if not pid:
        return None

    # (a) og:image
    og = extract_meta_image_url(soup, "og:image")
    if og:
        og_abs = urljoin(base_url, og)
        if (pid and pid in og_abs) or _looks_like_bottle_image(og_abs, pid):
            return og_abs

    # (b) known patterns (probe)
    candidates: list[str] = []
    for ext in (".jpg", ".png"):
        candidates.extend(
            [
                f"https://fimgs.net/mdimg/perfume-thumbs/375x500.{pid}{ext}",
                f"https://fimgs.net/mdimg/perfume/m.{pid}{ext}",
                f"https://fimgs.net/mdimg/perfume/375x500.{pid}{ext}",
            ]
        )
    for u in candidates:
        if _probe_url_exists(u, timeout_s=6.0):
            return u

    # (c) scan <img> tags
    best_url: Optional[str] = None
    best_score: tuple[int, int] = (-1, -1)  # (area_or_dim, -dom_idx)
    dom_idx = 0
    for img in soup.select("img"):
        dom_idx += 1
        src = img.get("src") or img.get("data-src") or img.get("data-lazy-src")
        if not isinstance(src, str) or not src.strip():
            continue
        u = urljoin(base_url, src.strip())
        ul = u.lower()
        if pid not in ul:
            continue
        if "/mdimg/perfume/" not in ul and "/mdimg/perfume-thumbs/" not in ul:
            continue
        if "/mdimg/perfume-social-cards/" in ul:
            continue

        w, h = _img_dims_from_tag(img)
        if w is not None and h is not None:
            score = (w * h, -dom_idx)
        elif w is not None:
            score = (w, -dom_idx)
        elif h is not None:
            score = (h, -dom_idx)
        else:
            # Try to infer from URL patterns like "375x500."
            m = re.search(r"/(\d+)x(\d+)\.", ul)
            if m:
                try:
                    ww = int(m.group(1))
                    hh = int(m.group(2))
                    score = (ww * hh, -dom_idx)
                except Exception:
                    score = (0, -dom_idx)
            else:
                score = (0, -dom_idx)

        if score > best_score:
            best_score = score
            best_url = u

    return best_url


# ---------------------------
# Notes pyramid with strength (DOM-based)
# ---------------------------


def _clean_note_name(name: str) -> str:
    s = (name or "").replace("\xa0", " ").strip()
    s = re.sub(r"\s+", " ", s).strip()
    # Remove common garbage artifacts while keeping original casing stable.
    s = s.replace("�", "").strip()
    return s


def _extract_note_label(img: Any) -> str:
    alt = img.get("alt") if hasattr(img, "get") else None
    title = img.get("title") if hasattr(img, "get") else None
    s = pick_first_str(alt if isinstance(alt, str) else None, title if isinstance(title, str) else None)
    if s:
        return _clean_note_name(s)

    # Try anchor text
    a = img.find_parent("a") if hasattr(img, "find_parent") else None
    if a is not None:
        t = text_of(a)
        if t:
            return _clean_note_name(t)

    # Try parent text
    p = getattr(img, "parent", None)
    t2 = text_of(p)
    if t2:
        return _clean_note_name(t2)
    return ""


def _strength_from_dims(w: Optional[int], h: Optional[int]) -> int:
    if w is not None and h is not None:
        return int(w) * int(h)
    if w is not None:
        return int(w)
    return 0


def _find_heading_tags(soup: BeautifulSoup, label: str) -> list[Any]:
    """
    Return tags whose text looks like the desired heading (e.g. "Top Notes").
    """
    out: list[Any] = []
    for t in soup.find_all(True):
        tt = text_of(t) or ""
        if not tt:
            continue
        if re.fullmatch(rf"\s*{re.escape(label)}\s*", tt, flags=re.I):
            out.append(t)
    return out


def _find_notes_container_from_heading(heading_tag: Any) -> Optional[Any]:
    """
    Heuristic: walk forward in DOM until we find a container that has
    multiple note images (usually /mdimg/sastojci/ or /mdimg/notes/).
    """
    if heading_tag is None:
        return None

    def looks_like_notes_container(tag: Any) -> bool:
        if tag is None or not hasattr(tag, "select"):
            return False
        # Avoid selecting the whole page.
        if getattr(tag, "name", "") in ("html", "body"):
            return False
        imgs = tag.select("img")
        if len(imgs) < 1:
            return False
        noteish = 0
        for img in imgs:
            src = img.get("src") or img.get("data-src") or img.get("data-lazy-src") or ""
            if not isinstance(src, str):
                continue
            sl = src.lower()
            if "/mdimg/sastojci/" in sl or "/mdimg/notes/" in sl:
                noteish += 1
        return noteish >= 1

    # Prefer nearby siblings: typically the notes container follows the heading.
    sib = heading_tag.find_next_sibling()
    if looks_like_notes_container(sib):
        return sib
    if heading_tag.parent is not None:
        sib2 = heading_tag.parent.find_next_sibling()
        if looks_like_notes_container(sib2):
            return sib2

    # If the heading and images share a tight wrapper, allow the parent (but never body/html).
    parent = heading_tag.parent
    if looks_like_notes_container(parent):
        return parent

    # Forward scan in document order, but stop at the next notes heading.
    steps = 0
    for el in heading_tag.find_all_next(True, limit=120):
        steps += 1
        t = (text_of(el) or "").strip()
        if t and re.fullmatch(r"(top|middle|base)\s+notes", t, flags=re.I):
            break
        if looks_like_notes_container(el):
            return el
    return None


def extract_notes_with_strength(soup: BeautifulSoup) -> dict[str, list[dict[str, Any]]]:
    """
    DOM-based extraction of note pyramid with per-note strength derived from image sizing.

    Returns:
      { "top": [...], "middle": [...], "base": [...] }
    Each item:
      {"note": str, "strength": int, "strength_norm": float}
    """
    groups: dict[str, list[dict[str, Any]]] = {"top": [], "middle": [], "base": []}
    labels = {"top": "Top Notes", "middle": "Middle Notes", "base": "Base Notes"}

    for key, heading_text in labels.items():
        heading_tags = _find_heading_tags(soup, heading_text)
        container = None
        for h in heading_tags:
            container = _find_notes_container_from_heading(h)
            if container is not None:
                break
        if container is None:
            continue

        # Collect in DOM order, then dedupe case-insensitively (keep first occurrence).
        items: list[dict[str, Any]] = []
        seen_ci: set[str] = set()
        dom_idx = 0
        imgs = container.select("img")
        for img in imgs:
            dom_idx += 1
            src = img.get("src") or img.get("data-src") or img.get("data-lazy-src") or ""
            if isinstance(src, str):
                sl = src.lower()
                # Strongly prefer note images, but don't hard-fail if Fragrantica changes paths.
                if "/mdimg/sastojci/" not in sl and "/mdimg/notes/" not in sl:
                    # Still allow, but only if it has a note label.
                    pass

            note = _extract_note_label(img)
            if not note:
                continue
            kci = note.lower()
            if kci in seen_ci:
                continue
            seen_ci.add(kci)

            w, h = _img_dims_from_tag(img)
            strength = _strength_from_dims(w, h)
            if strength == 0 and (w is None and h is None):
                # Keep it, but warn (once per note) that sizing couldn't be parsed.
                _warn("note_size_missing", note=note, group=key)

            items.append({"note": note, "strength": int(strength), "_dom_idx": dom_idx})

        # Sort by strength DESC, tie-break by original DOM order.
        items.sort(key=lambda x: (-int(x.get("strength") or 0), int(x.get("_dom_idx") or 0)))

        max_strength = max((int(x.get("strength") or 0) for x in items), default=0)
        for it in items:
            s = int(it.get("strength") or 0)
            it["strength_norm"] = float(s / max_strength) if max_strength > 0 else 0.0
            it.pop("_dom_idx", None)

        groups[key] = items

    return groups


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
    download_all_images: bool,
    write_notes_map: bool,
) -> tuple[dict[str, Any], Path, Optional[Path]]:
    url = canonicalize_url(perfume_url)
    pid = perfume_id_from_url(url) or sha256_text(url)[:16]

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

    # Name: DOM-based clean name (avoid SEO tails)
    name_raw = text_of(soup.select_one("h1")) or ""
    fallback_name, name_extras = clean_name_from_heading(name_raw, brand=brand)
    name_clean = extract_clean_name(soup, fallback_name or name_raw or "")

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
    # Keep legacy notes (text-based) for debugging/back-compat, but primary output uses DOM+strength.
    legacy_notes = extract_notes(soup)
    notes_strength = extract_notes_with_strength(soup)
    notes_ranked = {k: [x["note"] for x in v] for k, v in notes_strength.items()}

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
    if download_all_images:
        gallery_dir = images_dir / "gallery"
        ensure_dir(gallery_dir)

    sess = requests.Session()
    sess.headers.update({"User-Agent": USER_AGENT, "Accept": "image/*,*/*;q=0.8"})

    # Required: deterministic main product image (bottle photo)
    main_url = get_main_product_image(pid, soup, base_url=fetch.final_url)
    main_image_rel: Optional[str] = None
    if main_url:
        ext = _ext_from_url(main_url)
        content = download_bytes(sess, main_url, timeout_s=30)
        if content:
            out_path = images_dir / f"main{ext}"
            out_path.write_bytes(content)
            main_image_rel = str(out_path.relative_to(root).as_posix())
        else:
            _warn("main_image_download_failed", perfume_id=pid, url=main_url)
    else:
        _warn("main_image_not_found", perfume_id=pid, url=url)

    # Optional: keep legacy heroes/gallery only when explicitly enabled
    images_saved: dict[str, Any] = {"main": main_image_rel, "hero_og": None, "hero_twitter": None, "gallery": []}
    if download_all_images:
        images_info = gather_image_urls(soup, base_url=fetch.final_url, perfume_id=pid, include_photogram=include_photogram)

        def save_named_hero(url0: Optional[str], filename: str) -> Optional[str]:
            if not url0:
                return None
            content2 = download_bytes(sess, url0)
            if not content2:
                return None
            jpg2 = _try_convert_to_jpg(content2)
            out_path2 = images_dir / filename
            out_path2.write_bytes(jpg2 if jpg2 is not None else content2)
            return str(out_path2.relative_to(root).as_posix())

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

            content3 = download_bytes(sess, u)
            if not content3:
                continue
            jpg3 = _try_convert_to_jpg(content3) or content3
            digest = hashlib.sha256(jpg3).hexdigest()[:16]
            out_path3 = (images_dir / "gallery") / f"{digest}.jpg"
            if not out_path3.exists():
                out_path3.write_bytes(jpg3)
            images_saved["gallery"].append(str(out_path3.relative_to(root).as_posix()))
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
        "name": name_clean,
        "name_raw": name_raw,
        "name_extras": name_extras,
        "description": desc,
        "rating": {"value": rating, "votes": votes},
        "main_accords": accords,
        "main_image": main_image_rel,
        "notes_ranked": notes_ranked,
        "notes_strength": notes_strength,
        "notes_legacy": legacy_notes,
        "performance": {
            "longevity_score": longevity_score,
            "sillage_score": sillage_score,
            **sliders,
        },
        "images": images_saved,
    }

    meta_path = root / "meta.json"
    write_json(meta_path, meta)

    if write_notes_map:
        notes_map: dict[str, Any] = {
            name_clean: {
                "top_notes": notes_ranked.get("top") or [],
                "middle_notes": notes_ranked.get("middle") or [],
                "base_notes": notes_ranked.get("base") or [],
            }
        }
        write_json(root / "notes_map.json", notes_map)
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
    pid = perfume_id_from_url(url) or sha256_text(url)[:16]

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
            download_all_images=bool(args.download_all_images),
            write_notes_map=bool(args.write_notes_map),
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
                download_all_images=bool(args.download_all_images),
                write_notes_map=bool(args.write_notes_map),
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
    p_work.add_argument(
        "--download-all-images",
        action="store_true",
        help="Download extra images (heroes + gallery). Default is main image only.",
    )
    g_nm = p_work.add_mutually_exclusive_group()
    g_nm.add_argument("--write-notes-map", dest="write_notes_map", action="store_true", default=True)
    g_nm.add_argument("--no-write-notes-map", dest="write_notes_map", action="store_false")
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
    p_one.add_argument(
        "--download-all-images",
        action="store_true",
        help="Download extra images (heroes + gallery). Default is main image only.",
    )
    g_nm2 = p_one.add_mutually_exclusive_group()
    g_nm2.add_argument("--write-notes-map", dest="write_notes_map", action="store_true", default=True)
    g_nm2.add_argument("--no-write-notes-map", dest="write_notes_map", action="store_false")
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

