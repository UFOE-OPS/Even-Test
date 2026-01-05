from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import hashlib
import json
import os
import re
import time
from pathlib import Path
from typing import Any, Iterable, Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from slugify import slugify
from tqdm import tqdm


USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/121.0.0.0 Safari/537.36"
)


@dataclasses.dataclass(frozen=True)
class FetchResult:
    url: str
    final_url: str
    html: str
    used_playwright: bool
    extracted: Optional[dict[str, Any]] = None


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def is_probable_interstitial(html: str) -> bool:
    h = html.lower()
    # Common “checking your browser” / “just a moment” patterns.
    return any(
        s in h
        for s in (
            "checking your browser",
            "just a moment",
            "cf-browser-verification",
            "cloudflare",
            "attention required",
            "verify you are human",
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
    html = resp.text
    return FetchResult(
        url=url,
        final_url=str(resp.url),
        html=html,
        used_playwright=False,
        extracted=None,
    )


def fetch_html_playwright(url: str, timeout_s: int = 45) -> FetchResult:
    # Imported lazily so users without Playwright can still run for non-interstitial pages.
    from playwright.sync_api import sync_playwright  # type: ignore

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(user_agent=USER_AGENT, viewport={"width": 1280, "height": 900})
        page = ctx.new_page()
        page.goto(url, wait_until="domcontentloaded", timeout=timeout_s * 1000)
        # Let JS settle (Fragrantica often renders parts after DOMContentLoaded).
        page.wait_for_timeout(1500)
        extracted = page.evaluate(
            """
            () => {
              const norm = (s) => (s || "").replace(/\\s+/g, " ").trim();
              const textEq = (el, target) => norm(el?.innerText).toLowerCase() === target.toLowerCase();

              const findLabelEl = (label) => {
                const target = label.toLowerCase();
                // Prefer short elements
                const els = Array.from(document.querySelectorAll("span,div,li,strong,em,p,a,td,th,label,h1,h2,h3"));
                for (const el of els) {
                  if (!el) continue;
                  const t = norm(el.innerText).toLowerCase();
                  if (t === target) return el;
                }
                return null;
              };

              const percentFromStyle = (el) => {
                if (!el) return null;
                const w = (el.style && el.style.width) ? el.style.width : "";
                const m = /([0-9]{1,3})\\s*%/.exec(w);
                if (m) {
                  const v = parseInt(m[1], 10);
                  if (!isNaN(v) && v >= 0 && v <= 100) return v;
                }
                return null;
              };

              const percentFromGeometry = (bar, container) => {
                try {
                  const b = bar.getBoundingClientRect();
                  const c = container.getBoundingClientRect();
                  if (!c.width || c.width <= 0) return null;
                  const v = Math.round((b.width / c.width) * 100);
                  if (v >= 0 && v <= 100) return v;
                } catch (e) {}
                return null;
              };

              const findBarNear = (labelEl) => {
                if (!labelEl) return null;
                // Search within a small ancestor scope for a plausible bar element.
                let scope = labelEl.parentElement;
                for (let up = 0; up < 4 && scope; up++) {
                  const divs = Array.from(scope.querySelectorAll("div,span"));
                  // Heuristic: thin-ish, non-empty elements often represent bars.
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

              const h1 = norm(document.querySelector("h1")?.innerText);
              return {
                h1,
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
    return FetchResult(
        url=url,
        final_url=final_url,
        html=html,
        used_playwright=True,
        extracted=extracted if isinstance(extracted, dict) else None,
    )


def fetch_html(url: str, allow_playwright_fallback: bool = True) -> FetchResult:
    r = fetch_html_requests(url)
    if allow_playwright_fallback and is_probable_interstitial(r.html):
        return fetch_html_playwright(url)
    return r


def parse_jsonld_products(soup: BeautifulSoup) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for tag in soup.select('script[type="application/ld+json"]'):
        raw = (tag.string or "").strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except Exception:
            # Some pages contain multiple JSON objects or invalid JSON-LD; ignore quietly.
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
    v = v.strip()
    v = v.replace(",", "")
    m = re.search(r"(\d+)", v)
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None


def normalize_rating(v: str) -> Optional[float]:
    m = re.search(r"(\d+(?:\.\d+)?)", v.strip())
    if not m:
        return None
    try:
        return float(m.group(1))
    except Exception:
        return None


def extract_rating_votes_from_text(page_text: str) -> tuple[Optional[float], Optional[int]]:
    # Fragrantica shows patterns like “4.12 / 5” and “(4,321 votes)” or “4321 votes”.
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
    # Best-effort: “Longevity 3.50 out of 5” / “Sillage 2.40 out of 4”
    longevity = None
    sillage = None
    m = re.search(r"Longevity\s+(\d+(?:\.\d+)?)\s+out\s+of\s+5", page_text, flags=re.I)
    if m:
        longevity = normalize_rating(m.group(1))
    m2 = re.search(r"Sillage\s+(\d+(?:\.\d+)?)\s+out\s+of\s+4", page_text, flags=re.I)
    if m2:
        sillage = normalize_rating(m2.group(1))
    return longevity, sillage


def extract_season_daynight_percent(html: str) -> dict[str, dict[str, Optional[int]]]:
    # Pages sometimes embed percentages in JS / JSON-ish blobs.
    # We scan for common labels with percent values nearby.
    # Returns ints 0..100 when found.
    h = html

    def find_pct(label: str) -> Optional[int]:
        # Try a few regexes: label ... 45% OR "label":45 OR label: 45
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
    """
    Try to extract a clean product name from the page H1, plus optional structured fields.
    Example input:
      "Paradigme Prada cologne - a new fragrance for men 2025"
    Output:
      name="Paradigme", extras={"gender":"men","year":2025,"concentration":"cologne"}
    """
    if not h1_text:
        return None, {}
    t = re.sub(r"\s+", " ", h1_text).strip()

    extras: dict[str, Any] = {}

    # Peel off SEO tail.
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

    # Concentration (best-effort)
    m_conc = re.search(
        r"\b(parfum|extrait|edp|eau de parfum|edt|eau de toilette|cologne|eau de cologne)\b",
        t,
        flags=re.I,
    )
    if m_conc:
        conc = m_conc.group(1).lower()
        conc = {
            "edp": "eau de parfum",
            "edt": "eau de toilette",
        }.get(conc, conc)
        extras["concentration"] = conc
        t = re.sub(
            r"\b(parfum|extrait|edp|eau de parfum|edt|eau de toilette|cologne|eau de cologne)\b",
            "",
            t,
            flags=re.I,
        ).strip()

    # If brand appears in H1, take left side as the name.
    if brand:
        b = re.sub(r"\s+", " ", brand).strip()
        idx = t.lower().find(b.lower())
        if idx > 0:
            left = t[:idx].strip()
            if left:
                return left, extras

    # Otherwise: take the first chunk before common separators.
    t2 = re.split(r"\s+\bby\b\s+|\s+\(|\s+–\s+|\s+-\s+", t, maxsplit=1)[0].strip()
    return (t2 or None), extras


def extract_name_from_dom(soup: BeautifulSoup, brand: Optional[str]) -> tuple[Optional[str], dict[str, Any]]:
    # Prefer an actual H1 rather than og:title / <title> (which often include SEO text).
    h1 = soup.select_one("h1")
    h1_text = text_of(h1) or ""
    return clean_name_from_heading(h1_text, brand=brand)


def is_valid_accord_label(label: str) -> bool:
    l = re.sub(r"\s+", " ", (label or "")).strip()
    if not l:
        return False
    if len(l) > 30:
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
    # Only allow simple label-like strings.
    if not re.fullmatch(r"[A-Za-z][A-Za-z '\-]{0,29}", l):
        return False
    return True


def extract_main_accords_dom(soup: BeautifulSoup) -> list[str]:
    """
    Parse main accords from the accord bar DOM, not from page text (avoids UI/widget pollution).
    """
    out: list[str] = []
    # Look for elements whose class mentions "accord" and whose style indicates a bar width.
    for el in soup.find_all(True, class_=re.compile(r"accord", flags=re.I)):
        style = (el.get("style") or "").lower()
        if not re.search(r"width\s*:\s*\d{1,3}\s*%", style):
            continue
        label = el.get_text(" ", strip=True)
        if not is_valid_accord_label(label):
            continue
        out.append(re.sub(r"\s+", " ", label).strip())

    # Fallback: sometimes the label is in a nested span within the bar element.
    if not out:
        for bar in soup.select('[style*="width"]'):
            style = (bar.get("style") or "").lower()
            if "width" not in style or "%" not in style:
                continue
            cls = " ".join(bar.get("class") or [])
            if not re.search(r"accord", cls, flags=re.I):
                continue
            label = bar.get_text(" ", strip=True)
            if is_valid_accord_label(label):
                out.append(re.sub(r"\s+", " ", label).strip())

    # De-dupe while keeping order
    seen: set[str] = set()
    dedup: list[str] = []
    for a in out:
        k = a.lower()
        if k in seen:
            continue
        seen.add(k)
        dedup.append(a)
    return dedup[:15]


def filter_perfume_image_urls(image_urls: list[str], perfume_id: str) -> list[str]:
    """
    Keep only perfume-specific images and explicitly exclude known noise.
    Priority:
      1) hero 375x500.<ID> or m.<ID>
      2) perfume-social-cards with <ID>
      3) photogram images (fimgs.net/photogram), limited
    """
    pid = (perfume_id or "").strip()

    def is_noise(u: str) -> bool:
        return any(
            s in u
            for s in (
                "/mdimg/news/",
                "/mdimg/sastojci/",
                "/mdimg/perfume-thumbs/s.",
            )
        )

    def is_hero_375(u: str) -> bool:
        if not pid:
            return False
        return bool(re.search(rf"/mdimg/perfume-thumbs/375x500\.{re.escape(pid)}\.", u))

    def is_hero_m(u: str) -> bool:
        if not pid:
            return False
        return bool(re.search(rf"/mdimg/perfume/m\.{re.escape(pid)}\.", u))

    def is_social(u: str) -> bool:
        return bool(pid and ("/mdimg/perfume-social-cards/" in u) and (pid in u))

    def is_photogram(u: str) -> bool:
        return "fimgs.net/photogram/" in u

    def is_wrong_other_perfume(u: str) -> bool:
        if not pid:
            return False
        m = re.search(r"/mdimg/perfume/(?:m|s)\.(\d+)\.", u)
        return bool(m and m.group(1) != pid)

    cleaned = [u for u in image_urls if isinstance(u, str)]
    cleaned = [u for u in cleaned if not is_noise(u)]
    cleaned = [u for u in cleaned if not is_wrong_other_perfume(u)]

    hero = [u for u in cleaned if is_hero_375(u)] + [u for u in cleaned if is_hero_m(u)]
    social = [u for u in cleaned if is_social(u)]
    photogram = [u for u in cleaned if is_photogram(u)]

    # Keep order but de-dupe.
    out: list[str] = []
    seen: set[str] = set()
    for group, limit in ((hero, 2), (social, 3), (photogram, 10)):
        for u in group[:limit]:
            if u in seen:
                continue
            seen.add(u)
            out.append(u)
    return out

def extract_main_accords(soup: BeautifulSoup) -> list[str]:
    return extract_main_accords_dom(soup)


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

    # De-dupe within each bucket
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


def extract_minimal_image_urls(
    soup: BeautifulSoup, base_url: str, perfume_id: str
) -> list[str]:
    """
    Keep downloads minimal and useful:
    - JSON-LD Product `image`
    - `og:image`
    - first “large” <img> candidate
    """
    urls: list[str] = []

    # JSON-LD images
    for prod in parse_jsonld_products(soup):
        img = prod.get("image")
        if isinstance(img, str):
            urls.append(img)
        elif isinstance(img, list):
            for u in img:
                if isinstance(u, str):
                    urls.append(u)

    # og:image
    og = soup.select_one('meta[property="og:image"]')
    og_url = og.get("content") if og else None
    if isinstance(og_url, str) and og_url.strip():
        urls.append(og_url.strip())

    # heuristic: first <img> that looks like a perfume/bottle image
    candidates: list[str] = []
    for img in soup.select("img"):
        src = img.get("src") or img.get("data-src") or img.get("data-lazy-src")
        if not isinstance(src, str) or not src.strip():
            continue
        s = src.strip()
        alt = (img.get("alt") or "").lower()
        cls = " ".join(img.get("class") or []).lower()
        if any(t in alt for t in ("perfume", "eau de", "bottle", "flacon")) or any(
            t in cls for t in ("perfume", "bottle", "flacon", "product")
        ):
            candidates.append(s)
    if candidates:
        urls.append(candidates[0])

    # Normalize to absolute, de-dupe.
    normed: list[str] = []
    seen: set[str] = set()
    for u in urls:
        abs_u = urljoin(base_url, u)
        if abs_u in seen:
            continue
        seen.add(abs_u)
        normed.append(abs_u)
    return filter_perfume_image_urls(normed, perfume_id=perfume_id)


def infer_ext_from_content_type(ct: str) -> str:
    ct = (ct or "").split(";")[0].strip().lower()
    return {
        "image/jpeg": ".jpg",
        "image/jpg": ".jpg",
        "image/png": ".png",
        "image/webp": ".webp",
        "image/gif": ".gif",
    }.get(ct, "")


def download_images(
    image_urls: list[str],
    out_dir: Path,
    max_images: int = 3,
    timeout_s: int = 30,
    sleep_s: float = 0.25,
) -> tuple[Optional[str], list[str]]:
    """
    Downloads at most `max_images`.
    Returns (main_relative_path, extra_relative_paths) relative to perfume root.
    """
    ensure_dir(out_dir)
    extra_dir = out_dir / "extra"
    ensure_dir(extra_dir)
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT, "Accept": "image/*,*/*;q=0.8"})

    for idx, url in enumerate(image_urls[:max_images]):
        try:
            resp = session.get(url, timeout=timeout_s)
            resp.raise_for_status()
        except Exception:
            continue

        ext = infer_ext_from_content_type(resp.headers.get("Content-Type", ""))
        if not ext:
            # Try URL path ext
            path = urlparse(url).path
            _, ext2 = os.path.splitext(path)
            ext = ext2 if ext2.lower() in (".jpg", ".jpeg", ".png", ".webp", ".gif") else ".jpg"
        if ext == ".jpeg":
            ext = ".jpg"

        digest = hashlib.sha256(resp.content).hexdigest()[:16]
        if idx == 0:
            target = out_dir / f"main{ext}"
            target.write_bytes(resp.content)
        else:
            # Name deterministically by content hash to avoid duplicates across pages/runs.
            target = extra_dir / f"{digest}{ext}"
            if not target.exists():
                target.write_bytes(resp.content)
        time.sleep(sleep_s)

    main_candidates = sorted(out_dir.glob("main.*"))
    if not main_candidates:
        return None, []

    main = main_candidates[0].as_posix()
    extra = sorted([p.as_posix() for p in extra_dir.iterdir() if p.is_file()])
    return main, extra


def perfume_id_from_url(url: str) -> str:
    # Try to capture Fragrantica numeric id from trailing “-12345.html”
    path = urlparse(url).path
    m = re.search(r"-(\d+)\.html?$", path)
    return m.group(1) if m else ""


def find_existing_perfume_dir_by_id(out_root: Path, perfume_id: str) -> Optional[Path]:
    if not perfume_id:
        return None
    perfumes_dir = out_root / "perfumes"
    if not perfumes_dir.exists():
        return None
    try:
        for d in perfumes_dir.iterdir():
            if not d.is_dir():
                continue
            if not d.name.endswith(f"-{perfume_id}"):
                continue
            if (d / "meta.json").exists():
                return d
    except Exception:
        return None
    return None


def build_perfume_slug(name: Optional[str], brand: Optional[str], url: str) -> str:
    base = ""
    if brand and name:
        base = f"{brand} {name}"
    elif name:
        base = name
    else:
        base = urlparse(url).path.strip("/").split("/")[-1] or url
    s = slugify(base)
    pid = perfume_id_from_url(url)
    return f"{s}-{pid}" if pid and pid not in s else s


def parse_perfume(html: str, url: str) -> tuple[dict[str, Any], list[str]]:
    soup = BeautifulSoup(html, "lxml")
    prods = parse_jsonld_products(soup)
    prod0 = prods[0] if prods else {}

    og_title = soup.select_one('meta[property="og:title"]')
    og_desc = soup.select_one('meta[property="og:description"]')

    # Prefer DOM H1; fall back to JSON-LD; then og:title / <title>.
    brand = None
    b = prod0.get("brand") if isinstance(prod0, dict) else None
    if isinstance(b, dict) and isinstance(b.get("name"), str):
        brand = b["name"].strip()
    elif isinstance(b, str):
        brand = b.strip()

    h1_name, name_extras = extract_name_from_dom(soup, brand=brand)

    title_text = pick_first_str(
        h1_name,
        (prod0.get("name") if isinstance(prod0, dict) else None),
        (og_title.get("content") if og_title else None),
        (soup.title.string if soup.title else None),
    )
    desc_text = pick_first_str(
        (prod0.get("description") if isinstance(prod0, dict) else None),
        (og_desc.get("content") if og_desc else None),
    )

    page_text = soup.get_text(" ", strip=True)
    rating, votes = extract_rating_votes_from_text(page_text)
    longevity_score, sillage_score = extract_longevity_sillage_scores(page_text)
    accords = extract_main_accords(soup)
    notes = extract_notes(soup)
    perf_pcts = extract_season_daynight_percent(html)

    pid = perfume_id_from_url(url)
    images = extract_minimal_image_urls(soup, base_url=url, perfume_id=pid)

    meta: dict[str, Any] = {
        "id": None,  # filled after slug is computed
        "source": {
            "site": "fragrantica",
            "url": url,
            "scraped_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        },
        "name": title_text,
        "brand": brand,
        "description": desc_text,
        "name_extras": name_extras,
        "rating": {"value": rating, "votes": votes},
        "main_accords": accords,
        "notes": notes,
        "performance": {
            "longevity_score": longevity_score,
            "sillage_score": sillage_score,
            **perf_pcts,
        },
    }
    return meta, images


def write_json(path: Path, data: Any) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def scrape_one_perfume(
    url: str,
    out_root: Path,
    force: bool = False,
    max_images: int = 3,
    allow_playwright_fallback: bool = True,
) -> Path:
    # Repeatable runs: if we can identify the Fragrantica numeric id and it already exists locally,
    # skip network work unless --force is set.
    pid = perfume_id_from_url(url)
    if not force and pid:
        existing = find_existing_perfume_dir_by_id(out_root, pid)
        if existing:
            return existing

    fetch = fetch_html(url, allow_playwright_fallback=allow_playwright_fallback)
    html = fetch.html

    meta, image_urls = parse_perfume(html, url=fetch.final_url)

    slug = build_perfume_slug(meta.get("name"), meta.get("brand"), fetch.final_url)
    perfume_dir = out_root / "perfumes" / slug
    ensure_dir(perfume_dir)

    # Save HTML snapshot for debugging extraction improvements
    html_dir = perfume_dir / "html"
    ensure_dir(html_dir)
    html_path = html_dir / "page.html"
    if force or not html_path.exists():
        html_path.write_text(html, encoding="utf-8", errors="ignore")

    # Download minimal images
    images_dir = perfume_dir / "images"
    main_rel = None
    extra_rel: list[str] = []
    if force or not images_dir.exists() or not any(images_dir.iterdir()):
        main_abs, extra_abs = download_images(image_urls, images_dir, max_images=max_images)
        if main_abs:
            main_rel = str(Path(main_abs).relative_to(perfume_dir).as_posix())
        extra_rel = [str(Path(p).relative_to(perfume_dir).as_posix()) for p in extra_abs]
    else:
        # Reuse existing images if present (repeatable runs without re-downloading).
        main_candidates = sorted(images_dir.glob("main.*"))
        if main_candidates:
            main_rel = str(main_candidates[0].relative_to(perfume_dir).as_posix())
        extra_dir = images_dir / "extra"
        if extra_dir.exists():
            extras = sorted([p for p in extra_dir.iterdir() if p.is_file()])
            extra_rel = [str(p.relative_to(perfume_dir).as_posix()) for p in extras[: max(0, max_images - 1)]]

    meta["id"] = slug
    meta["source"]["final_url"] = fetch.final_url
    meta["source"]["html_sha256"] = sha256_text(html)
    meta["source"]["used_playwright"] = fetch.used_playwright
    meta["source"]["fetched_with"] = "playwright" if fetch.used_playwright else "requests"
    if fetch.extracted:
        meta["source"]["playwright_extracted"] = fetch.extracted
        # Prefer Playwright-extracted slider values when available.
        perf = meta.get("performance") or {}
        if isinstance(perf, dict):
            seasonality = (fetch.extracted.get("seasonality") if isinstance(fetch.extracted, dict) else None) or {}
            daynight = (fetch.extracted.get("daynight") if isinstance(fetch.extracted, dict) else None) or {}
            if isinstance(seasonality, dict) and seasonality:
                perf["seasonality"] = {
                    "spring": seasonality.get("spring"),
                    "summer": seasonality.get("summer"),
                    "fall": seasonality.get("fall"),
                    "winter": seasonality.get("winter"),
                }
            if isinstance(daynight, dict) and daynight:
                perf["daynight"] = {"day": daynight.get("day"), "night": daynight.get("night")}
            meta["performance"] = perf
    meta["images"] = {"main": main_rel, "extra": extra_rel}

    write_json(perfume_dir / "meta.json", meta)
    return perfume_dir


def discover_perfume_links_from_designer(designer_url: str, html: str) -> list[str]:
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
    # De-dupe while keeping order
    seen: set[str] = set()
    out: list[str] = []
    for u in links:
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out


def cmd_perfume(args: argparse.Namespace) -> None:
    out_root = Path(args.out).expanduser().resolve()
    ensure_dir(out_root)
    perfume_dir = scrape_one_perfume(
        args.url,
        out_root=out_root,
        force=args.force,
        max_images=args.max_images,
        allow_playwright_fallback=not args.no_playwright,
    )
    print(str(perfume_dir))


def cmd_designer(args: argparse.Namespace) -> None:
    out_root = Path(args.out).expanduser().resolve()
    ensure_dir(out_root)
    fetch = fetch_html(args.url, allow_playwright_fallback=not args.no_playwright)
    links = discover_perfume_links_from_designer(fetch.final_url, fetch.html)
    if args.limit:
        links = links[: args.limit]

    ensure_dir(out_root / "designer_runs")
    run_stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_path = out_root / "designer_runs" / f"{slugify(args.url)}_{run_stamp}.json"
    write_json(run_path, {"designer_url": args.url, "final_url": fetch.final_url, "links": links})

    for url in tqdm(links, desc="Scraping perfumes"):
        try:
            scrape_one_perfume(
                url,
                out_root=out_root,
                force=args.force,
                max_images=args.max_images,
                allow_playwright_fallback=not args.no_playwright,
            )
        except Exception:
            # Keep going; the HTML snapshots per perfume help debugging.
            continue
        time.sleep(args.sleep)

    print(str(run_path))


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Local Fragrantica harvester")
    sub = p.add_subparsers(dest="cmd", required=True)

    p_perf = sub.add_parser("perfume", help="Scrape a single perfume page")
    p_perf.add_argument("url", help="Fragrantica perfume URL")
    p_perf.add_argument("--out", default="data/fragrantica", help="Output dataset root folder")
    p_perf.add_argument("--force", action="store_true", help="Re-fetch/rewrite even if cached")
    p_perf.add_argument(
        "--max-images",
        type=int,
        default=3,
        help="Max images to download per perfume (default: 3)",
    )
    p_perf.add_argument(
        "--no-playwright",
        action="store_true",
        help="Disable Playwright fallback (faster setup; may fail on interstitial pages)",
    )
    p_perf.set_defaults(func=cmd_perfume)

    p_des = sub.add_parser("designer", help="Discover + scrape perfumes from a designer page")
    p_des.add_argument("url", help="Fragrantica designer URL")
    p_des.add_argument("--out", default="data/fragrantica", help="Output dataset root folder")
    p_des.add_argument("--limit", type=int, default=0, help="Max perfumes to scrape (0 = no limit)")
    p_des.add_argument(
        "--sleep", type=float, default=0.5, help="Seconds to sleep between perfume scrapes"
    )
    p_des.add_argument("--force", action="store_true", help="Re-fetch/rewrite even if cached")
    p_des.add_argument(
        "--max-images",
        type=int,
        default=3,
        help="Max images to download per perfume (default: 3)",
    )
    p_des.add_argument(
        "--no-playwright",
        action="store_true",
        help="Disable Playwright fallback (faster setup; may fail on interstitial pages)",
    )
    p_des.set_defaults(func=cmd_designer)

    return p


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()

