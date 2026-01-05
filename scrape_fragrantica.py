#!/usr/bin/env python3
"""
Fragrantica Scraper (local)

- Scrape a single perfume URL OR crawl a designer page to collect perfume URLs.
- Save per-perfume folders:
  out/fragrantica/<brand_slug>/<perfume_id>-<perfume_slug>/
    meta.json
    images/
    html/page.html (optional)

Best-effort extraction:
- rating + votes, main accords, notes pyramid, longevity, sillage
- tries to find season/day-night vote data (site-dependent)

Run:
  python scrape_fragrantica.py scrape --url "https://www.fragrantica.com/perfume/Prada/Paradigme-110661.html"
  python scrape_fragrantica.py crawl-designer --url "https://www.fragrantica.com/designers/Jean-Paul-Gaultier.html" --limit 50
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from slugify import slugify
from tqdm import tqdm


USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

DEFAULT_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}


@dataclass
class FetchResult:
    url: str
    html: str
    used: str  # "requests" or "playwright"


def _safe_filename(name: str, max_len: int = 140) -> str:
    name = re.sub(r"[^\w\-. ]+", "_", name, flags=re.UNICODE).strip()
    name = re.sub(r"\s+", " ", name).strip()
    if len(name) > max_len:
        name = name[:max_len].rstrip()
    return name or "file"


def _hash(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:16]


def polite_sleep(min_s: float = 1.0, max_s: float = 2.5) -> None:
    time.sleep(random.uniform(min_s, max_s))


def fetch_html_requests(url: str, timeout: int = 30, retries: int = 3) -> str:
    sess = requests.Session()
    sess.headers.update(DEFAULT_HEADERS)

    last_err: Optional[Exception] = None
    for _ in range(retries):
        try:
            r = sess.get(url, timeout=timeout)
            # Some sites return 200 with "checking your browser" interstitial text
            if r.status_code >= 400:
                raise RuntimeError(f"HTTP {r.status_code} for {url}")
            return r.text
        except Exception as e:
            last_err = e
            polite_sleep(1.2, 2.8)
    raise RuntimeError(f"Failed to fetch {url}: {last_err}")


def fetch_html_playwright(url: str, timeout_ms: int = 45_000) -> str:
    # Lazy import so it’s optional
    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(user_agent=USER_AGENT, locale="en-US")
        page = ctx.new_page()
        page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
        # Give some time for JS-rendered pieces (charts etc.)
        page.wait_for_timeout(1500)
        html = page.content()
        browser.close()
        return html


def fetch_html(url: str, allow_playwright: bool = True) -> FetchResult:
    html = fetch_html_requests(url)
    # Heuristic: if we got blocked/interstitial, try Playwright
    interstitial_signals = [
        "cf-chl", "cloudflare", "Checking your browser", "Attention Required",
        "Just a moment", "verify you are human"
    ]
    if allow_playwright and any(sig.lower() in html.lower() for sig in interstitial_signals):
        try:
            html = fetch_html_playwright(url)
            return FetchResult(url=url, html=html, used="playwright")
        except Exception:
            # fallback to requests content if playwright fails
            return FetchResult(url=url, html=html, used="requests")
    return FetchResult(url=url, html=html, used="requests")


def extract_json_ld(soup: BeautifulSoup) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for tag in soup.find_all("script", attrs={"type": "application/ld+json"}):
        try:
            data = json.loads(tag.get_text(strip=True))
            if isinstance(data, list):
                out.extend([d for d in data if isinstance(d, dict)])
            elif isinstance(data, dict):
                out.append(data)
        except Exception:
            continue
    return out


def pick_product_ld(jsonlds: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    # Not always present, but if it is, it’s usually the cleanest for name/image/description
    for d in jsonlds:
        t = d.get("@type")
        if t in ("Product", "IndividualProduct"):
            return d
    return None


def extract_meta_content(soup: BeautifulSoup, key: str, attr: str = "property") -> Optional[str]:
    tag = soup.find("meta", attrs={attr: key})
    if tag and tag.get("content"):
        return tag["content"].strip()
    return None


def extract_rating_votes(text: str) -> Tuple[Optional[float], Optional[int]]:
    # Example: "Perfume rating 3.72 out of 5 with 2,230 votes"
    m = re.search(r"Perfume rating\s*([0-9]+(?:\.[0-9]+)?)\s*out of\s*5\s*with\s*([\d,]+)\s*votes",
                  text, flags=re.IGNORECASE)
    if not m:
        return None, None
    rating = float(m.group(1))
    votes = int(m.group(2).replace(",", ""))
    return rating, votes


def extract_longevity_sillage(text: str) -> Tuple[Optional[float], Optional[float]]:
    # Example: "Perfume longevity:3.84 out of 5." and "Perfume sillage:2.63 out of 4."
    lon = None
    sil = None
    m1 = re.search(r"Perfume longevity\s*:\s*([0-9]+(?:\.[0-9]+)?)\s*out of\s*5", text, flags=re.IGNORECASE)
    if m1:
        lon = float(m1.group(1))
    m2 = re.search(r"Perfume sillage\s*:\s*([0-9]+(?:\.[0-9]+)?)\s*out of\s*4", text, flags=re.IGNORECASE)
    if m2:
        sil = float(m2.group(1))
    return lon, sil


def extract_main_accords(text: str) -> List[str]:
    """
    Best-effort: find the "main accords" block in the page text.
    We grab the next ~20 lines after the header until we hit a known boundary.
    """
    lines = [ln.strip() for ln in text.splitlines()]
    idx = None
    for i, ln in enumerate(lines):
        if ln.lower() == "main accords":
            idx = i
            break
    if idx is None:
        # fallback: sometimes it appears as "main accords" with surrounding whitespace
        for i, ln in enumerate(lines):
            if "main accords" in ln.lower():
                idx = i
                break
    if idx is None:
        return []

    accords: List[str] = []
    for ln in lines[idx + 1: idx + 40]:
        if not ln:
            continue
        boundary = ln.lower()
        if boundary.startswith("perfume rating") or boundary.startswith("online shops") or boundary.startswith("notes"):
            break
        # accords are usually short words/phrases
        if len(ln) <= 32 and re.search(r"[a-zA-Z]", ln):
            accords.append(ln)
    # de-dupe preserving order
    seen: Set[str] = set()
    out = []
    for a in accords:
        k = a.lower()
        if k not in seen:
            seen.add(k)
            out.append(a)
    return out


def extract_notes_pyramid(text: str) -> Dict[str, List[str]]:
    """
    Best-effort parse for Top/Middle/Base notes based on common headings.
    """
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    def collect_after(header: str, stop_headers: Set[str]) -> List[str]:
        try:
            start = next(i for i, ln in enumerate(lines) if ln.lower() == header.lower())
        except StopIteration:
            return []
        out: List[str] = []
        for ln in lines[start + 1:]:
            if ln.lower() in stop_headers:
                break
            # skip “Image” artifacts and other UI words
            if ln.lower() in {"image", "vote for ingredients", "designer"}:
                continue
            # keep note-looking tokens
            if 1 <= len(ln) <= 60:
                out.append(ln)
        # de-dupe
        seen: Set[str] = set()
        deduped = []
        for x in out:
            k = x.lower()
            if k not in seen:
                seen.add(k)
                deduped.append(x)
        return deduped

    stop = {"middle notes", "base notes", "vote for ingredients", "designer", "collections"}
    top = collect_after("Top Notes", stop_headers=stop)
    mid = collect_after("Middle Notes", stop_headers=stop)
    base = collect_after("Base Notes", stop_headers=stop)
    return {"top": top, "middle": mid, "base": base}


def extract_season_daynight_scores(html: str) -> Dict[str, Optional[float]]:
    """
    Fragrantica renders season/day-night voting in different ways over time.
    This function tries a few regex patterns to find numbers.

    Output values are 0-100 (percent) when found.
    """
    keys = ["winter", "spring", "summer", "fall", "day", "night"]
    out: Dict[str, Optional[float]] = {k: None for k in keys}

    # Pattern A: "winter ... 63%" style
    for k in keys:
        m = re.search(rf"{k}\D{{0,40}}([0-9]{{1,3}}(?:\.[0-9]+)?)\s*%", html, flags=re.IGNORECASE)
        if m:
            val = float(m.group(1))
            if 0 <= val <= 100:
                out[k] = val

    # Pattern B: JSON-ish: "winter":0.63 or "winter":63
    for k in keys:
        if out[k] is not None:
            continue
        m = re.search(rf"\"{k}\"\s*:\s*([0-9]+(?:\.[0-9]+)?)", html, flags=re.IGNORECASE)
        if m:
            raw = float(m.group(1))
            # if looks like 0-1, treat as fraction
            if 0 <= raw <= 1.0:
                out[k] = raw * 100.0
            elif 0 <= raw <= 100:
                out[k] = raw

    return out


def extract_image_urls(soup: BeautifulSoup, base_url: str) -> List[str]:
    urls: Set[str] = set()

    # OpenGraph / Twitter cards usually give you the bottle image
    for k, a in [("og:image", "property"), ("twitter:image", "name")]:
        v = extract_meta_content(soup, k, attr=a)
        if v:
            urls.add(urljoin(base_url, v))

    # Any image tag sources
    for img in soup.find_all("img"):
        src = img.get("src") or img.get("data-src")
        if not src:
            continue
        full = urljoin(base_url, src)
        urls.add(full)

    # Filter: prefer fimgs & perfume images; drop tiny icons/avatars if possible
    filtered: List[str] = []
    for u in urls:
        lu = u.lower()
        if not re.search(r"\.(jpg|jpeg|png|webp)(\?|$)", lu):
            continue
        # keep fragrantica/fimgs images; skip obvious avatars/icons
        if "fimgs.net" in lu or "fragrantica.com" in lu:
            if any(x in lu for x in ["avatar", "icons", "sprite", "flags"]):
                continue
            filtered.append(u)

    # Stable order
    return sorted(filtered)


def download_file(url: str, out_path: Path, timeout: int = 45) -> bool:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        r = requests.get(url, headers=DEFAULT_HEADERS, timeout=timeout, stream=True)
        if r.status_code >= 400:
            return False
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 64):
                if chunk:
                    f.write(chunk)
        return True
    except Exception:
        return False


def perfume_id_from_url(url: str) -> Optional[str]:
    # common pattern ends with "-12345.html"
    m = re.search(r"-(\d+)\.html?$", url)
    return m.group(1) if m else None


def build_perfume_outdir(base_out: Path, brand: str, perfume_url: str, perfume_name: str) -> Path:
    pid = perfume_id_from_url(perfume_url) or _hash(perfume_url)
    brand_slug = slugify(brand) if brand else "unknown-brand"
    perfume_slug = slugify(perfume_name) if perfume_name else pid
    folder = f"{pid}-{perfume_slug}"
    return base_out / brand_slug / folder


def scrape_perfume(url: str, out_root: Path, save_html: bool = True, allow_playwright: bool = True) -> Path:
    fetch = fetch_html(url, allow_playwright=allow_playwright)
    soup = BeautifulSoup(fetch.html, "lxml")
    page_text = soup.get_text("\n", strip=True)

    jsonlds = extract_json_ld(soup)
    product_ld = pick_product_ld(jsonlds)

    # Name
    name = None
    if product_ld and isinstance(product_ld.get("name"), str):
        name = product_ld["name"].strip()
    if not name:
        og_title = extract_meta_content(soup, "og:title", attr="property")
        name = og_title.strip() if og_title else None
    if not name:
        # fallback: <title>
        if soup.title and soup.title.string:
            name = soup.title.string.strip()

    # Description
    description = None
    if product_ld and isinstance(product_ld.get("description"), str):
        description = product_ld["description"].strip()
    if not description:
        desc = extract_meta_content(soup, "og:description", attr="property")
        description = desc.strip() if desc else None

    # Brand / designer
    brand = None
    if product_ld:
        b = product_ld.get("brand")
        if isinstance(b, dict) and isinstance(b.get("name"), str):
            brand = b["name"].strip()
        elif isinstance(b, str):
            brand = b.strip()

    if not brand:
        # weak fallback: try to locate "Designer" label in text
        m = re.search(r"\bDesigner\b\s+([A-Za-z0-9&' .-]{2,60})", page_text)
        if m:
            brand = m.group(1).strip()

    rating, votes = extract_rating_votes(page_text)
    longevity, sillage = extract_longevity_sillage(page_text)
    accords = extract_main_accords(page_text)
    notes = extract_notes_pyramid(page_text)

    season_day = extract_season_daynight_scores(fetch.html)

    image_urls = extract_image_urls(soup, url)

    # Output folder
    out_dir = build_perfume_outdir(out_root, brand or "unknown", url, name or "unknown")
    (out_dir / "images").mkdir(parents=True, exist_ok=True)
    (out_dir / "html").mkdir(parents=True, exist_ok=True)

    if save_html:
        (out_dir / "html" / "page.html").write_text(fetch.html, encoding="utf-8")

    # Download images with deterministic filenames
    downloaded: List[Dict[str, str]] = []
    for img_url in image_urls:
        ext = os.path.splitext(urlparse(img_url).path)[1].lower()
        if ext not in [".jpg", ".jpeg", ".png", ".webp"]:
            ext = ".jpg"
        fname = f"{_hash(img_url)}{ext}"
        ok = download_file(img_url, out_dir / "images" / fname)
        if ok:
            downloaded.append({"url": img_url, "file": f"images/{fname}"})

    meta: Dict[str, Any] = {
        "source": "fragrantica",
        "source_url": url,
        "fetched_with": fetch.used,
        "name": name,
        "brand": brand,
        "description": description,
        "rating": rating,
        "votes": votes,
        "main_accords": accords,
        "notes": notes,
        "longevity_score": longevity,   # out of 5
        "sillage_score": sillage,       # out of 4
        "season_daynight_percent": season_day,  # 0-100 if found
        "images": downloaded,
    }

    (out_dir / "meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    return out_dir


def is_perfume_link(href: str) -> bool:
    return bool(re.search(r"/perfume/.+\.html?$", href))


def collect_perfume_urls_from_designer(designer_url: str, allow_playwright: bool = True) -> List[str]:
    fetch = fetch_html(designer_url, allow_playwright=allow_playwright)
    soup = BeautifulSoup(fetch.html, "lxml")
    urls: Set[str] = set()
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href:
            continue
        full = urljoin(designer_url, href)
        if "fragrantica.com" not in urlparse(full).netloc:
            continue
        if is_perfume_link(full):
            urls.add(full.split("?")[0])
    return sorted(urls)


def main() -> None:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)

    ap_scrape = sub.add_parser("scrape", help="Scrape a single perfume URL")
    ap_scrape.add_argument("--url", required=True)
    ap_scrape.add_argument("--out", default="out/fragrantica")
    ap_scrape.add_argument("--no-html", action="store_true")
    ap_scrape.add_argument("--no-playwright", action="store_true")

    ap_crawl = sub.add_parser("crawl-designer", help="Crawl a designer page and scrape discovered perfumes")
    ap_crawl.add_argument("--url", required=True, help="Designer URL like https://www.fragrantica.com/designers/Jean-Paul-Gaultier.html")
    ap_crawl.add_argument("--out", default="out/fragrantica")
    ap_crawl.add_argument("--limit", type=int, default=0, help="0 = no limit")
    ap_crawl.add_argument("--no-html", action="store_true")
    ap_crawl.add_argument("--no-playwright", action="store_true")

    args = ap.parse_args()
    out_root = Path(args.out)

    if args.cmd == "scrape":
        out_dir = scrape_perfume(
            url=args.url,
            out_root=out_root,
            save_html=not args.no_html,
            allow_playwright=not args.no_playwright,
        )
        print(f"Saved: {out_dir}")

    elif args.cmd == "crawl-designer":
        urls = collect_perfume_urls_from_designer(args.url, allow_playwright=not args.no_playwright)
        if args.limit and args.limit > 0:
            urls = urls[: args.limit]

        print(f"Found {len(urls)} perfume URLs on designer page.")
        for u in tqdm(urls, desc="Scraping perfumes"):
            try:
                scrape_perfume(
                    url=u,
                    out_root=out_root,
                    save_html=not args.no_html,
                    allow_playwright=not args.no_playwright,
                )
                polite_sleep(1.1, 2.4)
            except Exception as e:
                print(f"[WARN] Failed {u}: {e}")

        print(f"Done. Output in: {out_root}")


if __name__ == "__main__":
    main()