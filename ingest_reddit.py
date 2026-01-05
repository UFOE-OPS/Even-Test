from __future__ import annotations

import argparse
import datetime as dt
import json
import os
from pathlib import Path
from typing import Any, Iterable, Optional

import requests


USER_AGENT = "perfume-by-vibe-harvester/0.1 (local dataset builder)"


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def write_ndjson(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def fetch_public_listing(subreddit: str, limit: int, sort: str = "new") -> list[dict[str, Any]]:
    """
    Best-effort unauthenticated fetch. Reddit may throttle/require auth; this is a fallback.
    """
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})
    url = f"https://www.reddit.com/r/{subreddit}/{sort}.json"
    params = {"limit": min(limit, 100)}
    out: list[dict[str, Any]] = []

    after: Optional[str] = None
    while len(out) < limit:
        if after:
            params["after"] = after
        resp = session.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        children = (data.get("data") or {}).get("children") or []
        for ch in children:
            d = ch.get("data") or {}
            out.append(d)
            if len(out) >= limit:
                break
        after = (data.get("data") or {}).get("after")
        if not after:
            break
    return out


def fetch_with_praw(subreddit: str, limit: int, sort: str = "new") -> list[dict[str, Any]]:
    # Lazy import so users can skip OAuth setup if they want.
    import praw  # type: ignore

    client_id = os.environ.get("REDDIT_CLIENT_ID")
    client_secret = os.environ.get("REDDIT_CLIENT_SECRET")
    user_agent = os.environ.get("REDDIT_USER_AGENT", USER_AGENT)
    if not client_id or not client_secret:
        raise RuntimeError("Missing REDDIT_CLIENT_ID / REDDIT_CLIENT_SECRET for OAuth ingestion.")

    reddit = praw.Reddit(
        client_id=client_id,
        client_secret=client_secret,
        user_agent=user_agent,
    )
    sr = reddit.subreddit(subreddit)
    listing = getattr(sr, sort)(limit=limit)
    out: list[dict[str, Any]] = []
    for post in listing:
        out.append(
            {
                "id": post.id,
                "name": post.name,
                "title": post.title,
                "selftext": post.selftext,
                "subreddit": str(post.subreddit),
                "permalink": post.permalink,
                "url": post.url,
                "created_utc": post.created_utc,
                "score": post.score,
                "num_comments": post.num_comments,
                "author": str(post.author) if post.author else None,
                "is_self": post.is_self,
                "link_flair_text": post.link_flair_text,
            }
        )
    return out


def normalize_post(d: dict[str, Any]) -> dict[str, Any]:
    # Normalize to stable keys regardless of source (public listing vs PRAW).
    return {
        "source": "reddit",
        "scraped_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "id": d.get("id"),
        "name": d.get("name"),
        "title": d.get("title"),
        "selftext": d.get("selftext") or d.get("selftext", ""),
        "subreddit": d.get("subreddit"),
        "permalink": d.get("permalink"),
        "url": d.get("url"),
        "created_utc": d.get("created_utc"),
        "score": d.get("score"),
        "num_comments": d.get("num_comments"),
        "author": d.get("author"),
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Ingest Reddit posts for vibe prompts (NDJSON)")
    ap.add_argument("--out", default="data/reddit", help="Output root folder")
    ap.add_argument("--subreddit", default="perfumesthatfeellike", help="Subreddit name")
    ap.add_argument("--limit", type=int, default=200, help="Max posts")
    ap.add_argument("--sort", default="new", choices=["new", "hot", "top"], help="Listing type")
    ap.add_argument(
        "--oauth",
        action="store_true",
        help="Use OAuth via PRAW (requires env vars); recommended if public listing fails",
    )
    args = ap.parse_args()

    out_root = Path(args.out).expanduser().resolve()
    ensure_dir(out_root)

    if args.oauth:
        raw = fetch_with_praw(args.subreddit, limit=args.limit, sort=args.sort)
    else:
        raw = fetch_public_listing(args.subreddit, limit=args.limit, sort=args.sort)

    rows = [normalize_post(d) for d in raw]
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_path = out_root / f"{args.subreddit}_{args.sort}_{stamp}.ndjson"
    write_ndjson(out_path, rows)
    print(str(out_path))


if __name__ == "__main__":
    main()

