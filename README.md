## Fragrantica + Reddit data harvester (local)

Local pipeline to produce a website-ready dataset for a **“perfume by vibe”** recommender.

You stated you have explicit written permission from Fragrantica to use their data; this repo assumes **local, respectful** harvesting with caching and minimal image downloading.

### Setup (macOS / Linux)

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Optional (only needed if Fragrantica serves an interstitial page):
python -m playwright install chromium
```

### Scrape a single perfume page

```bash
python scrape_fragrantica.py perfume "https://www.fragrantica.com/perfume/Chanel/No-5-123.html" --out data/fragrantica
```

Useful flags:

- `--max-images 1`: keep only the main image
- `--no-playwright`: disable Playwright fallback (won’t work on interstitial pages)

Outputs (per perfume):

- `data/fragrantica/perfumes/<slug>/meta.json`
- `data/fragrantica/perfumes/<slug>/html/page.html`
- `data/fragrantica/perfumes/<slug>/images/main.*` + `images/extra/*` (only “useful” images, capped)

### Discover perfume links from a designer page (optional)

```bash
python scrape_fragrantica.py designer "https://www.fragrantica.com/designers/Chanel.html" --out data/fragrantica --limit 50
```

This will discover perfume links, then scrape each perfume into the same dataset folder.

### Reddit ingestion (separate step)

```bash
python ingest_reddit.py --out data/reddit --subreddit perfumesthatfeellike --limit 200
```

Writes NDJSON of posts for later “vibe prompt” modeling.

### Dataset shape (website-friendly)

Each perfume gets a `meta.json` normalized to stable keys:

- `id`, `source.url`, `source.scraped_at`
- `name`, `brand`, `description`
- `rating.value`, `rating.votes`
- `main_accords`
- `notes.top|middle|base|other`
- `performance.longevity_score`, `performance.sillage_score`
- `performance.seasonality`, `performance.daynight` (percentages when available)
- `images.main`, `images.extra[]` (relative paths)

