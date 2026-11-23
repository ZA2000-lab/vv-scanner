# optionscanners.com

Cash-secured put scanner and portfolio tracker.

- `docs/` is the front end, served by GitHub Pages at optionscanners.com (`index.html` scanner, `tracker.html` portfolio tracker).
- `backend/` is the Flask API on Railway (`https://vv-scanner-production.up.railway.app`).

## Deploy

1. Push this branch to `main`. GitHub Pages republishes `docs/` automatically.
2. Railway redeploys the backend on push. The first scan after a deploy takes about 8 minutes.
3. Optional, recommended: add a Railway volume mounted at `/data` and set `DATA_DIR=/data`.
   Scan results then survive restarts, and true IV rank switches on once 20 days of IV history are stored.

## Backend settings (Railway variables, all optional)

| Variable | Default | What it does |
|---|---|---|
| `SCORING_CAP` | 700 | How many of the most liquid tickers get their option chains read. Lower it if Yahoo rate-limits. |
| `SCORING_WORKERS` | 4 | Parallel option-chain requests. |
| `MIN_MCAP` | 500000000 | Smallest market cap pulled from the Nasdaq screener. |
| `MIN_PRICE` | 5 | Smallest share price scanned. |
| `MIN_DOLLAR_VOL` | 15000000 | Minimum average daily dollar volume. |
| `TARGET_DELTA` | 0.30 | Delta of the suggested strike. |
| `REFRESH_COOLDOWN_MIN` | 10 | Minutes before the refresh button can start another full scan. |
| `AUTO_SCAN` | 1 | Scheduled scans at 9:50, 12:30 and 15:10 ET on weekdays. |
| `DATA_DIR` | /tmp | Where the scan cache and IV history are saved. |

## API

`/api/scan`, `/api/progress`, `/api/refresh` (POST), `/api/market`, `/api/analyze?ticker=XYZ`,
`/api/prices?tickers=A,B`, `/api/option-prices` (POST), `/api/status`, `/api/debug`.

Add `#demo` to the site URL to preview the UI with sample data.
