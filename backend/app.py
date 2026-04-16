from flask import Flask, jsonify, request
from flask_cors import CORS
import yfinance as yf
import pandas as pd
import requests
from datetime import datetime, timedelta, date
import numpy as np
import threading
import concurrent.futures
import time
import logging
import os

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(**name**)
app = Flask(**name**)
CORS(app)

try:
yf.set_tz_cache_location(”/tmp/yf_tz_cache”)
except Exception:
pass

# ── Constants ──────────────────────────────────────────────────────────────

CACHE_TTL        = 6 * 3600
DAYS_AHEAD       = 45          # fetch Nasdaq calendar 45 days out
CSP_MAX_WORKERS  = 25          # parallel workers

YF_HEADERS = {
“User-Agent”: “Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 “
“(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36”,
“Accept”: “application/json, text/plain, */*”,
“Accept-Language”: “en-US,en;q=0.9”,
}

NASDAQ_HEADERS = {
“User-Agent”: “Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36”,
“Accept”: “application/json, text/plain, */*”,
“Origin”: “https://www.nasdaq.com”,
“Referer”: “https://www.nasdaq.com/market-activity/earnings”,
}

# ── CSP Cache (single cache — no more L1) ─────────────────────────────────

_csp_cache = {
“results”:  [],
“ts”:       0,
“running”:  False,
“progress”: {
“done”: 0, “total”: 0, “phase”: “idle”,
“currentTicker”: “”, “found”: 0, “scanStart”: 0,
“passedPrefilter”: 0,
},
}

# ── Universe ───────────────────────────────────────────────────────────────

CSP_EXTRAS = [
# Specific adds
“CDE”,
# Mega-cap / large-cap tech
“AAPL”,“MSFT”,“NVDA”,“GOOGL”,“AMZN”,“META”,“TSLA”,“AVGO”,“ORCL”,“AMD”,
“ADBE”,“CRM”,“INTU”,“NOW”,“PANW”,“CRWD”,“NET”,“DDOG”,“SNOW”,“PLTR”,“MDB”,
“INTC”,“QCOM”,“TXN”,“AMAT”,“LRCX”,“KLAC”,“MU”,“SMCI”,“ARM”,“ON”,“MRVL”,
“CSCO”,“IBM”,“DELL”,“HPQ”,“HPE”,“ACN”,“IT”,“CTSH”,“EPAM”,“VRSK”,
# Financials
“JPM”,“BAC”,“WFC”,“GS”,“MS”,“C”,“V”,“MA”,“AXP”,“COF”,“DFS”,“ALLY”,“SYF”,
“BLK”,“BX”,“KKR”,“APO”,“ARES”,“SCHW”,“IBKR”,“HOOD”,“COIN”,“MSTR”,
# Healthcare
“UNH”,“LLY”,“JNJ”,“ABBV”,“MRK”,“PFE”,“AMGN”,“GILD”,“VRTX”,“REGN”,“ISRG”,
“TMO”,“DHR”,“A”,“WAT”,“IDXX”,“VEEV”,“MRNA”,“BIIB”,“INCY”,“SYK”,“MDT”,“BSX”,
“MCK”,“ABC”,“CAH”,“CI”,“ELV”,“HUM”,“CVS”,“CNC”,“MOH”,
# Energy
“XOM”,“CVX”,“COP”,“EOG”,“DVN”,“OXY”,“HES”,“MRO”,“APA”,“OVV”,“FANG”,
“SLB”,“HAL”,“BKR”,“VLO”,“PSX”,“MPC”,“KMI”,“WMB”,“OKE”,“LNG”,“EQT”,“AR”,
# Industrials
“GE”,“HON”,“CAT”,“DE”,“MMM”,“EMR”,“ETN”,“PH”,“ROK”,“ITW”,“DOV”,“AME”,“ROP”,
“RTX”,“LMT”,“GD”,“NOC”,“BA”,“HII”,“LHX”,“TDG”,“UPS”,“FDX”,“CSX”,“UNP”,“NSC”,
“WM”,“RSG”,“FAST”,“GWW”,“SHW”,“PPG”,“IR”,“OTIS”,“CARR”,“TT”,
# Consumer
“HD”,“WMT”,“COST”,“TGT”,“MCD”,“SBUX”,“CMG”,“YUM”,“NKE”,“LULU”,“DECK”,“ONON”,
“BKNG”,“EXPE”,“ABNB”,“UBER”,“LYFT”,“DASH”,“ETSY”,“EBAY”,“CHWY”,
“DIS”,“NFLX”,“CMCSA”,“WBD”,“PARA”,“RBLX”,“EA”,“TTWO”,“LYV”,“DKNG”,“MGM”,“WYNN”,
“GM”,“F”,“RIVN”,“NIO”,“LCID”,“XPEV”,“LI”,
“ROST”,“TJX”,“BURL”,“DG”,“DLTR”,“LOW”,“ANF”,“AEO”,“GPS”,“URBN”,“KSS”,“JWN”,“M”,
# Staples
“PG”,“KO”,“PEP”,“PM”,“MO”,“MDLZ”,“KHC”,“GIS”,“CL”,“CHD”,“KMB”,“CLX”,
“EL”,“ULTA”,“BBWI”,“TSN”,“HRL”,“MKC”,“SJM”,“CPB”,“SYY”,
# Utilities / Clean Energy
“NEE”,“DUK”,“SO”,“D”,“AEP”,“EXC”,“XEL”,“WEC”,“PCG”,“EIX”,“PPL”,“AES”,
“ENPH”,“FSLR”,“RUN”,“SEDG”,
# Materials
“LIN”,“APD”,“CF”,“NTR”,“FCX”,“NEM”,“GOLD”,“AEM”,“KGC”,“WPM”,“PAAS”,“CDE”,
“NUE”,“STLD”,“CLF”,“DD”,“DOW”,“LYB”,“ALB”,“MP”,“ECL”,
# REITs
“AMT”,“CCI”,“EQIX”,“SBAC”,“DLR”,“IRM”,“SPG”,“O”,“VICI”,“PLD”,“PSA”,“EXR”,“EQR”,“AVB”,
“WELL”,“VTR”,“OHI”,“ARE”,“BXP”,
# Telecom / Media
“VZ”,“T”,“TMUS”,“SNAP”,“PINS”,“SPOT”,“RDDT”,
# High-beta / meme
“HOOD”,“SOFI”,“MARA”,“RIOT”,“GME”,“AMC”,“SPCE”,“RKLB”,“ASTS”,“PLTR”,
# Mining / precious metals
“NEM”,“GOLD”,“AEM”,“KGC”,“WPM”,“PAAS”,“EGO”,“IAG”,“AU”,“HL”,“SILV”,“FSM”,
“NGD”,“GFI”,“HMY”,“SBSW”,
# Liquid ETFs
“SPY”,“QQQ”,“IWM”,“DIA”,“MDY”,“VTI”,“VOO”,“IVV”,“RSP”,
“GLD”,“SLV”,“GDX”,“GDXJ”,“IAU”,
“TLT”,“IEF”,“LQD”,“HYG”,“JNK”,“AGG”,“BND”,
“XLE”,“XLF”,“XLK”,“XLV”,“XLI”,“XLU”,“XLRE”,“XLB”,“XLY”,“XLP”,“XLC”,
“SMH”,“SOXX”,“IBB”,“ARKK”,“ARKG”,“EEM”,“EFA”,“EWZ”,“EWJ”,“FXI”,“KWEB”,“MCHI”,
“USO”,“UNG”,“CPER”,
“VXX”,“UVXY”,“TQQQ”,“SQQQ”,“UPRO”,“SSO”,“SDS”,“SH”,“QLD”,
]

def _fetch_wiki_tickers(url: str, name: str) -> list:
“”“Fetch ticker symbols from a Wikipedia table page.”””
try:
headers = {“User-Agent”: “Mozilla/5.0 (compatible; bot/1.0)”}
resp = requests.get(url, headers=headers, timeout=20)
if resp.status_code != 200:
log.warning(“Wikipedia %s HTTP %d”, name, resp.status_code)
return []
dfs = pd.read_html(resp.text)
for df in dfs:
cols = [str(c).lower() for c in df.columns]
sym_col = next(
(df.columns[i] for i, c in enumerate(cols)
if c in (“symbol”,“ticker”,“symbol[3]”,“ticker symbol”,“company”)),
None
)
if sym_col is None:
continue
batch = (df[sym_col]
.astype(str)
.str.split(r”[\s[]”).str[0]
.str.replace(r”..*”, “”, regex=True)
.str.upper()
.str.strip()
.tolist())
batch = [t.replace(”.”, “-”) for t in batch]
batch = [t for t in batch if 1 <= len(t) <= 6 and t.replace(”-”,””).isalpha()]
if len(batch) > 10:
log.info(“Wikipedia %s: %d tickers”, name, len(batch))
return batch
except Exception as e:
log.warning(“Wikipedia %s fetch error: %s”, name, e)
return []

def _build_csp_universe() -> list:
“””
Build full scan universe from:
- Hardcoded CSP_EXTRAS (liquid names, ETFs, high-vol)
- S&P 500 + S&P 400 + S&P 600 from Wikipedia (~1,500 tickers)
- NASDAQ-100 from Wikipedia
Deduplicates and returns a clean list.
“””
log.info(“Building CSP universe…”)
sources = {
“sp500”:    “https://en.wikipedia.org/wiki/List_of_S%26P_500_companies”,
“sp400”:    “https://en.wikipedia.org/wiki/List_of_S%26P_400_companies”,
“sp600”:    “https://en.wikipedia.org/wiki/List_of_S%26P_600_companies”,
“nasdaq100”:“https://en.wikipedia.org/wiki/Nasdaq-100”,
}
wiki_tickers = []
for name, url in sources.items():
wiki_tickers.extend(_fetch_wiki_tickers(url, name))

```
combined = CSP_EXTRAS + wiki_tickers
seen, result = set(), []
for t in combined:
    if t not in seen:
        seen.add(t)
        result.append(t)

log.info("CSP universe built: %d unique tickers (%d from Wikipedia, %d extras)",
         len(result), len(set(wiki_tickers)), len(set(CSP_EXTRAS)))
return result
```

# Build universe in background — scan won’t start until it’s ready

CSP_UNIVERSE = list(CSP_EXTRAS)
_csp_universe_lock = threading.Lock()

def _load_universe_bg():
global CSP_UNIVERSE
full = _build_csp_universe()
with _csp_universe_lock:
CSP_UNIVERSE = full
log.info(“CSP universe ready: %d tickers”, len(CSP_UNIVERSE))

threading.Thread(target=_load_universe_bg, daemon=True).start()

# ── Core computation helpers ───────────────────────────────────────────────

def yang_zhang(df, window=30, tp=252):
“”“Yang-Zhang realized volatility estimator.”””
try:
lho  = (df[“High”]  / df[“Open”]).apply(np.log)
llo  = (df[“Low”]   / df[“Open”]).apply(np.log)
lco  = (df[“Close”] / df[“Open”]).apply(np.log)
loc_ = (df[“Open”]  / df[“Close”].shift(1)).apply(np.log)
lcc  = (df[“Close”] / df[“Close”].shift(1)).apply(np.log)
rs   = lho*(lho-lco) + llo*(llo-lco)
cv   = (lcc**2).rolling(window).sum() / (window-1)
ov   = (loc_**2).rolling(window).sum() / (window-1)
wr   = rs.rolling(window).sum() / (window-1)
k    = 0.34 / (1.34 + (window+1)/(window-1))
return float(((ov + k*cv + (1-k)*wr).apply(np.sqrt) * np.sqrt(tp)).iloc[-1])
except Exception:
return 0.25

# ── Earnings date: multi-source, accurate lookup ───────────────────────────

def _extract_earnings(stock, sym: str, today_d: date, earnings_map: dict):
“””
Fetch next earnings date from multiple sources in priority order:
1. Yahoo Finance quoteSummary/calendarEvents  ← most accurate
2. yfinance .calendar (dict format, newer yfinance)
3. yfinance .earnings_dates (filtered for future)
4. Pre-fetched Nasdaq calendar (fallback)

```
Returns (display_str like 'Apr 25', days_from_today) or (None, None).
Uses the EARLIEST confirmed date across all sources (conservative).
"""
candidates = []  # list of (days_int, date_str)

# ── Source 1: Yahoo Finance quoteSummary ──────────────────────────────
try:
    url = (f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{sym}"
           f"?modules=calendarEvents")
    r = requests.get(url, headers=YF_HEADERS, timeout=5)
    if r.status_code == 200:
        res_list = (r.json().get("quoteSummary", {}).get("result") or [])
        if res_list:
            cal_ev = (res_list[0].get("calendarEvents") or {})
            earn_dates = cal_ev.get("earnings", {}).get("earningsDate", [])
            if earn_dates:
                ts_raw = earn_dates[0].get("raw", 0)
                if ts_raw:
                    earn_dt = date.fromtimestamp(ts_raw)
                    days = (earn_dt - today_d).days
                    if -3 <= days <= 180:
                        candidates.append((days, earn_dt.strftime("%b %d")))
except Exception:
    pass

# ── Source 2: yfinance .calendar (dict in yfinance >= 0.2) ───────────
try:
    cal = stock.calendar
    if isinstance(cal, dict):
        earn_list = cal.get("Earnings Date", [])
        if not isinstance(earn_list, list):
            earn_list = [earn_list]
        for ed in earn_list:
            if ed is None:
                continue
            # Could be Timestamp, datetime, or string
            if hasattr(ed, "date"):
                ed = ed.date()
            elif isinstance(ed, str):
                try:
                    ed = datetime.strptime(ed[:10], "%Y-%m-%d").date()
                except Exception:
                    continue
            days = (ed - today_d).days
            if -3 <= days <= 180:
                candidates.append((days, ed.strftime("%b %d")))
                break
    elif isinstance(cal, pd.DataFrame) and not cal.empty:
        # Old yfinance: DataFrame with dates as column headers
        for col in cal.columns:
            try:
                ed = pd.Timestamp(col).date()
                days = (ed - today_d).days
                if -3 <= days <= 180:
                    candidates.append((days, ed.strftime("%b %d")))
                    break
            except Exception:
                pass
except Exception:
    pass

# ── Source 3: yfinance .earnings_dates ───────────────────────────────
try:
    ei = stock.earnings_dates
    if ei is not None and not ei.empty:
        idx = ei.index
        # Handle tz-aware index
        tz = getattr(idx, "tz", None)
        today_ts = (pd.Timestamp(today_d).tz_localize(tz)
                    if tz else pd.Timestamp(today_d))
        future_ei = ei[idx > today_ts]
        if not future_ei.empty:
            # Index is sorted descending; last entry = closest upcoming date
            nxt = future_ei.index[-1]
            earn_dt = nxt.date() if hasattr(nxt, "date") else None
            if earn_dt:
                days = (earn_dt - today_d).days
                if 0 <= days <= 180:
                    candidates.append((days, earn_dt.strftime("%b %d")))
except Exception:
    pass

# ── Source 4: Pre-fetched Nasdaq calendar (fallback) ─────────────────
if sym in earnings_map:
    nd_str, nd_days = earnings_map[sym]
    if nd_days is not None and -2 <= nd_days <= 180:
        candidates.append((nd_days, nd_str))

if not candidates:
    return None, None

# Return the EARLIEST (most conservative) date found
candidates.sort(key=lambda x: x[0])
best_days, best_str = candidates[0]
return best_str, best_days
```

# ── Nasdaq earnings calendar (pre-fetched once per scan) ──────────────────

def fetch_nasdaq_calendar() -> dict:
“””
Bulk-fetch Nasdaq earnings calendar for the next DAYS_AHEAD days.
Returns {SYM: (date_str, days_from_today)}.
Used as a fast fallback; per-ticker Yahoo Finance calls are more accurate.
“””
earnings_map = {}
today = date.today()
session = requests.Session()
session.headers.update(NASDAQ_HEADERS)
d = today
while d <= today + timedelta(days=DAYS_AHEAD):
if d.weekday() < 5:
try:
resp = session.get(
“https://api.nasdaq.com/api/calendar/earnings?date=” + d.strftime(”%Y-%m-%d”),
timeout=6)
if resp.status_code == 200:
rows = (resp.json().get(“data”) or {}).get(“rows”) or []
diff = (d - today).days
for row in rows:
sym = (row.get(“symbol”) or “”).upper().strip().replace(”/”, “-”)
if sym and 1 <= len(sym) <= 6 and sym.replace(”-”,””).isalpha():
if sym not in earnings_map:
earnings_map[sym] = (d.strftime(”%b %d”), diff)
time.sleep(0.08)
except Exception as e:
log.warning(“Nasdaq cal %s: %s”, d, e)
d += timedelta(days=1)
log.info(“Nasdaq calendar: %d tickers with upcoming earnings”, len(earnings_map))
return earnings_map

# ── Pre-filter (fast, no options chain) ───────────────────────────────────

def fast_prefilter(sym: str):
“””
Quick check using only fast_info (~0.1s vs ~3s for full score).
Returns (price, avg_vol) tuple if passes, else None.
Thresholds: price > $2, 3-month avg volume > 250k.
“””
try:
fi  = yf.Ticker(sym).fast_info
px  = float(getattr(fi, “last_price”, None) or getattr(fi, “previous_close”, None) or 0)
vol = float(getattr(fi, “three_month_average_volume”, None) or 0)
if px < 2 or vol < 250_000:
return None
return (px, vol)
except Exception:
return None

# ── Full CSP scorer ────────────────────────────────────────────────────────

def score_csp_ticker(sym: str, earnings_map: dict):
“””
Full scoring for a cash-secured put setup.
Requires: 1yr price history, options chain, earnings date.
Returns a result dict or None if ticker doesn’t qualify.
“””
try:
stock   = yf.Ticker(sym)
today_d = date.today()

```
    # ── Price history ──────────────────────────────────────────────────
    h1y = stock.history(period="1y")
    if h1y.empty or len(h1y) < 60:
        return None
    price = float(h1y["Close"].iloc[-1])
    if price <= 0:
        return None

    # ── Volume ────────────────────────────────────────────────────────
    avg_vol = float(h1y["Volume"].tail(30).mean())
    if avg_vol < 300_000:
        return None

    # ── Realized volatility ───────────────────────────────────────────
    rv30 = yang_zhang(h1y, window=30)

    # IV rank proxy: where does ATM IV sit vs 1yr RV range
    rv_series = h1y["Close"].pct_change().rolling(30).std() * (252 ** 0.5)
    rv_series = rv_series.dropna()
    rv_min = float(rv_series.quantile(0.10))
    rv_max = float(rv_series.quantile(0.90))

    # ── Options chain ─────────────────────────────────────────────────
    opts = stock.options
    if not opts or len(opts) < 1:
        return None

    # Find best expiry: prefer 28-55 DTE, fall back to anything ≥ 28
    target_exp, target_dte = None, None
    for exp in sorted(opts):
        dte = (datetime.strptime(exp, "%Y-%m-%d").date() - today_d).days
        if 28 <= dte <= 55:
            target_exp, target_dte = exp, dte
            break
    if not target_exp:
        for exp in sorted(opts):
            dte = (datetime.strptime(exp, "%Y-%m-%d").date() - today_d).days
            if dte >= 28:
                target_exp, target_dte = exp, dte
                break
    if not target_exp or target_dte is None:
        return None

    ch = stock.option_chain(target_exp)
    puts = ch.puts
    if puts.empty:
        return None

    # ATM IV from puts
    pi     = (puts["strike"] - price).abs().idxmin()
    atm_iv = float(puts.loc[pi, "impliedVolatility"])
    if atm_iv <= 0:
        return None

    # IV rank
    if rv_max > rv_min:
        iv_rank = int(min(100, max(0, (atm_iv - rv_min) / (rv_max - rv_min) * 100)))
    else:
        iv_rank = 50

    iv_rv_ratio = atm_iv / rv30 if rv30 > 0 else 1.0

    # ── ~30-delta put strike selection ────────────────────────────────
    T   = target_dte / 365.0
    target_strike_raw = price * (1 - 0.45 * atm_iv * (T ** 0.5))

    otm_puts = puts[puts["strike"] <= price * 1.01].copy()
    if otm_puts.empty:
        return None
    otm_puts["dist"] = (otm_puts["strike"] - target_strike_raw).abs()
    best_row  = otm_puts.loc[otm_puts["dist"].idxmin()]
    put_strike = float(best_row["strike"])

    bid  = float(best_row.get("bid",  0) or 0)
    ask  = float(best_row.get("ask",  0) or 0)
    oi   = int(best_row.get("openInterest", 0) or 0)
    iv_p = float(best_row.get("impliedVolatility", atm_iv) or atm_iv)

    # Premium: prefer live bid/ask, fall back to BS estimate
    if bid > 0 and ask > 0:
        mid = round((bid + ask) / 2, 2)
    else:
        mid = round(price * iv_p * (T ** 0.5) * 0.40 * (put_strike / price) ** 0.5, 2)

    if mid <= 0.05:
        return None

    spread_pct = round((ask - bid) / mid * 100, 1) if mid > 0 and ask > bid else 0

    breakeven  = round(put_strike - mid, 2)
    collateral = put_strike * 100
    roc_pct    = round(mid * 100 / collateral * 100, 2) if collateral > 0 else 0
    roc_ann    = round(roc_pct * (365 / target_dte), 1) if target_dte > 0 else 0

    # ── Earnings date (multi-source, accurate) ────────────────────────
    earn_str, earn_days = _extract_earnings(stock, sym, today_d, earnings_map)
    earn_within = earn_days is not None and earn_days <= target_dte

    # ── Quality rating ────────────────────────────────────────────────
    if iv_rank < 45 or earn_within or spread_pct > 15:
        quality = "SKIP"
    elif iv_rank >= 60 and iv_rv_ratio >= 1.2 and not earn_within and spread_pct <= 8:
        quality = "STRONG"
    else:
        quality = "DECENT"

    # Company name
    try:
        name = getattr(stock.fast_info, "company_name", None) or sym
    except Exception:
        name = sym

    return {
        "ticker":        sym,
        "name":          name,
        "price":         round(price, 2),
        "quality":       quality,
        "ivRank":        iv_rank,
        "ivRvRatio":     round(iv_rv_ratio, 2),
        "putStrike":     put_strike,
        "expiration":    target_exp,
        "dte":           target_dte,
        "premium":       mid,
        "bid":           round(bid, 2),
        "ask":           round(ask, 2),
        "spreadPct":     spread_pct,
        "openInterest":  oi,
        "breakeven":     breakeven,
        "rocPct":        roc_pct,
        "rocAnn":        roc_ann,
        "collateral":    int(collateral),
        "avgVol":        int(avg_vol),
        "atm_iv":        round(atm_iv * 100, 1),
        "rv30":          round(rv30 * 100, 1),
        "earningsWithin":earn_within,
        "earningsDate":  earn_str,
        "earningsDays":  earn_days,
        "profitTarget":  round(mid * 0.50 * 100, 2),
        "stopLoss":      round(mid * 2.0  * 100, 2),
    }
except Exception as e:
    log.debug("CSP %s failed: %s", sym, e)
    return None
```

# ── Main scan orchestrator ─────────────────────────────────────────────────

def run_csp_scan():
“””
Two-stage scan:
Stage 1 — fast_prefilter: price + volume only, no options pull (~0.1s/ticker)
Stage 2 — score_csp_ticker: full options chain + earnings (~3-8s/ticker)
Results stream into _csp_cache[“results”] in real-time.
“””
if _csp_cache[“running”]:
return
_csp_cache[“running”] = True
scan_start = time.time()

```
_csp_cache["progress"] = {
    "done": 0, "total": 0, "phase": "loading universe",
    "currentTicker": "", "found": 0, "scanStart": scan_start,
    "passedPrefilter": 0,
}

# ── Wait up to 30s for background universe load ────────────────────────
deadline = time.time() + 30
while len(CSP_UNIVERSE) <= len(CSP_EXTRAS) and time.time() < deadline:
    time.sleep(1)

universe = list(CSP_UNIVERSE)
log.info("CSP scan starting — universe: %d tickers", len(universe))

# ── Fetch Nasdaq earnings calendar (batch, one-time) ──────────────────
_csp_cache["progress"]["phase"] = "earnings calendar"
try:
    earnings_map = fetch_nasdaq_calendar()
except Exception:
    earnings_map = {}
log.info("Earnings calendar loaded: %d entries", len(earnings_map))

# ── Stage 1: Fast pre-filter ───────────────────────────────────────────
_csp_cache["progress"].update({
    "done": 0, "total": len(universe), "phase": "pre-filter",
    "currentTicker": "", "found": 0,
})
log.info("Stage 1: pre-filtering %d tickers...", len(universe))

passed = []
lock1  = threading.Lock()

def prefilter_one(sym):
    result = fast_prefilter(sym)
    with lock1:
        _csp_cache["progress"]["done"] += 1
        _csp_cache["progress"]["currentTicker"] = sym
        if result:
            passed.append(sym)
            _csp_cache["progress"]["passedPrefilter"] = len(passed)

with concurrent.futures.ThreadPoolExecutor(max_workers=CSP_MAX_WORKERS) as pool:
    futs = {pool.submit(prefilter_one, sym): sym for sym in universe}
    for f in concurrent.futures.as_completed(futs):
        try:
            f.result()
        except Exception:
            pass

log.info("Stage 1 done: %d / %d passed pre-filter", len(passed), len(universe))

# ── Stage 2: Full scoring ──────────────────────────────────────────────
_csp_cache["progress"].update({
    "done": 0, "total": len(passed), "phase": "scoring",
    "currentTicker": "", "found": 0,
})
log.info("Stage 2: scoring %d tickers with options chains...", len(passed))

results = []
lock2   = threading.Lock()

def score_one(sym):
    r = score_csp_ticker(sym, earnings_map)
    with lock2:
        _csp_cache["progress"]["done"] += 1
        _csp_cache["progress"]["currentTicker"] = sym
        if r:
            results.append(r)
            _csp_cache["progress"]["found"] = len(results)
            # Stream partial results sorted by quality
            _csp_cache["results"] = sorted(
                results,
                key=lambda x: (
                    {"STRONG": 0, "DECENT": 1, "SKIP": 2}.get(x["quality"], 3),
                    -x["rocAnn"]
                )
            )

with concurrent.futures.ThreadPoolExecutor(max_workers=CSP_MAX_WORKERS) as pool:
    futs = {pool.submit(score_one, sym): sym for sym in passed}
    for f in concurrent.futures.as_completed(futs):
        try:
            f.result()
        except Exception:
            pass

# Final sort and cache update
final = sorted(
    results,
    key=lambda x: ({"STRONG": 0, "DECENT": 1, "SKIP": 2}.get(x["quality"], 3), -x["rocAnn"])
)
_csp_cache["results"]  = final
_csp_cache["ts"]       = time.time()
_csp_cache["running"]  = False
_csp_cache["progress"].update({
    "done": len(passed), "total": len(passed),
    "phase": "done", "currentTicker": "", "found": len(results),
})

elapsed = int(time.time() - scan_start)
log.info(
    "CSP scan complete — %d results from %d/%d tickers in %ds.",
    len(results), len(passed), len(universe), elapsed
)
```

# ── API Endpoints ──────────────────────────────────────────────────────────

@app.route(”/api/csp/scan”)
def api_csp_scan():
“”“Main CSP scan endpoint. Auto-starts scan if cache is stale.”””
if not _csp_cache[“ts”] or time.time() - _csp_cache[“ts”] > CACHE_TTL:
if not _csp_cache[“running”]:
threading.Thread(target=run_csp_scan, daemon=True).start()
return jsonify({
“results”:      _csp_cache[“results”],
“count”:        len(_csp_cache[“results”]),
“scannedAt”:    (datetime.fromtimestamp(_csp_cache[“ts”]).strftime(”%b %d %Y, %I:%M %p”)
if _csp_cache[“ts”] else None),
“ageMinutes”:   int((time.time() - _csp_cache[“ts”]) / 60) if _csp_cache[“ts”] else 0,
“isRefreshing”: _csp_cache[“running”],
“universe”:     len(CSP_UNIVERSE),
“progress”:     _csp_cache[“progress”],
})

@app.route(”/api/csp/progress”)
def api_csp_progress():
“”“Detailed progress for the loading screen. Polled every 2s by frontend.”””
p       = _csp_cache[“progress”]
pct     = int(p[“done”] / p[“total”] * 100) if p[“total”] > 0 else 0
elapsed = int(time.time() - p.get(“scanStart”, time.time())) if _csp_cache[“running”] else 0

```
# Estimated time remaining (only meaningful during scoring stage)
eta_s = None
if p["phase"] == "scoring" and p["done"] > 5 and _csp_cache["running"] and elapsed > 0:
    rate      = p["done"] / elapsed
    remaining = p["total"] - p["done"]
    eta_s     = int(remaining / rate) if rate > 0 else None

phase_labels = {
    "loading universe":  "Loading ticker universe...",
    "earnings calendar": "Fetching earnings calendar...",
    "pre-filter": (
        f"Stage 1 of 2: Fast pre-filter — checking {p['total']:,} tickers "
        f"({p.get('passedPrefilter',0)} passed so far)"
    ),
    "scoring": (
        f"Stage 2 of 2: Scoring options chains — "
        f"{p['done']}/{p['total']} tickers · {p.get('found',0)} setups found"
    ),
    "done": "Scan complete ✓",
    "idle": "Ready",
}

return jsonify({
    "done":          p["done"],
    "total":         p["total"],
    "pct":           pct,
    "phase":         p["phase"],
    "phaseLabel":    phase_labels.get(p["phase"], p["phase"]),
    "currentTicker": p.get("currentTicker", ""),
    "found":         p.get("found", 0),
    "passed":        p.get("passedPrefilter", 0),
    "elapsed":       elapsed,
    "eta":           eta_s,
    "universe":      len(CSP_UNIVERSE),
    "running":       _csp_cache["running"],
})
```

@app.route(”/api/csp/refresh”, methods=[“POST”])
def api_csp_refresh():
“”“Force a fresh scan, discarding the cache.”””
if _csp_cache[“running”]:
return jsonify({“message”: “Scan already in progress”, “running”: True}), 202
# Clear cache so results show as fresh
_csp_cache[“ts”]      = 0
_csp_cache[“results”] = []
threading.Thread(target=run_csp_scan, daemon=True).start()
return jsonify({“message”: “CSP refresh started”, “universe”: len(CSP_UNIVERSE)}), 202

@app.route(”/api/csp/status”)
def api_csp_status():
return jsonify({
“status”:   “ok”,
“cached”:   bool(_csp_cache[“ts”]),
“count”:    len(_csp_cache[“results”]),
“running”:  _csp_cache[“running”],
“universe”: len(CSP_UNIVERSE),
“progress”: _csp_cache[“progress”],
})

# ── Alias routes (keep backwards compatibility) ───────────────────────────

@app.route(”/api/scan”)
def api_scan_alias():
return api_csp_scan()

@app.route(”/api/progress”)
def api_progress_alias():
return api_csp_progress()

@app.route(”/api/refresh”, methods=[“POST”])
def api_refresh_alias():
return api_csp_refresh()

# ── Portfolio tracker price endpoint ──────────────────────────────────────

@app.route(”/api/prices”)
def api_prices():
“”“Lightweight price fetcher. ?tickers=AAPL,MSFT,SPY (max 25)”””
tickers_str = request.args.get(“tickers”, “”)
tickers     = [t.strip().upper() for t in tickers_str.split(”,”) if t.strip()][:25]
if not tickers:
return jsonify({“error”: “No tickers provided”}), 400

```
prices = {}
for sym in tickers:
    try:
        fi  = yf.Ticker(sym).fast_info
        px  = getattr(fi, "last_price", None) or getattr(fi, "previous_close", None)
        if px and float(px) > 0:
            prices[sym] = round(float(px), 2)
    except Exception:
        pass

return jsonify({"prices": prices, "ts": datetime.now().strftime("%I:%M %p")})
```

@app.route(”/api/option-prices”, methods=[“POST”])
def api_option_prices():
“””
Fetch live mid prices for specific option contracts.
POST JSON: [{“id”:“abc”,“ticker”:“AAPL”,“optionType”:“call”,“strike”:175,“expiration”:“2026-05-16”,“contracts”:1}, …]
“””
try:
contracts = request.get_json(force=True) or []
except Exception:
return jsonify({“error”: “Invalid JSON”}), 400

```
contracts = contracts[:10]
results   = {}
today_d   = date.today()

for c in contracts:
    cid        = c.get("id", "")
    ticker     = (c.get("ticker") or "").upper()
    opt_type   = (c.get("optionType") or "call").lower()
    strike     = float(c.get("strike") or 0)
    expiration = c.get("expiration") or ""
    if not ticker or not strike or not expiration:
        continue
    try:
        stock = yf.Ticker(ticker)
        fi    = stock.fast_info
        underlying = float(
            getattr(fi, "last_price", None) or getattr(fi, "previous_close", None) or 0
        )
        opts = stock.options
        if not opts:
            continue
        exp_date = datetime.strptime(expiration, "%Y-%m-%d").date()
        best_exp = min(opts, key=lambda e: abs(
            (datetime.strptime(e, "%Y-%m-%d").date() - exp_date).days
        ))
        chain = stock.option_chain(best_exp)
        df    = chain.calls if opt_type == "call" else chain.puts
        if df.empty:
            continue
        df      = df.copy()
        df["dist"] = (df["strike"] - strike).abs()
        row     = df.loc[df["dist"].idxmin()]
        bid     = float(row.get("bid",       0) or 0)
        ask     = float(row.get("ask",       0) or 0)
        last    = float(row.get("lastPrice", 0) or 0)
        iv      = float(row.get("impliedVolatility", 0) or 0)
        mid     = round((bid + ask) / 2, 2) if bid > 0 and ask > 0 else (round(last, 2) if last > 0 else None)
        actual_exp = datetime.strptime(best_exp, "%Y-%m-%d").date()
        dte = max(0, (actual_exp - today_d).days)
        results[cid] = {
            "mid":        mid,
            "bid":        round(bid, 2),
            "ask":        round(ask, 2),
            "underlying": round(underlying, 2),
            "dte":        dte,
            "iv":         round(iv * 100, 1) if iv else None,
            "expUsed":    best_exp,
            "strikeUsed": float(row["strike"]),
        }
    except Exception as e:
        log.debug("Option price %s: %s", ticker, e)

return jsonify({"prices": results, "ts": datetime.now().strftime("%I:%M %p")})
```

@app.route(”/api/debug”)
def api_debug():
p = _csp_cache[“progress”]
out = {
“universe”:       len(CSP_UNIVERSE),
“results”:        len(_csp_cache[“results”]),
“running”:        _csp_cache[“running”],
“progress”:       p,
“cacheAge”:       int(time.time() - _csp_cache[“ts”]) if _csp_cache[“ts”] else None,
}
# Quick Nasdaq API test
try:
resp = requests.get(
“https://api.nasdaq.com/api/calendar/earnings?date=” + date.today().strftime(”%Y-%m-%d”),
headers=NASDAQ_HEADERS, timeout=5)
rows = (resp.json().get(“data”) or {}).get(“rows”) or []
out[“nasdaq_test”] = {“status”: resp.status_code, “rows_today”: len(rows)}
except Exception as e:
out[“nasdaq_test”] = {“error”: str(e)}
# Quick yfinance test
try:
t    = yf.Ticker(“AAPL”)
cal  = t.calendar
opts = t.options
out[“yf_test”] = {
“aapl_options_count”: len(opts) if opts else 0,
“calendar_type”: type(cal).**name**,
}
except Exception as e:
out[“yf_test”] = {“error”: str(e)}
return jsonify(out)

@app.route(”/”)
def index():
p = _csp_cache[“progress”]
return (
f”VV CSP Scanner | Universe: {len(CSP_UNIVERSE)} tickers | “
f”Results: {len(_csp_cache[‘results’])} | “
f”Running: {_csp_cache[‘running’]} ({p[‘done’]}/{p[‘total’]}) | “
f”Phase: {p[‘phase’]} | /api/debug”
)

if **name** == “**main**”:
port = int(os.environ.get(“PORT”, 5000))
app.run(host=“0.0.0.0”, port=port, debug=False)