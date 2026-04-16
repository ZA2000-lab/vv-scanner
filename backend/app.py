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

CACHE_TTL             = 6 * 3600
DAYS_AHEAD            = 45
PREFILTER_WORKERS     = 12   # Stage 1: fast checks, can handle more parallelism
SCORING_WORKERS       = 6    # Stage 2: heavy options pulls — keep low to avoid rate limits

NASDAQ_HEADERS = {
“User-Agent”: “Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36”,
“Accept”: “application/json, text/plain, */*”,
“Origin”: “https://www.nasdaq.com”,
“Referer”: “https://www.nasdaq.com/market-activity/earnings”,
}

# ── Cache ──────────────────────────────────────────────────────────────────

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
“CDE”,
# Tech
“AAPL”,“MSFT”,“NVDA”,“GOOGL”,“AMZN”,“META”,“TSLA”,“AVGO”,“ORCL”,“AMD”,
“ADBE”,“CRM”,“INTU”,“NOW”,“PANW”,“CRWD”,“NET”,“DDOG”,“SNOW”,“PLTR”,“MDB”,
“INTC”,“QCOM”,“TXN”,“AMAT”,“LRCX”,“KLAC”,“MU”,“SMCI”,“ARM”,“ON”,“MRVL”,
“CSCO”,“IBM”,“DELL”,“HPQ”,“HPE”,“ACN”,“IT”,“CTSH”,“EPAM”,“VRSK”,
# Financials
“JPM”,“BAC”,“WFC”,“GS”,“MS”,“C”,“V”,“MA”,“AXP”,“COF”,“DFS”,“ALLY”,“SYF”,
“BLK”,“BX”,“KKR”,“APO”,“ARES”,“SCHW”,“IBKR”,“HOOD”,“COIN”,“MSTR”,“PYPL”,
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
“GM”,“F”,“RIVN”,“NIO”,“LCID”,“XPEV”,“LI”,“DAL”,“AAL”,“UAL”,“CCL”,“RCL”,
“ROST”,“TJX”,“BURL”,“DG”,“DLTR”,“LOW”,“ANF”,“AEO”,“GPS”,“URBN”,“KSS”,“JWN”,“M”,
# Staples
“PG”,“KO”,“PEP”,“PM”,“MO”,“MDLZ”,“KHC”,“GIS”,“CL”,“CHD”,“KMB”,“CLX”,
“EL”,“ULTA”,“BBWI”,“TSN”,“HRL”,“MKC”,“SJM”,“CPB”,“SYY”,
# Utilities
“NEE”,“DUK”,“SO”,“D”,“AEP”,“EXC”,“XEL”,“WEC”,“PCG”,“EIX”,“PPL”,“AES”,
“ENPH”,“FSLR”,“RUN”,“SEDG”,
# Materials
“LIN”,“APD”,“CF”,“NTR”,“FCX”,“NEM”,“GOLD”,“AEM”,“KGC”,“WPM”,“PAAS”,
“NUE”,“STLD”,“CLF”,“DD”,“DOW”,“LYB”,“ALB”,“MP”,“ECL”,
# REITs
“AMT”,“CCI”,“EQIX”,“SBAC”,“DLR”,“IRM”,“SPG”,“O”,“VICI”,“PLD”,“PSA”,“EXR”,“EQR”,“AVB”,
“WELL”,“VTR”,“OHI”,“ARE”,“BXP”,
# Telecom / Media
“VZ”,“T”,“TMUS”,“SNAP”,“PINS”,“SPOT”,“RDDT”,
# High-beta
“HOOD”,“SOFI”,“MARA”,“RIOT”,“GME”,“AMC”,“SPCE”,“RKLB”,“ASTS”,
# Mining / metals
“NEM”,“GOLD”,“AEM”,“KGC”,“WPM”,“PAAS”,“EGO”,“IAG”,“AU”,“HL”,“SILV”,“FSM”,
“NGD”,“GFI”,“HMY”,“SBSW”,
# ETFs
“SPY”,“QQQ”,“IWM”,“DIA”,“MDY”,“VTI”,“VOO”,“IVV”,“RSP”,
“GLD”,“SLV”,“GDX”,“GDXJ”,“IAU”,
“TLT”,“IEF”,“LQD”,“HYG”,“JNK”,“AGG”,“BND”,
“XLE”,“XLF”,“XLK”,“XLV”,“XLI”,“XLU”,“XLRE”,“XLB”,“XLY”,“XLP”,“XLC”,
“SMH”,“SOXX”,“IBB”,“ARKK”,“ARKG”,“EEM”,“EFA”,“EWZ”,“EWJ”,“FXI”,“KWEB”,“MCHI”,
“USO”,“UNG”,“CPER”,
“VXX”,“UVXY”,“TQQQ”,“SQQQ”,“UPRO”,“SSO”,
]

def _fetch_wiki_tickers(url: str, name: str) -> list:
try:
resp = requests.get(url, headers={“User-Agent”: “Mozilla/5.0”}, timeout=20)
if resp.status_code != 200:
return []
for df in pd.read_html(resp.text):
cols = [str(c).lower() for c in df.columns]
sym_col = next(
(df.columns[i] for i, c in enumerate(cols)
if c in (“symbol”,“ticker”,“symbol[3]”,“ticker symbol”)),
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
log.warning(“Wikipedia %s: %s”, name, e)
return []

def _build_csp_universe() -> list:
log.info(“Building CSP universe from Wikipedia…”)
sources = {
“sp500”:     “https://en.wikipedia.org/wiki/List_of_S%26P_500_companies”,
“sp400”:     “https://en.wikipedia.org/wiki/List_of_S%26P_400_companies”,
“sp600”:     “https://en.wikipedia.org/wiki/List_of_S%26P_600_companies”,
“nasdaq100”: “https://en.wikipedia.org/wiki/Nasdaq-100”,
}
wiki = []
for name, url in sources.items():
wiki.extend(_fetch_wiki_tickers(url, name))

```
combined = CSP_EXTRAS + wiki
seen, result = set(), []
for t in combined:
    if t not in seen:
        seen.add(t)
        result.append(t)
log.info("Universe: %d tickers total", len(result))
return result
```

CSP_UNIVERSE = list(CSP_EXTRAS)
_csp_universe_lock = threading.Lock()

def _load_universe_bg():
global CSP_UNIVERSE
full = _build_csp_universe()
with _csp_universe_lock:
CSP_UNIVERSE = full
log.info(“CSP universe ready: %d tickers”, len(CSP_UNIVERSE))

threading.Thread(target=_load_universe_bg, daemon=True).start()

# ── Yang-Zhang Realized Volatility ─────────────────────────────────────────

def yang_zhang(df, window=30, tp=252):
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

# ── Earnings date (NO extra HTTP calls) ────────────────────────────────────

def get_earnings_date(stock, sym: str, today_d: date, earnings_map: dict):
“””
Returns (display_str, days_int) or (None, None).
Sources checked in order:
1. yfinance .calendar — already fetched as part of the Ticker object
2. Pre-fetched Nasdaq calendar (earnings_map, bulk fetched once per scan)
We do NOT make additional HTTP requests here. Rate limit safety is critical.
“””
candidates = []

```
# Source 1: yfinance .calendar (dict format in yfinance >= 0.2)
try:
    cal = stock.calendar
    if isinstance(cal, dict):
        earn_list = cal.get("Earnings Date", [])
        if not isinstance(earn_list, list):
            earn_list = [earn_list]
        for ed in earn_list:
            if ed is None:
                continue
            # Handle Timestamp, date, or string
            if hasattr(ed, "date"):
                ed = ed.date()
            elif isinstance(ed, str):
                try:
                    ed = datetime.strptime(ed[:10], "%Y-%m-%d").date()
                except Exception:
                    continue
            elif isinstance(ed, (int, float)):
                try:
                    ed = date.fromtimestamp(ed)
                except Exception:
                    continue
            days = (ed - today_d).days
            if -5 <= days <= 180:
                candidates.append((days, ed.strftime("%b %d")))
                break
    elif isinstance(cal, pd.DataFrame) and not cal.empty:
        for col in cal.columns:
            try:
                ed   = pd.Timestamp(col).date()
                days = (ed - today_d).days
                if -5 <= days <= 180:
                    candidates.append((days, ed.strftime("%b %d")))
                    break
            except Exception:
                pass
except Exception:
    pass

# Source 2: pre-fetched Nasdaq calendar
if sym in earnings_map:
    nd_str, nd_days = earnings_map[sym]
    if nd_days is not None and -2 <= nd_days <= 180:
        candidates.append((nd_days, nd_str))

if not candidates:
    return None, None

candidates.sort(key=lambda x: x[0])
return candidates[0][1], candidates[0][0]
```

# ── Nasdaq bulk earnings calendar ──────────────────────────────────────────

def fetch_nasdaq_calendar() -> dict:
“””
Fetches Nasdaq earnings calendar up to DAYS_AHEAD days out.
Hard stop after 90 seconds total so it never blocks the scan indefinitely.
“””
earnings_map = {}
today     = date.today()
deadline  = time.time() + 90   # never run longer than 90s
session   = requests.Session()
session.headers.update(NASDAQ_HEADERS)
d = today
while d <= today + timedelta(days=DAYS_AHEAD):
if time.time() > deadline:
log.warning(“Nasdaq calendar: hit 90s deadline at %s”, d)
break
if d.weekday() < 5:
try:
resp = session.get(
“https://api.nasdaq.com/api/calendar/earnings?date=” + d.strftime(”%Y-%m-%d”),
timeout=5)   # 5s per request
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
log.info(“Nasdaq calendar: %d tickers (%d days scanned)”, len(earnings_map), (d - today).days)
return earnings_map

# ── Stage 1: Fast pre-filter ───────────────────────────────────────────────

def fast_prefilter(sym: str):
“””
Quick price + volume check. Returns (price, vol) or None.
Tries fast_info first, falls back to a small history pull if needed.
Threshold: price > $2, avg daily volume > 200k.
“””
try:
t  = yf.Ticker(sym)
fi = t.fast_info

```
    # Price — try multiple attribute names across yfinance versions
    px = None
    for attr in ("last_price", "previous_close", "regular_market_price"):
        val = getattr(fi, attr, None)
        if val is not None:
            try:
                f = float(val)
                if f > 0:
                    px = f
                    break
            except Exception:
                pass

    if px is None or px < 2:
        return None

    # Volume — try multiple attribute names
    vol = None
    for attr in ("three_month_average_volume", "regular_market_volume",
                 "average_volume", "average_daily_volume3_month",
                 "average_daily_volume10_day"):
        val = getattr(fi, attr, None)
        if val is not None:
            try:
                f = float(val)
                if f > 0:
                    vol = f
                    break
            except Exception:
                pass

    # Fallback: grab 10-day history for volume if fast_info didn't have it
    if vol is None or vol == 0:
        h = t.history(period="10d")
        if h.empty:
            return None
        vol = float(h["Volume"].mean())

    if vol < 200_000:
        return None

    return (px, vol)

except Exception as e:
    log.debug("Prefilter %s: %s", sym, e)
    return None
```

# ── Stage 2: Full CSP scorer ───────────────────────────────────────────────

def score_csp_ticker(sym: str, earnings_map: dict):
“””
Full CSP scoring. Makes 3 network calls per ticker max:
1. stock.history(period=“1y”)
2. stock.options  (list of expiry dates)
3. stock.option_chain(target_exp)
.calendar is a cached property — no additional call.
“””
try:
stock   = yf.Ticker(sym)
today_d = date.today()

```
    # 1. Price history
    h1y = stock.history(period="1y")
    if h1y.empty or len(h1y) < 60:
        return None
    price = float(h1y["Close"].iloc[-1])
    if price <= 0:
        return None

    # 2. Volume
    avg_vol = float(h1y["Volume"].tail(30).mean())
    if avg_vol < 300_000:
        return None

    # 3. Realized vol
    rv30 = yang_zhang(h1y, window=30)
    rv_series = h1y["Close"].pct_change().rolling(30).std() * (252 ** 0.5)
    rv_series = rv_series.dropna()
    if rv_series.empty:
        return None
    rv_min = float(rv_series.quantile(0.10))
    rv_max = float(rv_series.quantile(0.90))

    # 4. Options expiries
    try:
        opts = stock.options
    except Exception:
        return None
    if not opts:
        return None

    # Find best expiry: prefer 28-55 DTE, fall back to >= 21
    target_exp, target_dte = None, None
    for exp in sorted(opts):
        try:
            dte = (datetime.strptime(exp, "%Y-%m-%d").date() - today_d).days
            if 28 <= dte <= 55:
                target_exp, target_dte = exp, dte
                break
        except Exception:
            continue
    if not target_exp:
        for exp in sorted(opts):
            try:
                dte = (datetime.strptime(exp, "%Y-%m-%d").date() - today_d).days
                if dte >= 21:
                    target_exp, target_dte = exp, dte
                    break
            except Exception:
                continue
    if not target_exp:
        return None

    # 5. Put chain
    try:
        puts = stock.option_chain(target_exp).puts
    except Exception:
        return None
    if puts is None or puts.empty:
        return None

    # ATM IV
    atm_idx = (puts["strike"] - price).abs().idxmin()
    atm_iv  = float(puts.loc[atm_idx, "impliedVolatility"])
    if atm_iv <= 0 or atm_iv > 5.0:
        return None

    # IV Rank
    iv_rank = (int(min(100, max(0, (atm_iv - rv_min) / (rv_max - rv_min) * 100)))
               if rv_max > rv_min else 50)
    iv_rv_ratio = round(atm_iv / rv30, 2) if rv30 > 0 else 1.0

    # ~30-delta put strike
    T = target_dte / 365.0
    target_strike_raw = price * (1 - 0.45 * atm_iv * (T ** 0.5))
    otm_puts = puts[puts["strike"] <= price * 1.02].copy()
    if otm_puts.empty:
        return None
    otm_puts["dist"] = (otm_puts["strike"] - target_strike_raw).abs()
    best_row   = otm_puts.loc[otm_puts["dist"].idxmin()]
    put_strike = float(best_row["strike"])

    bid  = float(best_row.get("bid",  0) or 0)
    ask  = float(best_row.get("ask",  0) or 0)
    oi   = int(best_row.get("openInterest", 0) or 0)
    iv_p = float(best_row.get("impliedVolatility", atm_iv) or atm_iv)

    if bid > 0 and ask > 0:
        mid = round((bid + ask) / 2, 2)
    else:
        mid = round(price * iv_p * (T ** 0.5) * 0.40 * ((put_strike / price) ** 0.5), 2)

    if mid < 0.05:
        return None

    spread_pct = round((ask - bid) / mid * 100, 1) if (mid > 0 and ask > bid) else 0
    breakeven  = round(put_strike - mid, 2)
    collateral = put_strike * 100
    roc_pct    = round(mid * 100 / collateral * 100, 2) if collateral > 0 else 0
    roc_ann    = round(roc_pct * (365 / target_dte), 1) if target_dte > 0 else 0

    # Earnings — uses only cached/pre-fetched data, no extra HTTP calls
    earn_str, earn_days = get_earnings_date(stock, sym, today_d, earnings_map)
    earn_within = (earn_days is not None and earn_days <= target_dte)

    # Quality rating
    if earn_within or spread_pct > 20 or iv_rank < 45:
        quality = "SKIP"
    elif iv_rank >= 60 and iv_rv_ratio >= 1.2 and spread_pct <= 10:
        quality = "STRONG"
    else:
        quality = "DECENT"

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
        "ivRvRatio":     iv_rv_ratio,
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

# ── Scan orchestrator ──────────────────────────────────────────────────────

def _sort_results(results):
return sorted(
results,
key=lambda x: ({“STRONG”: 0, “DECENT”: 1, “SKIP”: 2}.get(x[“quality”], 3), -x[“rocAnn”])
)

def run_csp_scan():
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
_csp_cache["results"] = []

# Wait for Wikipedia universe load (up to 25s)
deadline = time.time() + 25
while len(CSP_UNIVERSE) <= len(CSP_EXTRAS) and time.time() < deadline:
    time.sleep(1)

universe = list(CSP_UNIVERSE)
log.info("Scan start — %d tickers", len(universe))

# Fetch Nasdaq earnings calendar (once)
_csp_cache["progress"]["phase"] = "earnings calendar"
try:
    earnings_map = fetch_nasdaq_calendar()
except Exception as e:
    log.warning("Nasdaq calendar error: %s — proceeding without it", e)
    earnings_map = {}

# Stage 1: Fast pre-filter
_csp_cache["progress"].update({
    "done": 0, "total": len(universe), "phase": "pre-filter",
    "currentTicker": "", "passedPrefilter": 0,
})
log.info("Stage 1: pre-filtering %d tickers...", len(universe))

passed = []
lock1  = threading.Lock()

def prefilter_one(sym):
    ok = fast_prefilter(sym)
    with lock1:
        _csp_cache["progress"]["done"] += 1
        _csp_cache["progress"]["currentTicker"] = sym
        if ok:
            passed.append(sym)
            _csp_cache["progress"]["passedPrefilter"] = len(passed)

with concurrent.futures.ThreadPoolExecutor(max_workers=PREFILTER_WORKERS) as pool:
    futs = [pool.submit(prefilter_one, sym) for sym in universe]
    for f in concurrent.futures.as_completed(futs):
        try:
            f.result()
        except Exception:
            pass

log.info("Stage 1 done: %d / %d passed", len(passed), len(universe))

if not passed:
    log.error("Zero tickers passed pre-filter — check yfinance connectivity")
    _csp_cache["running"]  = False
    _csp_cache["progress"]["phase"] = "done"
    return

# Stage 2: Full scoring
_csp_cache["progress"].update({
    "done": 0, "total": len(passed), "phase": "scoring",
    "currentTicker": "", "found": 0,
})
log.info("Stage 2: scoring %d tickers...", len(passed))

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
            _csp_cache["results"] = _sort_results(results)

with concurrent.futures.ThreadPoolExecutor(max_workers=SCORING_WORKERS) as pool:
    futs = [pool.submit(score_one, sym) for sym in passed]
    for f in concurrent.futures.as_completed(futs):
        try:
            f.result()
        except Exception:
            pass

_csp_cache["results"]  = _sort_results(results)
_csp_cache["ts"]       = time.time()
_csp_cache["running"]  = False
_csp_cache["progress"].update({
    "done":  len(passed), "total": len(passed),
    "phase": "done",      "found": len(results),
})
elapsed = int(time.time() - scan_start)
log.info("Scan done — %d results / %d scored / %d universe / %ds",
         len(results), len(passed), len(universe), elapsed)
```

# ── API endpoints ──────────────────────────────────────────────────────────

def _maybe_start():
if not _csp_cache[“running”] and (
not _csp_cache[“ts”] or time.time() - _csp_cache[“ts”] > CACHE_TTL
):
threading.Thread(target=run_csp_scan, daemon=True).start()

@app.route(”/api/csp/scan”)
@app.route(”/api/scan”)
def api_csp_scan():
_maybe_start()
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
@app.route(”/api/progress”)
def api_csp_progress():
p       = _csp_cache[“progress”]
pct     = int(p[“done”] / p[“total”] * 100) if p[“total”] > 0 else 0
elapsed = int(time.time() - p.get(“scanStart”, time.time())) if _csp_cache[“running”] else 0
eta_s   = None
if p[“phase”] == “scoring” and p[“done”] > 3 and _csp_cache[“running”] and elapsed > 0:
rate  = p[“done”] / elapsed
eta_s = int((p[“total”] - p[“done”]) / rate) if rate > 0 else None

```
phase_labels = {
    "loading universe":  "Loading ticker universe...",
    "earnings calendar": "Fetching Nasdaq earnings calendar...",
    "pre-filter": (
        f"Stage 1 of 2: Checking {p['total']:,} tickers — "
        f"{p.get('passedPrefilter', 0)} passed so far"
    ),
    "scoring": (
        f"Stage 2 of 2: Scoring options chains — "
        f"{p['done']}/{p['total']} done · {p.get('found', 0)} setups found"
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
@app.route(”/api/refresh”, methods=[“POST”])
def api_csp_refresh():
if _csp_cache[“running”]:
return jsonify({“message”: “Scan already running”, “running”: True}), 202
_csp_cache[“ts”]      = 0
_csp_cache[“results”] = []
threading.Thread(target=run_csp_scan, daemon=True).start()
return jsonify({“message”: “CSP refresh started”, “universe”: len(CSP_UNIVERSE)}), 202

@app.route(”/api/csp/status”)
@app.route(”/api/status”)
def api_csp_status():
return jsonify({
“status”:   “ok”,
“cached”:   bool(_csp_cache[“ts”]),
“count”:    len(_csp_cache[“results”]),
“running”:  _csp_cache[“running”],
“universe”: len(CSP_UNIVERSE),
“progress”: _csp_cache[“progress”],
})

@app.route(”/api/debug”)
def api_debug():
p   = _csp_cache[“progress”]
out = {
“universe_size”:     len(CSP_UNIVERSE),
“results_count”:     len(_csp_cache[“results”]),
“running”:           _csp_cache[“running”],
“progress”:          p,
“cache_age_secs”:    int(time.time() - _csp_cache[“ts”]) if _csp_cache[“ts”] else None,
“prefilter_workers”: PREFILTER_WORKERS,
“scoring_workers”:   SCORING_WORKERS,
}

```
# Nasdaq connectivity test
try:
    resp = requests.get(
        "https://api.nasdaq.com/api/calendar/earnings?date=" + date.today().strftime("%Y-%m-%d"),
        headers=NASDAQ_HEADERS, timeout=6)
    rows = (resp.json().get("data") or {}).get("rows") or []
    out["nasdaq_test"] = {
        "status": resp.status_code,
        "rows_today": len(rows),
        "sample": [r.get("symbol") for r in rows[:5]],
    }
except Exception as e:
    out["nasdaq_test"] = {"error": str(e)}

# yfinance test on AAPL
try:
    t   = yf.Ticker("AAPL")
    fi  = t.fast_info
    h5  = t.history(period="5d")
    cal = t.calendar
    opts = t.options
    out["yf_test"] = {
        "last_price":         getattr(fi, "last_price", "N/A"),
        "previous_close":     getattr(fi, "previous_close", "N/A"),
        "options_count":      len(opts) if opts else 0,
        "history_rows":       len(h5),
        "calendar_type":      type(cal).__name__,
        "calendar_preview":   str(cal)[:300] if cal is not None else None,
    }
except Exception as e:
    out["yf_test"] = {"error": str(e)}

# Prefilter test on known-good tickers
test_syms = ["AAPL", "MSFT", "SPY", "TSLA", "XYZ_FAKE"]
pf = {}
for sym in test_syms:
    try:
        pf[sym] = fast_prefilter(sym)
    except Exception as e:
        pf[sym] = str(e)
out["prefilter_test"] = pf

return jsonify(out)
```

# ── Portfolio endpoints ────────────────────────────────────────────────────

@app.route(”/api/prices”)
def api_prices():
tickers = [t.strip().upper()
for t in request.args.get(“tickers”, “”).split(”,”) if t.strip()][:25]
if not tickers:
return jsonify({“error”: “No tickers provided”}), 400
prices = {}
for sym in tickers:
try:
fi = yf.Ticker(sym).fast_info
for attr in (“last_price”, “previous_close”, “regular_market_price”):
px = getattr(fi, attr, None)
if px and float(px) > 0:
prices[sym] = round(float(px), 2)
break
except Exception:
pass
return jsonify({“prices”: prices, “ts”: datetime.now().strftime(”%I:%M %p”)})

@app.route(”/api/option-prices”, methods=[“POST”])
def api_option_prices():
try:
contracts = request.get_json(force=True) or []
except Exception:
return jsonify({“error”: “Invalid JSON”}), 400
results = {}
today_d = date.today()
for c in (contracts[:10]):
cid        = c.get(“id”, “”)
ticker     = (c.get(“ticker”) or “”).upper()
opt_type   = (c.get(“optionType”) or “call”).lower()
strike     = float(c.get(“strike”) or 0)
expiration = c.get(“expiration”) or “”
if not ticker or not strike or not expiration:
continue
try:
stock = yf.Ticker(ticker)
fi    = stock.fast_info
underlying = 0.0
for attr in (“last_price”, “previous_close”):
val = getattr(fi, attr, None)
if val and float(val) > 0:
underlying = float(val)
break
opts = stock.options
if not opts:
continue
exp_date = datetime.strptime(expiration, “%Y-%m-%d”).date()
best_exp = min(opts, key=lambda e: abs(
(datetime.strptime(e, “%Y-%m-%d”).date() - exp_date).days
))
chain = stock.option_chain(best_exp)
df    = chain.calls if opt_type == “call” else chain.puts
if df.empty:
continue
df = df.copy()
df[“dist”] = (df[“strike”] - strike).abs()
row  = df.loc[df[“dist”].idxmin()]
bid  = float(row.get(“bid”, 0) or 0)
ask  = float(row.get(“ask”, 0) or 0)
last = float(row.get(“lastPrice”, 0) or 0)
iv   = float(row.get(“impliedVolatility”, 0) or 0)
mid  = (round((bid+ask)/2, 2) if bid > 0 and ask > 0
else round(last, 2) if last > 0 else None)
dte  = max(0, (datetime.strptime(best_exp, “%Y-%m-%d”).date() - today_d).days)
results[cid] = {
“mid”: mid, “bid”: round(bid,2), “ask”: round(ask,2),
“underlying”: round(underlying,2), “dte”: dte,
“iv”: round(iv*100,1) if iv else None,
“expUsed”: best_exp, “strikeUsed”: float(row[“strike”]),
}
except Exception as e:
log.debug(“Option price %s: %s”, ticker, e)
return jsonify({“prices”: results, “ts”: datetime.now().strftime(”%I:%M %p”)})

@app.route(”/”)
def index():
p = _csp_cache[“progress”]
return (f”VV CSP Scanner | Universe: {len(CSP_UNIVERSE)} | “
f”Results: {len(_csp_cache[‘results’])} | Running: {_csp_cache[‘running’]} | “
f”Phase: {p[‘phase’]} ({p[‘done’]}/{p[‘total’]}) | /api/debug”)

if **name** == “**main**”:
port = int(os.environ.get(“PORT”, 5000))
app.run(host=“0.0.0.0”, port=port, debug=False)