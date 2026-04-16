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

CACHE_TTL         = 6 * 3600
DAYS_AHEAD        = 45
PREFILTER_WORKERS = 8    # Stage 1 - fast_info checks
SCORING_WORKERS   = 3    # Stage 2 - MUST stay low: each thread pulls 3 yfinance calls
# gunicorn has 2 workers x 4 threads = 8 threads total for HTTP
# so scan threads must leave headroom for HTTP to be served
SCORING_CAP       = 200  # Hard cap: 200 tickers x ~4s each / 3 workers = ~4.5 min max

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

# ── Universe — hardcoded only, NO Wikipedia fetch ──────────────────────────

# ~350 liquid, optionable tickers with good open interest.

# Scanning all of S&P 1500 takes 30+ min. This list covers 95% of useful CSP setups.

UNIVERSE = [
# Mega-cap tech
“AAPL”,“MSFT”,“NVDA”,“GOOGL”,“AMZN”,“META”,“TSLA”,“AVGO”,“ORCL”,“AMD”,
“ADBE”,“CRM”,“INTU”,“NOW”,“PANW”,“CRWD”,“NET”,“DDOG”,“SNOW”,“PLTR”,“MDB”,
“INTC”,“QCOM”,“TXN”,“AMAT”,“LRCX”,“KLAC”,“MU”,“SMCI”,“ARM”,“ON”,“MRVL”,
“CSCO”,“IBM”,“DELL”,“HPE”,“ACN”,“VRSK”,“PYPL”,“SQ”,“SHOP”,
# Financials
“JPM”,“BAC”,“WFC”,“GS”,“MS”,“C”,“V”,“MA”,“AXP”,“COF”,“SCHW”,“IBKR”,
“BLK”,“BX”,“KKR”,“APO”,“COIN”,“MSTR”,“HOOD”,“SOFI”,“ALLY”,
# Healthcare
“UNH”,“LLY”,“JNJ”,“ABBV”,“MRK”,“PFE”,“AMGN”,“GILD”,“VRTX”,“REGN”,“ISRG”,
“TMO”,“DHR”,“MRNA”,“BIIB”,“SYK”,“MDT”,“BSX”,“CI”,“ELV”,“HUM”,“CVS”,
# Energy
“XOM”,“CVX”,“COP”,“EOG”,“DVN”,“OXY”,“SLB”,“HAL”,“VLO”,“MPC”,“LNG”,
# Industrials
“GE”,“HON”,“CAT”,“DE”,“MMM”,“ETN”,“RTX”,“LMT”,“GD”,“NOC”,“BA”,“UPS”,“FDX”,
“CSX”,“UNP”,“WM”,“SHW”,“EMR”,“TT”,“CARR”,
# Consumer
“HD”,“WMT”,“COST”,“TGT”,“MCD”,“SBUX”,“CMG”,“NKE”,“LULU”,“BKNG”,“UBER”,
“ABNB”,“DIS”,“NFLX”,“EA”,“DKNG”,“MGM”,“GM”,“F”,“RIVN”,“DAL”,“AAL”,“UAL”,
“ROST”,“TJX”,“BURL”,“DG”,“DLTR”,“LOW”,
# Staples
“PG”,“KO”,“PEP”,“PM”,“MO”,“CL”,“KMB”,“EL”,“ULTA”,“TSN”,“SYY”,
# Utilities / Energy transition
“NEE”,“DUK”,“SO”,“PCG”,“ENPH”,“FSLR”,
# Materials
“LIN”,“FCX”,“NEM”,“GOLD”,“ALB”,“DD”,“DOW”,“NUE”,“STLD”,
# REITs
“AMT”,“CCI”,“EQIX”,“DLR”,“SPG”,“O”,“PLD”,“PSA”,“VICI”,
# Telecom / Media
“VZ”,“T”,“TMUS”,“SNAP”,“PINS”,“SPOT”,“RDDT”,
# High-vol / speculative
“MARA”,“RIOT”,“GME”,“RKLB”,“ASTS”,“SPCE”,“NIO”,“XPEV”,“RIVN”,“LCID”,
# Mining / metals
“WPM”,“PAAS”,“KGC”,“AEM”,“CDE”,“HL”,“EGO”,“GFI”,“HMY”,“NGD”,
# Liquid ETFs — always great CSP candidates
“SPY”,“QQQ”,“IWM”,“GLD”,“SLV”,“GDX”,“GDXJ”,“TLT”,“HYG”,
“XLE”,“XLF”,“XLK”,“XLV”,“XLI”,“XLU”,“XLY”,“XLP”,
“SMH”,“SOXX”,“IBB”,“ARKK”,“EEM”,“EWZ”,“KWEB”,“FXI”,
“TQQQ”,“SQQQ”,“UVXY”,“VXX”,
]

# ── Yang-Zhang Realized Volatility ─────────────────────────────────────────

def yang_zhang(df, window=20, tp=252):
try:
if len(df) < window + 2:
return float(df[“Close”].pct_change().std() * np.sqrt(tp))
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

# ── Earnings date ──────────────────────────────────────────────────────────

def get_earnings_date(stock, sym, today_d, earnings_map):
“””
Returns (display_str, days_int) or (None, None).
Only uses yfinance .calendar (cached) and pre-fetched Nasdaq map — no extra HTTP calls.
“””
candidates = []
try:
cal = stock.calendar
if isinstance(cal, dict):
earn_list = cal.get(“Earnings Date”, [])
if not isinstance(earn_list, list):
earn_list = [earn_list]
for ed in earn_list:
if ed is None:
continue
if hasattr(ed, “date”):
ed = ed.date()
elif isinstance(ed, str):
try:
ed = datetime.strptime(ed[:10], “%Y-%m-%d”).date()
except Exception:
continue
days = (ed - today_d).days
if -5 <= days <= 180:
candidates.append((days, ed.strftime(”%b %d”)))
break
elif isinstance(cal, pd.DataFrame) and not cal.empty:
for col in cal.columns:
try:
ed   = pd.Timestamp(col).date()
days = (ed - today_d).days
if -5 <= days <= 180:
candidates.append((days, ed.strftime(”%b %d”)))
break
except Exception:
pass
except Exception:
pass

```
if sym in earnings_map:
    nd_str, nd_days = earnings_map[sym]
    if nd_days is not None and -2 <= nd_days <= 180:
        candidates.append((nd_days, nd_str))

if not candidates:
    return None, None
candidates.sort(key=lambda x: x[0])
return candidates[0][1], candidates[0][0]
```

# ── Nasdaq calendar ────────────────────────────────────────────────────────

def fetch_nasdaq_calendar():
earnings_map = {}
today    = date.today()
deadline = time.time() + 60
session  = requests.Session()
session.headers.update(NASDAQ_HEADERS)
d = today
while d <= today + timedelta(days=DAYS_AHEAD):
if time.time() > deadline:
log.warning(“Nasdaq calendar: 60s deadline hit at %s”, d)
break
if d.weekday() < 5:
try:
resp = session.get(
“https://api.nasdaq.com/api/calendar/earnings?date=” + d.strftime(”%Y-%m-%d”),
timeout=4)
if resp.status_code == 200:
rows = (resp.json().get(“data”) or {}).get(“rows”) or []
diff = (d - today).days
for row in rows:
sym = (row.get(“symbol”) or “”).upper().strip().replace(”/”, “-”)
if sym and 1 <= len(sym) <= 6 and sym.replace(”-”,””).isalpha():
if sym not in earnings_map:
earnings_map[sym] = (d.strftime(”%b %d”), diff)
time.sleep(0.05)
except Exception as e:
log.debug(“Nasdaq cal %s: %s”, d, e)
d += timedelta(days=1)
log.info(“Nasdaq calendar: %d tickers”, len(earnings_map))
return earnings_map

# ── Stage 1: Fast prefilter ────────────────────────────────────────────────

def fast_prefilter(sym):
“”“Returns (price, vol) if passes, else None. Uses fast_info only.”””
try:
fi = yf.Ticker(sym).fast_info
px = None
for attr in (“last_price”, “previous_close”, “regular_market_price”):
v = getattr(fi, attr, None)
if v is not None:
try:
f = float(v)
if f > 0:
px = f
break
except Exception:
pass
if px is None or px < 2:
return None

```
    vol = None
    for attr in ("three_month_average_volume", "regular_market_volume",
                 "average_volume", "average_daily_volume3_month"):
        v = getattr(fi, attr, None)
        if v is not None:
            try:
                f = float(v)
                if f > 0:
                    vol = f
                    break
            except Exception:
                pass

    # volume fallback: quick 5d history
    if not vol:
        h = yf.Ticker(sym).history(period="5d")
        if h.empty:
            return None
        vol = float(h["Volume"].mean())

    if vol < 500_000:
        return None
    return (px, vol)
except Exception:
    return None
```

# ── Stage 2: Full CSP scorer ───────────────────────────────────────────────

def score_csp_ticker(sym, earnings_map):
“”“3 yfinance calls: history(3mo), options list, option_chain.”””
try:
stock   = yf.Ticker(sym)
today_d = date.today()

```
    # History — 3mo is enough for RV; much faster than 6mo/1y
    hist = stock.history(period="3mo")
    if hist.empty or len(hist) < 30:
        return None
    price = float(hist["Close"].iloc[-1])
    if price <= 0:
        return None

    avg_vol = float(hist["Volume"].tail(20).mean())
    if avg_vol < 500_000:
        return None

    rv30 = yang_zhang(hist, window=20)

    # IV rank proxy from 3mo RV range
    rv_series = hist["Close"].pct_change().rolling(20).std() * (252 ** 0.5)
    rv_series = rv_series.dropna()
    if rv_series.empty:
        return None
    rv_min = float(rv_series.quantile(0.10))
    rv_max = float(rv_series.quantile(0.90))

    # Options expiries
    try:
        opts = stock.options
    except Exception:
        return None
    if not opts:
        return None

    # Best expiry: 28-55 DTE preferred
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

    # Put chain
    try:
        puts = stock.option_chain(target_exp).puts
    except Exception:
        return None
    if puts is None or puts.empty:
        return None

    atm_idx = (puts["strike"] - price).abs().idxmin()
    atm_iv  = float(puts.loc[atm_idx, "impliedVolatility"])
    if atm_iv <= 0 or atm_iv > 5.0:
        return None

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

    earn_str, earn_days = get_earnings_date(stock, sym, today_d, earnings_map)
    earn_within = (earn_days is not None and earn_days <= target_dte)

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
    log.debug("CSP %s: %s", sym, e)
    return None
```

# ── Sort helper ────────────────────────────────────────────────────────────

def _sort(results):
return sorted(results,
key=lambda x: ({“STRONG”:0,“DECENT”:1,“SKIP”:2}.get(x[“quality”],3), -x[“rocAnn”]))

# ── Scan orchestrator ──────────────────────────────────────────────────────

def run_csp_scan():
if _csp_cache[“running”]:
return
_csp_cache[“running”] = True
_csp_cache[“results”] = []
scan_start = time.time()

```
_csp_cache["progress"] = {
    "done": 0, "total": 0, "phase": "starting",
    "currentTicker": "", "found": 0, "scanStart": scan_start,
    "passedPrefilter": 0,
}

universe = list(UNIVERSE)
log.info("Scan start — %d tickers", len(universe))

# Nasdaq calendar runs concurrently with Stage 1
earnings_map = {}
cal_done = threading.Event()

def fetch_cal():
    try:
        result = fetch_nasdaq_calendar()
        earnings_map.update(result)
    except Exception as e:
        log.warning("Calendar bg: %s", e)
    finally:
        cal_done.set()

threading.Thread(target=fetch_cal, daemon=True).start()

# Stage 1: fast prefilter
_csp_cache["progress"].update({
    "done": 0, "total": len(universe), "phase": "pre-filter",
})

passed = []
passed_vols = {}
lock1 = threading.Lock()

def prefilter_one(sym):
    ok = fast_prefilter(sym)
    with lock1:
        _csp_cache["progress"]["done"] += 1
        _csp_cache["progress"]["currentTicker"] = sym
        if ok:
            px, vol = ok
            passed.append(sym)
            passed_vols[sym] = vol
            _csp_cache["progress"]["passedPrefilter"] = len(passed)

with concurrent.futures.ThreadPoolExecutor(max_workers=PREFILTER_WORKERS) as pool:
    futs = [pool.submit(prefilter_one, sym) for sym in universe]
    for f in concurrent.futures.as_completed(futs):
        try: f.result()
        except Exception: pass

log.info("Stage 1: %d / %d passed prefilter", len(passed), len(universe))

if not passed:
    log.error("Zero passed prefilter — yfinance connectivity issue")
    _csp_cache["running"] = False
    _csp_cache["progress"]["phase"] = "done"
    return

# Sort by volume, cap at SCORING_CAP
sorted_passed = sorted(passed, key=lambda s: -passed_vols.get(s, 0))
to_score = sorted_passed[:SCORING_CAP]
log.info("Stage 2: scoring %d tickers (cap %d)", len(to_score), SCORING_CAP)

# Wait for calendar (should be done by now, max 15s extra)
cal_done.wait(timeout=15)
log.info("Calendar ready: %d entries", len(earnings_map))

# Stage 2: full scoring
_csp_cache["progress"].update({
    "done": 0, "total": len(to_score), "phase": "scoring",
    "currentTicker": "", "found": 0,
})

results = []
lock2 = threading.Lock()

def score_one(sym):
    r = score_csp_ticker(sym, earnings_map)
    with lock2:
        _csp_cache["progress"]["done"] += 1
        _csp_cache["progress"]["currentTicker"] = sym
        if r:
            results.append(r)
            _csp_cache["progress"]["found"] = len(results)
            _csp_cache["results"] = _sort(results)

with concurrent.futures.ThreadPoolExecutor(max_workers=SCORING_WORKERS) as pool:
    futs = [pool.submit(score_one, sym) for sym in to_score]
    for f in concurrent.futures.as_completed(futs):
        try: f.result()
        except Exception: pass

_csp_cache["results"]  = _sort(results)
_csp_cache["ts"]       = time.time()
_csp_cache["running"]  = False
_csp_cache["progress"].update({
    "done": len(to_score), "total": len(to_score),
    "phase": "done", "found": len(results),
})
elapsed = int(time.time() - scan_start)
log.info("Scan done: %d results / %d scored / %ds", len(results), len(to_score), elapsed)
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
“universe”:     len(UNIVERSE),
“progress”:     _csp_cache[“progress”],
})

@app.route(”/api/csp/progress”)
@app.route(”/api/progress”)
def api_csp_progress():
p       = _csp_cache[“progress”]
pct     = int(p[“done”] / p[“total”] * 100) if p[“total”] > 0 else 0
elapsed = int(time.time() - p.get(“scanStart”, time.time())) if _csp_cache[“running”] else 0
eta_s   = None
if p[“phase”] == “scoring” and p[“done”] > 2 and elapsed > 0:
rate  = p[“done”] / elapsed
eta_s = int((p[“total”] - p[“done”]) / rate) if rate > 0 else None

```
labels = {
    "starting":    "Starting scan...",
    "pre-filter":  f"Stage 1 of 2: Quick price & volume check — {p.get('passedPrefilter',0)} tickers passed so far",
    "scoring":     f"Stage 2 of 2: Scoring options — {p['done']}/{p['total']} done · {p.get('found',0)} setups found",
    "done":        "Scan complete ✓",
    "idle":        "Ready",
}

return jsonify({
    "done":          p["done"],
    "total":         p["total"],
    "pct":           pct,
    "phase":         p["phase"],
    "phaseLabel":    labels.get(p["phase"], p["phase"]),
    "currentTicker": p.get("currentTicker", ""),
    "found":         p.get("found", 0),
    "passed":        p.get("passedPrefilter", 0),
    "elapsed":       elapsed,
    "eta":           eta_s,
    "universe":      len(UNIVERSE),
    "running":       _csp_cache["running"],
})
```

@app.route(”/api/csp/refresh”, methods=[“POST”])
@app.route(”/api/refresh”, methods=[“POST”])
def api_csp_refresh():
if _csp_cache[“running”]:
return jsonify({“message”: “Already running”, “running”: True}), 202
_csp_cache[“ts”]      = 0
_csp_cache[“results”] = []
threading.Thread(target=run_csp_scan, daemon=True).start()
return jsonify({“message”: “Started”, “universe”: len(UNIVERSE)}), 200

@app.route(”/api/csp/status”)
@app.route(”/api/status”)
def api_csp_status():
return jsonify({
“status”:   “ok”,
“cached”:   bool(_csp_cache[“ts”]),
“count”:    len(_csp_cache[“results”]),
“running”:  _csp_cache[“running”],
“universe”: len(UNIVERSE),
“progress”: _csp_cache[“progress”],
})

@app.route(”/api/debug”)
def api_debug():
p = _csp_cache[“progress”]
out = {
“universe”:          len(UNIVERSE),
“results”:           len(_csp_cache[“results”]),
“running”:           _csp_cache[“running”],
“phase”:             p[“phase”],
“done”:              p[“done”],
“total”:             p[“total”],
“found”:             p.get(“found”, 0),
“passed_prefilter”:  p.get(“passedPrefilter”, 0),
“scoring_workers”:   SCORING_WORKERS,
“prefilter_workers”: PREFILTER_WORKERS,
“scoring_cap”:       SCORING_CAP,
“cache_age_secs”:    int(time.time() - _csp_cache[“ts”]) if _csp_cache[“ts”] else None,
}
# Quick yfinance test
try:
t  = yf.Ticker(“AAPL”)
fi = t.fast_info
h  = t.history(period=“5d”)
out[“yf_aapl”] = {
“last_price”:   getattr(fi, “last_price”, “N/A”),
“history_rows”: len(h),
“options_count”: len(t.options) if t.options else 0,
}
except Exception as e:
out[“yf_aapl”] = {“error”: str(e)}
# Quick prefilter test
pf = {}
for sym in [“AAPL”, “SPY”, “TSLA”]:
try:
pf[sym] = fast_prefilter(sym)
except Exception as e:
pf[sym] = str(e)
out[“prefilter_test”] = pf
return jsonify(out)

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
for attr in (“last_price”, “previous_close”):
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
for c in contracts[:10]:
cid = c.get(“id”, “”)
ticker = (c.get(“ticker”) or “”).upper()
opt_type = (c.get(“optionType”) or “call”).lower()
strike = float(c.get(“strike”) or 0)
expiration = c.get(“expiration”) or “”
if not ticker or not strike or not expiration:
continue
try:
stock = yf.Ticker(ticker)
fi = stock.fast_info
underlying = 0.0
for attr in (“last_price”, “previous_close”):
v = getattr(fi, attr, None)
if v and float(v) > 0:
underlying = float(v)
break
opts = stock.options
if not opts:
continue
exp_date = datetime.strptime(expiration, “%Y-%m-%d”).date()
best_exp = min(opts, key=lambda e: abs(
(datetime.strptime(e, “%Y-%m-%d”).date() - exp_date).days))
chain = stock.option_chain(best_exp)
df = chain.calls if opt_type == “call” else chain.puts
if df.empty:
continue
df = df.copy()
df[“dist”] = (df[“strike”] - strike).abs()
row = df.loc[df[“dist”].idxmin()]
bid  = float(row.get(“bid”, 0) or 0)
ask  = float(row.get(“ask”, 0) or 0)
last = float(row.get(“lastPrice”, 0) or 0)
iv   = float(row.get(“impliedVolatility”, 0) or 0)
mid  = (round((bid+ask)/2, 2) if bid > 0 and ask > 0
else round(last, 2) if last > 0 else None)
dte = max(0, (datetime.strptime(best_exp, “%Y-%m-%d”).date() - today_d).days)
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
return (f”VV CSP Scanner | Universe: {len(UNIVERSE)} | “
f”Results: {len(_csp_cache[‘results’])} | “
f”Running: {_csp_cache[‘running’]} | “
f”Phase: {p[‘phase’]} ({p[‘done’]}/{p[‘total’]}) | “
f”/api/debug”)

if **name** == “**main**”:
port = int(os.environ.get(“PORT”, 5000))
app.run(host=“0.0.0.0”, port=port, debug=False, threaded=True)