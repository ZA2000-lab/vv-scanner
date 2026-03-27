“””
VV Scanner Backend — Full Market Edition
Earnings IV Crush Scanner — Calendar Spread Strategy

Exact thresholds from VolatilityVibes calculator.py:

- ts_slope_0_45 <= -0.00406
- iv30_rv30 >= 1.25
- avg_volume >= 1,500,000

Universe: S&P 500 + Nasdaq 100 (fetched live from Wikipedia)
+ large supplemental list = 1,000-1,400 unique tickers
Only surfaces stocks with earnings within the next 35 days AND options available.
“””

from flask import Flask, jsonify
from flask_cors import CORS
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import numpy as np
import threading
import concurrent.futures
import time
import logging

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(**name**)

app = Flask(**name**)
CORS(app)

# ── Config ────────────────────────────────────────────────────────────────────

CACHE_TTL      = 6 * 3600   # refresh every 6 hours
EARNINGS_WINDOW = 50         # days ahead to look for earnings
MAX_WORKERS    = 8           # parallel yfinance threads

_cache = {
“results”:  None,
“universe”: [],
“ts”:       0,
“running”:  False,
“progress”: {“done”: 0, “total”: 0, “phase”: “idle”},
}

# ── Universe builders ─────────────────────────────────────────────────────────

def fetch_sp500():
try:
df = pd.read_html(“https://en.wikipedia.org/wiki/List_of_S%26P_500_companies”)[0]
tickers = [str(t).replace(”.”, “-”) for t in df[“Symbol”].tolist()]
log.info(f”  S&P 500: {len(tickers)} tickers”)
return tickers
except Exception as e:
log.warning(f”  S&P 500 fetch failed: {e}”)
return []

def fetch_nasdaq100():
try:
tables = pd.read_html(“https://en.wikipedia.org/wiki/Nasdaq-100”)
for df in tables:
cols_lower = [str(c).lower() for c in df.columns]
if “ticker” in cols_lower or “symbol” in cols_lower:
col = df.columns[[str(c).lower() in (“ticker”, “symbol”) for c in df.columns]][0]
tickers = [str(t).replace(”.”, “-”) for t in df[col].dropna().tolist()
if isinstance(t, str) and 1 <= len(str(t)) <= 6]
if len(tickers) > 50:
log.info(f”  Nasdaq 100: {len(tickers)} tickers”)
return tickers
return []
except Exception as e:
log.warning(f”  Nasdaq 100 fetch failed: {e}”)
return []

# Large supplemental list — everything that might have active options but isn’t

# guaranteed to be in S&P 500 or Nasdaq 100

SUPPLEMENTAL = [
# Mega cap / most liquid
“AAPL”,“MSFT”,“NVDA”,“AMZN”,“META”,“GOOGL”,“GOOG”,“TSLA”,“NFLX”,“ORCL”,
# Semis
“AMD”,“INTC”,“QCOM”,“TXN”,“AVGO”,“MU”,“AMAT”,“LRCX”,“KLAC”,“MRVL”,
“ARM”,“ASML”,“TSM”,“SMCI”,“ON”,“SWKS”,“MPWR”,“ENPH”,“FSLR”,“ACLS”,
“AMKR”,“COHU”,“CEVA”,“FORM”,“IPGP”,“KLIC”,“ONTO”,“UCTT”,“WOLF”,
# Cloud / SaaS
“CRM”,“NOW”,“PANW”,“CRWD”,“NET”,“DDOG”,“SNOW”,“MDB”,“OKTA”,“ZS”,
“FTNT”,“HUBS”,“WDAY”,“TEAM”,“SHOP”,“MELI”,“TTD”,“RBLX”,“PATH”,
“ZM”,“DOCU”,“TWLO”,“BILL”,“GTLB”,“SMAR”,“FRSH”,“CFLT”,“DOMO”,
“APPN”,“FROG”,“BRZE”,“TASK”,“DV”,“PUBM”,“MGNI”,“RAMP”,“ASAN”,
“MNDY”,“CWAN”,“ALTR”,“TOST”,“LSPD”,“WIX”,“FIVN”,“NICE”,“CLOU”,
# Fintech / payments
“PYPL”,“SQ”,“AFRM”,“UPST”,“SOFI”,“HOOD”,“COIN”,“NU”,“DAVE”,
“PSFE”,“OPFI”,“FLYW”,“GREE”,“NCNO”,“PAGS”,“STNE”,“DLO”,“RELY”,
# Crypto adjacent
“MARA”,“RIOT”,“MSTR”,“HUT”,“CLSK”,“CIFR”,“BTBT”,“CORZ”,“HIVE”,
“WGMI”,“BITI”,“BITO”,
# Biotech / pharma
“MRNA”,“BNTX”,“NVAX”,“ALNY”,“BMRN”,“RARE”,“HALO”,“EXAS”,“NBIX”,
“PTGX”,“ACAD”,“SAGE”,“KRYS”,“BEAM”,“EDIT”,“NTLA”,“CRSP”,“FATE”,
“KYMR”,“MRUS”,“TGTX”,“RCKT”,“ARVN”,“KRTX”,“AXSM”,“INVA”,“IMVT”,
“ARQT”,“PRTA”,“PTCT”,“RETA”,“RVMD”,“VERV”,“RXDX”,“SRRK”,“TPVG”,
“DNLI”,“KPTI”,“MDGL”,“NUVL”,“ORIC”,“PRAX”,“RDNT”,“RLAY”,“VKTX”,
“XNCR”,“ZNTL”,“ACMR”,“ADMA”,“ADPT”,“AGIO”,“AKBA”,“ALEC”,“ALLK”,
# EV / Auto
“RIVN”,“LCID”,“NKLA”,“F”,“GM”,“TM”,“HMC”,“STLA”,“RACE”,“POAI”,
# Consumer / retail
“HD”,“WMT”,“COST”,“MCD”,“SBUX”,“NKE”,“TGT”,“LOW”,“BKNG”,“ABNB”,
“UBER”,“LYFT”,“DASH”,“ETSY”,“EBAY”,“AMZN”,“CPNG”,“SE”,“GRAB”,
“PTON”,“W”,“BIRD”,“LULU”,“DECK”,“SKX”,“CROX”,“YETI”,“PVH”,“RL”,
“HBI”,“GOOS”,“UAA”,“PLNT”,“WING”,“TXRH”,“CMG”,“DRI”,“CAKE”,“DINE”,
“EAT”,“JACK”,“PLAY”,“SHAK”,“WEN”,“YUM”,“YUMC”,“QSR”,“RRGB”,
# Consumer staples
“PG”,“KO”,“PEP”,“PM”,“MO”,“CL”,“KMB”,“GIS”,“K”,“HSY”,“MNST”,
“CELH”,“FIZZ”,“COKE”,“KDP”,“STZ”,“BF-B”,“MGPI”,“ABEV”,“SAM”,
# Energy
“XOM”,“CVX”,“COP”,“EOG”,“SLB”,“OXY”,“FANG”,“DVN”,“MRO”,“HES”,
“APA”,“AR”,“EQT”,“RRC”,“CNX”,“SM”,“VET”,“NOG”,“CIVI”,“BTU”,
“ARCH”,“AMR”,“METC”,“ARCH”,“TALO”,“VTLE”,“ESTE”,“FLNC”,“CLNE”,
# Financials
“JPM”,“BAC”,“WFC”,“GS”,“MS”,“C”,“V”,“MA”,“AXP”,“BLK”,“SCHW”,
“COF”,“DFS”,“SYF”,“AIG”,“MET”,“PRU”,“ALL”,“TRV”,“CB”,“HIG”,
“BRK-B”,“USB”,“PNC”,“TFC”,“RF”,“FITB”,“HBAN”,“KEY”,“CFG”,“ZION”,
“MTB”,“SIVB”,“PACW”,“WAL”,“BOKF”,“FFIN”,“IBOC”,“SNV”,“UMBF”,
# Healthcare / insurance
“UNH”,“JNJ”,“LLY”,“ABBV”,“MRK”,“PFE”,“AMGN”,“GILD”,“VRTX”,“REGN”,
“BMY”,“BIIB”,“DXCM”,“ISRG”,“EW”,“ZBH”,“HUM”,“CVS”,“CI”,“CNC”,
“HCA”,“THC”,“UHS”,“MCK”,“ABC”,“CAH”,“OMI”,“PDCO”,“HSIC”,“XRAY”,
# Industrials / defense
“BA”,“CAT”,“DE”,“GE”,“HON”,“RTX”,“LMT”,“NOC”,“UPS”,“FDX”,“ETN”,
“EMR”,“PH”,“ROK”,“IR”,“AME”,“GNRC”,“AXON”,“CACI”,“KTOS”,“HII”,
“LDOS”,“BAH”,“SAIC”,“FTAI”,“TDG”,“HEICO”,“HWM”,“TXT”,“DRS”,
“MOOG”,“CW”,“AEIS”,“AVAV”,“CDRE”,“DRS”,“ESLT”,“FLIR”,“KTOS”,
# Communication / media
“DIS”,“CMCSA”,“T”,“VZ”,“TMUS”,“SNAP”,“PINS”,“SPOT”,“WBD”,“PARA”,
“FUBO”,“SIRI”,“IMAX”,“AMC”,“CNK”,“NWSA”,“NYT”,“FOXA”,“FOX”,
# Chinese ADRs
“BABA”,“BIDU”,“JD”,“PDD”,“NIO”,“XPEV”,“LI”,“BILI”,“TME”,“VNET”,
“IQ”,“TIGR”,“FUTU”,“UP”,“LABU”,“DIDI”,“CANG”,“CAN”,“GOTU”,
# Real estate / REIT
“AMT”,“PLD”,“EQIX”,“CCI”,“SPG”,“O”,“AVB”,“EQR”,“VTR”,“WELL”,
“HST”,“RHP”,“SLG”,“BXP”,“KIM”,“REG”,“UDR”,“NNN”,“WPC”,“STAG”,
# Utilities
“NEE”,“DUK”,“SO”,“AEP”,“EXC”,“XEL”,“PCG”,“ED”,“ETR”,“PEG”,
# Materials
“LIN”,“APD”,“FCX”,“NEM”,“NUE”,“VMC”,“MLM”,“CF”,“MOS”,“IFF”,
# Meme / speculative / high-vol
“GME”,“AMC”,“BB”,“SPCE”,“NKLA”,“FCEL”,“PLUG”,“BLNK”,“CHPT”,
“EVGO”,“DKNG”,“PENN”,“WYNN”,“MGM”,“LVS”,“CZR”,“MLCO”,“NCLH”,
“CCL”,“RCL”,“AAL”,“DAL”,“UAL”,“LUV”,“JBLU”,“ALK”,“SAVE”,
# Quantum / AI speculative
“IONQ”,“QUBT”,“RGTI”,“SOUN”,“BBAI”,“QBTS”,“ARQT”,“LAES”,“DJTWW”,
# ETFs with liquid options
“SPY”,“QQQ”,“IWM”,“SMH”,“XLE”,“XLF”,“XLK”,“XLV”,“XLI”,“XLU”,“XLP”,
“ARKK”,“GLD”,“SLV”,“TLT”,“HYG”,“EEM”,“EFA”,“VWO”,“KWEB”,“SOXS”,“SOXL”,
“TQQQ”,“SQQQ”,“SPXU”,“UPRO”,“UVXY”,“SVXY”,
]

def build_universe():
log.info(“Building universe…”)
tickers = set()
tickers.update(fetch_sp500())
tickers.update(fetch_nasdaq100())
tickers.update(SUPPLEMENTAL)
cleaned = sorted({
str(t).strip().upper()
for t in tickers
if str(t).strip().upper().replace(”-”,””).isalpha()
and 1 <= len(str(t).strip()) <= 6
})
log.info(f”Universe: {len(cleaned)} unique tickers”)
return cleaned

# ── Core scoring (exact calculator.py logic) ──────────────────────────────────

def filter_dates(dates):
today = datetime.today().date()
cutoff = today + timedelta(days=45)
sorted_dates = sorted(datetime.strptime(d, “%Y-%m-%d”).date() for d in dates)
arr = []
for i, dt in enumerate(sorted_dates):
if dt >= cutoff:
arr = [d.strftime(”%Y-%m-%d”) for d in sorted_dates[:i+1]]
break
if arr:
return arr[1:] if arr[0] == today.strftime(”%Y-%m-%d”) else arr
raise ValueError(“No expiry within 45 days.”)

def yang_zhang(price_data, window=30, trading_periods=252):
log_ho = (price_data[‘High’]  / price_data[‘Open’]).apply(np.log)
log_lo = (price_data[‘Low’]   / price_data[‘Open’]).apply(np.log)
log_co = (price_data[‘Close’] / price_data[‘Open’]).apply(np.log)
log_oc    = (price_data[‘Open’] / price_data[‘Close’].shift(1)).apply(np.log)
log_oc_sq = log_oc ** 2
log_cc    = (price_data[‘Close’] / price_data[‘Close’].shift(1)).apply(np.log)
log_cc_sq = log_cc ** 2
rs        = log_ho * (log_ho - log_co) + log_lo * (log_lo - log_co)
close_vol = log_cc_sq.rolling(window, center=False).sum() / (window - 1)
open_vol  = log_oc_sq.rolling(window, center=False).sum() / (window - 1)
window_rs = rs.rolling(window, center=False).sum() / (window - 1)
k = 0.34 / (1.34 + (window + 1) / (window - 1))
return ((open_vol + k * close_vol + (1 - k) * window_rs).apply(np.sqrt) * np.sqrt(trading_periods)).iloc[-1]

def build_ts(days, ivs):
days = np.array(days, dtype=float);  ivs = np.array(ivs, dtype=float)
idx  = days.argsort();               days = days[idx];  ivs = ivs[idx]
def ts(dte):
if   dte <= days[0]:   return float(ivs[0])
elif dte >= days[-1]:  return float(ivs[-1])
else:                  return float(np.interp(dte, days, ivs))
return ts

def _check_dates(dates, today):
“”“Check a list of date-like values for upcoming earnings.”””
for d in dates:
try:
dt   = pd.Timestamp(d).date()
diff = (dt - today).days
if 0 <= diff <= EARNINGS_WINDOW:
return dt.strftime(”%b %d”), diff
except:
continue
return None, None

def get_earnings(stock):
“”“Return (earnings_str, days_away) or (None, None).
Tries multiple yfinance methods — calendar API changed between versions.”””
today = datetime.today().date()

```
# Method 1: stock.calendar (returns dict or DataFrame depending on yfinance version)
try:
    cal = stock.calendar
    if cal is not None:
        dates = []
        if isinstance(cal, dict):
            raw = cal.get('Earnings Date') or cal.get('earningsDate') or []
            if raw is not None:
                dates = raw if hasattr(raw, '__iter__') and not isinstance(raw, str) else [raw]
        elif isinstance(cal, pd.DataFrame):
            for col in ['Earnings Date', 'earningsDate']:
                if col in cal.columns:
                    dates = cal[col].dropna().tolist(); break
                if col in cal.index:
                    dates = [cal.loc[col]]; break
        result = _check_dates(dates, today)
        if result[0]: return result
except:
    pass

# Method 2: stock.get_earnings_dates() — returns DataFrame of past+future earnings
try:
    ed = stock.get_earnings_dates(limit=8)
    if ed is not None and not ed.empty:
        future = [idx for idx in ed.index if pd.Timestamp(idx).date() >= today]
        result = _check_dates(sorted(future), today)
        if result[0]: return result
except:
    pass

# Method 3: stock.earnings_dates property
try:
    ed = stock.earnings_dates
    if ed is not None and not ed.empty:
        future = [idx for idx in ed.index if pd.Timestamp(idx).date() >= today]
        result = _check_dates(sorted(future), today)
        if result[0]: return result
except:
    pass

# Method 4: stock.info dict fields
try:
    info = stock.info
    for key in ('earningsDate', 'earningsTimestamp', 'nextEarningsDate'):
        val = info.get(key)
        if val:
            try:
                # earningsTimestamp is a Unix timestamp int
                if isinstance(val, (int, float)):
                    dt = datetime.fromtimestamp(val).date()
                else:
                    dt = pd.Timestamp(val).date()
                diff = (dt - today).days
                if 0 <= diff <= EARNINGS_WINDOW:
                    return dt.strftime("%b %d"), diff
            except:
                pass
except:
    pass

return None, None
```

def score_ticker(sym):
try:
stock = yf.Ticker(sym)

```
    # Quick earnings check first
    earn_str, earn_days = get_earnings(stock)
    if earn_str is None:
        return None

    # Price
    h1 = stock.history(period="1d")
    if h1.empty:
        return None
    price = float(h1['Close'].iloc[-1])
    if price <= 0:
        return None

    # Options
    if not stock.options or len(stock.options) < 2:
        return None
    try:
        exp_dates = filter_dates(list(stock.options))
    except:
        return None
    if len(exp_dates) < 2:
        return None

    # Build IV term structure
    today = datetime.today().date()
    dtes, ivs, straddle = [], [], None

    for i, exp in enumerate(exp_dates):
        try:
            ch   = stock.option_chain(exp)
            c, p = ch.calls, ch.puts
            if c.empty or p.empty:
                continue
            ci = (c['strike'] - price).abs().idxmin()
            pi = (p['strike'] - price).abs().idxmin()
            atm_iv = (c.loc[ci,'impliedVolatility'] + p.loc[pi,'impliedVolatility']) / 2
            dte = (datetime.strptime(exp, "%Y-%m-%d").date() - today).days
            dtes.append(dte);  ivs.append(atm_iv)
            if i == 0:
                straddle = (c.loc[ci,'bid'] + c.loc[ci,'ask']) / 2 + (p.loc[pi,'bid'] + p.loc[pi,'ask']) / 2
        except:
            continue

    if len(dtes) < 2:
        return None

    ts = build_ts(dtes, ivs)

    # Exact thresholds
    slope  = (ts(45) - ts(dtes[0])) / (45 - dtes[0])
    h3     = stock.history(period="3mo")
    if h3.empty or len(h3) < 30:
        return None
    ivrv   = ts(30) / yang_zhang(h3)
    avgvol = h3['Volume'].rolling(30).mean().dropna().iloc[-1]

    c1 = slope  <= -0.00406
    c2 = avgvol >= 1_500_000
    c3 = ivrv   >= 1.25

    rec = ("RECOMMENDED" if c1 and c2 and c3 else
           "CONSIDER"    if c1 and (c2 or c3) else
           "AVOID")

    # Strike
    s = price
    strike = (round(s*2)/2 if s<20 else round(s) if s<50 else
              round(s/5)*5 if s<200 else round(s/10)*10 if s<500 else round(s/25)*25)

    debit = round(price * ts(dtes[0]) * (max(dtes[0],1)/365)**0.5 * 0.4, 2)

    try:
        fi  = stock.fast_info
        name   = getattr(fi, 'company_name', None) or sym
        sector = getattr(fi, 'sector', None) or ""
    except:
        name, sector = sym, ""

    try:
        ed  = datetime.strptime(earn_str, "%b %d").replace(year=datetime.today().year)
        entry_str = (ed - timedelta(days=1)).strftime("%b %d")
    except:
        entry_str = earn_str

    return {
        "ticker":         sym,
        "name":           name,
        "sector":         sector,
        "price":          round(price, 2),
        "earningsDate":   earn_str,
        "daysToEarnings": int(earn_days),
        "rec":            rec,
        "c1":             bool(c1),
        "c2":             bool(c2),
        "c3":             bool(c3),
        "tsSlope":        round(float(slope), 6),
        "ivRv":           round(float(ivrv), 3),
        "avgVol":         int(avgvol),
        "expectedMove":   round((straddle / price) * 100, 2) if straddle else None,
        "frontIV":        round(ts(dtes[0]) * 100, 1),
        "backIV":         round(ts(45) * 100, 1),
        "strike":         strike,
        "frontExp":       exp_dates[0],
        "backExp":        exp_dates[min(1, len(exp_dates)-1)],
        "debitEst":       debit,
        "entryDate":      entry_str,
        "exitDate":       earn_str,
    }
except Exception as e:
    log.debug(f"{sym}: {e}")
    return None
```

# ── Scan runner ───────────────────────────────────────────────────────────────

def run_scan():
if _cache[“running”]:
return
_cache[“running”] = True
universe = build_universe()
_cache[“universe”] = universe
_cache[“progress”] = {“done”: 0, “total”: len(universe), “phase”: “scanning”}
log.info(f”Scanning {len(universe)} tickers…”)

```
results = []
lock = threading.Lock()

def scan_one(sym):
    r = score_ticker(sym)
    with lock:
        _cache["progress"]["done"] += 1
        if r:
            results.append(r)

with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
    futs = {pool.submit(scan_one, sym): sym for sym in universe}
    for f in concurrent.futures.as_completed(futs):
        try: f.result()
        except: pass

order = {"RECOMMENDED": 0, "CONSIDER": 1, "AVOID": 2}
results.sort(key=lambda x: (order.get(x["rec"], 3), x["daysToEarnings"]))

_cache["results"]  = results
_cache["ts"]       = time.time()
_cache["running"]  = False
_cache["progress"] = {"done": len(universe), "total": len(universe), "phase": "done"}
log.info(f"Done. {len(results)} earnings setups from {len(universe)} tickers.")
```

def get_or_refresh():
if _cache[“results”] is None:
run_scan()
elif time.time() - _cache[“ts”] > CACHE_TTL and not _cache[“running”]:
threading.Thread(target=run_scan, daemon=True).start()
return _cache[“results”]

# ── Routes ────────────────────────────────────────────────────────────────────

@app.route(”/api/scan”)
def api_scan():
data = get_or_refresh()
return jsonify({
“results”:      data or [],
“count”:        len(data) if data else 0,
“scannedAt”:    datetime.fromtimestamp(_cache[“ts”]).strftime(”%b %d %Y, %I:%M %p”) if _cache[“ts”] else None,
“ageMinutes”:   int((time.time() - _cache[“ts”]) / 60) if _cache[“ts”] else 0,
“isRefreshing”: _cache[“running”],
“universe”:     len(_cache[“universe”]),
“progress”:     _cache[“progress”],
})

@app.route(”/api/progress”)
def api_progress():
p   = _cache[“progress”]
pct = int(p[“done”] / p[“total”] * 100) if p[“total”] > 0 else 0
return jsonify({“done”: p[“done”], “total”: p[“total”], “pct”: pct,
“phase”: p[“phase”], “running”: _cache[“running”]})

@app.route(”/api/status”)
def api_status():
return jsonify({“status”: “ok”, “cached”: _cache[“results”] is not None,
“count”: len(_cache[“results”]) if _cache[“results”] else 0,
“ageMinutes”: int((time.time()-_cache[“ts”])/60) if _cache[“ts”] else None,
“running”: _cache[“running”], “universe”: len(_cache[“universe”]),
“progress”: _cache[“progress”]})

@app.route(”/api/refresh”, methods=[“POST”])
def api_refresh():
if _cache[“running”]:
return jsonify({“message”: “Scan already in progress”, “progress”: _cache[“progress”]}), 202
threading.Thread(target=run_scan, daemon=True).start()
return jsonify({“message”: “Refresh started”}), 202

@app.route(”/”)
def index():
p = _cache[“progress”]
return (f”VV Scanner API — {len(_cache[‘universe’])} ticker universe | “
f”{len(_cache[‘results’]) if _cache[‘results’] else 0} setups found | “
f”Running: {_cache[‘running’]} ({p[‘done’]}/{p[‘total’]}) | “
f”Use /api/scan”)

if **name** == “**main**”:
threading.Thread(target=run_scan, daemon=True).start()
app.run(host=“0.0.0.0”, port=5000, debug=False)