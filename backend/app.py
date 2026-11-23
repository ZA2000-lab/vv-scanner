# optionscanners.com — cash-secured put scanner backend
#
# Stage 0  Universe: Nasdaq screener (all US-listed stocks, filtered by market cap
#          and price) + curated optionable ETFs. Falls back to Wikipedia index
#          lists, then a hardcoded list, so a single source failing never shrinks
#          the scan silently.
# Stage 1  Bulk daily history (1y) in batches of 100 -> price / liquidity / realized
#          vol / trend / support levels. No per-ticker calls.
# Stage 2  Options chains for the most liquid survivors -> delta-targeted strike
#          ladder, POP, IV rank, earnings check, grade + composite score.

from flask import Flask, jsonify, request
from flask_cors import CORS
import yfinance as yf
import pandas as pd
import numpy as np
import requests
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
import concurrent.futures
import threading
import logging
import json
import math
import os
import re
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("optionscanners")
app = Flask(__name__)
CORS(app)

try:
    yf.set_tz_cache_location("/tmp/yf_tz_cache")
except Exception:
    pass


def _env_num(name, default, cast=float):
    try:
        return cast(os.environ.get(name, default))
    except Exception:
        return default


# -- Config (all overridable with Railway env vars) ---------------------------
CACHE_TTL        = _env_num("CACHE_TTL_HOURS", 6) * 3600
DAYS_AHEAD       = 60                                     # earnings look-ahead
SCORING_WORKERS  = _env_num("SCORING_WORKERS", 4, int)    # options-chain threads
SCORING_CAP      = _env_num("SCORING_CAP", 700, int)      # tickers sent to Stage 2
MIN_PRICE        = _env_num("MIN_PRICE", 5.0)
MIN_AVG_VOL      = _env_num("MIN_AVG_VOL", 300_000)
MIN_DOLLAR_VOL   = _env_num("MIN_DOLLAR_VOL", 15_000_000)
MIN_MCAP         = _env_num("MIN_MCAP", 500_000_000)
TARGET_DELTA     = _env_num("TARGET_DELTA", 0.30)
DTE_MIN, DTE_MAX, DTE_TARGET = 28, 52, 40
RISK_FREE        = _env_num("RISK_FREE", 0.04)
REFRESH_COOLDOWN = _env_num("REFRESH_COOLDOWN_MIN", 10) * 60
DATA_DIR         = os.environ.get("DATA_DIR", "/tmp")    # point at a Railway volume to persist
AUTO_SCAN        = os.environ.get("AUTO_SCAN", "1") == "1"
NY               = ZoneInfo("America/New_York")

# Grading thresholds — Z's STRONG rule: IVR>=60, IV/RV>=1.2, no earnings, spread<=8%
STRONG_IVR, STRONG_IVRV, STRONG_SPREAD = 60, 1.2, 8.0
SKIP_IVR, SKIP_SPREAD = 45, 20.0

CACHE_FILE = os.path.join(DATA_DIR, "scan_cache.json")
IVHIST_FILE = os.path.join(DATA_DIR, "iv_history.json")

NASDAQ_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://www.nasdaq.com",
    "Referer": "https://www.nasdaq.com/",
}
WIKI_HEADERS = {"User-Agent": "optionscanners.com/2.0 (universe builder; contact via optionscanners.com)"}

# -- Static lists ------------------------------------------------------------
ETFS = {
    "SPY": "SPDR S&P 500", "QQQ": "Invesco QQQ", "IWM": "iShares Russell 2000",
    "DIA": "SPDR Dow Jones", "MDY": "SPDR S&P MidCap 400", "RSP": "Invesco S&P 500 Equal Weight",
    "VOO": "Vanguard S&P 500", "IVV": "iShares Core S&P 500", "VTI": "Vanguard Total Market",
    "XLE": "Energy Select SPDR", "XLF": "Financial Select SPDR", "XLK": "Technology Select SPDR",
    "XLV": "Health Care Select SPDR", "XLI": "Industrial Select SPDR", "XLU": "Utilities Select SPDR",
    "XLRE": "Real Estate Select SPDR", "XLB": "Materials Select SPDR", "XLY": "Cons. Discretionary SPDR",
    "XLP": "Cons. Staples SPDR", "XLC": "Communication Services SPDR",
    "SMH": "VanEck Semiconductor", "SOXX": "iShares Semiconductor", "IBB": "iShares Biotech",
    "XBI": "SPDR S&P Biotech", "KRE": "SPDR Regional Banking", "KBE": "SPDR Bank",
    "XHB": "SPDR Homebuilders", "ITB": "iShares Home Construction", "XRT": "SPDR Retail",
    "XOP": "SPDR Oil & Gas E&P", "OIH": "VanEck Oil Services", "JETS": "U.S. Global Jets",
    "ARKK": "ARK Innovation", "TAN": "Invesco Solar", "LIT": "Global X Lithium",
    "URA": "Global X Uranium", "COPX": "Global X Copper Miners", "IGV": "iShares Software",
    "EEM": "iShares MSCI Emerging", "EFA": "iShares MSCI EAFE", "EWZ": "iShares MSCI Brazil",
    "EWJ": "iShares MSCI Japan", "FXI": "iShares China Large-Cap", "KWEB": "KraneShares China Internet",
    "MCHI": "iShares MSCI China", "INDA": "iShares MSCI India", "EWW": "iShares MSCI Mexico",
    "EWT": "iShares MSCI Taiwan", "EWY": "iShares MSCI South Korea",
    "GLD": "SPDR Gold", "IAU": "iShares Gold", "SLV": "iShares Silver", "GDX": "VanEck Gold Miners",
    "GDXJ": "VanEck Junior Gold Miners", "SIL": "Global X Silver Miners",
    "TLT": "iShares 20+ Yr Treasury", "IEF": "iShares 7-10 Yr Treasury", "HYG": "iShares High Yield",
    "LQD": "iShares IG Corporate", "JNK": "SPDR High Yield", "TMF": "Direxion 20Y Treasury Bull 3x",
    "USO": "United States Oil", "UNG": "United States Natural Gas", "DBA": "Invesco Agriculture",
    "UVXY": "ProShares Ultra VIX", "VXX": "iPath VIX Short-Term",
    "TQQQ": "ProShares UltraPro QQQ", "SQQQ": "ProShares UltraPro Short QQQ",
    "SOXL": "Direxion Semis Bull 3x", "SOXS": "Direxion Semis Bear 3x", "UPRO": "ProShares UltraPro S&P",
    "SPXL": "Direxion S&P Bull 3x", "TNA": "Direxion Small Cap Bull 3x", "LABU": "Direxion Biotech Bull 3x",
    "FAS": "Direxion Financial Bull 3x", "NUGT": "Direxion Gold Miners Bull 3x",
    "BITO": "ProShares Bitcoin Strategy", "IBIT": "iShares Bitcoin Trust", "FBTC": "Fidelity Bitcoin",
    "ETHA": "iShares Ethereum Trust", "MSOS": "AdvisorShares US Cannabis",
}
LEVERAGED = {"TQQQ", "SQQQ", "SOXL", "SOXS", "UPRO", "SPXL", "TNA", "LABU", "FAS", "NUGT",
             "UVXY", "VXX", "TMF"}

_FALLBACK = [
    "AAPL","MSFT","NVDA","GOOGL","AMZN","META","TSLA","AVGO","ORCL","AMD","ADBE","CRM","INTU",
    "NOW","PANW","CRWD","NET","DDOG","SNOW","PLTR","MDB","INTC","QCOM","TXN","AMAT","LRCX","KLAC",
    "MU","SMCI","ARM","ON","MRVL","CSCO","IBM","DELL","HPE","PYPL","XYZ","SHOP","JPM","BAC","WFC",
    "GS","MS","C","V","MA","AXP","COF","SCHW","BLK","BX","KKR","APO","COIN","MSTR","HOOD","SOFI",
    "UNH","LLY","JNJ","ABBV","MRK","PFE","AMGN","GILD","VRTX","REGN","ISRG","TMO","MRNA","CVS",
    "XOM","CVX","COP","EOG","DVN","OXY","SLB","HAL","VLO","MPC","GE","HON","CAT","DE","RTX","LMT",
    "BA","UPS","FDX","HD","WMT","COST","TGT","MCD","SBUX","CMG","NKE","LULU","BKNG","ABNB","UBER",
    "DASH","DIS","NFLX","CMCSA","DKNG","RBLX","GM","F","RIVN","DAL","UAL","CCL","RCL","PG","KO",
    "PEP","PM","MO","NEE","DUK","SO","FCX","NEM","CLF","AMT","O","PLD","VZ","T","TMUS","SNAP",
    "PINS","SPOT","RDDT","MARA","RIOT","RKLB","ASTS","APP","HIMS","CVNA","AFRM","UPST","IONQ",
]

# -- State -------------------------------------------------------------------
_state_lock = threading.Lock()
_scan_lock = threading.Lock()
_cache = {
    "results": [], "ts": 0, "running": False, "market": None, "error": None,
    "progress": {"done": 0, "total": 0, "phase": "idle", "ticker": "", "found": 0,
                 "start": 0, "downloaded": 0, "passed": 0},
    "stats": {},
}
UNIVERSE = []
META = {}           # sym -> {name, sector, industry, mcap, etf}
UNIVERSE_SOURCES = {}
_universe_ready = threading.Event()
_analyze_cache = {}  # sym -> (ts, result)


# -- Math helpers ------------------------------------------------------------
def _ncdf(x):
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _npdf(x):
    return math.exp(-0.5 * x * x) / math.sqrt(2 * math.pi)


def bs_put(S, K, T, iv, r=RISK_FREE):
    """Black-Scholes put greeks. Returns (delta, theta_per_day_per_share, d1, d2)."""
    if S <= 0 or K <= 0 or T <= 0 or iv <= 0:
        return None
    st = iv * math.sqrt(T)
    d1 = (math.log(S / K) + (r + 0.5 * iv * iv) * T) / st
    d2 = d1 - st
    delta = _ncdf(d1) - 1.0
    theta = (-S * _npdf(d1) * iv / (2 * math.sqrt(T)) + r * K * math.exp(-r * T) * _ncdf(-d2)) / 365.0
    return delta, theta, d1, d2


def prob_above(S, K, T, iv, r=RISK_FREE):
    """Risk-neutral probability the stock finishes above K at expiry."""
    if S <= 0 or K <= 0 or T <= 0 or iv <= 0:
        return None
    st = iv * math.sqrt(T)
    d2 = (math.log(S / K) + (r - 0.5 * iv * iv) * T) / st
    return _ncdf(d2)


def _f(x, default=0.0):
    try:
        v = float(x)
        return default if (v != v or math.isinf(v)) else v
    except Exception:
        return default


def _retry(fn, attempts=3, base_sleep=4):
    """Retry yfinance calls; back off harder on rate limits."""
    last = None
    for i in range(attempts):
        try:
            return fn()
        except Exception as e:
            last = e
            msg = str(e).lower()
            if "rate" in msg or "too many" in msg or "429" in msg:
                time.sleep(base_sleep * (i + 1) * 2)
            else:
                time.sleep(0.5 * (i + 1))
    raise last


# -- Universe ----------------------------------------------------------------
_NAME_SUFFIX = re.compile(
    r"\s+(Class [A-Z] )?(Common Stock|Ordinary Shares|American Depositary Shares|"
    r"Common Shares|Depositary Shares)\b.*$", re.I)


def _clean_name(n):
    n = (n or "").strip()
    n = _NAME_SUFFIX.sub("", n)
    return n[:60] or None


def _load_nasdaq_screener():
    url = "https://api.nasdaq.com/api/screener/stocks?tableonly=true&limit=10000&download=true"
    r = requests.get(url, headers=NASDAQ_HEADERS, timeout=30)
    r.raise_for_status()
    rows = ((r.json() or {}).get("data") or {}).get("rows") or []
    out = []
    for row in rows:
        sym = (row.get("symbol") or "").strip().upper().replace("/", "-")
        if not sym or not re.fullmatch(r"[A-Z]{1,5}(-[A-Z])?", sym):
            continue  # drops preferreds (^), warrants, units
        mcap = _f(str(row.get("marketCap") or "0").replace(",", ""))
        price = _f(str(row.get("lastsale") or "0").replace("$", "").replace(",", ""))
        vol = _f(str(row.get("volume") or "0").replace(",", ""))
        if mcap < MIN_MCAP or price < MIN_PRICE or vol < 50_000:
            continue
        out.append(sym)
        META[sym] = {"name": _clean_name(row.get("name")) or sym,
                     "sector": (row.get("sector") or "").strip() or "Other",
                     "industry": (row.get("industry") or "").strip(),
                     "mcap": mcap, "etf": False}
    return out


def _load_wiki(url, name):
    resp = requests.get(url, headers=WIKI_HEADERS, timeout=20)
    resp.raise_for_status()
    from io import StringIO
    for df in pd.read_html(StringIO(resp.text)):
        cols = [str(c).lower() for c in df.columns]
        ci = next((i for i, c in enumerate(cols) if c in ("symbol", "ticker", "ticker symbol")), None)
        if ci is None:
            continue
        ni = next((i for i, c in enumerate(cols) if c in ("security", "company", "name")), None)
        si = next((i for i, c in enumerate(cols) if "sector" in c), None)
        out = []
        for _, row in df.iterrows():
            t = str(row.iloc[ci]).split()[0].strip().upper().replace(".", "-")
            if not re.fullmatch(r"[A-Z]{1,5}(-[A-Z])?", t):
                continue
            out.append(t)
            if t not in META:
                META[t] = {"name": str(row.iloc[ni]) if ni is not None else t,
                           "sector": str(row.iloc[si]) if si is not None else "Other",
                           "industry": "", "mcap": 0, "etf": False}
        if len(out) > 10:
            return out
    return []


def _build_universe():
    global UNIVERSE
    syms, sources = [], {}
    try:
        n = _load_nasdaq_screener()
        syms += n
        sources["nasdaq_screener"] = len(n)
    except Exception as e:
        sources["nasdaq_screener"] = "failed: %s" % str(e)[:80]
        log.warning("Nasdaq screener failed: %s", e)

    if len(syms) < 800:  # screener missing or thin -> add index constituents
        for name, url in [
            ("sp500", "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"),
            ("sp400", "https://en.wikipedia.org/wiki/List_of_S%26P_400_companies"),
            ("sp600", "https://en.wikipedia.org/wiki/List_of_S%26P_600_companies"),
            ("nasdaq100", "https://en.wikipedia.org/wiki/Nasdaq-100"),
        ]:
            try:
                w = _load_wiki(url, name)
                syms += w
                sources[name] = len(w)
            except Exception as e:
                sources[name] = "failed: %s" % str(e)[:80]
                log.warning("Wiki %s failed: %s", name, e)

    for t, nm in ETFS.items():
        META[t] = {"name": nm, "sector": "ETF", "industry": "Leveraged" if t in LEVERAGED else "ETF",
                   "mcap": 0, "etf": True}
    syms += list(ETFS)
    syms += _FALLBACK
    sources["etfs"] = len(ETFS)

    seen, result = set(), []
    for t in syms:
        if t not in seen:
            seen.add(t)
            result.append(t)
    with _state_lock:
        UNIVERSE = result
        UNIVERSE_SOURCES.clear()
        UNIVERSE_SOURCES.update(sources)
    _universe_ready.set()
    log.info("Universe ready: %d tickers %s", len(result), sources)


# -- Earnings ----------------------------------------------------------------
def fetch_earnings_map():
    out = {}
    today = date.today()
    s = requests.Session()
    s.headers.update(NASDAQ_HEADERS)
    deadline = time.time() + 75
    for delta in range(DAYS_AHEAD + 1):
        if time.time() > deadline:
            break
        d = today + timedelta(days=delta)
        if d.weekday() >= 5:
            continue
        try:
            r = s.get("https://api.nasdaq.com/api/calendar/earnings?date=" + d.isoformat(), timeout=5)
            if r.status_code == 200:
                for row in ((r.json() or {}).get("data") or {}).get("rows") or []:
                    sym = (row.get("symbol") or "").upper().strip().replace("/", "-")
                    if sym and sym not in out:
                        out[sym] = (d.strftime("%b %d"), delta)
            time.sleep(0.05)
        except Exception:
            pass
    log.info("Earnings map: %d tickers", len(out))
    return out


def get_earn(stock, sym, today_d, emap):
    """Earliest upcoming earnings from yfinance calendar OR Nasdaq calendar (either can be stale)."""
    cands = []
    if not META.get(sym, {}).get("etf"):
        try:
            cal = stock.calendar
            dates = []
            if isinstance(cal, dict):
                dates = cal.get("Earnings Date") or []
            elif isinstance(cal, pd.DataFrame) and not cal.empty:
                dates = list(cal.columns)
            for ed in dates:
                try:
                    ed = pd.Timestamp(ed).date()
                except Exception:
                    continue
                days = (ed - today_d).days
                if -2 <= days <= 180:
                    cands.append((days, ed.strftime("%b %d")))
        except Exception:
            pass
    if sym in emap:
        nd_str, nd_days = emap[sym]
        cands.append((nd_days, nd_str))
    if not cands:
        return None, None
    cands.sort()
    return cands[0][1], cands[0][0]


# -- IV history (true IV rank once enough days are stored) -------------------
_ivhist = {}
_ivhist_lock = threading.Lock()


def _load_json(path, default):
    try:
        with open(path) as fh:
            return json.load(fh)
    except Exception:
        return default


def _save_json(path, obj):
    try:
        tmp = path + ".tmp"
        with open(tmp, "w") as fh:
            json.dump(obj, fh, separators=(",", ":"))
        os.replace(tmp, path)
    except Exception as e:
        log.warning("save %s: %s", path, e)


def _iv_rank_from_history(sym, iv):
    h = _ivhist.get(sym) or {}
    vals = list(h.values())
    if len(vals) < 20:
        return None, len(vals)
    lo, hi = min(vals + [iv]), max(vals + [iv])
    if hi - lo < 1e-6:
        return 50, len(vals)
    return int(round((iv - lo) / (hi - lo) * 100)), len(vals)


def _record_iv(sym, iv):
    today_s = date.today().isoformat()
    with _ivhist_lock:
        h = _ivhist.setdefault(sym, {})
        h[today_s] = round(iv, 4)
        if len(h) > 260:
            for k in sorted(h)[:-260]:
                h.pop(k, None)


# -- Stage 1: per-ticker metrics from bulk history ---------------------------
def history_metrics(h, filters=True):
    """h: DataFrame with Open/High/Low/Close/Volume (daily, ~1y). Returns dict or None.
    filters=False skips the liquidity screen (used for explicit single-ticker lookups)."""
    h = h.dropna(how="all")
    if h is None or len(h) < 60 or "Close" not in h:
        return None
    close = h["Close"].dropna()
    if len(close) < 60:
        return None
    price = _f(close.iloc[-1])
    if price <= 0 or (filters and price < MIN_PRICE):
        return None
    avg_vol = _f(h["Volume"].tail(20).mean())
    if filters and (avg_vol < MIN_AVG_VOL or price * avg_vol < MIN_DOLLAR_VOL):
        return None
    rets = close.pct_change().dropna()
    rv20 = _f(rets.tail(20).std() * math.sqrt(252))
    roll = (rets.rolling(20).std() * math.sqrt(252)).dropna()
    rv_lo = _f(roll.min(), rv20 * 0.5) if len(roll) > 20 else rv20 * 0.5
    rv_hi = _f(roll.max(), rv20 * 1.5) if len(roll) > 20 else rv20 * 1.5
    sma50 = _f(close.tail(50).mean())
    sma200 = _f(close.tail(200).mean()) if len(close) >= 150 else None
    low = h["Low"].dropna() if "Low" in h else close
    high = h["High"].dropna() if "High" in h else close
    low60 = _f(low.tail(60).min())
    prev = _f(close.iloc[-2]) if len(close) > 1 else price
    if sma200:
        if price > sma50 > sma200:
            trend = "up"
        elif price < sma50 < sma200:
            trend = "down"
        else:
            trend = "mixed"
    else:
        trend = "up" if price > sma50 else "down"
    spark = [round(_f(x), 2) for x in close.tail(60).iloc[::3].tolist()]
    return {
        "price": price, "avg_vol": avg_vol, "dollar_vol": price * avg_vol,
        "rv20": rv20, "rv_lo": rv_lo, "rv_hi": rv_hi,
        "sma50": sma50, "sma200": sma200, "low60": low60,
        "hi52": _f(high.max()), "lo52": _f(low.min()),
        "chg1d": (price / prev - 1) * 100 if prev else 0.0,
        "trend": trend, "spark": spark,
    }


# -- Stage 2: options ----------------------------------------------------------
def _pick_expiration(opts, today_d):
    best, best_d = None, None
    fallback = None
    for e in sorted(opts):
        try:
            d = (datetime.strptime(e, "%Y-%m-%d").date() - today_d).days
        except Exception:
            continue
        if DTE_MIN <= d <= DTE_MAX:
            if best is None or abs(d - DTE_TARGET) < abs(best_d - DTE_TARGET):
                best, best_d = e, d
        elif fallback is None and 21 <= d <= 75:
            fallback = (e, d)
    if best:
        return best, best_d
    return fallback if fallback else (None, None)


def _grade(ivr, ivrv, spread, earn_in):
    if earn_in or spread > SKIP_SPREAD or ivr < SKIP_IVR:
        return "SKIP"
    if ivr >= STRONG_IVR and ivrv >= STRONG_IVRV and spread <= STRONG_SPREAD:
        return "STRONG"
    return "DECENT"


def _score(ivr, ivrv, roc_ann, spread, pop, earn_in):
    s = (min(ivr, 100) / 100 * 35
         + min(max(ivrv - 0.8, 0) / 0.8, 1) * 20
         + min(roc_ann / 40, 1) * 20
         + max(0, 1 - spread / 20) * 15
         + (pop or 0) * 10)
    if earn_in:
        s = min(s, 20)
    return int(round(s))


def build_ladder(puts, price, T, atm_iv):
    """OTM puts with real bids -> list of strike dicts with greeks, sorted by strike desc."""
    rows = []
    for _, row in puts.iterrows():
        K = _f(row.get("strike"))
        bid, ask = _f(row.get("bid")), _f(row.get("ask"))
        if K <= 0 or K >= price or bid <= 0 or ask < bid:
            continue
        iv = _f(row.get("impliedVolatility"))
        if not (0.03 <= iv <= 4.0):
            iv = atm_iv
        g = bs_put(price, K, T, iv)
        if not g:
            continue
        delta, theta, _, _ = g
        mid = round((bid + ask) / 2, 2)
        if mid < 0.05:
            continue
        be = K - mid
        rows.append({
            "strike": K, "delta": round(delta, 3), "iv": round(iv * 100, 1),
            "bid": round(bid, 2), "ask": round(ask, 2), "mid": mid,
            "spreadPct": round((ask - bid) / mid * 100, 1),
            "oi": int(_f(row.get("openInterest"))), "volume": int(_f(row.get("volume"))),
            "breakeven": round(be, 2),
            "otmPct": round((1 - K / price) * 100, 1),
            "rocPct": round(mid / K * 100, 2),
            "pop": round(prob_above(price, be, T, iv) or 0, 3),
            "thetaDay": round(-theta * 100, 2),   # $ per contract per day earned by seller
        })
    rows.sort(key=lambda x: -x["strike"])
    return rows


def score_options(sym, m, emap, record_iv=True):
    stock = yf.Ticker(sym)
    today_d = date.today()
    opts = _retry(lambda: stock.options)
    if not opts:
        return None
    exp, dte = _pick_expiration(opts, today_d)
    if not exp:
        return None
    chain = _retry(lambda: stock.option_chain(exp))
    puts = chain.puts
    if puts is None or puts.empty:
        return None
    price = m["price"]
    T = max(dte, 1) / 365.0

    # ATM IV: average of the two strikes nearest spot with live bids and sane IV
    p = puts.copy()
    p["_d"] = (p["strike"] - price).abs()
    near = p[(p["bid"] > 0) & (p["impliedVolatility"].between(0.03, 4.0))].nsmallest(2, "_d")
    if near.empty:
        return None
    atm_iv = _f(near["impliedVolatility"].mean())
    if atm_iv <= 0:
        return None

    ladder = build_ladder(puts, price, T, atm_iv)
    if not ladder:
        return None
    primary = min(ladder, key=lambda x: abs(abs(x["delta"]) - TARGET_DELTA))
    # compact ladder: nearest strike for each delta bucket
    picks = []
    for tgt in (0.15, 0.20, 0.25, 0.30, 0.35, 0.40):
        c = min(ladder, key=lambda x: abs(abs(x["delta"]) - tgt))
        if c not in picks and abs(abs(c["delta"]) - tgt) < 0.06:
            picks.append(c)
    if primary not in picks:
        picks.append(primary)
    picks.sort(key=lambda x: -x["strike"])

    hist_rank, hist_n = _iv_rank_from_history(sym, atm_iv)
    if hist_rank is not None:
        iv_rank, ivr_src = hist_rank, "history"
    else:
        rv_lo, rv_hi = m["rv_lo"], m["rv_hi"]
        iv_rank = int(min(100, max(0, (atm_iv - rv_lo) / (rv_hi - rv_lo) * 100))) if rv_hi > rv_lo else 50
        ivr_src = "proxy"
    if record_iv:
        _record_iv(sym, atm_iv)
    ivrv = round(atm_iv / m["rv20"], 2) if m["rv20"] > 0 else 1.0

    estr, edays = get_earn(stock, sym, today_d, emap)
    earn_in = edays is not None and edays <= dte

    spread = primary["spreadPct"]
    q = _grade(iv_rank, ivrv, spread, earn_in)
    roc_ann = round(primary["rocPct"] * 365 / dte, 1)
    meta = META.get(sym, {})
    exp_move = price * atm_iv * math.sqrt(T)

    flags = []
    if primary["oi"] < 100:
        flags.append("Low open interest")
    if m["trend"] == "down":
        flags.append("Below 50 & 200-day averages")
    if sym in LEVERAGED:
        flags.append("Leveraged/volatility product")
    if primary["strike"] < m["low60"]:
        flags.append("+Strike below 60-day low")
    if m.get("sma200") and primary["strike"] < m["sma200"]:
        flags.append("+Strike below 200-day average")

    return {
        "ticker": sym,
        "name": meta.get("name") or sym,
        "sector": meta.get("sector") or "Other",
        "industry": meta.get("industry") or "",
        "isEtf": bool(meta.get("etf")),
        "mcap": meta.get("mcap") or 0,
        "price": round(price, 2),
        "chg1d": round(m["chg1d"], 2),
        "quality": q,
        "score": _score(iv_rank, ivrv, roc_ann, spread, primary["pop"], earn_in),
        "ivRank": iv_rank, "ivRankSource": ivr_src, "ivHistoryDays": hist_n,
        "ivRvRatio": ivrv,
        "atm_iv": round(atm_iv * 100, 1),
        "rv30": round(m["rv20"] * 100, 1),
        "expiration": exp, "dte": dte,
        "putStrike": primary["strike"], "delta": primary["delta"],
        "premium": primary["mid"], "bid": primary["bid"], "ask": primary["ask"],
        "spreadPct": spread, "openInterest": primary["oi"],
        "breakeven": primary["breakeven"], "otmPct": primary["otmPct"],
        "rocPct": primary["rocPct"], "rocAnn": roc_ann, "pop": primary["pop"],
        "thetaDay": primary["thetaDay"],
        "collateral": int(primary["strike"] * 100),
        "avgVol": int(m["avg_vol"]), "dollarVol": int(m["dollar_vol"]),
        "expectedMove": round(exp_move, 2),
        "sma50": round(m["sma50"], 2), "sma200": round(m["sma200"], 2) if m.get("sma200") else None,
        "low60": round(m["low60"], 2), "hi52": round(m["hi52"], 2), "lo52": round(m["lo52"], 2),
        "trend": m["trend"], "spark": m["spark"],
        "earningsWithin": earn_in, "earningsDate": estr, "earningsDays": edays,
        "profitTarget": round(primary["mid"] * 0.50 * 100, 2),
        "stopLoss": round(primary["mid"] * 2.0 * 100, 2),
        "ladder": picks,
        "flags": flags,
    }


def _sort(lst):
    order = {"STRONG": 0, "DECENT": 1, "SKIP": 2}
    return sorted(lst, key=lambda x: (order.get(x["quality"], 3), -x["score"], -x["rocAnn"]))


# -- Market regime -----------------------------------------------------------
def fetch_market():
    try:
        raw = _retry(lambda: yf.download(["SPY", "QQQ", "IWM", "^VIX"], period="1y", interval="1d",
                                         group_by="ticker", auto_adjust=True, threads=True,
                                         progress=False, timeout=30))
    except Exception as e:
        log.warning("market: %s", e)
        return None
    out = {}
    for sym in ["SPY", "QQQ", "IWM", "^VIX"]:
        try:
            c = raw[sym]["Close"].dropna()
            px, prev = _f(c.iloc[-1]), _f(c.iloc[-2])
            out[sym.replace("^", "")] = {
                "price": round(px, 2), "chg": round((px / prev - 1) * 100, 2),
                "sma50": round(_f(c.tail(50).mean()), 2),
                "sma200": round(_f(c.tail(200).mean()), 2),
                "spark": [round(_f(x), 2) for x in c.tail(60).iloc[::3].tolist()],
            }
        except Exception:
            pass
    spy, vix = out.get("SPY"), out.get("VIX")
    if spy and vix:
        above = spy["price"] > spy["sma50"] and spy["price"] > spy["sma200"]
        v = vix["price"]
        if v >= 30:
            regime, note = "stress", "VIX 30+: premiums are fat but gaps are real. Size down, go further OTM."
        elif v >= 20 and above:
            regime, note = "rich", "Elevated VIX in an uptrend: the best environment for selling puts."
        elif v >= 20:
            regime, note = "caution", "Elevated VIX with SPY below trend. Favor strong support and smaller size."
        elif above:
            regime, note = "calm", "Low VIX uptrend: premiums are thin. Be picky and stick to STRONG grades."
        else:
            regime, note = "soft", "Low VIX but SPY below trend. Thin premium for the risk; be selective."
        out["regime"] = {"label": regime, "note": note, "spyAboveTrend": above}
    out["asOf"] = datetime.now(NY).strftime("%b %d, %I:%M %p ET")
    return out


# -- Main scan ---------------------------------------------------------------
def run_scan():
    if not _scan_lock.acquire(blocking=False):
        return
    try:
        _run_scan_inner()
    except Exception as e:
        log.exception("scan crashed")
        _cache["error"] = str(e)[:200]
    finally:
        _cache["running"] = False
        _cache["progress"]["phase"] = "done"
        _scan_lock.release()


def _run_scan_inner():
    _cache["running"] = True
    _cache["error"] = None
    t0 = time.time()
    _universe_ready.wait(timeout=60)
    universe = list(UNIVERSE) or list(_FALLBACK)
    log.info("Scan start: %d tickers", len(universe))
    _cache["progress"] = {"done": 0, "total": len(universe), "phase": "bulk download",
                          "ticker": "", "found": 0, "start": t0, "downloaded": 0, "passed": 0}

    emap = {}
    cal_done = threading.Event()

    def _cal():
        try:
            emap.update(fetch_earnings_map())
        finally:
            cal_done.set()
    threading.Thread(target=_cal, daemon=True).start()

    try:
        mk = fetch_market()
        if mk:
            _cache["market"] = mk
    except Exception:
        pass

    # Stage 1
    BATCH = 100
    batches = [universe[i:i + BATCH] for i in range(0, len(universe), BATCH)]
    candidates = []
    for bi, batch in enumerate(batches):
        _cache["progress"]["phase"] = "bulk download %d/%d" % (bi + 1, len(batches))
        _cache["progress"]["done"] = bi * BATCH
        try:
            raw = _retry(lambda: yf.download(tickers=batch, period="1y", interval="1d",
                                             group_by="ticker", auto_adjust=True, threads=True,
                                             progress=False, timeout=60), attempts=2)
        except Exception as e:
            log.warning("Batch %d failed: %s", bi + 1, e)
            continue
        if raw is None or raw.empty:
            continue
        is_multi = isinstance(raw.columns, pd.MultiIndex)
        lvl0 = set(raw.columns.get_level_values(0)) if is_multi else set()
        for sym in batch:
            try:
                if is_multi:
                    if sym not in lvl0:
                        continue
                    h = raw[sym]
                else:
                    h = raw
                m = history_metrics(h)
                if m:
                    candidates.append((sym, m))
            except Exception:
                continue
        del raw
        _cache["progress"]["downloaded"] = min(len(universe), (bi + 1) * BATCH)
        _cache["progress"]["passed"] = len(candidates)
        time.sleep(0.3)

    log.info("Stage 1: %d / %d passed", len(candidates), len(universe))
    if not candidates:
        _cache["error"] = "No tickers passed the liquidity filter (data source may be rate-limiting)."
        return

    candidates.sort(key=lambda x: -x[1]["dollar_vol"])
    to_score = candidates[:SCORING_CAP]
    cal_done.wait(timeout=30)

    _cache["progress"].update({"done": 0, "total": len(to_score), "phase": "scoring options",
                               "ticker": "", "found": 0})
    results, lock = [], threading.Lock()
    fails = {"n": 0}

    def score_one(item):
        sym, m = item
        r = None
        try:
            r = score_options(sym, m, emap)
        except Exception as e:
            fails["n"] += 1
            log.debug("options %s: %s", sym, e)
        with lock:
            _cache["progress"]["done"] += 1
            _cache["progress"]["ticker"] = sym
            if r:
                results.append(r)
                _cache["progress"]["found"] = len(results)
                if len(results) % 10 == 0:
                    _cache["results"] = _sort(results)

    with concurrent.futures.ThreadPoolExecutor(max_workers=SCORING_WORKERS) as pool:
        list(pool.map(score_one, to_score))

    final = _sort(results)
    _cache["results"] = final
    _cache["ts"] = time.time()
    _cache["stats"] = {
        "universe": len(universe), "passedLiquidity": len(candidates), "scored": len(to_score),
        "withSetups": len(final), "optionFailures": fails["n"],
        "strong": sum(1 for r in final if r["quality"] == "STRONG"),
        "durationSec": int(time.time() - t0),
    }
    _save_json(CACHE_FILE, {"results": final, "ts": _cache["ts"], "market": _cache["market"],
                            "stats": _cache["stats"]})
    with _ivhist_lock:
        _save_json(IVHIST_FILE, _ivhist)
    log.info("Scan done: %s", _cache["stats"])


def _start_scan_thread():
    if not _cache["running"]:
        threading.Thread(target=run_scan, daemon=True).start()


def _auto_start():
    if not _cache["running"] and (not _cache["ts"] or time.time() - _cache["ts"] > CACHE_TTL):
        _start_scan_thread()


def _scheduler():
    """Scan shortly after the open, midday, and before the close on weekdays (ET)."""
    slots = {(9, 50), (12, 30), (15, 10)}
    fired = set()
    while True:
        try:
            now = datetime.now(NY)
            key = (now.date(), now.hour, now.minute)
            if now.weekday() < 5 and (now.hour, now.minute) in slots and key not in fired:
                fired.add(key)
                if time.time() - _cache["ts"] > 45 * 60:
                    log.info("Scheduled scan %s", now.strftime("%H:%M"))
                    _start_scan_thread()
        except Exception:
            pass
        time.sleep(30)


def _boot():
    saved = _load_json(CACHE_FILE, None)
    if saved and saved.get("results"):
        _cache.update({"results": saved["results"], "ts": saved.get("ts", 0),
                       "market": saved.get("market"), "stats": saved.get("stats", {})})
        _cache["progress"]["phase"] = "done"
        log.info("Restored %d cached results", len(saved["results"]))
    _ivhist.update(_load_json(IVHIST_FILE, {}))
    _build_universe()


threading.Thread(target=_boot, daemon=True).start()
if AUTO_SCAN:
    threading.Thread(target=_scheduler, daemon=True).start()


# -- API -----------------------------------------------------------------------
def _scan_payload():
    p = _cache["progress"]
    return {
        "results": _cache["results"],
        "count": len(_cache["results"]),
        "scannedAt": datetime.fromtimestamp(_cache["ts"], NY).strftime("%b %d, %I:%M %p ET") if _cache["ts"] else None,
        "ts": int(_cache["ts"]) if _cache["ts"] else None,
        "ageMinutes": int((time.time() - _cache["ts"]) / 60) if _cache["ts"] else None,
        "isRefreshing": _cache["running"],
        "universe": len(UNIVERSE),
        "universeSources": UNIVERSE_SOURCES,
        "progress": p,
        "market": _cache["market"],
        "stats": _cache["stats"],
        "error": _cache["error"],
        "config": {"strongIvr": STRONG_IVR, "strongIvrv": STRONG_IVRV, "strongSpread": STRONG_SPREAD,
                   "targetDelta": TARGET_DELTA, "dteMin": DTE_MIN, "dteMax": DTE_MAX,
                   "scoringCap": SCORING_CAP, "minPrice": MIN_PRICE},
    }


@app.route("/api/csp/scan")
@app.route("/api/scan")
def api_scan():
    _auto_start()
    return jsonify(_scan_payload())


@app.route("/api/csp/progress")
@app.route("/api/progress")
def api_progress():
    p = _cache["progress"]
    tot = max(p["total"], 1)
    ela = int(time.time() - p.get("start", time.time())) if _cache["running"] else 0
    eta = None
    if p["phase"] == "scoring options" and p["done"] > 5 and ela > 0:
        rate = p["done"] / max(ela, 1)
        eta = int((tot - p["done"]) / rate) if rate > 0 else None
    return jsonify({"done": p["done"], "total": tot, "pct": int(p["done"] / tot * 100),
                    "phase": p["phase"], "currentTicker": p.get("ticker", ""),
                    "found": p.get("found", 0), "passed": p.get("passed", 0),
                    "downloaded": p.get("downloaded", 0), "elapsed": ela, "eta": eta,
                    "universe": len(UNIVERSE), "running": _cache["running"]})


@app.route("/api/csp/refresh", methods=["POST"])
@app.route("/api/refresh", methods=["POST"])
def api_refresh():
    if _cache["running"]:
        return jsonify({"message": "Already running", "running": True}), 202
    age = time.time() - _cache["ts"] if _cache["ts"] else 1e9
    if age < REFRESH_COOLDOWN and request.args.get("force") != "1":
        return jsonify({"message": "Fresh scan is %d min old; cooldown active" % (age // 60),
                        "running": False, "cooldown": True}), 429
    _start_scan_thread()
    return jsonify({"message": "Started", "universe": len(UNIVERSE)}), 200


@app.route("/api/market")
def api_market():
    if not _cache["market"]:
        _cache["market"] = fetch_market()
    return jsonify(_cache["market"] or {})


@app.route("/api/analyze")
def api_analyze():
    """Score any single ticker on demand (not limited to the scan universe)."""
    sym = (request.args.get("ticker") or "").strip().upper().replace(".", "-")
    if not re.fullmatch(r"[A-Z]{1,5}(-[A-Z])?", sym):
        return jsonify({"error": "Enter a valid ticker"}), 400
    hit = _analyze_cache.get(sym)
    if hit and time.time() - hit[0] < 900:
        return jsonify(hit[1])
    try:
        h = _retry(lambda: yf.Ticker(sym).history(period="1y", interval="1d", auto_adjust=True))
        if h is None or h.empty:
            return jsonify({"error": "No price history for %s" % sym}), 404
        m = history_metrics(h, filters=False)
        if not m:
            return jsonify({"error": "Not enough history for %s" % sym}), 404
        if sym not in META:
            try:
                nm = yf.Ticker(sym).info.get("shortName")
            except Exception:
                nm = None
            META[sym] = {"name": nm or sym, "sector": "Other", "industry": "", "mcap": 0, "etf": False}
        r = score_options(sym, m, {}, record_iv=False)
        if not r:
            return jsonify({"error": "%s has no listed puts with live bids 3-7 weeks out" % sym}), 404
        r["liquidityWarning"] = m["avg_vol"] < 300_000 or m["dollar_vol"] < MIN_DOLLAR_VOL
        _analyze_cache[sym] = (time.time(), r)
        return jsonify(r)
    except Exception as e:
        log.warning("analyze %s: %s", sym, e)
        return jsonify({"error": "Lookup failed. Yahoo may be rate-limiting; try again in a minute."}), 502


@app.route("/api/csp/status")
@app.route("/api/status")
def api_status():
    return jsonify({"status": "ok", "cached": bool(_cache["ts"]), "count": len(_cache["results"]),
                    "running": _cache["running"], "universe": len(UNIVERSE),
                    "universeSources": UNIVERSE_SOURCES, "progress": _cache["progress"],
                    "stats": _cache["stats"], "error": _cache["error"], "version": "2.0"})


@app.route("/api/debug")
def api_debug():
    out = {"universe": len(UNIVERSE), "sources": UNIVERSE_SOURCES, "results": len(_cache["results"]),
           "running": _cache["running"], "progress": _cache["progress"], "stats": _cache["stats"],
           "scoring_workers": SCORING_WORKERS, "scoring_cap": SCORING_CAP, "data_dir": DATA_DIR,
           "iv_history_tickers": len(_ivhist),
           "cache_age_secs": int(time.time() - _cache["ts"]) if _cache["ts"] else None}
    try:
        t = yf.Ticker("AAPL")
        out["yf_test"] = {"price": _f(getattr(t.fast_info, "last_price", 0)),
                          "options": len(t.options or [])}
    except Exception as e:
        out["yf_test"] = str(e)[:200]
    return jsonify(out)


# -- Tracker support (unchanged contract) ------------------------------------
@app.route("/api/prices")
def api_prices():
    tickers = [t.strip().upper() for t in request.args.get("tickers", "").split(",") if t.strip()][:40]
    if not tickers:
        return jsonify({"error": "No tickers"}), 400
    prices = {}
    for sym in tickers:
        try:
            fi = yf.Ticker(sym).fast_info
            for attr in ("last_price", "previous_close"):
                px = getattr(fi, attr, None)
                if px and float(px) > 0:
                    prices[sym] = round(float(px), 2)
                    break
        except Exception:
            pass
    return jsonify({"prices": prices, "ts": datetime.now(NY).strftime("%I:%M %p")})


@app.route("/api/option-prices", methods=["POST"])
def api_option_prices():
    try:
        contracts = request.get_json(force=True) or []
    except Exception:
        return jsonify({"error": "Invalid JSON"}), 400
    results = {}
    today_d = date.today()
    for c in contracts[:20]:
        cid = c.get("id", "")
        sym = (c.get("ticker") or "").upper()
        ot = (c.get("optionType") or "call").lower()
        st = _f(c.get("strike"))
        ex = c.get("expiration") or ""
        if not sym or not st or not ex:
            continue
        try:
            tk = yf.Ticker(sym)
            fi = tk.fast_info
            und = 0.0
            for a in ("last_price", "previous_close"):
                v = getattr(fi, a, None)
                if v and float(v) > 0:
                    und = float(v)
                    break
            opts = tk.options
            if not opts:
                continue
            target = datetime.strptime(ex, "%Y-%m-%d").date()
            best = min(opts, key=lambda e: abs((datetime.strptime(e, "%Y-%m-%d").date() - target).days))
            chain = tk.option_chain(best)
            df = chain.calls if ot == "call" else chain.puts
            if df.empty:
                continue
            df = df.copy()
            df["_d"] = (df["strike"] - st).abs()
            row = df.loc[df["_d"].idxmin()]
            bid, ask = _f(row.get("bid")), _f(row.get("ask"))
            last, iv = _f(row.get("lastPrice")), _f(row.get("impliedVolatility"))
            mid = round((bid + ask) / 2, 2) if bid > 0 and ask > 0 else (round(last, 2) if last > 0 else None)
            dte = max(0, (datetime.strptime(best, "%Y-%m-%d").date() - today_d).days)
            results[cid] = {"mid": mid, "bid": round(bid, 2), "ask": round(ask, 2),
                            "underlying": round(und, 2), "dte": dte,
                            "iv": round(iv * 100, 1) if iv else None,
                            "expUsed": best, "strikeUsed": float(row["strike"])}
        except Exception as e:
            log.debug("opt-price %s: %s", sym, e)
    return jsonify({"prices": results, "ts": datetime.now(NY).strftime("%I:%M %p")})


@app.route("/")
def index():
    p = _cache["progress"]
    return ("optionscanners.com API v2 | universe:%d | phase:%s | %d/%d | found:%d | running:%s"
            % (len(UNIVERSE), p["phase"], p["done"], p["total"], p.get("found", 0), _cache["running"]))


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
