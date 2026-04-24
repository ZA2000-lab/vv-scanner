# VV CSP Scanner - batched bulk download approach
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
log = logging.getLogger(__name__)
app = Flask(__name__)
CORS(app)

try:
    yf.set_tz_cache_location("/tmp/yf_tz_cache")
except Exception:
    pass

CACHE_TTL      = 6 * 3600
DAYS_AHEAD     = 45
SCORING_WORKERS = 4     # workers for options-chain stage only
SCORING_CAP    = 300    # top N by volume sent to options stage

NASDAQ_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://www.nasdaq.com",
    "Referer": "https://www.nasdaq.com/market-activity/earnings",
}

# -- Cache ------------------------------------------------------------------
_cache = {
    "results":  [],
    "ts":       0,
    "running":  False,
    "progress": {
        "done": 0, "total": 0, "phase": "idle",
        "ticker": "", "found": 0, "start": 0,
        "downloaded": 0, "passed": 0,
    },
}

# -- Universe ---------------------------------------------------------------
# Hardcoded liquid names -- Wikipedia adds S&P 500/400/600 at startup.
_EXTRAS = [
    "CDE",
    "AAPL","MSFT","NVDA","GOOGL","AMZN","META","TSLA","AVGO","ORCL","AMD",
    "ADBE","CRM","INTU","NOW","PANW","CRWD","NET","DDOG","SNOW","PLTR","MDB",
    "INTC","QCOM","TXN","AMAT","LRCX","KLAC","MU","SMCI","ARM","ON","MRVL",
    "CSCO","IBM","DELL","HPE","ACN","VRSK","PYPL","SQ","SHOP",
    "JPM","BAC","WFC","GS","MS","C","V","MA","AXP","COF","SCHW","IBKR",
    "BLK","BX","KKR","APO","COIN","MSTR","HOOD","SOFI","ALLY","DFS","SYF",
    "UNH","LLY","JNJ","ABBV","MRK","PFE","AMGN","GILD","VRTX","REGN","ISRG",
    "TMO","DHR","MRNA","BIIB","SYK","MDT","BSX","CI","ELV","HUM","CVS","CNC","MOH",
    "XOM","CVX","COP","EOG","DVN","OXY","HES","SLB","HAL","VLO","MPC","LNG","EQT",
    "GE","HON","CAT","DE","MMM","ETN","PH","RTX","LMT","GD","NOC","BA","UPS","FDX",
    "CSX","UNP","WM","SHW","EMR","TT","CARR","OTIS","ROK","ITW","DOV","AME",
    "HD","WMT","COST","TGT","MCD","SBUX","CMG","YUM","NKE","LULU","DECK","ONON",
    "BKNG","EXPE","ABNB","UBER","LYFT","DASH","ETSY","EBAY","CHWY",
    "DIS","NFLX","CMCSA","EA","TTWO","DKNG","MGM","WYNN","LYV","RBLX",
    "GM","F","RIVN","NIO","LCID","XPEV","LI","DAL","AAL","UAL","CCL","RCL",
    "ROST","TJX","BURL","DG","DLTR","LOW","KSS","JWN","ANF","AEO","GPS","URBN",
    "PG","KO","PEP","PM","MO","MDLZ","KHC","GIS","CL","CHD","KMB","CLX",
    "EL","ULTA","BBWI","TSN","HRL","MKC","SJM","CPB","SYY",
    "NEE","DUK","SO","D","AEP","EXC","XEL","PCG","EIX","PPL","AES","WEC",
    "ENPH","FSLR","RUN","SEDG",
    "LIN","APD","CF","NTR","FCX","NEM","GOLD","AEM","KGC","WPM","PAAS",
    "NUE","STLD","CLF","DD","DOW","LYB","ALB","MP","ECL","SHW",
    "AMT","CCI","EQIX","SBAC","DLR","IRM","SPG","O","VICI","PLD","PSA",
    "EXR","EQR","AVB","WELL","VTR","OHI","ARE","BXP",
    "VZ","T","TMUS","SNAP","PINS","SPOT","RDDT",
    "HOOD","SOFI","MARA","RIOT","GME","AMC","SPCE","RKLB","ASTS",
    "WPM","PAAS","EGO","IAG","AU","HL","SILV","FSM","NGD","GFI","HMY","SBSW",
    "SPY","QQQ","IWM","DIA","MDY","VTI","VOO","IVV","RSP",
    "GLD","SLV","GDX","GDXJ","IAU",
    "TLT","IEF","LQD","HYG","JNK","AGG","BND",
    "XLE","XLF","XLK","XLV","XLI","XLU","XLRE","XLB","XLY","XLP","XLC",
    "SMH","SOXX","IBB","ARKK","ARKG","EEM","EFA","EWZ","EWJ","FXI","KWEB","MCHI",
    "USO","UNG","CPER","VXX","UVXY","TQQQ","SQQQ","UPRO","SSO",
]

UNIVERSE = list(_EXTRAS)
_uni_lock = threading.Lock()


def _load_wiki(url, name):
    try:
        resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
        if resp.status_code != 200:
            return []
        for df in pd.read_html(resp.text):
            cols = [str(c).lower() for c in df.columns]
            col = next((df.columns[i] for i, c in enumerate(cols)
                        if c in ("symbol","ticker","symbol[3]","ticker symbol")), None)
            if col is None:
                continue
            tickers = (df[col].astype(str)
                       .str.split(r"[\s\[]").str[0]
                       .str.replace(r"\..*","",regex=True)
                       .str.upper().str.strip().tolist())
            tickers = [t.replace(".","-") for t in tickers
                       if 1 <= len(t) <= 6 and t.replace("-","").isalpha()]
            if len(tickers) > 10:
                log.info("Wiki %s: %d tickers", name, len(tickers))
                return tickers
    except Exception as e:
        log.warning("Wiki %s: %s", name, e)
    return []


def _build_universe():
    global UNIVERSE
    log.info("Loading Wikipedia universe...")
    wiki = []
    for name, url in [
        ("sp500",    "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"),
        ("sp400",    "https://en.wikipedia.org/wiki/List_of_S%26P_400_companies"),
        ("sp600",    "https://en.wikipedia.org/wiki/List_of_S%26P_600_companies"),
        ("nasdaq100","https://en.wikipedia.org/wiki/Nasdaq-100"),
    ]:
        wiki.extend(_load_wiki(url, name))

    combined = _EXTRAS + wiki
    seen, result = set(), []
    for t in combined:
        if t not in seen:
            seen.add(t)
            result.append(t)
    with _uni_lock:
        UNIVERSE = result
    log.info("Universe ready: %d tickers", len(result))


threading.Thread(target=_build_universe, daemon=True).start()


# -- Earnings calendar ------------------------------------------------------

def fetch_earnings_map():
    out = {}
    today = date.today()
    s = requests.Session()
    s.headers.update(NASDAQ_HEADERS)
    deadline = time.time() + 60
    for delta in range(DAYS_AHEAD + 1):
        if time.time() > deadline:
            break
        d = today + timedelta(days=delta)
        if d.weekday() >= 5:
            continue
        try:
            r = s.get(
                "https://api.nasdaq.com/api/calendar/earnings?date=" + d.strftime("%Y-%m-%d"),
                timeout=4)
            if r.status_code == 200:
                for row in (r.json().get("data") or {}).get("rows") or []:
                    sym = (row.get("symbol") or "").upper().strip().replace("/","-")
                    if sym and sym not in out:
                        out[sym] = (d.strftime("%b %d"), delta)
            time.sleep(0.05)
        except Exception:
            pass
    log.info("Earnings map: %d tickers", len(out))
    return out


def get_earn(stock, sym, today_d, emap):
    cands = []
    try:
        cal = stock.calendar
        if isinstance(cal, dict):
            for ed in (cal.get("Earnings Date") or []):
                if ed is None:
                    continue
                if hasattr(ed, "date"):
                    ed = ed.date()
                elif isinstance(ed, str):
                    try: ed = datetime.strptime(ed[:10],"%Y-%m-%d").date()
                    except: continue
                days = (ed - today_d).days
                if -5 <= days <= 180:
                    cands.append((days, ed.strftime("%b %d")))
                    break
        elif isinstance(cal, pd.DataFrame) and not cal.empty:
            for col in cal.columns:
                try:
                    ed   = pd.Timestamp(col).date()
                    days = (ed - today_d).days
                    if -5 <= days <= 180:
                        cands.append((days, ed.strftime("%b %d")))
                        break
                except Exception:
                    pass
    except Exception:
        pass
    if sym in emap:
        nd_str, nd_days = emap[sym]
        if -2 <= nd_days <= 180:
            cands.append((nd_days, nd_str))
    if not cands:
        return None, None
    cands.sort()
    return cands[0][1], cands[0][0]


# -- Options scoring (Stage 2) ----------------------------------------------

def score_options(sym, price, rv30, rv_lo, rv_hi, avg_vol, emap):
    """
    Only fetches options data -- price/vol/rv already computed from bulk download.
    Makes 2 API calls: stock.options + stock.option_chain(exp).
    """
    try:
        stock   = yf.Ticker(sym)
        today_d = date.today()

        opts = stock.options
        if not opts:
            return None

        exp, dte = None, None
        for e in sorted(opts):
            try:
                d = (datetime.strptime(e,"%Y-%m-%d").date() - today_d).days
                if 28 <= d <= 55:
                    exp, dte = e, d
                    break
            except Exception:
                continue
        if not exp:
            for e in sorted(opts):
                try:
                    d = (datetime.strptime(e,"%Y-%m-%d").date() - today_d).days
                    if d >= 21:
                        exp, dte = e, d
                        break
                except Exception:
                    continue
        if not exp:
            return None

        puts = stock.option_chain(exp).puts
        if puts is None or puts.empty:
            return None

        idx    = (puts["strike"] - price).abs().idxmin()
        atm_iv = float(puts.loc[idx, "impliedVolatility"])
        if atm_iv <= 0 or atm_iv > 5:
            return None

        iv_rank = int(min(100, max(0,
            (atm_iv - rv_lo) / (rv_hi - rv_lo) * 100))) if rv_hi > rv_lo else 50
        ivrv    = round(atm_iv / rv30, 2) if rv30 > 0 else 1.0

        T  = dte / 365.0
        sk = price * (1 - 0.45 * atm_iv * T**0.5)
        op = puts[puts["strike"] <= price * 1.02].copy()
        if op.empty:
            return None
        op["_d"] = (op["strike"] - sk).abs()
        row = op.loc[op["_d"].idxmin()]
        put_strike = float(row["strike"])

        bid  = float(row.get("bid",  0) or 0)
        ask  = float(row.get("ask",  0) or 0)
        oi   = int(row.get("openInterest", 0) or 0)
        iv_p = float(row.get("impliedVolatility", atm_iv) or atm_iv)

        mid = round((bid+ask)/2, 2) if bid>0 and ask>0 else \
              round(price * iv_p * T**0.5 * 0.40 * (put_strike/price)**0.5, 2)
        if mid < 0.05:
            return None

        spread = round((ask-bid)/mid*100, 1) if mid>0 and ask>bid else 0
        be     = round(put_strike - mid, 2)
        coll   = put_strike * 100
        roc    = round(mid*100/coll*100, 2) if coll>0 else 0
        roc_a  = round(roc * 365/dte, 1)  if dte>0  else 0

        estr, edays = get_earn(stock, sym, today_d, emap)
        earn_in     = edays is not None and edays <= dte

        if earn_in or spread > 20 or iv_rank < 45:
            q = "SKIP"
        elif iv_rank >= 60 and ivrv >= 1.2 and spread <= 10:
            q = "STRONG"
        else:
            q = "DECENT"

        try:    name = getattr(stock.fast_info, "company_name", None) or sym
        except: name = sym

        return {
            "ticker":        sym,
            "name":          name,
            "price":         round(price, 2),
            "quality":       q,
            "ivRank":        iv_rank,
            "ivRvRatio":     ivrv,
            "putStrike":     put_strike,
            "expiration":    exp,
            "dte":           dte,
            "premium":       mid,
            "bid":           round(bid, 2),
            "ask":           round(ask, 2),
            "spreadPct":     spread,
            "openInterest":  oi,
            "breakeven":     be,
            "rocPct":        roc,
            "rocAnn":        roc_a,
            "collateral":    int(coll),
            "avgVol":        int(avg_vol),
            "atm_iv":        round(atm_iv*100, 1),
            "rv30":          round(rv30*100, 1),
            "earningsWithin":earn_in,
            "earningsDate":  estr,
            "earningsDays":  edays,
            "profitTarget":  round(mid*0.50*100, 2),
            "stopLoss":      round(mid*2.0*100, 2),
        }
    except Exception as e:
        log.debug("options %s: %s", sym, e)
        return None


def _sort(lst):
    return sorted(lst,
        key=lambda x: ({"STRONG":0,"DECENT":1,"SKIP":2}.get(x["quality"],3), -x["rocAnn"]))


# -- Main scan --------------------------------------------------------------

def run_scan():
    if _cache["running"]:
        return
    _cache["running"] = True
    _cache["results"] = []
    t0 = time.time()

    # Wait up to 20s for Wikipedia universe to finish loading
    deadline = time.time() + 20
    while len(UNIVERSE) <= len(_EXTRAS) and time.time() < deadline:
        time.sleep(1)

    universe = list(UNIVERSE)
    log.info("Scan start -- %d tickers", len(universe))

    _cache["progress"] = {
        "done": 0, "total": len(universe), "phase": "bulk download",
        "ticker": "", "found": 0, "start": t0,
        "downloaded": 0, "passed": 0,
    }

    # -- Earnings calendar runs concurrently with bulk download ------------
    emap = {}
    cal_done = threading.Event()
    def _cal():
        try: emap.update(fetch_earnings_map())
        except Exception: pass
        finally: cal_done.set()
    threading.Thread(target=_cal, daemon=True).start()

    # ---------------------------------------------------------------------
    # -- STAGE 1: Batched bulk download ------------------------------------
    # Download in batches of 100 to keep RAM under Railway's 512MB limit.
    # Each batch: ~100 tickers x 126 days x 6 cols = ~6MB. Process then discard.
    BATCH_SIZE = 100
    batches    = [universe[i:i+BATCH_SIZE] for i in range(0, len(universe), BATCH_SIZE)]
    candidates = []  # (sym, price, vol, rv30, rv_lo, rv_hi)

    log.info("Stage 1: %d batches of up to %d tickers", len(batches), BATCH_SIZE)

    for bi, batch in enumerate(batches):
        _cache["progress"]["phase"] = f"bulk download {bi+1}/{len(batches)}"
        try:
            raw = yf.download(
                tickers     = batch,
                period      = "6mo",
                interval    = "1d",
                group_by    = "ticker",
                auto_adjust = True,
                threads     = True,
                progress    = False,
                timeout     = 60,
            )
        except Exception as e:
            log.warning("Batch %d failed: %s", bi+1, e)
            continue

        is_multi = isinstance(raw.columns, pd.MultiIndex)

        for sym in batch:
            try:
                h = raw[sym].dropna(how="all") if is_multi and sym in raw.columns.get_level_values(0) \
                    else (raw.dropna(how="all") if not is_multi else None)
                if h is None or len(h) < 20:
                    continue
                price = float(h["Close"].iloc[-1])
                if price < 2 or np.isnan(price):
                    continue
                avg_vol = float(h["Volume"].tail(20).mean())
                if avg_vol < 300_000 or np.isnan(avg_vol):
                    continue
                rets     = h["Close"].dropna().pct_change().dropna()
                if len(rets) < 15:
                    continue
                rv30     = float(rets.tail(20).std() * np.sqrt(252))
                rv_roll  = rets.rolling(20).std().dropna() * np.sqrt(252)
                rv_lo    = float(rv_roll.min()) if len(rv_roll) > 5 else rv30 * 0.5
                rv_hi    = float(rv_roll.max()) if len(rv_roll) > 5 else rv30 * 1.5
                candidates.append((sym, price, avg_vol, rv30, rv_lo, rv_hi))
            except Exception:
                continue

        del raw  # free memory immediately after each batch
        log.info("Batch %d/%d done -- %d candidates so far", bi+1, len(batches), len(candidates))

    log.info("Stage 1 complete: %d / %d tickers passed", len(candidates), len(universe))
    _cache["progress"]["downloaded"] = len(universe)
    _cache["progress"]["passed"]     = len(candidates)

    if not candidates:
        log.error("No tickers passed bulk filter")
        _cache["running"] = False
        _cache["progress"]["phase"] = "done"
        return

    # Sort by volume, take top SCORING_CAP
    candidates.sort(key=lambda x: -x[2])
    to_score = candidates[:SCORING_CAP]
    log.info("Stage 2: scoring options on top %d tickers...", len(to_score))

    # Wait for earnings calendar (should be done by now)
    cal_done.wait(timeout=10)
    log.info("Calendar ready: %d entries", len(emap))

    # ---------------------------------------------------------------------
    # STAGE 2: Options chains -- 4 parallel workers, each ticker 2 API calls
    # ---------------------------------------------------------------------
    _cache["progress"].update({
        "done": 0, "total": len(to_score), "phase": "scoring options",
        "ticker": "", "found": 0,
    })

    results = []
    lock    = threading.Lock()

    def score_one(item):
        sym, price, avg_vol, rv30, rv_lo, rv_hi = item
        r = score_options(sym, price, rv30, rv_lo, rv_hi, avg_vol, emap)
        with lock:
            _cache["progress"]["done"]   += 1
            _cache["progress"]["ticker"]  = sym
            if r:
                results.append(r)
                _cache["progress"]["found"] = len(results)
                _cache["results"] = _sort(results)

    with concurrent.futures.ThreadPoolExecutor(max_workers=SCORING_WORKERS) as pool:
        futs = [pool.submit(score_one, item) for item in to_score]
        for f in concurrent.futures.as_completed(futs):
            try: f.result()
            except Exception: pass

    _cache["results"]  = _sort(results)
    _cache["ts"]       = time.time()
    _cache["running"]  = False
    _cache["progress"].update({
        "done":  len(to_score), "phase": "done", "found": len(results),
    })
    log.info("Scan done: %d results / %d scored / %d universe / %.0fs",
             len(results), len(to_score), len(universe), time.time()-t0)


# -- API endpoints ----------------------------------------------------------

def _auto_start():
    if not _cache["running"] and (
        not _cache["ts"] or time.time() - _cache["ts"] > CACHE_TTL
    ):
        threading.Thread(target=run_scan, daemon=True).start()


@app.route("/api/csp/scan")
@app.route("/api/scan")
def api_scan():
    _auto_start()
    return jsonify({
        "results":      _cache["results"],
        "count":        len(_cache["results"]),
        "scannedAt":    datetime.fromtimestamp(_cache["ts"]).strftime("%b %d %Y, %I:%M %p") if _cache["ts"] else None,
        "ageMinutes":   int((time.time()-_cache["ts"])/60) if _cache["ts"] else 0,
        "isRefreshing": _cache["running"],
        "universe":     len(UNIVERSE),
        "progress":     _cache["progress"],
    })


@app.route("/api/csp/progress")
@app.route("/api/progress")
def api_progress():
    p   = _cache["progress"]
    tot = max(p["total"], 1)
    pct = int(p["done"] / tot * 100)
    ela = int(time.time() - p.get("start", time.time())) if _cache["running"] else 0
    eta = None
    if p["phase"] == "scoring options" and p["done"] > 2 and ela > 0:
        rate = p["done"] / ela
        eta  = int((tot - p["done"]) / rate) if rate > 0 else None

    labels = {
        "bulk download":   f"Stage 1 of 2: Bulk downloading {len(UNIVERSE):,} tickers at once...",
        "scoring options": f"Stage 2 of 2: Scoring options -- {p['done']}/{tot} tickers . {p.get('found',0)} setups found",
        "done":            "Scan complete ?",
        "idle":            "Ready",
    }

    return jsonify({
        "done":          p["done"],
        "total":         tot,
        "pct":           pct,
        "phase":         p["phase"],
        "phaseLabel":    labels.get(p["phase"], p["phase"]),
        "currentTicker": p.get("ticker",""),
        "found":         p.get("found",0),
        "passed":        p.get("passed",0),
        "downloaded":    p.get("downloaded",0),
        "elapsed":       ela,
        "eta":           eta,
        "universe":      len(UNIVERSE),
        "running":       _cache["running"],
    })


@app.route("/api/csp/refresh", methods=["POST"])
@app.route("/api/refresh", methods=["POST"])
def api_refresh():
    if _cache["running"]:
        return jsonify({"message":"Already running","running":True}), 202
    _cache["ts"]      = 0
    _cache["results"] = []
    threading.Thread(target=run_scan, daemon=True).start()
    return jsonify({"message":"Started","universe":len(UNIVERSE)}), 200


@app.route("/api/csp/status")
@app.route("/api/status")
def api_status():
    return jsonify({
        "status":"ok","cached":bool(_cache["ts"]),
        "count":len(_cache["results"]),"running":_cache["running"],
        "universe":len(UNIVERSE),"progress":_cache["progress"],
    })


@app.route("/api/debug")
def api_debug():
    p = _cache["progress"]
    out = {
        "universe":        len(UNIVERSE),
        "results":         len(_cache["results"]),
        "running":         _cache["running"],
        "phase":           p["phase"],
        "done":            p["done"],
        "total":           p["total"],
        "found":           p.get("found",0),
        "passed_filter":   p.get("passed",0),
        "scoring_workers": SCORING_WORKERS,
        "scoring_cap":     SCORING_CAP,
        "cache_age_secs":  int(time.time()-_cache["ts"]) if _cache["ts"] else None,
    }
    try:
        t = yf.Ticker("AAPL")
        h = t.history(period="5d")
        out["yf_test"] = {
            "price":   getattr(t.fast_info,"last_price","N/A"),
            "history": len(h),
            "options": len(t.options) if t.options else 0,
        }
    except Exception as e:
        out["yf_test"] = str(e)
    # Test bulk download on 5 tickers
    try:
        test = yf.download(["AAPL","MSFT","SPY","TSLA","QQQ"],
                           period="5d", group_by="ticker",
                           auto_adjust=True, threads=True, progress=False, timeout=15)
        out["bulk_test"] = {
            "shape": list(test.shape),
            "columns_type": type(test.columns).__name__,
        }
    except Exception as e:
        out["bulk_test"] = str(e)
    return jsonify(out)


@app.route("/api/prices")
def api_prices():
    tickers = [t.strip().upper()
               for t in request.args.get("tickers","").split(",") if t.strip()][:25]
    if not tickers:
        return jsonify({"error":"No tickers"}), 400
    prices = {}
    for sym in tickers:
        try:
            fi = yf.Ticker(sym).fast_info
            for attr in ("last_price","previous_close"):
                px = getattr(fi,attr,None)
                if px and float(px)>0:
                    prices[sym]=round(float(px),2); break
        except Exception: pass
    return jsonify({"prices":prices,"ts":datetime.now().strftime("%I:%M %p")})


@app.route("/api/option-prices", methods=["POST"])
def api_option_prices():
    try: contracts = request.get_json(force=True) or []
    except Exception: return jsonify({"error":"Invalid JSON"}),400
    results = {}
    today_d = date.today()
    for c in contracts[:10]:
        cid=c.get("id",""); sym=(c.get("ticker") or "").upper()
        ot=(c.get("optionType") or "call").lower()
        st=float(c.get("strike") or 0); ex=c.get("expiration") or ""
        if not sym or not st or not ex: continue
        try:
            tk=yf.Ticker(sym); fi=tk.fast_info; und=0.0
            for a in ("last_price","previous_close"):
                v=getattr(fi,a,None)
                if v and float(v)>0: und=float(v); break
            opts=tk.options
            if not opts: continue
            best=min(opts,key=lambda e:abs(
                (datetime.strptime(e,"%Y-%m-%d").date()-datetime.strptime(ex,"%Y-%m-%d").date()).days))
            chain=tk.option_chain(best)
            df=chain.calls if ot=="call" else chain.puts
            if df.empty: continue
            df=df.copy(); df["_d"]=(df["strike"]-st).abs()
            row=df.loc[df["_d"].idxmin()]
            bid=float(row.get("bid",0) or 0); ask=float(row.get("ask",0) or 0)
            last=float(row.get("lastPrice",0) or 0); iv=float(row.get("impliedVolatility",0) or 0)
            mid=round((bid+ask)/2,2) if bid>0 and ask>0 else (round(last,2) if last>0 else None)
            dte=max(0,(datetime.strptime(best,"%Y-%m-%d").date()-today_d).days)
            results[cid]={"mid":mid,"bid":round(bid,2),"ask":round(ask,2),
                          "underlying":round(und,2),"dte":dte,
                          "iv":round(iv*100,1) if iv else None,
                          "expUsed":best,"strikeUsed":float(row["strike"])}
        except Exception as e: log.debug("opt-price %s: %s",sym,e)
    return jsonify({"prices":results,"ts":datetime.now().strftime("%I:%M %p")})


@app.route("/")
def index():
    p = _cache["progress"]
    return (f"VV Scanner | universe:{len(UNIVERSE)} | "
            f"phase:{p['phase']} | {p['done']}/{p['total']} | "
            f"found:{p.get('found',0)} | running:{_cache['running']} | /api/debug")


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
