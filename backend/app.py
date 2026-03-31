from flask import Flask, jsonify
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

# yfinance 0.2.55+ handles cookies automatically, but we set headers to help
import yfinance.utils as yf_utils
try:
    import requests as req_session
    _yf_session = req_session.Session()
    _yf_session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    })
    yf.set_tz_cache_location("/tmp/yf_tz_cache")
except Exception as e:
    log.warning("Session setup: %s", e)

CACHE_TTL  = 6 * 3600
DAYS_AHEAD = 42
MAX_TICKERS = 200
MAX_WORKERS = 5

_cache = {
    "results": [], "universe": [], "ts": 0,
    "running": False, "progress": {"done":0,"total":0,"phase":"idle"},
    "debug": {}
}

# Layer 2 cache — cash-secured puts
_csp_cache = {
    "results": [], "ts": 0,
    "running": False, "progress": {"done":0,"total":0,"phase":"idle"},
}

# Layer 2 universe — quality large caps with liquid options
CSP_UNIVERSE = [
    "AAPL","MSFT","GOOGL","AMZN","META","V","MA","JPM","BAC","GS",
    "JNJ","UNH","HD","COST","WMT","PG","KO","MCD","XOM","CVX",
    "AMD","TSLA","NFLX","ORCL","CRM","ADBE","QCOM","INTC","TXN",
    "MS","BLK","SCHW","LLY","ABBV","MRK","PFE","AMGN",
    "SPY","QQQ","IWM","XLE","XLF","XLK","GLD","SMH",
]

NASDAQ_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://www.nasdaq.com",
    "Referer": "https://www.nasdaq.com/market-activity/earnings",
}

# Prioritized universe - most liquid options first
TOP_LIQUID = [
    "AAPL","MSFT","NVDA","AMZN","META","GOOGL","TSLA","NFLX","AMD","ORCL",
    "JPM","BAC","WFC","GS","MS","C","V","MA","AXP","SCHW","COIN","SOFI","PYPL",
    "UNH","LLY","ABBV","MRK","PFE","AMGN","VRTX","MRNA","ISRG","REGN","GILD",
    "HD","WMT","COST","MCD","SBUX","NKE","TGT","BKNG","UBER","DASH","ABNB",
    "XOM","CVX","COP","SLB","OXY","BA","CAT","GE","HON","RTX","LMT","UPS","FDX",
    "DIS","SNAP","PINS","SPOT","PLTR","CRWD","NET","DDOG","NOW","CRM","PANW",
    "AVGO","TXN","QCOM","INTC","MU","AMAT","LRCX","ARM","SMCI","ON",
    "F","GM","RIVN","NIO","XPEV","LI","DAL","AAL","UAL","CCL","RCL",
    "MARA","RIOT","MSTR","GME","AMC","MGM","DKNG","WYNN","PENN",
    "SPY","QQQ","IWM","GLD","TLT","SMH","XLE","XLF","XLK","ARKK",
]


def fetch_earnings_calendar():
    earnings_map = {}
    today = date.today()
    session = requests.Session()
    session.headers.update(NASDAQ_HEADERS)
    d = today
    while d <= today + timedelta(days=DAYS_AHEAD):
        if d.weekday() < 5:
            try:
                resp = session.get(
                    "https://api.nasdaq.com/api/calendar/earnings?date=" + d.strftime("%Y-%m-%d"),
                    timeout=5)
                if resp.status_code == 200:
                    rows = (resp.json().get("data") or {}).get("rows") or []
                    diff = (d - today).days
                    for row in rows:
                        sym = (row.get("symbol") or "").upper().strip().replace("/","-")
                        if sym and 1 <= len(sym) <= 6 and sym.replace("-","").isalpha():
                            if sym not in earnings_map:
                                earnings_map[sym] = (d.strftime("%b %d"), diff)
                time.sleep(0.08)
            except Exception as e:
                log.warning("Cal %s: %s", d, e)
        d += timedelta(days=1)
    log.info("Calendar: %d tickers with earnings in next %d days", len(earnings_map), DAYS_AHEAD)
    _cache["debug"]["calendar_count"] = len(earnings_map)
    _cache["debug"]["sample"] = list(earnings_map.items())[:8]
    return earnings_map


def yang_zhang(df, window=30, tp=252):
    try:
        lho = (df["High"]  / df["Open"]).apply(np.log)
        llo = (df["Low"]   / df["Open"]).apply(np.log)
        lco = (df["Close"] / df["Open"]).apply(np.log)
        loc_ = (df["Open"] / df["Close"].shift(1)).apply(np.log)
        lcc  = (df["Close"] / df["Close"].shift(1)).apply(np.log)
        rs  = lho*(lho-lco) + llo*(llo-lco)
        cv  = (lcc**2).rolling(window).sum() / (window-1)
        ov  = (loc_**2).rolling(window).sum() / (window-1)
        wr  = rs.rolling(window).sum() / (window-1)
        k   = 0.34 / (1.34 + (window+1)/(window-1))
        return float(((ov + k*cv + (1-k)*wr).apply(np.sqrt) * np.sqrt(tp)).iloc[-1])
    except:
        return 0.25  # fallback


def score_ticker(sym, earn_str, earn_days):
    try:
        stock   = yf.Ticker(sym)
        today_d = date.today()

        # Get price history - needed for RV and price
        h3 = stock.history(period="3mo")
        if h3.empty or len(h3) < 15:
            return None
        price = float(h3["Close"].iloc[-1])
        if price <= 0:
            return None

        # Volume check
        avg_vol_10 = float(h3["Volume"].tail(10).mean())
        if avg_vol_10 < 100_000:
            return None

        # Get options expiries
        opts = stock.options
        if not opts or len(opts) < 2:
            return None

        # Convert to dates and sort
        exp_dates_all = sorted(opts)
        today_str = today_d.strftime("%Y-%m-%d")

        # Get expiries from now out - need at least 2
        future_exps = [e for e in exp_dates_all if e > today_str]
        if len(future_exps) < 2:
            return None

        # Use first 4 expiries to build term structure
        selected = future_exps[:4]

        dtes, ivs, straddle = [], [], None

        for i, exp in enumerate(selected):
            try:
                ch = stock.option_chain(exp)
                c, p = ch.calls, ch.puts
                if c.empty or p.empty:
                    continue

                # ATM strike
                ci = (c["strike"] - price).abs().idxmin()
                pi = (p["strike"] - price).abs().idxmin()

                c_iv = float(c.loc[ci, "impliedVolatility"])
                p_iv = float(p.loc[pi, "impliedVolatility"])
                if c_iv <= 0 or p_iv <= 0:
                    continue

                atm_iv = (c_iv + p_iv) / 2.0
                dte = (datetime.strptime(exp, "%Y-%m-%d").date() - today_d).days

                if dte <= 0:
                    continue

                dtes.append(float(dte))
                ivs.append(atm_iv)

                if i == 0:
                    c_mid = (float(c.loc[ci,"bid"]) + float(c.loc[ci,"ask"])) / 2
                    p_mid = (float(p.loc[pi,"bid"]) + float(p.loc[pi,"ask"])) / 2
                    straddle = c_mid + p_mid

            except Exception as e:
                log.debug("%s exp %s: %s", sym, exp, e)
                continue

        if len(dtes) < 2:
            return None

        # Build term structure using available points
        dtes_arr = np.array(dtes)
        ivs_arr  = np.array(ivs)
        idx = dtes_arr.argsort()
        dtes_arr = dtes_arr[idx]
        ivs_arr  = ivs_arr[idx]

        def ts(x):
            if x <= dtes_arr[0]:  return float(ivs_arr[0])
            if x >= dtes_arr[-1]: return float(ivs_arr[-1])
            return float(np.interp(x, dtes_arr, ivs_arr))

        # Term structure slope: use first expiry vs last expiry we have
        dte_front = dtes_arr[0]
        dte_back  = min(dtes_arr[-1], 45.0)
        if dte_back <= dte_front:
            dte_back = dtes_arr[-1]

        slope = (ts(dte_back) - ts(dte_front)) / (dte_back - dte_front) if dte_back != dte_front else 0

        # IV30 / RV30
        rv30  = yang_zhang(h3)
        iv30  = ts(30.0) if dtes_arr[-1] >= 30 else ts(dtes_arr[-1])
        ivrv  = iv30 / rv30 if rv30 > 0 else 1.0

        # Average volume (30d)
        avgvol = float(h3["Volume"].rolling(30).mean().dropna().iloc[-1])

        # Score conditions (exact thresholds from calculator.py)
        c1 = slope  <= -0.00406
        c2 = avgvol >= 1_500_000
        c3 = ivrv   >= 1.25

        rec = ("RECOMMENDED" if c1 and c2 and c3 else
               "CONSIDER"    if c1 and (c2 or c3)  else
               "AVOID")

        s = price
        strike = (round(s*2)/2  if s < 20  else
                  round(s)      if s < 50  else
                  round(s/5)*5  if s < 200 else
                  round(s/10)*10 if s < 500 else
                  round(s/25)*25)

        debit = round(price * ts(dte_front) * (max(dte_front,1)/365)**0.5 * 0.4, 2)

        try:    name = getattr(stock.fast_info, "company_name", None) or sym
        except: name = sym

        try:
            ed = datetime.strptime(earn_str, "%b %d").replace(year=today_d.year)
            entry_str = (ed - timedelta(days=1)).strftime("%b %d")
        except:
            entry_str = earn_str

        front_exp = selected[0]
        back_exp  = selected[min(1, len(selected)-1)]

        return {
            "ticker":         sym,
            "name":           name,
            "sector":         "",
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
            "expectedMove":   round((straddle/price)*100, 2) if straddle and price > 0 else None,
            "frontIV":        round(ts(dte_front)*100, 1),
            "backIV":         round(ts(dte_back)*100, 1),
            "strike":         strike,
            "frontExp":       front_exp,
            "backExp":        back_exp,
            "debitEst":       debit,
            "entryDate":      entry_str,
            "exitDate":       earn_str,
        }
    except Exception as e:
        log.debug("%s failed: %s", sym, e)
        return None


def score_csp_ticker(sym, earnings_map):
    """Score a ticker for a cash-secured put setup (Layer 2)."""
    try:
        stock   = yf.Ticker(sym)
        today_d = date.today()

        # Price history — need 1yr for RV range
        h1y = stock.history(period="1y")
        if h1y.empty or len(h1y) < 60:
            return None
        price = float(h1y["Close"].iloc[-1])
        if price <= 0:
            return None

        # Volume check
        avg_vol = float(h1y["Volume"].tail(30).mean())
        if avg_vol < 500_000:
            return None

        # RV calculations
        rv30 = yang_zhang(h1y, window=30)

        # 52-week RV range (rolling) to estimate IV rank proxy
        rv_series = h1y["Close"].pct_change().rolling(30).std() * (252**0.5)
        rv_series = rv_series.dropna()
        rv_min = float(rv_series.quantile(0.10))
        rv_max = float(rv_series.quantile(0.90))

        # Get options
        opts = stock.options
        if not opts or len(opts) < 1:
            return None

        # Find expiry 30-50 DTE
        target_exp = None
        target_dte = None
        for exp in sorted(opts):
            dte = (datetime.strptime(exp, "%Y-%m-%d").date() - today_d).days
            if 28 <= dte <= 55:
                target_exp = exp
                target_dte = dte
                break

        if not target_exp:
            # Fall back to closest beyond 28 days
            for exp in sorted(opts):
                dte = (datetime.strptime(exp, "%Y-%m-%d").date() - today_d).days
                if dte >= 28:
                    target_exp = exp
                    target_dte = dte
                    break

        if not target_exp or target_dte is None:
            return None

        # Get put chain
        ch = stock.option_chain(target_exp)
        puts = ch.puts
        if puts.empty:
            return None

        # ATM IV from puts
        pi = (puts["strike"] - price).abs().idxmin()
        atm_iv = float(puts.loc[pi, "impliedVolatility"])
        if atm_iv <= 0:
            return None

        # IV rank proxy: where does current IV sit vs RV range
        if rv_max > rv_min:
            iv_rank = int(min(100, max(0, (atm_iv - rv_min) / (rv_max - rv_min) * 100)))
        else:
            iv_rank = 50

        iv_rv_ratio = atm_iv / rv30 if rv30 > 0 else 1.0

        # Find ~30-delta put strike
        # 30-delta put ≈ strike where delta ~ -0.30
        # Approximation: price * exp(-0.5 * sigma * sqrt(T)) for ~30 delta
        sigma = atm_iv
        T     = target_dte / 365.0
        target_strike_raw = price * (1 - 0.45 * sigma * (T**0.5))

        # Find closest actual strike below that target
        otm_puts = puts[puts["strike"] <= price * 1.01].copy()
        if otm_puts.empty:
            return None
        otm_puts["dist"] = (otm_puts["strike"] - target_strike_raw).abs()
        best_row = otm_puts.loc[otm_puts["dist"].idxmin()]
        put_strike = float(best_row["strike"])

        bid  = float(best_row.get("bid",  0) or 0)
        ask  = float(best_row.get("ask",  0) or 0)
        oi   = int(best_row.get("openInterest", 0) or 0)
        iv_p = float(best_row.get("impliedVolatility", atm_iv) or atm_iv)

        if bid <= 0 or ask <= 0:
            # Estimate premium via BS approximation
            mid = round(price * iv_p * (T**0.5) * 0.40 * (put_strike/price)**0.5, 2)
        else:
            mid = round((bid + ask) / 2, 2)

        if mid <= 0.05:
            return None

        spread_pct = round((ask - bid) / mid * 100, 1) if mid > 0 and ask > bid else 0

        # Breakeven
        breakeven = round(put_strike - mid, 2)

        # Return on collateral
        collateral = put_strike * 100  # 1 contract
        roc_pct    = round(mid * 100 / collateral * 100, 2) if collateral > 0 else 0
        roc_ann    = round(roc_pct * (365 / target_dte), 1) if target_dte > 0 else 0

        # Earnings check — skip if earnings within expiry window
        earn_within = sym in earnings_map and earnings_map[sym][1] <= target_dte
        earn_str    = earnings_map[sym][0] if sym in earnings_map else None
        earn_days   = earnings_map[sym][1] if sym in earnings_map else None

        # Quality rating
        if iv_rank < 45 or earn_within or spread_pct > 15:
            quality = "SKIP"
        elif iv_rank >= 60 and iv_rv_ratio >= 1.2 and not earn_within and spread_pct <= 8:
            quality = "STRONG"
        else:
            quality = "DECENT"

        try:    name = getattr(stock.fast_info, "company_name", None) or sym
        except: name = sym

        return {
            "ticker":         sym,
            "name":           name,
            "price":          round(price, 2),
            "quality":        quality,
            "ivRank":         iv_rank,
            "ivRvRatio":      round(iv_rv_ratio, 2),
            "putStrike":      put_strike,
            "expiration":     target_exp,
            "dte":            target_dte,
            "premium":        mid,
            "bid":            round(bid, 2),
            "ask":            round(ask, 2),
            "spreadPct":      spread_pct,
            "openInterest":   oi,
            "breakeven":      breakeven,
            "rocPct":         roc_pct,
            "rocAnn":         roc_ann,
            "collateral":     int(collateral),
            "avgVol":         int(avg_vol),
            "atm_iv":         round(atm_iv * 100, 1),
            "rv30":           round(rv30 * 100, 1),
            "earningsWithin": earn_within,
            "earningsDate":   earn_str,
            "earningsDays":   earn_days,
            "profitTarget":   round(mid * 0.50 * 100, 2),
            "stopLoss":       round(mid * 2.0 * 100, 2),
        }
    except Exception as e:
        log.debug("CSP %s failed: %s", sym, e)
        return None


def run_csp_scan():
    """Layer 2: scan CSP_UNIVERSE for cash-secured put setups."""
    if _csp_cache["running"]:
        return
    _csp_cache["running"] = True
    _csp_cache["progress"] = {"done": 0, "total": len(CSP_UNIVERSE), "phase": "scoring"}
    log.info("CSP scan started (%d tickers)", len(CSP_UNIVERSE))

    # Reuse earnings map from L1 cache if fresh, else fetch fresh
    try:
        earnings_map = fetch_earnings_calendar()
    except Exception:
        earnings_map = {}

    results = []
    lock = threading.Lock()

    def score_one(sym):
        r = score_csp_ticker(sym, earnings_map)
        with lock:
            _csp_cache["progress"]["done"] += 1
            if r:
                results.append(r)
                _csp_cache["results"] = sorted(
                    results,
                    key=lambda x: ({"STRONG":0,"DECENT":1,"SKIP":2}.get(x["quality"],3),
                                   -x["rocAnn"])
                )

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futs = {pool.submit(score_one, sym): sym for sym in CSP_UNIVERSE}
        for f in concurrent.futures.as_completed(futs):
            try: f.result()
            except: pass

    _csp_cache["results"] = sorted(
        results,
        key=lambda x: ({"STRONG":0,"DECENT":1,"SKIP":2}.get(x["quality"],3), -x["rocAnn"])
    )
    _csp_cache["ts"] = time.time()
    _csp_cache["running"] = False
    _csp_cache["progress"] = {"done": len(CSP_UNIVERSE), "total": len(CSP_UNIVERSE), "phase": "done"}
    log.info("CSP scan done. %d results.", len(results))


def run_scan():
    if _cache["running"]:
        return
    _cache["running"] = True
    _cache["progress"] = {"done": 0, "total": 0, "phase": "calendar"}
    log.info("Scan started")

    # Step 1: Earnings calendar
    try:
        earnings_map = fetch_earnings_calendar()
    except Exception as e:
        log.error("Calendar failed: %s", e)
        earnings_map = {}

    if not earnings_map:
        log.error("No earnings found")
        _cache["running"] = False
        _cache["progress"] = {"done":0,"total":0,"phase":"done"}
        _cache["ts"] = time.time()
        return

    # Step 2: Prioritize liquid tickers
    ordered = [s for s in TOP_LIQUID if s in earnings_map]
    ordered += [s for s in earnings_map if s not in ordered]
    tickers = ordered[:MAX_TICKERS]
    _cache["universe"] = tickers
    _cache["progress"] = {"done": 0, "total": len(tickers), "phase": "scoring"}
    log.info("Scoring %d tickers...", len(tickers))

    results = []
    lock = threading.Lock()

    def score_one(sym):
        earn_str, earn_days = earnings_map[sym]
        r = score_ticker(sym, earn_str, earn_days)
        with lock:
            _cache["progress"]["done"] += 1
            if r:
                results.append(r)
                # Update partial results immediately
                _cache["results"] = sorted(
                    results,
                    key=lambda x: ({"RECOMMENDED":0,"CONSIDER":1,"AVOID":2}.get(x["rec"],3),
                                   x["daysToEarnings"])
                )

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futs = {pool.submit(score_one, sym): sym for sym in tickers}
        for f in concurrent.futures.as_completed(futs):
            try: f.result()
            except: pass

    _cache["results"] = sorted(
        results,
        key=lambda x: ({"RECOMMENDED":0,"CONSIDER":1,"AVOID":2}.get(x["rec"],3),
                        x["daysToEarnings"])
    )
    _cache["ts"] = time.time()
    _cache["running"] = False
    _cache["progress"] = {"done": len(tickers), "total": len(tickers), "phase": "done"}
    log.info("Scan done. %d results from %d tickers.", len(results), len(tickers))


def maybe_start_scan():
    if not _cache["running"] and (not _cache["ts"] or time.time()-_cache["ts"] > CACHE_TTL):
        threading.Thread(target=run_scan, daemon=True).start()


@app.route("/api/scan")
def api_scan():
    maybe_start_scan()
    return jsonify({
        "results":      _cache["results"],
        "count":        len(_cache["results"]),
        "scannedAt":    datetime.fromtimestamp(_cache["ts"]).strftime("%b %d %Y, %I:%M %p") if _cache["ts"] else None,
        "ageMinutes":   int((time.time()-_cache["ts"])/60) if _cache["ts"] else 0,
        "isRefreshing": _cache["running"],
        "universe":     len(_cache["universe"]),
        "progress":     _cache["progress"],
    })


@app.route("/api/progress")
def api_progress():
    p = _cache["progress"]
    pct = int(p["done"]/p["total"]*100) if p["total"] > 0 else 0
    return jsonify({"done":p["done"],"total":p["total"],"pct":pct,
                    "phase":p["phase"],"running":_cache["running"]})


@app.route("/api/status")
def api_status():
    return jsonify({"status":"ok","cached":bool(_cache["ts"]),
                    "count":len(_cache["results"]),"running":_cache["running"],
                    "universe":len(_cache["universe"]),"progress":_cache["progress"]})


@app.route("/api/refresh", methods=["POST"])
def api_refresh():
    if _cache["running"]:
        return jsonify({"message":"Scan in progress"}), 202
    threading.Thread(target=run_scan, daemon=True).start()
    return jsonify({"message":"Refresh started"}), 202


# ── Layer 2: Cash-Secured Put endpoints ────────────────────────────────────

@app.route("/api/csp/scan")
def api_csp_scan():
    if not _csp_cache["ts"] or time.time() - _csp_cache["ts"] > CACHE_TTL:
        if not _csp_cache["running"]:
            threading.Thread(target=run_csp_scan, daemon=True).start()
    return jsonify({
        "results":      _csp_cache["results"],
        "count":        len(_csp_cache["results"]),
        "scannedAt":    datetime.fromtimestamp(_csp_cache["ts"]).strftime("%b %d %Y, %I:%M %p") if _csp_cache["ts"] else None,
        "ageMinutes":   int((time.time() - _csp_cache["ts"]) / 60) if _csp_cache["ts"] else 0,
        "isRefreshing": _csp_cache["running"],
        "universe":     len(CSP_UNIVERSE),
        "progress":     _csp_cache["progress"],
    })


@app.route("/api/csp/progress")
def api_csp_progress():
    p = _csp_cache["progress"]
    pct = int(p["done"] / p["total"] * 100) if p["total"] > 0 else 0
    return jsonify({"done": p["done"], "total": p["total"], "pct": pct,
                    "phase": p["phase"], "running": _csp_cache["running"]})


@app.route("/api/csp/refresh", methods=["POST"])
def api_csp_refresh():
    if _csp_cache["running"]:
        return jsonify({"message": "CSP scan in progress"}), 202
    threading.Thread(target=run_csp_scan, daemon=True).start()
    return jsonify({"message": "CSP refresh started"}), 202


@app.route("/api/debug")
def api_debug():
    out = {
        "universe":  len(_cache["universe"]),
        "results":   len(_cache["results"]),
        "running":   _cache["running"],
        "progress":  _cache["progress"],
        "debug":     _cache.get("debug", {}),
    }
    try:
        resp = requests.get(
            "https://api.nasdaq.com/api/calendar/earnings?date=" + date.today().strftime("%Y-%m-%d"),
            headers=NASDAQ_HEADERS, timeout=5)
        rows = (resp.json().get("data") or {}).get("rows") or []
        out["nasdaq_today"] = {"status": resp.status_code, "rows": len(rows), "sample": rows[:3]}
    except Exception as e:
        out["nasdaq_today"] = {"error": str(e)}
    # Test yfinance on one ticker
    try:
        t = yf.Ticker("AAPL")
        opts = t.options
        out["yfinance_test"] = {"aapl_options": list(opts[:3]) if opts else "none"}
    except Exception as e:
        out["yfinance_test"] = {"error": str(e)}
    return jsonify(out)


@app.route("/")
def index():
    p = _cache["progress"]
    return (f"VV Scanner | {len(_cache['universe'])} tickers | "
            f"{len(_cache['results'])} results | "
            f"Running:{_cache['running']} ({p['done']}/{p['total']}) | "
            f"/api/debug")


if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
