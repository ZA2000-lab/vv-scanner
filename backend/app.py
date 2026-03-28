# VV Scanner Backend - Full Market Edition
# Earnings IV Crush Scanner - Calendar Spread Strategy
# Thresholds: ts_slope_0_45 <= -0.00406 | iv30_rv30 >= 1.25 | avg_volume >= 1500000

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
log = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

CACHE_TTL = 6 * 3600
EARNINGS_WINDOW = 60
MAX_WORKERS = 8

_cache = {
    "results":  None,
    "universe": [],
    "ts":       0,
    "running":  False,
    "progress": {"done": 0, "total": 0, "phase": "idle"},
}


def fetch_sp500():
    try:
        df = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")[0]
        tickers = [str(t).replace(".", "-") for t in df["Symbol"].tolist()]
        log.info("  S&P 500: %d tickers", len(tickers))
        return tickers
    except Exception as e:
        log.warning("  S&P 500 fetch failed: %s", e)
        return []


def fetch_nasdaq100():
    try:
        tables = pd.read_html("https://en.wikipedia.org/wiki/Nasdaq-100")
        for df in tables:
            cols_lower = [str(c).lower() for c in df.columns]
            if "ticker" in cols_lower or "symbol" in cols_lower:
                col = df.columns[[str(c).lower() in ("ticker","symbol") for c in df.columns]][0]
                tickers = [str(t).replace(".", "-") for t in df[col].dropna().tolist()
                           if isinstance(t, str) and 1 <= len(str(t)) <= 6]
                if len(tickers) > 50:
                    log.info("  Nasdaq 100: %d tickers", len(tickers))
                    return tickers
        return []
    except Exception as e:
        log.warning("  Nasdaq 100 fetch failed: %s", e)
        return []


SUPPLEMENTAL = [
    "AAPL","MSFT","NVDA","AMZN","META","GOOGL","GOOG","TSLA","NFLX","ORCL",
    "AMD","INTC","QCOM","TXN","AVGO","MU","AMAT","LRCX","KLAC","MRVL",
    "ARM","ASML","TSM","SMCI","ON","MPWR","ENPH","FSLR",
    "CRM","NOW","PANW","CRWD","NET","DDOG","SNOW","MDB","OKTA","ZS",
    "FTNT","HUBS","WDAY","TEAM","SHOP","MELI","TTD","RBLX","PATH",
    "ZM","DOCU","TWLO","BILL","PLTR","BRZE","CFLT","SMAR",
    "JPM","BAC","WFC","GS","MS","C","V","MA","AXP","BLK","SCHW","COF",
    "DFS","SYF","AIG","MET","PRU","ALL","TRV","BRK-B","USB","PNC","TFC",
    "COIN","HOOD","SOFI","PYPL","SQ","AFRM","UPST","NU",
    "UNH","JNJ","LLY","ABBV","MRK","PFE","AMGN","GILD","VRTX","REGN",
    "BMY","BIIB","DXCM","ISRG","EW","HUM","CVS","CI","CNC","HCA",
    "MRNA","NVAX","ALNY","NBIB","VKTX","KYMR",
    "HD","WMT","COST","MCD","SBUX","NKE","TGT","LOW","BKNG","ABNB",
    "UBER","LYFT","DASH","ETSY","CPNG","PTON","LULU",
    "RIVN","F","GM","LCID","PG","KO","PEP","PM","MO","CL","KMB",
    "XOM","CVX","COP","EOG","SLB","OXY","FANG","DVN","MRO",
    "BA","CAT","DE","GE","HON","RTX","LMT","NOC","UPS","FDX","ETN",
    "DIS","CMCSA","T","VZ","TMUS","SNAP","PINS","SPOT","WBD","PARA",
    "BABA","BIDU","JD","PDD","NIO","XPEV","LI","BILI",
    "AMT","PLD","EQIX","NEE","DUK","LIN","FCX","NEM",
    "MARA","RIOT","MSTR","HUT","IONQ","QUBT","RGTI","SOUN","BBAI",
    "GME","AMC","DKNG","WYNN","MGM","CCL","AAL","DAL","UAL",
    "SPY","QQQ","IWM","SMH","XLE","XLF","XLK","XLV","ARKK","GLD","TLT",
]


def build_universe():
    log.info("Building universe...")
    tickers = set()
    tickers.update(fetch_sp500())
    tickers.update(fetch_nasdaq100())
    tickers.update(SUPPLEMENTAL)
    cleaned = sorted({
        str(t).strip().upper()
        for t in tickers
        if str(t).strip().upper().replace("-","").isalpha()
        and 1 <= len(str(t).strip()) <= 6
    })
    log.info("Universe: %d unique tickers", len(cleaned))
    return cleaned


def get_earnings_date(sym):
    today = datetime.today().date()

    def check(val):
        try:
            if isinstance(val, (int, float)):
                dt = datetime.utcfromtimestamp(val).date()
            else:
                dt = pd.Timestamp(val).date()
            diff = (dt - today).days
            if 0 <= diff <= EARNINGS_WINDOW:
                return dt.strftime("%b %d"), diff
        except:
            pass
        return None, None

    stock = yf.Ticker(sym)

    # Method 1: get_earnings_dates
    try:
        ed = stock.get_earnings_dates(limit=12)
        if ed is not None and not ed.empty:
            for idx in sorted(ed.index):
                r, d = check(idx)
                if r: return r, d
    except:
        pass

    # Method 2: earnings_dates property
    try:
        ed = stock.earnings_dates
        if ed is not None and not ed.empty:
            for idx in sorted(ed.index):
                r, d = check(idx)
                if r: return r, d
    except:
        pass

    # Method 3: calendar
    try:
        cal = stock.calendar
        if cal is not None:
            dates = []
            if isinstance(cal, dict):
                for key in ("Earnings Date", "earningsDate"):
                    raw = cal.get(key)
                    if raw is not None:
                        dates = list(raw) if hasattr(raw, "__iter__") and not isinstance(raw, str) else [raw]
                        break
            elif isinstance(cal, pd.DataFrame):
                for key in ("Earnings Date", "earningsDate"):
                    if key in cal.columns:
                        dates = cal[key].dropna().tolist(); break
                    if key in cal.index:
                        dates = [cal.loc[key]]; break
            for d in dates:
                r, days = check(d)
                if r: return r, days
    except:
        pass

    # Method 4: info dict
    try:
        info = stock.info
        for key in ("earningsDate", "earningsTimestamp"):
            val = info.get(key)
            if val:
                items = val if isinstance(val, list) else [val]
                for item in items:
                    r, d = check(item)
                    if r: return r, d
    except:
        pass

    return None, None


def filter_dates(dates):
    today = datetime.today().date()
    cutoff = today + timedelta(days=45)
    sorted_dates = sorted(datetime.strptime(d, "%Y-%m-%d").date() for d in dates)
    arr = []
    for i, dt in enumerate(sorted_dates):
        if dt >= cutoff:
            arr = [d.strftime("%Y-%m-%d") for d in sorted_dates[:i+1]]
            break
    if arr:
        return arr[1:] if arr[0] == today.strftime("%Y-%m-%d") else arr
    raise ValueError("No expiry within 45 days.")


def yang_zhang(price_data, window=30, trading_periods=252):
    log_ho = (price_data['High']  / price_data['Open']).apply(np.log)
    log_lo = (price_data['Low']   / price_data['Open']).apply(np.log)
    log_co = (price_data['Close'] / price_data['Open']).apply(np.log)
    log_oc    = (price_data['Open'] / price_data['Close'].shift(1)).apply(np.log)
    log_oc_sq = log_oc ** 2
    log_cc    = (price_data['Close'] / price_data['Close'].shift(1)).apply(np.log)
    log_cc_sq = log_cc ** 2
    rs        = log_ho * (log_ho - log_co) + log_lo * (log_lo - log_co)
    close_vol = log_cc_sq.rolling(window, center=False).sum() / (window - 1)
    open_vol  = log_oc_sq.rolling(window, center=False).sum() / (window - 1)
    window_rs = rs.rolling(window, center=False).sum() / (window - 1)
    k = 0.34 / (1.34 + (window + 1) / (window - 1))
    return ((open_vol + k * close_vol + (1 - k) * window_rs).apply(np.sqrt) * np.sqrt(trading_periods)).iloc[-1]


def build_ts(days, ivs):
    days = np.array(days, dtype=float)
    ivs  = np.array(ivs,  dtype=float)
    idx  = days.argsort()
    days = days[idx]; ivs = ivs[idx]
    def ts(dte):
        if   dte <= days[0]:  return float(ivs[0])
        elif dte >= days[-1]: return float(ivs[-1])
        else:                 return float(np.interp(dte, days, ivs))
    return ts


def score_ticker(sym):
    try:
        earn_str, earn_days = get_earnings_date(sym)
        if earn_str is None:
            return None

        stock = yf.Ticker(sym)
        h1 = stock.history(period='1d')
        if h1.empty: return None
        price = float(h1['Close'].iloc[-1])
        if price <= 0: return None

        if not stock.options or len(stock.options) < 2: return None
        try:
            exp_dates = filter_dates(list(stock.options))
        except:
            return None
        if len(exp_dates) < 2: return None

        today = datetime.today().date()
        dtes, ivs, straddle = [], [], None
        for i, exp in enumerate(exp_dates):
            try:
                ch = stock.option_chain(exp)
                c, p = ch.calls, ch.puts
                if c.empty or p.empty: continue
                ci = (c['strike'] - price).abs().idxmin()
                pi = (p['strike'] - price).abs().idxmin()
                atm_iv = (c.loc[ci,'impliedVolatility'] + p.loc[pi,'impliedVolatility']) / 2
                dte = (datetime.strptime(exp, '%Y-%m-%d').date() - today).days
                dtes.append(dte); ivs.append(atm_iv)
                if i == 0:
                    straddle = ((c.loc[ci,'bid'] + c.loc[ci,'ask']) / 2 +
                                (p.loc[pi,'bid'] + p.loc[pi,'ask']) / 2)
            except:
                continue

        if len(dtes) < 2: return None
        ts = build_ts(dtes, ivs)
        slope  = (ts(45) - ts(dtes[0])) / (45 - dtes[0])
        h3     = stock.history(period='3mo')
        if h3.empty or len(h3) < 30: return None
        ivrv   = ts(30) / yang_zhang(h3)
        avgvol = h3['Volume'].rolling(30).mean().dropna().iloc[-1]

        c1 = slope  <= -0.00406
        c2 = avgvol >= 1_500_000
        c3 = ivrv   >= 1.25
        rec = ("RECOMMENDED" if c1 and c2 and c3 else
               "CONSIDER"    if c1 and (c2 or c3) else
               "AVOID")

        s = price
        strike = (round(s*2)/2 if s<20 else round(s) if s<50 else
                  round(s/5)*5 if s<200 else round(s/10)*10 if s<500 else round(s/25)*25)
        debit = round(price * ts(dtes[0]) * (max(dtes[0],1)/365)**0.5 * 0.4, 2)

        try:
            fi   = stock.fast_info
            name = getattr(fi, 'company_name', None) or sym
        except:
            name = sym

        try:
            ed  = datetime.strptime(earn_str, '%b %d').replace(year=datetime.today().year)
            entry_str = (ed - timedelta(days=1)).strftime('%b %d')
        except:
            entry_str = earn_str

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
            "expectedMove":   round((straddle/price)*100, 2) if straddle else None,
            "frontIV":        round(ts(dtes[0])*100, 1),
            "backIV":         round(ts(45)*100, 1),
            "strike":         strike,
            "frontExp":       exp_dates[0],
            "backExp":        exp_dates[min(1, len(exp_dates)-1)],
            "debitEst":       debit,
            "entryDate":      entry_str,
            "exitDate":       earn_str,
        }
    except Exception as e:
        log.debug("%s: %s", sym, e)
        return None


def run_scan():
    if _cache['running']: return
    _cache['running'] = True
    universe = build_universe()
    _cache['universe'] = universe
    _cache['progress'] = {'done': 0, 'total': len(universe), 'phase': 'scanning'}
    log.info("Scanning %d tickers...", len(universe))

    results = []
    lock = threading.Lock()

    def scan_one(sym):
        r = score_ticker(sym)
        with lock:
            _cache['progress']['done'] += 1
            if r: results.append(r)

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futs = {pool.submit(scan_one, sym): sym for sym in universe}
        for f in concurrent.futures.as_completed(futs):
            try: f.result()
            except: pass

    order = {'RECOMMENDED': 0, 'CONSIDER': 1, 'AVOID': 2}
    results.sort(key=lambda x: (order.get(x['rec'], 3), x['daysToEarnings']))
    _cache['results']  = results
    _cache['ts']       = time.time()
    _cache['running']  = False
    _cache['progress'] = {'done': len(universe), 'total': len(universe), 'phase': 'done'}
    log.info("Done. %d setups from %d tickers.", len(results), len(universe))


def get_or_refresh():
    if _cache['results'] is None:
        run_scan()
    elif time.time() - _cache['ts'] > CACHE_TTL and not _cache['running']:
        threading.Thread(target=run_scan, daemon=True).start()
    return _cache['results']


@app.route('/api/scan')
def api_scan():
    data = get_or_refresh()
    return jsonify({
        "results":      data or [],
        "count":        len(data) if data else 0,
        "scannedAt":    datetime.fromtimestamp(_cache["ts"]).strftime("%b %d %Y, %I:%M %p") if _cache["ts"] else None,
        "ageMinutes":   int((time.time()-_cache["ts"])/60) if _cache["ts"] else 0,
        "isRefreshing": _cache["running"],
        "universe":     len(_cache["universe"]),
        "progress":     _cache["progress"],
    })


@app.route('/api/progress')
def api_progress():
    p = _cache['progress']
    pct = int(p['done']/p['total']*100) if p['total'] > 0 else 0
    return jsonify({'done':p['done'],'total':p['total'],'pct':pct,'phase':p['phase'],'running':_cache['running']})


@app.route('/api/status')
def api_status():
    return jsonify({'status':'ok','cached':_cache['results'] is not None,
                    'count':len(_cache['results']) if _cache['results'] else 0,
                    'ageMinutes':int((time.time()-_cache['ts'])/60) if _cache['ts'] else None,
                    'running':_cache['running'],'universe':len(_cache['universe']),
                    'progress':_cache['progress']})


@app.route('/api/refresh', methods=['POST'])
def api_refresh():
    if _cache['running']:
        return jsonify({'message':'Scan already in progress','progress':_cache['progress']}), 202
    threading.Thread(target=run_scan, daemon=True).start()
    return jsonify({'message':'Refresh started'}), 202


@app.route('/api/debug/<sym>')
def api_debug(sym):
    sym = sym.upper()
    stock = yf.Ticker(sym)
    out = {'ticker': sym, 'methods': {}}
    try:
        ed = stock.get_earnings_dates(limit=6)
        out['methods']['get_earnings_dates'] = str(ed.index.tolist()[:6]) if ed is not None and not ed.empty else 'empty'
    except Exception as e:
        out['methods']['get_earnings_dates'] = 'error: ' + str(e)
    try:
        cal = stock.calendar
        out['methods']['calendar'] = str(cal)[:300] if cal is not None else 'None'
    except Exception as e:
        out['methods']['calendar'] = 'error: ' + str(e)
    try:
        info = stock.info
        out['methods']['info_earningsDate'] = str(info.get('earningsDate'))
        out['methods']['info_earningsTimestamp'] = str(info.get('earningsTimestamp'))
    except Exception as e:
        out['methods']['info'] = 'error: ' + str(e)
    earn_str, earn_days = get_earnings_date(sym)
    out['result'] = {'earn_str': earn_str, 'earn_days': earn_days}
    return jsonify(out)


@app.route('/')
def index():
    p = _cache['progress']
    return (f"VV Scanner API | Universe: {len(_cache['universe'])} | "
            f"Results: {len(_cache['results']) if _cache['results'] else 0} | "
            f"Running: {_cache['running']} ({p['done']}/{p['total']}) | "
            f"Debug: /api/debug/AAPL")


if __name__ == '__main__':
    threading.Thread(target=run_scan, daemon=True).start()
    app.run(host='0.0.0.0', port=5000, debug=False)
