# VV Scanner Backend

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

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

CACHE_TTL   = 6 * 3600
MAX_WORKERS = 2
DAYS_AHEAD  = 35

_cache = {
    "results":  None,
    "universe": [],
    "ts":       0,
    "running":  False,
    "progress": {"done": 0, "total": 0, "phase": "idle"},
    "debug":    {},
}

NASDAQ_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://www.nasdaq.com",
    "Referer": "https://www.nasdaq.com/market-activity/earnings",
}

LIQUID_WHITELIST = set([
    "AAPL","MSFT","NVDA","AMZN","META","GOOGL","GOOG","TSLA","NFLX","ORCL",
    "AMD","INTC","QCOM","TXN","AVGO","MU","AMAT","LRCX","KLAC","MRVL",
    "ARM","ASML","TSM","SMCI","ON","MPWR","ENPH","FSLR",
    "CRM","NOW","PANW","CRWD","NET","DDOG","SNOW","MDB","OKTA","ZS",
    "FTNT","HUBS","WDAY","TEAM","SHOP","MELI","TTD","RBLX","PLTR",
    "ZM","DOCU","TWLO","BILL","BRZE","CFLT","SMAR","GTLB","COIN",
    "JPM","BAC","WFC","GS","MS","C","V","MA","AXP","BLK","SCHW","COF",
    "DFS","SYF","AIG","MET","PRU","ALL","TRV","BRK-B","USB","PNC","TFC",
    "RF","FITB","HBAN","KEY","CFG","HOOD","SOFI","PYPL","SQ","AFRM","UPST","NU",
    "UNH","JNJ","LLY","ABBV","MRK","PFE","AMGN","GILD","VRTX","REGN",
    "BMY","BIIB","DXCM","ISRG","EW","ZBH","HUM","CVS","CI","CNC","HCA",
    "MRNA","NVAX","ALNY","VKTX","KYMR","BMRN","RARE","EXAS",
    "HD","WMT","COST","MCD","SBUX","NKE","TGT","LOW","BKNG","ABNB",
    "UBER","LYFT","DASH","ETSY","CPNG","LULU","RIVN","F","GM",
    "PG","KO","PEP","PM","MO","CL","KMB","HSY","MNST","CELH",
    "XOM","CVX","COP","EOG","SLB","OXY","FANG","DVN","MRO","HES",
    "BA","CAT","DE","GE","HON","RTX","LMT","NOC","UPS","FDX","ETN",
    "DIS","CMCSA","T","VZ","TMUS","SNAP","PINS","SPOT","WBD","PARA",
    "BABA","BIDU","JD","PDD","NIO","XPEV","LI","BILI","TME",
    "AMT","PLD","EQIX","NEE","DUK","LIN","FCX","NEM",
    "MARA","RIOT","MSTR","IONQ","SOUN","BBAI","GME","AMC",
    "DKNG","WYNN","MGM","CCL","RCL","AAL","DAL","UAL","LUV",
    "SPY","QQQ","IWM","SMH","XLE","XLF","XLK","XLV","ARKK","GLD","TLT",
])


def fetch_earnings_calendar():
    """Fetch all upcoming earnings in one batch using the Nasdaq API."""
    earnings_map = {}
    today = date.today()
    end   = today + timedelta(days=DAYS_AHEAD)

    # Build list of weekdays only
    weekdays = []
    d = today
    while d <= end:
        if d.weekday() < 5:
            weekdays.append(d)
        d += timedelta(days=1)

    log.info('Fetching Nasdaq calendar for %d weekdays...', len(weekdays))

    # Use a session for connection reuse
    session = requests.Session()
    session.headers.update(NASDAQ_HEADERS)

    for target in weekdays:
        date_str = target.strftime('%Y-%m-%d')
        try:
            resp = session.get(
                'https://api.nasdaq.com/api/calendar/earnings?date=' + date_str,
                timeout=6
            )
            if resp.status_code != 200:
                time.sleep(0.1)
                continue
            rows = (resp.json().get('data') or {}).get('rows') or []
            diff = (target - today).days
            for row in rows:
                sym = (row.get('symbol') or '').upper().strip().replace('/', '-')
                if sym and 1 <= len(sym) <= 6 and sym.replace('-','').isalpha():
                    if sym not in earnings_map:
                        earnings_map[sym] = (target.strftime('%b %d'), diff)
            time.sleep(0.1)
        except Exception as e:
            log.warning('Calendar %s: %s', date_str, e)
            continue

    log.info('Calendar: %d tickers with earnings in next %d days', len(earnings_map), DAYS_AHEAD)
    _cache['debug']['calendar_count'] = len(earnings_map)
    _cache['debug']['calendar_sample'] = list(earnings_map.items())[:5]
    return earnings_map


def filter_dates(dates):
    today = date.today()
    cutoff = today + timedelta(days=45)
    sorted_dates = sorted(datetime.strptime(d, '%Y-%m-%d').date() for d in dates)
    arr = []
    for i, dt in enumerate(sorted_dates):
        if dt >= cutoff:
            arr = [d.strftime('%Y-%m-%d') for d in sorted_dates[:i+1]]
            break
    if arr:
        return arr[1:] if arr[0] == today.strftime('%Y-%m-%d') else arr
    raise ValueError('No expiry within 45 days.')


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


def score_ticker(sym, earn_str, earn_days):
    try:
        stock   = yf.Ticker(sym)
        today_d = date.today()

        h3 = stock.history(period='3mo')
        if h3.empty or len(h3) < 30: return None
        price = float(h3['Close'].iloc[-1])
        if price <= 0: return None

        # Skip very low volume stocks
        if float(h3['Volume'].tail(10).mean()) < 200_000: return None

        if not stock.options or len(stock.options) < 2: return None
        try:
            exp_dates = filter_dates(list(stock.options))
        except:
            return None
        if len(exp_dates) < 2: return None

        dtes, ivs, straddle = [], [], None
        for i, exp in enumerate(exp_dates):
            try:
                ch = stock.option_chain(exp)
                c, p = ch.calls, ch.puts
                if c.empty or p.empty: continue
                ci = (c['strike'] - price).abs().idxmin()
                pi = (p['strike'] - price).abs().idxmin()
                atm_iv = (c.loc[ci,'impliedVolatility'] + p.loc[pi,'impliedVolatility']) / 2
                dte = (datetime.strptime(exp, '%Y-%m-%d').date() - today_d).days
                dtes.append(dte); ivs.append(atm_iv)
                if i == 0:
                    straddle = ((c.loc[ci,'bid'] + c.loc[ci,'ask']) / 2 +
                                (p.loc[pi,'bid'] + p.loc[pi,'ask']) / 2)
            except:
                continue

        if len(dtes) < 2: return None
        ts     = build_ts(dtes, ivs)
        slope  = (ts(45) - ts(dtes[0])) / (45 - dtes[0])
        ivrv   = ts(30) / yang_zhang(h3)
        avgvol = h3['Volume'].rolling(30).mean().dropna().iloc[-1]

        c1 = slope  <= -0.00406
        c2 = avgvol >= 1_500_000
        c3 = ivrv   >= 1.25
        rec = ('RECOMMENDED' if c1 and c2 and c3 else
               'CONSIDER'    if c1 and (c2 or c3) else
               'AVOID')

        s = price
        strike = (round(s*2)/2 if s<20 else round(s) if s<50 else
                  round(s/5)*5 if s<200 else round(s/10)*10 if s<500 else round(s/25)*25)
        debit = round(price * ts(dtes[0]) * (max(dtes[0],1)/365)**0.5 * 0.4, 2)

        try:
            name = getattr(stock.fast_info, 'company_name', None) or sym
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
        log.debug('%s: %s', sym, e)
        return None


def run_scan():
    if _cache['running']: return
    _cache['running'] = True
    _cache["progress"] = {"done": 0, "total": 0, "phase": "calendar"}

    earnings_map = fetch_earnings_calendar()

    if not earnings_map:
        log.error('No earnings found from Nasdaq calendar')
        _cache['running'] = False
        _cache["results"] = []
        _cache['ts'] = time.time()
        _cache["progress"] = {"done": 0, "total": 0, "phase": "done"}
        return

    # Whitelist-first ordering, cap at 200
    whitelist = {k:v for k,v in earnings_map.items() if k in LIQUID_WHITELIST}
    others    = {k:v for k,v in earnings_map.items() if k not in LIQUID_WHITELIST}
    capped    = dict(list(whitelist.items()) + list(others.items())[:max(0, 200-len(whitelist))])

    tickers = list(capped.keys())
    _cache['universe'] = tickers
    _cache["progress"] = {"done": 0, "total": len(tickers), "phase": "scoring"}
    log.info('Scoring %d tickers...', len(tickers))

    results = []
    lock = threading.Lock()

    def score_one(sym):
        earn_str, earn_days = capped[sym]
        r = score_ticker(sym, earn_str, earn_days)
        with lock:
            _cache["progress"]["done"] += 1
            if r: results.append(r)

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futs = {pool.submit(score_one, sym): sym for sym in tickers}
        for f in concurrent.futures.as_completed(futs):
            try: f.result()
            except: pass

    order = {'RECOMMENDED': 0, 'CONSIDER': 1, 'AVOID': 2}
    results.sort(key=lambda x: (order.get(x['rec'], 3), x['daysToEarnings']))
    _cache["results"]  = results
    _cache['ts']       = time.time()
    _cache['running']  = False
    _cache["progress"] = {"done": len(tickers), "total": len(tickers), "phase": "done"}
    log.info('Done. %d results from %d tickers.', len(results), len(tickers))


def get_or_refresh():
    if _cache['running']: return _cache['results']
    if _cache['results'] is None or time.time() - _cache['ts'] > CACHE_TTL:
        threading.Thread(target=run_scan, daemon=True).start()
        # Wait up to 3 seconds for calendar phase to start
        time.sleep(1)
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
    p = _cache["progress"]
    pct = int(p['done']/p['total']*100) if p['total'] > 0 else 0
    return jsonify({'done':p['done'],'total':p['total'],'pct':pct,'phase':p['phase'],'running':_cache['running']})


@app.route('/api/status')
def api_status():
    return jsonify({
        "status":  "ok",
        "cached":  _cache["results"] is not None,
        "count":   len(_cache["results"]) if _cache["results"] else 0,
        "running": _cache["running"],
        "universe":len(_cache["universe"]),
        "progress":_cache["progress"],
    })


@app.route('/api/refresh', methods=['POST'])
def api_refresh():
    if _cache['running']:
        return jsonify({'message':'Scan in progress'}), 202
    threading.Thread(target=run_scan, daemon=True).start()
    return jsonify({'message':'Refresh started'}), 202


@app.route('/api/debug')
def api_debug():
    out = {
        "universe":  len(_cache["universe"]),
        "results":   len(_cache["results"]) if _cache["results"] else 0,
        "running":   _cache["running"],
        "progress":  _cache["progress"],
        "debug":     _cache.get("debug", {}),
    }
    try:
        url  = 'https://api.nasdaq.com/api/calendar/earnings?date=' + date.today().strftime('%Y-%m-%d')
        resp = requests.get(url, headers=NASDAQ_HEADERS, timeout=6)
        rows = (resp.json().get('data') or {}).get('rows') or []
        out["nasdaq_today"] = {"status": resp.status_code, "rows": len(rows), "sample": rows[:2]}
    except Exception as e:
        out["nasdaq_today"] = {"error": str(e)}
    return jsonify(out)


@app.route('/')
def index():
    p = _cache['progress']
    return (f'VV Scanner | {len(_cache["universe"])} tickers | '
            f'{len(_cache["results"]) if _cache["results"] else 0} results | '
            f'Running: {_cache["running"]} ({p["done"]}/{p["total"]}) | '
            f'/api/debug')


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)
