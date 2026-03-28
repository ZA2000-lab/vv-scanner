# VV Scanner Backend - Full Market Edition
# Strategy: pull Yahoo earnings calendar first, then score only upcoming earners

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

CACHE_TTL = 6 * 3600
MAX_WORKERS = 4

_cache = {
    'results':  None,
    'universe': [],
    'ts':       0,
    'running':  False,
    'progress': {'done': 0, 'total': 0, 'phase': 'idle'},
}

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json',
}


def get_yahoo_earnings_calendar(days_ahead=50):
    # Fetch upcoming earnings directly from Yahoo Finance calendar API
    # Returns dict: {ticker: (date_str, days_away)}
    earnings_map = {}
    today = date.today()
    
    for week_offset in range(0, days_ahead, 7):
        try:
            from_date = today + timedelta(days=week_offset)
            to_date   = min(today + timedelta(days=week_offset+6), today + timedelta(days=days_ahead))
            url = (f'https://query2.finance.yahoo.com/v1/finance/earning_reports/upcoming'
                   f'?period1={int(datetime.combine(from_date, datetime.min.time()).timestamp())}'
                   f'&period2={int(datetime.combine(to_date, datetime.max.time()).timestamp())}')
            resp = requests.get(url, headers=HEADERS, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                reports = (data.get('upcomingEarningReports', {}).get('earningReports', [])
                           or data.get('earningReports', [])
                           or [])
                for r in reports:
                    try:
                        sym = r.get('ticker', '').upper()
                        ts  = r.get('startdatetimetype') or r.get('startDateTime')
                        if sym and ts:
                            dt   = pd.Timestamp(ts).date()
                            diff = (dt - today).days
                            if 0 <= diff <= days_ahead and sym not in earnings_map:
                                earnings_map[sym] = (dt.strftime('%b %d'), diff)
                    except:
                        continue
            time.sleep(0.5)
        except Exception as e:
            log.warning('Calendar week fetch failed: %s', e)
            continue
    
    log.info('Yahoo calendar: %d upcoming earnings found', len(earnings_map))
    return earnings_map


def get_earnings_yfinance(sym):
    # Fallback: try multiple yfinance methods for a single ticker
    today = date.today()
    stock = yf.Ticker(sym)
    
    def check(val):
        try:
            if isinstance(val, (int, float)):
                dt = date.fromtimestamp(val)
            else:
                ts = pd.Timestamp(val)
                # Handle timezone-aware timestamps
                if ts.tzinfo is not None:
                    ts = ts.tz_convert('UTC').tz_localize(None)
                dt = ts.date()
            diff = (dt - today).days
            if 0 <= diff <= 60:
                return dt.strftime('%b %d'), diff
        except:
            pass
        return None, None

    # Try info dict first - most reliable
    try:
        info = stock.info
        for key in ('earningsDate', 'earningsTimestamp'):
            val = info.get(key)
            if val:
                items = val if isinstance(val, list) else [val]
                for item in items:
                    r, d = check(item)
                    if r: return r, d
    except:
        pass

    # Try get_earnings_dates
    try:
        ed = stock.get_earnings_dates(limit=10)
        if ed is not None and not ed.empty:
            for idx in sorted(ed.index):
                r, d = check(idx)
                if r: return r, d
    except:
        pass

    # Try calendar
    try:
        cal = stock.calendar
        if isinstance(cal, dict):
            for key in ('Earnings Date', 'earningsDate'):
                raw = cal.get(key)
                if raw:
                    items = list(raw) if hasattr(raw, '__iter__') and not isinstance(raw, str) else [raw]
                    for item in items:
                        r, d = check(item)
                        if r: return r, d
    except:
        pass

    return None, None


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

        today_d = date.today()
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
        ts = build_ts(dtes, ivs)
        slope  = (ts(45) - ts(dtes[0])) / (45 - dtes[0])
        h3     = stock.history(period='3mo')
        if h3.empty or len(h3) < 30: return None
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
            'ticker':         sym,
            'name':           name,
            'sector':         '',
            'price':          round(price, 2),
            'earningsDate':   earn_str,
            'daysToEarnings': int(earn_days),
            'rec':            rec,
            'c1':             bool(c1),
            'c2':             bool(c2),
            'c3':             bool(c3),
            'tsSlope':        round(float(slope), 6),
            'ivRv':           round(float(ivrv), 3),
            'avgVol':         int(avgvol),
            'expectedMove':   round((straddle/price)*100, 2) if straddle else None,
            'frontIV':        round(ts(dtes[0])*100, 1),
            'backIV':         round(ts(45)*100, 1),
            'strike':         strike,
            'frontExp':       exp_dates[0],
            'backExp':        exp_dates[min(1, len(exp_dates)-1)],
            'debitEst':       debit,
            'entryDate':      entry_str,
            'exitDate':       earn_str,
        }
    except Exception as e:
        log.debug('%s: %s', sym, e)
        return None


def run_scan():
    if _cache['running']: return
    _cache['running'] = True
    _cache['progress'] = {'done': 0, 'total': 0, 'phase': 'fetching_calendar'}

    # Step 1: get earnings calendar from Yahoo (one API call gets all upcoming earners)
    log.info('Step 1: fetching Yahoo earnings calendar...')
    earnings_map = get_yahoo_earnings_calendar(days_ahead=50)

    # Step 2: if Yahoo calendar API failed, fall back to yfinance per-ticker
    if len(earnings_map) < 5:
        log.info('Yahoo calendar returned %d results, falling back to yfinance per-ticker...', len(earnings_map))
        # Build a universe of liquid stocks to check
        fallback_universe = [
            'AAPL','MSFT','NVDA','AMZN','META','GOOGL','TSLA','NFLX','AMD','ORCL',
            'JPM','BAC','WFC','GS','MS','C','V','MA','AXP','COIN','HOOD','SOFI',
            'UNH','JNJ','LLY','ABBV','MRK','PFE','AMGN','GILD','VRTX','MRNA',
            'HD','WMT','COST','MCD','SBUX','NKE','TGT','LOW','BKNG','ABNB','UBER',
            'XOM','CVX','COP','BA','CAT','DE','GE','HON','RTX','LMT','UPS','FDX',
            'DIS','CMCSA','SNAP','PINS','SPOT','BABA','JD','NIO','PLTR','CRWD',
            'PANW','NET','DDOG','SNOW','NOW','CRM','SHOP','MELI','MARA','RIOT','MSTR',
            'TSLA','RIVN','F','GM','DAL','AAL','CCL','MGM','WYNN','DKNG',
        ]
        _cache['universe'] = fallback_universe
        _cache['progress'] = {'done': 0, 'total': len(fallback_universe), 'phase': 'scanning'}
        for sym in fallback_universe:
            earn_str, earn_days = get_earnings_yfinance(sym)
            if earn_str:
                earnings_map[sym] = (earn_str, earn_days)
            _cache['progress']['done'] += 1
            time.sleep(0.3)
        log.info('Fallback found %d upcoming earners', len(earnings_map))

    # Step 3: score each ticker that has upcoming earnings
    tickers_to_score = list(earnings_map.keys())
    _cache['universe'] = tickers_to_score
    _cache['progress'] = {'done': 0, 'total': len(tickers_to_score), 'phase': 'scoring'}
    log.info('Step 3: scoring %d upcoming earners...', len(tickers_to_score))

    results = []
    lock = threading.Lock()

    def score_one(sym):
        earn_str, earn_days = earnings_map[sym]
        r = score_ticker(sym, earn_str, earn_days)
        with lock:
            _cache['progress']['done'] += 1
            if r: results.append(r)

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futs = {pool.submit(score_one, sym): sym for sym in tickers_to_score}
        for f in concurrent.futures.as_completed(futs):
            try: f.result()
            except: pass

    order = {'RECOMMENDED': 0, 'CONSIDER': 1, 'AVOID': 2}
    results.sort(key=lambda x: (order.get(x['rec'], 3), x['daysToEarnings']))
    _cache['results']  = results
    _cache['ts']       = time.time()
    _cache['running']  = False
    _cache['progress'] = {'done': len(tickers_to_score), 'total': len(tickers_to_score), 'phase': 'done'}
    log.info('Done. %d scored setups from %d earners.', len(results), len(tickers_to_score))


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
        'results':      data or [],
        'count':        len(data) if data else 0,
        'scannedAt':    datetime.fromtimestamp(_cache['ts']).strftime('%b %d %Y, %I:%M %p') if _cache['ts'] else None,
        'ageMinutes':   int((time.time()-_cache['ts'])/60) if _cache['ts'] else 0,
        'isRefreshing': _cache['running'],
        'universe':     len(_cache['universe']),
        'progress':     _cache['progress'],
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
                    'running':_cache['running'],'universe':len(_cache['universe']),
                    'progress':_cache['progress']})


@app.route('/api/refresh', methods=['POST'])
def api_refresh():
    if _cache['running']:
        return jsonify({'message':'Scan already in progress'}), 202
    threading.Thread(target=run_scan, daemon=True).start()
    return jsonify({'message':'Refresh started'}), 202


@app.route('/api/debug')
def api_debug():
    sym = 'AAPL'
    out = {'yfinance_version': yf.__version__, 'methods': {}}
    stock = yf.Ticker(sym)
    try:
        ed = stock.get_earnings_dates(limit=4)
        out['methods']['get_earnings_dates'] = str(ed.index.tolist()[:4]) if ed is not None and not ed.empty else 'empty'
    except Exception as e:
        out['methods']['get_earnings_dates'] = 'error:' + str(e)
    try:
        info = stock.info
        out['methods']['info_earningsDate'] = str(info.get('earningsDate'))
    except Exception as e:
        out['methods']['info'] = 'error:' + str(e)
    try:
        cal_resp = requests.get(
            'https://query2.finance.yahoo.com/v1/finance/earning_reports/upcoming?period1=1743120000&period2=1745712000',
            headers=HEADERS, timeout=8)
        out['calendar_api'] = {'status': cal_resp.status_code, 'body': str(cal_resp.text)[:500]}
    except Exception as e:
        out['calendar_api'] = 'error:' + str(e)
    earn_str, earn_days = get_earnings_yfinance(sym)
    out['aapl_result'] = {'earn_str': earn_str, 'earn_days': earn_days}
    return jsonify(out)


@app.route('/')
def index():
    p = _cache['progress']
    return (f'VV Scanner | Universe: {len(_cache["universe"])} | '
            f'Results: {len(_cache["results"]) if _cache["results"] else 0} | '
            f'Running: {_cache["running"]} ({p["done"]}/{p["total"]}) | '
            f'Debug: /api/debug')


if __name__ == '__main__':
    threading.Thread(target=run_scan, daemon=True).start()
    app.run(host='0.0.0.0', port=5000, debug=False)
