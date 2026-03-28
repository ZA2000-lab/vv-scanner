# VV Scanner Backend
# Uses Nasdaq earnings calendar (no rate limits) + yfinance for IV data

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
    'debug':    {},
}

NASDAQ_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Origin': 'https://www.nasdaq.com',
    'Referer': 'https://www.nasdaq.com/market-activity/earnings',
}


def fetch_nasdaq_calendar(days_ahead=50):
    # Nasdaq earnings calendar API - no auth, generous rate limits
    # Returns {ticker: (date_str, days_away)}
    earnings_map = {}
    today = date.today()

    for offset in range(0, days_ahead, 1):
        target = today + timedelta(days=offset)
        date_str = target.strftime('%Y-%m-%d')
        try:
            url = f'https://api.nasdaq.com/api/calendar/earnings?date={date_str}'
            resp = requests.get(url, headers=NASDAQ_HEADERS, timeout=10)
            if resp.status_code != 200:
                continue
            data = resp.json()
            rows = (data.get('data', {}).get('rows') or
                    data.get('data', {}).get('earningsCalendar', {}).get('rows') or [])
            for row in rows:
                try:
                    sym = (row.get('symbol') or row.get('ticker') or '').upper().strip()
                    if not sym or len(sym) > 6: continue
                    diff = (target - today).days
                    if sym not in earnings_map:
                        earnings_map[sym] = (target.strftime('%b %d'), diff)
                except:
                    continue
            time.sleep(0.2)
        except Exception as e:
            log.warning('Nasdaq calendar %s failed: %s', date_str, e)
            continue

    log.info('Nasdaq calendar: %d tickers with upcoming earnings', len(earnings_map))
    _cache['debug']['nasdaq_count'] = len(earnings_map)
    _cache['debug']['sample'] = list(earnings_map.items())[:5]
    return earnings_map


def fetch_wallstreetmojo_calendar(days_ahead=50):
    # Alternative: TheStreet/Zacks-style free calendar
    # Try marketbeat earnings calendar as backup
    earnings_map = {}
    today = date.today()
    try:
        url = 'https://www.wisesheets.io/earnings-calendar-api'
        resp = requests.get(url, timeout=8)
        if resp.status_code == 200:
            data = resp.json()
            for item in data:
                try:
                    sym  = item.get('symbol','').upper().strip()
                    dstr = item.get('date','')
                    if not sym or not dstr: continue
                    dt   = datetime.strptime(dstr[:10], '%Y-%m-%d').date()
                    diff = (dt - today).days
                    if 0 <= diff <= days_ahead and sym not in earnings_map:
                        earnings_map[sym] = (dt.strftime('%b %d'), diff)
                except:
                    continue
    except:
        pass
    return earnings_map


def fetch_earningswhispers_calendar(days_ahead=50):
    # EarningsWhispers has a free JSON feed
    earnings_map = {}
    today = date.today()
    try:
        url = 'https://www.earningswhispers.com/api/earningscal'
        resp = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            for item in (data if isinstance(data, list) else []):
                try:
                    sym  = item.get('ticker','').upper().strip()
                    dstr = item.get('epsdate','') or item.get('date','')
                    if not sym or not dstr: continue
                    dt   = datetime.strptime(dstr[:10], '%Y-%m-%d').date()
                    diff = (dt - today).days
                    if 0 <= diff <= days_ahead and sym not in earnings_map:
                        earnings_map[sym] = (dt.strftime('%b %d'), diff)
                except:
                    continue
    except:
        pass
    return earnings_map


def fetch_yfinance_batch(syms, days_ahead=50):
    # Slow but reliable fallback - checks one ticker at a time with delays
    earnings_map = {}
    today = date.today()

    def check_ts(val):
        try:
            if isinstance(val, (int, float)):
                dt = date.fromtimestamp(val)
            else:
                ts = pd.Timestamp(val)
                if hasattr(ts, 'tzinfo') and ts.tzinfo is not None:
                    ts = ts.tz_convert('UTC').tz_localize(None)
                dt = ts.date()
            diff = (dt - today).days
            if 0 <= diff <= days_ahead:
                return dt.strftime('%b %d'), diff
        except:
            pass
        return None, None

    for sym in syms:
        try:
            stock = yf.Ticker(sym)
            found = False

            # Try info - single request, includes earningsDate
            try:
                info = stock.info
                val = info.get('earningsDate')
                if val:
                    items = val if isinstance(val, list) else [val]
                    for item in items:
                        r, d = check_ts(item)
                        if r:
                            earnings_map[sym] = (r, d)
                            found = True
                            break
            except:
                pass

            if not found:
                try:
                    ed = stock.get_earnings_dates(limit=6)
                    if ed is not None and not ed.empty:
                        for idx in sorted(ed.index):
                            r, d = check_ts(idx)
                            if r:
                                earnings_map[sym] = (r, d)
                                found = True
                                break
                except:
                    pass

            time.sleep(0.5)  # Gentle rate limiting
        except:
            pass

    log.info('yfinance batch: %d/%d found earnings', len(earnings_map), len(syms))
    return earnings_map


def get_earnings_calendar(days_ahead=50):
    # Try sources in order of reliability
    log.info('Fetching earnings calendar...')

    # Primary: Nasdaq API
    earnings_map = fetch_nasdaq_calendar(days_ahead)
    if len(earnings_map) >= 10:
        return earnings_map

    log.warning('Nasdaq calendar returned %d results, trying alternatives...', len(earnings_map))

    # Secondary: EarningsWhispers
    em2 = fetch_earningswhispers_calendar(days_ahead)
    earnings_map.update(em2)
    if len(earnings_map) >= 10:
        return earnings_map

    log.warning('Still only %d results, falling back to yfinance batch...', len(earnings_map))

    # Last resort: yfinance one-by-one on core liquid universe
    core = [
        'AAPL','MSFT','NVDA','AMZN','META','GOOGL','TSLA','NFLX','AMD',
        'JPM','BAC','WFC','GS','MS','V','MA','COIN','SOFI','PYPL',
        'UNH','LLY','ABBV','MRK','PFE','AMGN','MRNA',
        'HD','WMT','COST','MCD','SBUX','BKNG','UBER',
        'XOM','CVX','BA','CAT','GE','RTX','LMT','UPS',
        'DIS','SNAP','PLTR','CRWD','NET','DDOG','NOW','CRM',
        'MARA','RIOT','MSTR','GME','DAL','AAL','CCL','MGM',
    ]
    em3 = fetch_yfinance_batch(core, days_ahead)
    earnings_map.update(em3)
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
    _cache['progress'] = {'done': 0, 'total': 0, 'phase': 'calendar'}

    earnings_map = get_earnings_calendar(days_ahead=50)

    if not earnings_map:
        log.error('No earnings found from any source')
        _cache['running'] = False
        _cache['results'] = []
        _cache['ts'] = time.time()
        _cache['progress'] = {'done': 0, 'total': 0, 'phase': 'done'}
        return

    tickers = list(earnings_map.keys())
    _cache['universe'] = tickers
    _cache['progress'] = {'done': 0, 'total': len(tickers), 'phase': 'scoring'}
    log.info('Scoring %d tickers...', len(tickers))

    results = []
    lock = threading.Lock()

    def score_one(sym):
        earn_str, earn_days = earnings_map[sym]
        r = score_ticker(sym, earn_str, earn_days)
        with lock:
            _cache['progress']['done'] += 1
            if r: results.append(r)

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futs = {pool.submit(score_one, sym): sym for sym in tickers}
        for f in concurrent.futures.as_completed(futs):
            try: f.result()
            except: pass

    order = {'RECOMMENDED': 0, 'CONSIDER': 1, 'AVOID': 2}
    results.sort(key=lambda x: (order.get(x['rec'], 3), x['daysToEarnings']))
    _cache['results']  = results
    _cache['ts']       = time.time()
    _cache['running']  = False
    _cache['progress'] = {'done': len(tickers), 'total': len(tickers), 'phase': 'done'}
    log.info('Done. %d results from %d earners.', len(results), len(tickers))


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
        return jsonify({'message':'Scan in progress'}), 202
    threading.Thread(target=run_scan, daemon=True).start()
    return jsonify({'message':'Refresh started'}), 202


@app.route('/api/debug')
def api_debug():
    out = {'debug': _cache.get('debug',{}), 'universe_size': len(_cache['universe']),
           'results': len(_cache['results']) if _cache['results'] else 0,
           'running': _cache['running'], 'progress': _cache['progress']}
    # Test Nasdaq calendar for today
    try:
        url = f'https://api.nasdaq.com/api/calendar/earnings?date={date.today().strftime("%Y-%m-%d")}'
        resp = requests.get(url, headers=NASDAQ_HEADERS, timeout=8)
        out['nasdaq_test'] = {'status': resp.status_code, 'body': resp.text[:400]}
    except Exception as e:
        out['nasdaq_test'] = 'error: ' + str(e)
    return jsonify(out)


@app.route('/')
def index():
    p = _cache['progress']
    return (f'VV Scanner | Universe: {len(_cache["universe"])} earners | '
            f'Results: {len(_cache["results"]) if _cache["results"] else 0} | '
            f'Running: {_cache["running"]} ({p["done"]}/{p["total"]}) | '
            f'Debug: /api/debug')


if __name__ == '__main__':
    threading.Thread(target=run_scan, daemon=True).start()
    app.run(host='0.0.0.0', port=5000, debug=False)
