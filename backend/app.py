# VV Scanner Backend - optimized for speed and rate limit avoidance

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
MAX_WORKERS = 2       # Low to avoid Yahoo rate limits
DAYS_AHEAD  = 35      # Only next 5 weeks of earnings
MIN_AVG_VOL = 500_000 # Skip very illiquid stocks before scoring

_cache = {
    'results':  None,
    'universe': [],
    'ts':       0,
    'running':  False,
    'progress': {'done': 0, 'total': 0, 'phase': 'idle'},
}

NASDAQ_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Origin': 'https://www.nasdaq.com',
    'Referer': 'https://www.nasdaq.com/market-activity/earnings',
}

# Known liquid tickers - used to filter Nasdaq calendar results
# Only score tickers that are likely to have options and sufficient volume
LIQUID_WHITELIST = set([
    'AAPL','MSFT','NVDA','AMZN','META','GOOGL','GOOG','TSLA','NFLX','ORCL',
    'AMD','INTC','QCOM','TXN','AVGO','MU','AMAT','LRCX','KLAC','MRVL',
    'ARM','ASML','TSM','SMCI','ON','MPWR','ENPH','FSLR',
    'CRM','NOW','PANW','CRWD','NET','DDOG','SNOW','MDB','OKTA','ZS',
    'FTNT','HUBS','WDAY','TEAM','SHOP','MELI','TTD','RBLX','PATH',
    'ZM','DOCU','TWLO','BILL','PLTR','BRZE','CFLT','SMAR','GTLB',
    'JPM','BAC','WFC','GS','MS','C','V','MA','AXP','BLK','SCHW','COF',
    'DFS','SYF','AIG','MET','PRU','ALL','TRV','BRK-B','USB','PNC','TFC',
    'RF','FITB','HBAN','KEY','CFG','ZION','MTB','SNV','UMBF',
    'COIN','HOOD','SOFI','PYPL','SQ','AFRM','UPST','NU',
    'UNH','JNJ','LLY','ABBV','MRK','PFE','AMGN','GILD','VRTX','REGN',
    'BMY','BIIB','DXCM','ISRG','EW','ZBH','HUM','CVS','CI','CNC','HCA',
    'MRNA','NVAX','ALNY','NBIB','VKTX','KYMR','BMRN','RARE','EXAS',
    'HD','WMT','COST','MCD','SBUX','NKE','TGT','LOW','BKNG','ABNB',
    'UBER','LYFT','DASH','ETSY','CPNG','LULU','PTON','W','RIVN','F','GM',
    'LCID','PG','KO','PEP','PM','MO','CL','KMB','HSY','MNST','CELH',
    'XOM','CVX','COP','EOG','SLB','OXY','FANG','DVN','MRO','HES','APA',
    'BA','CAT','DE','GE','HON','RTX','LMT','NOC','UPS','FDX','ETN','EMR',
    'AXON','TDG','HEICO','PH','ROK','IR','AME','GNRC',
    'DIS','CMCSA','T','VZ','TMUS','SNAP','PINS','SPOT','WBD','PARA',
    'NWSA','NYT','FOXA','SIRI','IMAX','AMC','CNK',
    'BABA','BIDU','JD','PDD','NIO','XPEV','LI','BILI','TME',
    'AMT','PLD','EQIX','CCI','SPG','O','NEE','DUK','SO','LIN','FCX','NEM',
    'MARA','RIOT','MSTR','HUT','CLSK','IONQ','QUBT','RGTI','SOUN','BBAI',
    'GME','AMC','DKNG','PENN','WYNN','MGM','LVS','CCL','RCL',
    'AAL','DAL','UAL','LUV','JBLU','ALK',
    'SPY','QQQ','IWM','SMH','XLE','XLF','XLK','XLV','ARKK','GLD','TLT',
])


def fetch_nasdaq_calendar():
    earnings_map = {}
    today = date.today()
    # Skip weekends in loop for efficiency
    for offset in range(0, DAYS_AHEAD):
        target = today + timedelta(days=offset)
        if target.weekday() >= 5:
            continue
        date_str = target.strftime('%Y-%m-%d')
        try:
            url  = 'https://api.nasdaq.com/api/calendar/earnings?date=' + date_str
            resp = requests.get(url, headers=NASDAQ_HEADERS, timeout=8)
            if resp.status_code != 200:
                continue
            data = resp.json()
            rows = (data.get('data', {}) or {}).get('rows') or []
            if not rows:
                time.sleep(0.1)
                continue
            for row in rows:
                try:
                    sym  = (row.get('symbol') or row.get('ticker') or '').upper().strip()
                    sym  = sym.replace('/', '-')  # Normalize BRK/B -> BRK-B etc
                    if not sym or len(sym) > 6 or not sym.replace('-','').isalpha():
                        continue
                    diff = (target - today).days
                    if sym not in earnings_map:
                        earnings_map[sym] = (target.strftime('%b %d'), diff)
                except:
                    continue
            time.sleep(0.15)
        except Exception as e:
            log.warning('Nasdaq %s: %s', date_str, e)
            continue
    log.info('Nasdaq calendar: %d tickers', len(earnings_map))
    return earnings_map


def yf_get_with_retry(fn, retries=3):
    for attempt in range(retries):
        try:
            result = fn()
            return result
        except Exception as e:
            if 'rate' in str(e).lower() or '429' in str(e) or 'too many' in str(e).lower():
                wait = 5 * (attempt + 1)
                log.warning('Rate limited, waiting %ds...', wait)
                time.sleep(wait)
            else:
                raise
    return None


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
        stock  = yf.Ticker(sym)
        today_d = date.today()

        # Get price + 3mo history in one call
        h3 = yf_get_with_retry(lambda: stock.history(period='3mo'))
        if h3 is None or h3.empty or len(h3) < 30:
            return None
        price  = float(h3['Close'].iloc[-1])
        if price <= 0: return None

        # Quick volume filter - skip very illiquid names
        avg_vol_quick = float(h3['Volume'].tail(10).mean())
        if avg_vol_quick < MIN_AVG_VOL:
            return None

        # Options
        opts = yf_get_with_retry(lambda: stock.options)
        if not opts or len(opts) < 2: return None
        try:
            exp_dates = filter_dates(list(opts))
        except:
            return None
        if len(exp_dates) < 2: return None

        dtes, ivs, straddle = [], [], None
        for i, exp in enumerate(exp_dates):
            try:
                ch = yf_get_with_retry(lambda e=exp: stock.option_chain(e))
                if ch is None: continue
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

    # Step 1: fetch earnings calendar
    earnings_map = fetch_nasdaq_calendar()

    if not earnings_map:
        log.error('No earnings found')
        _cache['running'] = False
        _cache['results'] = []
        _cache['ts'] = time.time()
        _cache['progress'] = {'done': 0, 'total': 0, 'phase': 'done'}
        return

    # Step 2: filter to liquid whitelist + any ticker the Nasdaq API returned
    # Priority: whitelist tickers first, then others
    whitelist_earners = {k:v for k,v in earnings_map.items() if k in LIQUID_WHITELIST}
    other_earners     = {k:v for k,v in earnings_map.items() if k not in LIQUID_WHITELIST}

    # Cap total at 200 to keep scan under 10 minutes
    # Whitelist first (most important), then fill with others
    capped = dict(list(whitelist_earners.items()) + list(other_earners.items())[:max(0, 200-len(whitelist_earners))])

    tickers = list(capped.keys())
    _cache['universe'] = tickers
    _cache['progress'] = {'done': 0, 'total': len(tickers), 'phase': 'scoring'}
    log.info('Scoring %d tickers (%d whitelisted, %d other)...',
             len(tickers), len(whitelist_earners), len(other_earners))

    results = []
    lock = threading.Lock()

    def score_one(sym):
        earn_str, earn_days = capped[sym]
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
    log.info('Done. %d results from %d tickers.', len(results), len(tickers))


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
    out = {
        'universe': len(_cache['universe']),
        'results':  len(_cache['results']) if _cache['results'] else 0,
        'running':  _cache['running'],
        'progress': _cache['progress'],
    }
    try:
        url  = 'https://api.nasdaq.com/api/calendar/earnings?date=' + date.today().strftime('%Y-%m-%d')
        resp = requests.get(url, headers=NASDAQ_HEADERS, timeout=8)
        body = resp.json()
        rows = (body.get('data',{}) or {}).get('rows') or []
        out['nasdaq'] = {'status': resp.status_code, 'rows_today': len(rows), 'sample': rows[:3]}
    except Exception as e:
        out['nasdaq'] = {'error': str(e)}
    return jsonify(out)


@app.route('/')
def index():
    p = _cache['progress']
    return (f'VV Scanner | {len(_cache["universe"])} tickers | '
            f'{len(_cache["results"]) if _cache["results"] else 0} results | '
            f'Running: {_cache["running"]} ({p["done"]}/{p["total"]}) | '
            f'Debug: /api/debug')


if __name__ == '__main__':
    threading.Thread(target=run_scan, daemon=True).start()
    app.run(host='0.0.0.0', port=5000, debug=False)
