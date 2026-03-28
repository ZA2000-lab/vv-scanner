# VV Scanner Backend - batch download approach to avoid rate limits

from flask import Flask, jsonify
from flask_cors import CORS
import yfinance as yf
import pandas as pd
import requests
from datetime import datetime, timedelta, date
import numpy as np
import threading
import time
import logging

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

CACHE_TTL  = 6 * 3600
DAYS_AHEAD = 35

_cache = {"results": None, "universe": [], "ts": 0, "running": False,
          "progress": {"done": 0, "total": 0, "phase": "idle"}, "debug": {}}

NASDAQ_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://www.nasdaq.com",
    "Referer": "https://www.nasdaq.com/market-activity/earnings",
}


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
                    timeout=6)
                if resp.status_code == 200:
                    rows = (resp.json().get('data') or {}).get('rows') or []
                    diff = (d - today).days
                    for row in rows:
                        sym = (row.get('symbol') or '').upper().strip().replace('/', '-')
                        if sym and 1 <= len(sym) <= 6 and sym.replace('-','').isalpha():
                            if sym not in earnings_map:
                                earnings_map[sym] = (d.strftime('%b %d'), diff)
                time.sleep(0.1)
            except Exception as e:
                log.warning('Calendar %s: %s', d, e)
        d += timedelta(days=1)
    log.info('Calendar: %d tickers', len(earnings_map))
    _cache['debug']['calendar_count'] = len(earnings_map)
    _cache['debug']['sample'] = list(earnings_map.items())[:8]
    return earnings_map


def batch_filter_by_volume(tickers, min_vol=1_000_000):
    """Download 1mo price history for all tickers in one yf.download() call.
    Returns list of tickers passing the volume filter.
    Much more efficient than one-by-one calls."""
    log.info('Batch downloading volume data for %d tickers...', len(tickers))
    try:
        # yf.download fetches all tickers in a single HTTP request
        raw = yf.download(
            tickers,
            period='1mo',
            interval='1d',
            group_by='ticker',
            auto_adjust=True,
            progress=False,
            threads=True,
        )
        passing = []
        for sym in tickers:
            try:
                if len(tickers) == 1:
                    vol_col = raw['Volume']
                else:
                    vol_col = raw[sym]['Volume']
                avg_vol = float(vol_col.dropna().tail(10).mean())
                if avg_vol >= min_vol:
                    passing.append(sym)
            except:
                pass
        log.info('Volume filter: %d/%d tickers pass >= %dM avg vol',
                 len(passing), len(tickers), min_vol//1_000_000)
        return passing
    except Exception as e:
        log.warning('Batch download failed: %s -- using all tickers', e)
        return tickers


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
    log_cc    = (price_data['Close'] / price_data['Close'].shift(1)).apply(np.log)
    rs        = log_ho*(log_ho-log_co) + log_lo*(log_lo-log_co)
    close_vol = (log_cc**2).rolling(window,center=False).sum()/(window-1)
    open_vol  = (log_oc**2).rolling(window,center=False).sum()/(window-1)
    window_rs = rs.rolling(window,center=False).sum()/(window-1)
    k = 0.34/(1.34+(window+1)/(window-1))
    return ((open_vol+k*close_vol+(1-k)*window_rs).apply(np.sqrt)*np.sqrt(trading_periods)).iloc[-1]


def build_ts(days, ivs):
    days=np.array(days,dtype=float); ivs=np.array(ivs,dtype=float)
    idx=days.argsort(); days=days[idx]; ivs=ivs[idx]
    def ts(dte):
        if dte<=days[0]: return float(ivs[0])
        if dte>=days[-1]: return float(ivs[-1])
        return float(np.interp(dte,days,ivs))
    return ts


def score_ticker(sym, earn_str, earn_days, h3_data=None):
    try:
        stock   = yf.Ticker(sym)
        today_d = date.today()

        # Use pre-downloaded data if available, else fetch
        if h3_data is not None and not h3_data.empty:
            h3 = h3_data
        else:
            h3 = stock.history(period='3mo')
        if h3.empty or len(h3) < 20: return None
        price = float(h3['Close'].iloc[-1])
        if price <= 0: return None

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
                ci = (c['strike']-price).abs().idxmin()
                pi = (p['strike']-price).abs().idxmin()
                atm_iv = (c.loc[ci,'impliedVolatility']+p.loc[pi,'impliedVolatility'])/2
                dte = (datetime.strptime(exp,'%Y-%m-%d').date()-today_d).days
                dtes.append(dte); ivs.append(atm_iv)
                if i==0:
                    straddle=((c.loc[ci,'bid']+c.loc[ci,'ask'])/2+
                              (p.loc[pi,'bid']+p.loc[pi,'ask'])/2)
            except:
                continue
            time.sleep(0.3)  # Be polite between options calls

        if len(dtes)<2: return None
        ts     = build_ts(dtes,ivs)
        slope  = (ts(45)-ts(dtes[0]))/(45-dtes[0])
        ivrv   = ts(30)/yang_zhang(h3)
        avgvol = h3['Volume'].rolling(30).mean().dropna().iloc[-1]

        c1 = slope  <= -0.00406
        c2 = avgvol >= 1_500_000
        c3 = ivrv   >= 1.25
        rec=('RECOMMENDED' if c1 and c2 and c3 else
             'CONSIDER'    if c1 and (c2 or c3) else 'AVOID')

        s=price
        strike=(round(s*2)/2 if s<20 else round(s) if s<50 else
                round(s/5)*5 if s<200 else round(s/10)*10 if s<500 else round(s/25)*25)
        debit=round(price*ts(dtes[0])*(max(dtes[0],1)/365)**0.5*0.4,2)
        try: name=getattr(stock.fast_info,'company_name',None) or sym
        except: name=sym
        try:
            ed=datetime.strptime(earn_str,'%b %d').replace(year=datetime.today().year)
            entry_str=(ed-timedelta(days=1)).strftime('%b %d')
        except: entry_str=earn_str

        return {
            "ticker":sym,"name":name,"sector":"",
            "price":round(price,2),"earningsDate":earn_str,"daysToEarnings":int(earn_days),
            "rec":rec,"c1":bool(c1),"c2":bool(c2),"c3":bool(c3),
            "tsSlope":round(float(slope),6),"ivRv":round(float(ivrv),3),"avgVol":int(avgvol),
            "expectedMove":round((straddle/price)*100,2) if straddle else None,
            "frontIV":round(ts(dtes[0])*100,1),"backIV":round(ts(45)*100,1),
            "strike":strike,"frontExp":exp_dates[0],"backExp":exp_dates[min(1,len(exp_dates)-1)],
            "debitEst":debit,"entryDate":entry_str,"exitDate":earn_str,
        }
    except Exception as e:
        log.debug('%s: %s', sym, e)
        return None


def run_scan():
    if _cache['running']: return
    _cache['running'] = True
    _cache["progress"] = {"done":0,"total":0,"phase":"calendar"}

    # Step 1: get earnings calendar
    earnings_map = fetch_earnings_calendar()
    if not earnings_map:
        _cache['running']=False; _cache['results']=[]; _cache['ts']=time.time()
        _cache["progress"]={"done":0,"total":0,"phase":"done"}
        return

    # Step 2: batch download volume to filter illiquid names
    _cache["progress"]={"done":0,"total":len(earnings_map),"phase":"volume_filter"}
    all_syms = list(earnings_map.keys())
    liquid   = batch_filter_by_volume(all_syms, min_vol=500_000)
    log.info('%d/%d tickers pass volume filter', len(liquid), len(all_syms))
    _cache['debug']['liquid_count'] = len(liquid)

    # Step 3: batch download 3mo history for liquid tickers
    _cache["progress"]={"done":0,"total":len(liquid),"phase":"downloading"}
    log.info('Batch downloading 3mo history for %d tickers...', len(liquid))
    try:
        bulk = yf.download(
            liquid, period='3mo', interval='1d',
            group_by='ticker', auto_adjust=True,
            progress=False, threads=True,
        )
    except:
        bulk = None

    # Step 4: score each ticker (options chains fetched individually)
    _cache['universe'] = liquid
    _cache["progress"]={"done":0,"total":len(liquid),"phase":"scoring"}
    log.info('Scoring %d tickers...', len(liquid))

    results = []
    for i, sym in enumerate(liquid):
        earn_str, earn_days = earnings_map[sym]
        # Extract pre-downloaded history if available
        h3 = None
        if bulk is not None:
            try:
                h3 = bulk[sym] if len(liquid)>1 else bulk
                h3 = h3.dropna(how='all')
            except:
                h3 = None
        r = score_ticker(sym, earn_str, earn_days, h3_data=h3)
        if r: results.append(r)
        _cache["progress"]["done"] = i+1
        # Throttle to avoid rate limits
        time.sleep(0.5)

    order={'RECOMMENDED':0,'CONSIDER':1,'AVOID':2}
    results.sort(key=lambda x:(order.get(x['rec'],3),x['daysToEarnings']))
    _cache["results"]=results; _cache["ts"]=time.time(); _cache["running"]=False
    _cache["progress"]={"done":len(liquid),"total":len(liquid),"phase":"done"}
    log.info('Done. %d results from %d tickers.', len(results), len(liquid))


def get_or_refresh():
    if _cache['running']: return _cache['results']
    if _cache['results'] is None or time.time()-_cache['ts']>CACHE_TTL:
        threading.Thread(target=run_scan,daemon=True).start()
        time.sleep(1)
    return _cache['results']


@app.route('/api/scan')
def api_scan():
    data=get_or_refresh()
    return jsonify({
        "results":data or [],"count":len(data) if data else 0,
        "scannedAt":datetime.fromtimestamp(_cache["ts"]).strftime("%b %d %Y, %I:%M %p") if _cache["ts"] else None,
        "ageMinutes":int((time.time()-_cache["ts"])/60) if _cache["ts"] else 0,
        "isRefreshing":_cache["running"],"universe":len(_cache["universe"]),
        "progress":_cache["progress"],
    })

@app.route('/api/progress')
def api_progress():
    p=_cache["progress"]
    pct=int(p['done']/p['total']*100) if p['total']>0 else 0
    return jsonify({'done':p['done'],'total':p['total'],'pct':pct,'phase':p['phase'],'running':_cache['running']})

@app.route('/api/status')
def api_status():
    return jsonify({"status":"ok","cached":_cache["results"] is not None,
                    "count":len(_cache["results"]) if _cache["results"] else 0,
                    "running":_cache["running"],"universe":len(_cache["universe"]),
                    "progress":_cache["progress"]})

@app.route('/api/refresh', methods=['POST'])
def api_refresh():
    if _cache['running']: return jsonify({'message':'Scan in progress'}),202
    threading.Thread(target=run_scan,daemon=True).start()
    return jsonify({'message':'Refresh started'}),202

@app.route('/api/debug')
def api_debug():
    out={
        "universe":len(_cache["universe"]),"results":len(_cache["results"]) if _cache["results"] else 0,
        "running":_cache["running"],"progress":_cache["progress"],"debug":_cache.get("debug",{}),
    }
    try:
        url='https://api.nasdaq.com/api/calendar/earnings?date='+date.today().strftime('%Y-%m-%d')
        resp=requests.get(url,headers=NASDAQ_HEADERS,timeout=6)
        rows=(resp.json().get('data') or {}).get('rows') or []
        out["nasdaq_today"]={"status":resp.status_code,"rows":len(rows),"sample":rows[:2]}
    except Exception as e:
        out["nasdaq_today"]={"error":str(e)}
    return jsonify(out)

@app.route('/')
def index():
    p=_cache['progress']
    return (f'VV Scanner | {len(_cache["universe"])} tickers | '
            f'{len(_cache["results"]) if _cache["results"] else 0} results | '
            f'Running:{_cache["running"]} ({p["done"]}/{p["total"]}) | /api/debug')

if __name__=='__main__':
    app.run(host='0.0.0.0',port=5000,debug=False)
