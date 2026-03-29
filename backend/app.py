# VV Scanner - non-blocking architecture

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
DAYS_AHEAD  = 35
MAX_TICKERS = 40
MAX_WORKERS = 3

_cache = {"results":[],"universe":[],"ts":0,"running":False,
          "progress":{"done":0,"total":0,"phase":"idle"},"debug":{}}

NASDAQ_HEADERS = {
    "User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept":"application/json",
    "Origin":"https://www.nasdaq.com",
    "Referer":"https://www.nasdaq.com/market-activity/earnings",
}

TOP_LIQUID = [
    "AAPL","MSFT","NVDA","AMZN","META","GOOGL","TSLA","NFLX","AMD","ORCL",
    "JPM","BAC","WFC","GS","MS","C","V","MA","AXP","SCHW","COIN","SOFI",
    "UNH","LLY","ABBV","MRK","PFE","AMGN","VRTX","MRNA","ISRG","REGN",
    "HD","WMT","COST","MCD","SBUX","NKE","TGT","BKNG","UBER","DASH",
    "XOM","CVX","COP","SLB","BA","CAT","GE","HON","RTX","LMT","UPS",
    "DIS","SNAP","PLTR","CRWD","NET","DDOG","NOW","CRM","AVGO","TXN",
    "QCOM","MU","INTC","AMAT","ARM","SMCI","F","GM","RIVN","DAL","AAL",
    "MARA","RIOT","MSTR","GME","MGM","DKNG","CCL","WYNN",
    "SPY","QQQ","IWM","GLD","TLT",
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
                resp = session.get("https://api.nasdaq.com/api/calendar/earnings?date="+d.strftime("%Y-%m-%d"),timeout=5)
                if resp.status_code == 200:
                    rows = (resp.json().get('data') or {}).get('rows') or []
                    diff = (d - today).days
                    for row in rows:
                        sym = (row.get('symbol') or '').upper().strip().replace('/','- '[:1])
                        if sym and 1<=len(sym)<=6 and sym.replace('-','').isalpha():
                            if sym not in earnings_map:
                                earnings_map[sym] = (d.strftime('%b %d'), diff)
                time.sleep(0.08)
            except Exception as e:
                log.warning('Cal %s: %s', d, e)
        d += timedelta(days=1)
    log.info('Calendar: %d tickers', len(earnings_map))
    _cache['debug']['cal'] = len(earnings_map)
    _cache['debug']['sample'] = list(earnings_map.items())[:6]
    return earnings_map


def filter_dates(dates):
    today = date.today()
    cutoff = today + timedelta(days=45)
    sd = sorted(datetime.strptime(d,'%Y-%m-%d').date() for d in dates)
    for i,dt in enumerate(sd):
        if dt >= cutoff:
            arr = [d.strftime('%Y-%m-%d') for d in sd[:i+1]]
            return arr[1:] if arr[0]==today.strftime('%Y-%m-%d') else arr
    raise ValueError('no expiry')


def yang_zhang(df,window=30,tp=252):
    lho=(df['High']/df['Open']).apply(np.log)
    llo=(df['Low']/df['Open']).apply(np.log)
    lco=(df['Close']/df['Open']).apply(np.log)
    loc_=(df['Open']/df['Close'].shift(1)).apply(np.log)
    lcc=(df['Close']/df['Close'].shift(1)).apply(np.log)
    rs=lho*(lho-lco)+llo*(llo-lco)
    cv=(lcc**2).rolling(window).sum()/(window-1)
    ov=(loc_**2).rolling(window).sum()/(window-1)
    wr=rs.rolling(window).sum()/(window-1)
    k=0.34/(1.34+(window+1)/(window-1))
    return((ov+k*cv+(1-k)*wr).apply(np.sqrt)*np.sqrt(tp)).iloc[-1]


def build_ts(days,ivs):
    d=np.array(days,dtype=float); v=np.array(ivs,dtype=float)
    i=d.argsort(); d=d[i]; v=v[i]
    def ts(x):
        if x<=d[0]: return float(v[0])
        if x>=d[-1]: return float(v[-1])
        return float(np.interp(x,d,v))
    return ts


def score_ticker(sym, earn_str, earn_days):
    try:
        stock = yf.Ticker(sym)
        today_d = date.today()
        h3 = stock.history(period='3mo')
        if h3.empty or len(h3)<20: return None
        price = float(h3['Close'].iloc[-1])
        if price<=0: return None
        if float(h3['Volume'].tail(10).mean())<200_000: return None
        if not stock.options or len(stock.options)<2: return None
        try: exp_dates=filter_dates(list(stock.options))
        except: return None
        if len(exp_dates)<2: return None
        dtes,ivs,straddle=[],[],None
        for i,exp in enumerate(exp_dates[:4]):
            try:
                ch=stock.option_chain(exp)
                c,p=ch.calls,ch.puts
                if c.empty or p.empty: continue
                ci=(c['strike']-price).abs().idxmin()
                pi=(p['strike']-price).abs().idxmin()
                atm_iv=(c.loc[ci,'impliedVolatility']+p.loc[pi,'impliedVolatility'])/2
                dte=(datetime.strptime(exp,'%Y-%m-%d').date()-today_d).days
                dtes.append(dte); ivs.append(atm_iv)
                if i==0: straddle=((c.loc[ci,'bid']+c.loc[ci,'ask'])/2+(p.loc[pi,'bid']+p.loc[pi,'ask'])/2)
            except: continue
        if len(dtes)<2: return None
        ts=build_ts(dtes,ivs)
        slope=(ts(45)-ts(dtes[0]))/(45-dtes[0])
        ivrv=ts(30)/yang_zhang(h3)
        avgvol=h3['Volume'].rolling(30).mean().dropna().iloc[-1]
        c1=slope<=-0.00406; c2=avgvol>=1_500_000; c3=ivrv>=1.25
        rec=('RECOMMENDED' if c1 and c2 and c3 else 'CONSIDER' if c1 and(c2 or c3) else 'AVOID')
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
            "ticker":sym,"name":name,"sector":"","price":round(price,2),
            "earningsDate":earn_str,"daysToEarnings":int(earn_days),"rec":rec,
            "c1":bool(c1),"c2":bool(c2),"c3":bool(c3),
            "tsSlope":round(float(slope),6),"ivRv":round(float(ivrv),3),"avgVol":int(avgvol),
            "expectedMove":round((straddle/price)*100,2) if straddle else None,
            "frontIV":round(ts(dtes[0])*100,1),"backIV":round(ts(45)*100,1),
            "strike":strike,"frontExp":exp_dates[0],"backExp":exp_dates[min(1,len(exp_dates)-1)],
            "debitEst":debit,"entryDate":entry_str,"exitDate":earn_str,
        }
    except Exception as e:
        log.debug('%s: %s',sym,e); return None


def run_scan():
    if _cache['running']: return
    _cache['running'] = True
    _cache["progress"] = {"done":0,"total":0,"phase":"calendar"}
    log.info('Starting scan...')

    try:
        earnings_map = fetch_earnings_calendar()
    except Exception as e:
        log.error('Calendar failed: %s', e)
        earnings_map = {}

    if not earnings_map:
        _cache['running']=False
        _cache["progress"]={"done":0,"total":0,"phase":"done"}
        return

    ordered = [s for s in TOP_LIQUID if s in earnings_map]
    ordered += [s for s in earnings_map if s not in TOP_LIQUID]
    tickers = ordered[:MAX_TICKERS]
    _cache['universe'] = tickers
    _cache["progress"] = {"done":0,"total":len(tickers),"phase":"scoring"}
    log.info('Scoring %d tickers...', len(tickers))

    results = []
    lock = threading.Lock()

    def score_one(sym):
        r = score_ticker(sym, *earnings_map[sym])
        with lock:
            _cache["progress"]["done"] += 1
            if r:
                results.append(r)
                # Save partial results so frontend can show them as they come in
                _cache["results"] = sorted(results,
                    key=lambda x:({'RECOMMENDED':0,'CONSIDER':1,'AVOID':2}.get(x['rec'],3),x['daysToEarnings']))

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futs = {pool.submit(score_one,sym):sym for sym in tickers}
        for f in concurrent.futures.as_completed(futs):
            try: f.result()
            except: pass

    _cache["results"] = sorted(results,
        key=lambda x:({'RECOMMENDED':0,'CONSIDER':1,'AVOID':2}.get(x['rec'],3),x['daysToEarnings']))
    _cache['ts'] = time.time()
    _cache['running'] = False
    _cache["progress"] = {"done":len(tickers),"total":len(tickers),"phase":"done"}
    log.info('Done. %d results.', len(results))


@app.route('/api/scan')
def api_scan():
    # ALWAYS returns immediately - never blocks
    # If no data yet, kicks off background scan and returns empty
    if not _cache['running'] and not _cache['ts']:
        threading.Thread(target=run_scan, daemon=True).start()
    return jsonify({
        "results": _cache["results"],
        "count": len(_cache["results"]),
        "scannedAt": datetime.fromtimestamp(_cache["ts"]).strftime("%b %d %Y, %I:%M %p") if _cache["ts"] else None,
        "ageMinutes": int((time.time()-_cache["ts"])/60) if _cache["ts"] else 0,
        "isRefreshing": _cache["running"],
        "universe": len(_cache["universe"]),
        "progress": _cache["progress"],
    })

@app.route('/api/progress')
def api_progress():
    p = _cache["progress"]
    pct = int(p['done']/p['total']*100) if p['total']>0 else 0
    return jsonify({'done':p['done'],'total':p['total'],'pct':pct,'phase':p['phase'],'running':_cache['running']})

@app.route('/api/status')
def api_status():
    return jsonify({"status":"ok","cached":bool(_cache["ts"]),
                    "count":len(_cache["results"]),"running":_cache["running"],
                    "universe":len(_cache["universe"]),"progress":_cache["progress"]})

@app.route('/api/refresh', methods=['POST'])
def api_refresh():
    if _cache['running']: return jsonify({'message':'Scan in progress'}),202
    threading.Thread(target=run_scan, daemon=True).start()
    return jsonify({'message':'Refresh started'}),202

@app.route('/api/debug')
def api_debug():
    out={'universe':len(_cache['universe']),'results':len(_cache['results']),
         'running':_cache['running'],'progress':_cache['progress'],'debug':_cache.get('debug',{})}
    try:
        resp=requests.get('https://api.nasdaq.com/api/calendar/earnings?date='+date.today().strftime('%Y-%m-%d'),
                          headers=NASDAQ_HEADERS,timeout=5)
        rows=(resp.json().get('data') or {}).get('rows') or []
        out['nasdaq_today']={'status':resp.status_code,'rows':len(rows),'sample':rows[:2]}
    except Exception as e:
        out['nasdaq_today']={'error':str(e)}
    return jsonify(out)

@app.route('/')
def index():
    p=_cache['progress']
    return(f'VV Scanner|{len(_cache["universe"])} tickers|{len(_cache["results"])} results|'
           f'Running:{_cache["running"]}({p["done"]}/{p["total"]})|/api/debug')

if __name__=='__main__':
    app.run(host='0.0.0.0',port=5000,debug=False)
