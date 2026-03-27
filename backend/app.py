<!DOCTYPE html>

<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1.0, viewport-fit=cover"/>
  <meta name="apple-mobile-web-app-capable" content="yes"/>
  <meta name="apple-mobile-web-app-status-bar-style" content="black-translucent"/>
  <meta name="theme-color" content="#060810"/>
  <title>VV Scanner</title>
  <style>
    :root {
      --bg:      #060810;
      --card:    #0c1018;
      --border:  #161f2e;
      --borderhi:#1e2d44;
      --green:   #00e87a;
      --blue:    #38b6ff;
      --yellow:  #f5c842;
      --red:     #ff4455;
      --purple:  #a78bfa;
      --dim:     #2d3d52;
      --muted:   #506070;
      --text:    #b8ccd8;
      --bright:  #e8f2f8;
    }
    * { box-sizing:border-box; margin:0; padding:0; -webkit-tap-highlight-color:transparent; }
    body { background:var(--bg); color:var(--text); font-family:'SF Mono','Fira Code','Courier New',monospace; min-height:100vh; padding-bottom:env(safe-area-inset-bottom); }
    button { cursor:pointer; border:none; background:none; font-family:inherit; }
    input  { outline:none; font-family:inherit; }

```
@keyframes spin   { to { transform:rotate(360deg); } }
@keyframes fadeUp { from { opacity:0; transform:translateY(8px); } to { opacity:1; transform:translateY(0); } }
@keyframes pulse  { 0%,100% { opacity:1; } 50% { opacity:.3; } }

/* ── Layout ── */
#app { max-width:520px; margin:0 auto; }

/* ── Header ── */
#header {
  background:#080c12ee;
  backdrop-filter:blur(12px);
  -webkit-backdrop-filter:blur(12px);
  border-bottom:1px solid var(--border);
  padding:8px 14px 10px;
  position:sticky; top:0; z-index:100;
}
.header-row { display:flex; align-items:center; justify-content:space-between; margin-bottom:8px; }
.logo { display:flex; align-items:center; gap:8px; }
.logo-badge { width:26px; height:26px; background:linear-gradient(135deg,var(--green),var(--blue)); border-radius:6px; display:flex; align-items:center; justify-content:center; font-size:10px; font-weight:900; color:#000; flex-shrink:0; }
.logo-title { font-size:13px; font-weight:700; color:var(--bright); letter-spacing:.03em; }
.logo-sub   { font-size:9px; color:var(--muted); letter-spacing:.06em; }
.refresh-btn { background:var(--border); border:1px solid var(--dim); border-radius:7px; padding:6px 11px; color:var(--text); font-size:11px; font-weight:600; display:flex; align-items:center; gap:4px; transition:all .15s; }
.refresh-btn:active { opacity:.7; }
.spin { display:inline-block; animation:spin .8s linear infinite; }

/* Tabs */
.tabs { display:flex; gap:6px; margin-bottom:8px; }
.tab { flex:1; padding:7px 0; border-radius:7px; font-size:12px; font-weight:400; color:var(--muted); background:transparent; border:1px solid var(--border); transition:all .15s; }
.tab.active { font-weight:700; color:var(--bright); background:var(--border); border-color:var(--dim); }

/* Search */
.search-wrap { position:relative; margin-bottom:6px; }
.search-icon { position:absolute; left:10px; top:50%; transform:translateY(-50%); color:var(--dim); font-size:15px; pointer-events:none; }
#search { width:100%; background:var(--card); border:1px solid var(--border); border-radius:8px; color:var(--bright); font-size:13px; padding:8px 32px 8px 30px; transition:border-color .15s; }
#search:focus { border-color:var(--blue); }
.search-clear { position:absolute; right:10px; top:50%; transform:translateY(-50%); color:var(--muted); font-size:18px; line-height:1; }

/* Filter pills */
.filters { display:flex; gap:5px; }
.fpill { flex:1; padding:5px 2px; border-radius:6px; font-size:10px; font-weight:400; color:var(--muted); background:transparent; border:1px solid var(--border); transition:all .15s; text-align:center; white-space:nowrap; }
.fpill.active-all    { color:var(--muted);   background:#ffffff10; border-color:var(--dim); font-weight:700; }
.fpill.active-rec    { color:var(--green);   background:#00e87a15; border-color:#00e87a50; font-weight:700; }
.fpill.active-con    { color:var(--yellow);  background:#f5c84215; border-color:#f5c84250; font-weight:700; }
.fpill.active-av     { color:var(--red);     background:#ff445515; border-color:#ff445550; font-weight:700; }

/* ── Content ── */
#content { padding:12px 12px 48px; }

/* ── Loading ── */
#loading { text-align:center; padding:64px 20px; animation:fadeUp .3s ease; }
.spinner { width:36px; height:36px; border:3px solid var(--border); border-top:3px solid var(--green); border-radius:50%; animation:spin .8s linear infinite; margin:0 auto 16px; }
.load-title { font-size:13px; color:var(--green); font-weight:600; margin-bottom:10px; }
.load-step  { font-size:11px; color:var(--dim); margin-bottom:4px; }

/* ── Cards ── */
.trade-card {
  background:var(--card);
  border:1px solid var(--border);
  border-radius:12px;
  margin-bottom:10px;
  overflow:hidden;
  transition:border-color .2s, background .2s;
  animation:fadeUp .4s ease both;
  cursor:pointer;
}
.trade-card:active { transform:scale(.99); opacity:.9; }
.trade-card.rec { border-color:#003320; }
.trade-card.con { border-color:#332800; }
.trade-card.av  { border-color:#1a0208; }
.trade-card.open.rec { background:#001a0d; }
.trade-card.open.con { background:#1a1500; }
.trade-card.open.av  { background:#1a0408; }

.card-header { padding:14px 14px 12px; }
.card-top    { display:flex; align-items:flex-start; justify-content:space-between; margin-bottom:10px; }
.ticker-badge {
  border-radius:8px;
  padding:6px 10px;
  text-align:center;
  min-width:52px;
  flex-shrink:0;
}
.ticker-sym  { font-size:15px; font-weight:800; color:var(--bright); letter-spacing:.02em; }
.ticker-price{ font-size:11px; font-weight:700; }
.stock-info  { flex:1; margin:0 10px; }
.stock-name  { font-size:13px; color:var(--text); font-weight:600; margin-bottom:2px; }
.stock-sector{ font-size:11px; color:var(--muted); }
.card-right  { display:flex; flex-direction:column; align-items:flex-end; gap:4px; }
.rec-badge   { display:inline-flex; align-items:center; gap:4px; padding:4px 10px; border-radius:20px; font-size:11px; font-weight:800; letter-spacing:.06em; }
.earn-date   { font-size:11px; color:var(--muted); text-align:right; }
.earn-soon   { color:var(--yellow); font-weight:700; margin-left:4px; }

/* Condition pills */
.cond-pills { display:flex; gap:6px; flex-wrap:wrap; margin-bottom:10px; }
.cpill { display:inline-flex; align-items:center; gap:3px; padding:3px 8px; border-radius:20px; font-size:11px; font-weight:700; }
.cpill.pass { color:var(--green); background:#002010; border:1px solid #00e87a30; }
.cpill.fail { color:var(--red);   background:#1a0208; border:1px solid #ff445530; }

/* Meters */
.meters { display:flex; flex-direction:column; gap:6px; }
.meter-row { display:flex; align-items:center; gap:8px; }
.meter-label { font-size:10px; color:var(--muted); width:78px; flex-shrink:0; }
.meter-track { flex:1; background:var(--border); border-radius:4px; height:5px; overflow:hidden; }
.meter-fill  { height:100%; border-radius:4px; transition:width .6s ease; }
.meter-val   { font-size:10px; font-weight:700; width:42px; text-align:right; flex-shrink:0; }

.card-toggle { text-align:right; font-size:11px; color:var(--dim); margin-top:8px; }

/* ── Expanded panel ── */
.card-body { border-top:1px solid var(--borderhi); padding:14px 14px; animation:fadeUp .2s ease; }

.verdict {
  border-radius:8px;
  padding:10px 12px;
  margin-bottom:12px;
  font-size:12px;
  color:var(--text);
  line-height:1.6;
}

.trade-block { background:#07090e; border:1px solid var(--border); border-radius:8px; padding:12px 14px; margin-bottom:10px; }
.block-title { font-size:10px; letter-spacing:.12em; font-weight:800; text-transform:uppercase; margin-bottom:10px; }
.trade-grid  { display:grid; grid-template-columns:1fr 1fr; gap:10px; font-size:12px; color:var(--text); line-height:1.9; }
.tg-label    { font-size:10px; color:var(--muted); margin-bottom:2px; }
.tg-value    { font-weight:700; }

.timing-row { display:flex; justify-content:space-between; align-items:center; font-size:12px; padding:4px 0; }
.timing-row + .timing-row { border-top:1px solid var(--border); margin-top:4px; padding-top:8px; }
.timing-label { color:var(--muted); }

.verify-note { margin-top:10px; font-size:10px; color:var(--dim); line-height:1.6; text-align:center; }

/* ── Status bar ── */
#status-bar { text-align:center; font-size:10px; color:var(--dim); margin-top:12px; line-height:1.7; }

/* ── Error ── */
#error-box { background:#1a0208; border:1px solid #ff445540; border-radius:8px; padding:16px; text-align:center; color:var(--red); font-size:13px; }

/* ── Empty ── */
#empty { text-align:center; padding:48px 20px; color:var(--dim); }

/* ── Playbook ── */
.pb-intro { background:#001a0d; border:1px solid #00e87a22; border-left:3px solid var(--green); border-radius:8px; padding:12px 14px; margin-bottom:12px; font-size:12px; color:var(--text); line-height:1.75; }
.pb-section { background:var(--card); border:1px solid var(--border); border-radius:10px; padding:13px 14px; margin-bottom:10px; }
.pb-title { font-size:11px; letter-spacing:.1em; font-weight:800; text-transform:uppercase; margin-bottom:10px; }
.pb-row { display:flex; gap:8px; font-size:12px; color:var(--text); line-height:1.65; margin-bottom:8px; align-items:flex-start; }
.pb-bullet { font-weight:800; flex-shrink:0; }
.pb-tag { border-radius:3px; padding:1px 6px; font-size:11px; font-weight:700; margin-right:6px; }
.disclaimer { font-size:10px; color:var(--dim); text-align:center; line-height:1.7; margin-top:12px; }
```

  </style>
</head>
<body>
<div id="app">

  <!-- Header -->

  <div id="header">
    <div class="header-row">
      <div class="logo">
        <div class="logo-badge">VV</div>
        <div>
          <div class="logo-title">IV Crush Scanner</div>
          <div class="logo-sub">LIVE EARNINGS · CALENDAR SPREADS</div>
        </div>
      </div>
      <button class="refresh-btn" onclick="doRefresh()">
        <span id="refresh-icon">↻</span>
        <span id="refresh-label">Refresh</span>
      </button>
    </div>

```
<div class="tabs">
  <button class="tab active" id="tab-scan"     onclick="setTab('scan')">Scan Results</button>
  <button class="tab"        id="tab-playbook" onclick="setTab('playbook')">Playbook</button>
</div>

<div id="scan-controls">
  <div class="search-wrap">
    <span class="search-icon">⌕</span>
    <input id="search" type="text" placeholder="Filter ticker or name..." oninput="applyFilter()"/>
    <button class="search-clear" id="search-clear" style="display:none" onclick="clearSearch()">×</button>
  </div>
  <div class="filters">
    <button class="fpill active-all" id="fpill-all" onclick="setFilter('all')">All</button>
    <button class="fpill"            id="fpill-rec" onclick="setFilter('rec')">✓ Rec</button>
    <button class="fpill"            id="fpill-con" onclick="setFilter('con')">◐ Consider</button>
    <button class="fpill"            id="fpill-av"  onclick="setFilter('av')">○ Avoid</button>
  </div>
</div>
```

  </div>

  <!-- Content -->

  <div id="content">
    <div id="loading">
      <div class="spinner"></div>
      <div class="load-title" id="load-title">Scanning earnings calendar...</div>
      <div id="progress-wrap" style="display:none;margin:12px 0 8px">
        <div style="background:var(--border);border-radius:6px;height:6px;overflow:hidden;margin-bottom:6px">
          <div id="progress-bar" style="height:100%;background:linear-gradient(90deg,var(--green),var(--blue));border-radius:6px;width:0%;transition:width .5s ease"></div>
        </div>
        <div id="progress-label" style="font-size:11px;color:var(--muted);text-align:center"></div>
      </div>
      <div class="load-step">› Fetching S&P 500 + Nasdaq 100 + supplemental universe</div>
      <div class="load-step">› Checking earnings calendars for all tickers</div>
      <div class="load-step">› Pulling live options chains (8 parallel threads)</div>
      <div class="load-step">› Computing term structure slopes (threshold: ≤ -0.00406)</div>
      <div class="load-step">› Calculating IV30/RV30 ratios (threshold: ≥ 1.25)</div>
      <div class="load-step">› Checking avg volume (threshold: ≥ 1.5M shares)</div>
    </div>
    <div id="results" style="display:none"></div>
    <div id="error-box" style="display:none"></div>
    <div id="empty" style="display:none">
      <div style="font-size:28px;margin-bottom:12px;opacity:.3">⬡</div>
      <div style="font-size:13px;color:var(--muted)">No results match your filter</div>
    </div>
    <div id="status-bar" style="display:none"></div>

```
<!-- Playbook (hidden by default) -->
<div id="playbook" style="display:none">
  <div class="pb-intro">
    <strong style="color:var(--green)">Strategy: Earnings IV Crush — Long Calendar Spread</strong><br>
    Sell front-month ATM into earnings, buy back-month ATM +30 days. Profit from IV crush + smaller-than-expected move.
    Backtested: <strong style="color:var(--bright)">72,500 events, 4,500 stocks, 2007–present</strong>.
    Filtered win rate: <strong style="color:var(--green)">~66%</strong>, avg return: <strong style="color:var(--green)">7.3%/trade</strong>.
  </div>

  <div class="pb-section">
    <div class="pb-title" style="color:var(--green)">Three Entry Conditions</div>
    <div class="pb-row"><span class="pb-bullet" style="color:var(--green)">›</span><span><span class="pb-tag" style="background:#00e87a14;color:var(--green);border:1px solid #00e87a28">C1 — CRITICAL</span>Term structure slope ≤ -0.00406/day. Near-term IV significantly above 45-day IV. NOT met = AVOID always.</span></div>
    <div class="pb-row"><span class="pb-bullet" style="color:var(--blue)">›</span><span><span class="pb-tag" style="background:#38b6ff14;color:var(--blue);border:1px solid #38b6ff28">C2 — Volume</span>30-day avg volume ≥ 1,500,000 shares. More participants = more inflated premiums = larger edge.</span></div>
    <div class="pb-row"><span class="pb-bullet" style="color:var(--yellow)">›</span><span><span class="pb-tag" style="background:#f5c84214;color:var(--yellow);border:1px solid #f5c84228">C3 — IV/RV</span>IV30/RV30 ratio ≥ 1.25. Options structurally overpriced vs realized volatility.</span></div>
  </div>

  <div class="pb-section">
    <div class="pb-title" style="color:var(--blue)">Trade Execution</div>
    <div class="pb-row"><span class="pb-bullet" style="color:var(--muted)">›</span><span><span class="pb-tag" style="background:#ffffff10;color:var(--text);border:1px solid #ffffff20">Structure</span>Long Calendar Spread. Sell front-month ATM call (earnings expiry). Buy back-month ATM call same strike +30 days. Debit trade.</span></div>
    <div class="pb-row"><span class="pb-bullet" style="color:var(--muted)">›</span><span><span class="pb-tag" style="background:#ffffff10;color:var(--text);border:1px solid #ffffff20">Enter</span>15 min before market close, day BEFORE earnings (~3:45 PM ET).</span></div>
    <div class="pb-row"><span class="pb-bullet" style="color:var(--muted)">›</span><span><span class="pb-tag" style="background:#ffffff10;color:var(--text);border:1px solid #ffffff20">Exit</span>15 min after market open, day OF earnings (~9:45 AM ET). ~16-18 hr total hold.</span></div>
    <div class="pb-row"><span class="pb-bullet" style="color:var(--muted)">›</span><span><span class="pb-tag" style="background:#ffffff10;color:var(--text);border:1px solid #ffffff20">Size</span>Max 6% of portfolio per trade (10% Kelly). Max loss = full debit paid (defined risk).</span></div>
  </div>

  <div class="pb-section">
    <div class="pb-title" style="color:var(--yellow)">Signal Definitions</div>
    <div class="pb-row"><span class="pb-bullet" style="color:var(--green)">›</span><span><span class="pb-tag" style="background:#00e87a14;color:var(--green);border:1px solid #00e87a28">RECOMMENDED</span>All 3 conditions confirmed. Full position size. Execute.</span></div>
    <div class="pb-row"><span class="pb-bullet" style="color:var(--yellow)">›</span><span><span class="pb-tag" style="background:#f5c84214;color:var(--yellow);border:1px solid #f5c84228">CONSIDER</span>C1 + one other met. Half position size or skip if uncertain.</span></div>
    <div class="pb-row"><span class="pb-bullet" style="color:var(--red)">›</span><span><span class="pb-tag" style="background:#ff445514;color:var(--red);border:1px solid #ff445528">AVOID</span>C1 not met. No IV crush edge. Do not trade.</span></div>
  </div>

  <div class="pb-section">
    <div class="pb-title" style="color:var(--red)">Critical Rules</div>
    <div class="pb-row"><span class="pb-bullet" style="color:var(--red)">›</span><span><span class="pb-tag" style="background:#ff445514;color:var(--red);border:1px solid #ff445528">Exit</span>Always exit at open. Do NOT hold through the day — post-earnings drift works against the calendar.</span></div>
    <div class="pb-row"><span class="pb-bullet" style="color:var(--red)">›</span><span><span class="pb-tag" style="background:#ff445514;color:var(--red);border:1px solid #ff445528">Variance</span>34% of trades lose even when all 3 conditions met. Expected variance — stay consistent.</span></div>
    <div class="pb-row"><span class="pb-bullet" style="color:var(--red)">›</span><span><span class="pb-tag" style="background:#ff445514;color:var(--red);border:1px solid #ff445528">Sizing</span>Never exceed 6% per trade. Even with edge, oversizing will blow up the account eventually.</span></div>
  </div>

  <div class="pb-section">
    <div class="pb-title" style="color:var(--muted)">Live Verification</div>
    <div style="font-size:12px;color:var(--text);line-height:1.75">
      This app uses live data from Yahoo Finance with the exact thresholds from the VolatilityVibes Python script.<br><br>
      Thresholds used:<br>
      <span style="color:var(--green)">ts_slope_0_45 ≤ -0.00406</span><br>
      <span style="color:var(--green)">iv30_rv30 ≥ 1.25</span><br>
      <span style="color:var(--green)">avg_volume ≥ 1,500,000 shares</span><br><br>
      Data refreshes every 6 hours. Use Refresh button to force update.
    </div>
  </div>

  <div class="disclaimer">FOR EDUCATIONAL PURPOSES ONLY · NOT FINANCIAL ADVICE<br>ALWAYS VERIFY BEFORE TRADING</div>
</div>
```

  </div>
</div>

<script>
  // ── CONFIG — replace with your Render URL after deploying ────────────────
  // Example: "https://vv-scanner.onrender.com"
  const API_BASE = "YOUR_RENDER_URL_HERE";
  // ─────────────────────────────────────────────────────────────────────────

  let allResults = [];
  let activeFilter = "all";
  let activeTab = "scan";
  let expandedCards = new Set();

  // ── On load ───────────────────────────────────────────────────────────────
  window.addEventListener("DOMContentLoaded", () => { loadScan(); });

  let progressInterval = null;

  async function loadScan() {
    showLoading();
    // Start polling progress while we wait
    startProgressPoll();
    try {
      const res = await fetch(`${API_BASE}/api/scan`);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      stopProgressPoll();
      allResults = data.results || [];
      updateFilterCounts();
      renderResults();
      showStatusBar(data);
    } catch(e) {
      stopProgressPoll();
      showError(e.message);
    }
  }

  function startProgressPoll() {
    stopProgressPoll();
    progressInterval = setInterval(async () => {
      try {
        const p = await fetch(`${API_BASE}/api/progress`).then(r=>r.json());
        const wrap = document.getElementById("progress-wrap");
        const bar  = document.getElementById("progress-bar");
        const lbl  = document.getElementById("progress-label");
        const title = document.getElementById("load-title");
        if (p.total > 0) {
          wrap.style.display = "";
          bar.style.width = p.pct + "%";
          lbl.textContent = `${p.done} / ${p.total} tickers scanned (${p.pct}%)`;
          title.textContent = p.phase === "done" ? "Finalizing results..." : "Scanning earnings calendar...";
        }
      } catch(e) {}
    }, 1500);
  }

  function stopProgressPoll() {
    if (progressInterval) { clearInterval(progressInterval); progressInterval = null; }
  }

  async function doRefresh() {
    const icon  = document.getElementById("refresh-icon");
    const label = document.getElementById("refresh-label");
    icon.classList.add("spin");
    label.textContent = "...";
    showLoading();
    startProgressPoll();
    try {
      await fetch(`${API_BASE}/api/refresh`, {method:"POST"});
      // Wait for scan to finish
      for(let i=0;i<120;i++){
        await sleep(3000);
        const s = await fetch(`${API_BASE}/api/status`).then(r=>r.json());
        if(!s.running){ break; }
      }
      stopProgressPoll();
      await loadScan();
    } catch(e) {
      stopProgressPoll();
      showError(e.message);
    } finally {
      icon.classList.remove("spin");
      label.textContent = "Refresh";
    }
  }

  function sleep(ms){ return new Promise(r=>setTimeout(r,ms)); }

  // ── Tab switching ─────────────────────────────────────────────────────────
  function setTab(t) {
    activeTab = t;
    document.getElementById("tab-scan").classList.toggle("active", t==="scan");
    document.getElementById("tab-playbook").classList.toggle("active", t==="playbook");
    document.getElementById("scan-controls").style.display = t==="scan" ? "" : "none";
    document.getElementById("playbook").style.display = t==="playbook" ? "" : "none";
    if(t==="scan"){
      document.getElementById("results").style.display = allResults.length?"":"none";
      document.getElementById("status-bar").style.display = allResults.length?"":"none";
    } else {
      document.getElementById("results").style.display = "none";
      document.getElementById("loading").style.display = "none";
      document.getElementById("error-box").style.display = "none";
      document.getElementById("status-bar").style.display = "none";
      document.getElementById("empty").style.display = "none";
    }
  }

  // ── Filter & search ───────────────────────────────────────────────────────
  function setFilter(f) {
    activeFilter = f;
    ["all","rec","con","av"].forEach(id => {
      const el = document.getElementById(`fpill-${id}`);
      el.className = `fpill ${f===id ? "active-"+id : ""}`;
    });
    applyFilter();
  }

  function applyFilter() {
    const q = document.getElementById("search").value.trim().toUpperCase();
    document.getElementById("search-clear").style.display = q ? "" : "none";
    const visible = allResults.filter(r => {
      const sigOk =
        activeFilter==="all" ||
        (activeFilter==="rec" && r.rec==="RECOMMENDED") ||
        (activeFilter==="con" && r.rec==="CONSIDER") ||
        (activeFilter==="av"  && r.rec==="AVOID");
      const srchOk = !q || r.ticker.includes(q) || r.name?.toUpperCase().includes(q);
      return sigOk && srchOk;
    });
    renderCards(visible);
  }

  function clearSearch() {
    document.getElementById("search").value = "";
    applyFilter();
  }

  function updateFilterCounts() {
    const rec = allResults.filter(r=>r.rec==="RECOMMENDED").length;
    const con = allResults.filter(r=>r.rec==="CONSIDER").length;
    const av  = allResults.filter(r=>r.rec==="AVOID").length;
    document.getElementById("fpill-all").textContent = `All (${allResults.length})`;
    document.getElementById("fpill-rec").textContent = `✓ ${rec} Rec`;
    document.getElementById("fpill-con").textContent = `◐ ${con} Consider`;
    document.getElementById("fpill-av").textContent  = `○ ${av} Avoid`;
  }

  // ── Render ────────────────────────────────────────────────────────────────
  function renderResults() {
    hideLoading();
    if(!allResults.length){ showEmpty(); return; }
    applyFilter();
  }

  function renderCards(items) {
    const container = document.getElementById("results");
    document.getElementById("empty").style.display = items.length ? "none" : "";
    if(!items.length){ container.style.display="none"; return; }
    container.style.display = "";
    container.innerHTML = items.map((r, i) => cardHTML(r, i)).join("");
  }

  const REC_CFG = {
    RECOMMENDED: { color:"var(--green)",  bg:"var(--bg)", border:"#003320", icon:"●", cls:"rec" },
    CONSIDER:    { color:"var(--yellow)", bg:"var(--bg)", border:"#332800", icon:"◐", cls:"con" },
    AVOID:       { color:"var(--red)",    bg:"var(--bg)", border:"#1a0208", icon:"○", cls:"av"  },
  };

  function cardHTML(r, i) {
    const cfg = REC_CFG[r.rec] || REC_CFG.AVOID;
    const isOpen = expandedCards.has(r.ticker);
    const delay  = `${i * 0.05}s`;
    const daysLabel = r.daysToEarnings <= 7
      ? `<span class="earn-soon">(${r.daysToEarnings}d)</span>` : "";

    const meters = [
      { label:"Term Slope", val:Math.min(100,Math.abs(r.tsSlope)*24000), color:r.c1?"var(--green)":"var(--red)", note:`${r.tsSlope?.toFixed(4)}` },
      { label:"IV/RV Ratio", val:Math.min(100,(r.ivRv/2)*100),          color:r.c3?"var(--green)":"var(--red)", note:`${r.ivRv?.toFixed(2)}x` },
      { label:"Volume",      val:Math.min(100,(r.avgVol/3000000)*100),  color:r.c2?"var(--green)":"var(--red)", note:`${(r.avgVol/1e6).toFixed(1)}M` },
    ];

    return `
    <div class="trade-card ${cfg.cls} ${isOpen?"open":""}" id="card-${r.ticker}"
         onclick="toggleCard('${r.ticker}')" style="animation-delay:${delay}">
      <div class="card-header">
        <div class="card-top">
          <div style="display:flex;align-items:center;gap:10px;flex:1;min-width:0">
            <div class="ticker-badge" style="background:${cfg.color}18;border:1px solid ${cfg.color}30">
              <div class="ticker-sym">${r.ticker}</div>
              <div class="ticker-price" style="color:${cfg.color}">$${r.price?.toLocaleString()}</div>
            </div>
            <div class="stock-info">
              <div class="stock-name" style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${r.name||r.ticker}</div>
              <div class="stock-sector">${r.sector||""}</div>
            </div>
          </div>
          <div class="card-right">
            <span class="rec-badge" style="color:${cfg.color};background:${cfg.color}18;border:1px solid ${cfg.color}50">
              <span style="${r.rec==='RECOMMENDED'?'animation:pulse 1.5s infinite':''}">${cfg.icon}</span>
              ${r.rec==="RECOMMENDED"?"REC":r.rec}
            </span>
            <div class="earn-date">
              ${r.earningsDate}${daysLabel}
            </div>
          </div>
        </div>
        <div class="cond-pills">
          <span class="cpill ${r.c1?"pass":"fail"}"><span>${r.c1?"✓":"✗"}</span> Slope</span>
          <span class="cpill ${r.c2?"pass":"fail"}"><span>${r.c2?"✓":"✗"}</span> Volume</span>
          <span class="cpill ${r.c3?"pass":"fail"}"><span>${r.c3?"✓":"✗"}</span> IV/RV</span>
        </div>
        <div class="meters">
          ${meters.map(m=>`
          <div class="meter-row">
            <span class="meter-label">${m.label}</span>
            <div class="meter-track"><div class="meter-fill" style="width:${Math.round(m.val)}%;background:${m.color}"></div></div>
            <span class="meter-val" style="color:${m.color}">${m.note}</span>
          </div>`).join("")}
        </div>
        <div class="card-toggle">${isOpen?"▲ Hide trade":"▼ Show trade"}</div>
      </div>
      ${isOpen ? expandedHTML(r, cfg) : ""}
    </div>`;
  }

  function expandedHTML(r, cfg) {
    const verdictText = r.rec==="RECOMMENDED"
      ? "All 3 conditions confirmed with live data. High-probability IV crush setup — enter at recommended timing."
      : r.rec==="CONSIDER"
      ? "C1 confirmed + one other condition met. Weaker setup — use half position size or skip if uncertain."
      : "Term structure not in backwardation. No IV crush edge present for this earnings cycle. Skip.";

    return `
    <div class="card-body">
      <div class="verdict" style="background:${cfg.color}0f;border:1px solid ${cfg.color}25">
        <strong style="color:${cfg.color}">${r.rec}: </strong>${verdictText}
      </div>

      <div class="trade-block">
        <div class="block-title" style="color:var(--green)">◆ Exact Trade Entry</div>
        <div class="trade-grid">
          <div><div class="tg-label">STRUCTURE</div><div class="tg-value" style="color:var(--bright)">Long Calendar Spread</div></div>
          <div><div class="tg-label">ATM STRIKE</div><div class="tg-value" style="color:var(--bright)">$${r.strike}</div></div>
          <div><div class="tg-label" style="color:var(--red)">SELL (front)</div><div>${r.ticker} Call $${r.strike}<br><strong style="color:var(--bright)">${r.frontExp}</strong></div></div>
          <div><div class="tg-label" style="color:var(--green)">BUY (back)</div><div>${r.ticker} Call $${r.strike}<br><strong style="color:var(--bright)">${r.backExp}</strong></div></div>
          <div><div class="tg-label" style="color:var(--yellow)">EST. DEBIT</div><div class="tg-value" style="color:var(--yellow)">~$${r.debitEst}/contract</div></div>
          <div><div class="tg-label" style="color:var(--purple)">IMPL. MOVE</div><div class="tg-value" style="color:var(--purple)">±${r.expectedMove}%</div></div>
        </div>
      </div>

      <div class="trade-block">
        <div class="block-title" style="color:var(--blue)">⏱ Timing</div>
        <div class="timing-row"><span class="timing-label">Enter</span><strong style="color:var(--green)">${r.entryDate} · 3:45 PM ET</strong></div>
        <div class="timing-row"><span class="timing-label">Exit</span><strong style="color:var(--red)">${r.exitDate} · 9:45 AM ET</strong></div>
        <div class="timing-row"><span class="timing-label">Hold</span><span>~16–18 hours (overnight)</span></div>
      </div>

      <div class="trade-block">
        <div class="block-title" style="color:var(--red)">⚠ Risk & Sizing</div>
        <div class="timing-row"><span class="timing-label">Max loss</span><strong style="color:var(--red)">Full debit (defined risk)</strong></div>
        <div class="timing-row"><span class="timing-label">Position size</span><strong>Max 6% of portfolio</strong></div>
        <div class="timing-row"><span class="timing-label">Win rate</span><strong style="color:var(--green)">~66% (when all 3 met)</strong></div>
        <div class="timing-row"><span class="timing-label">Front IV</span><span>${r.frontIV}% · Back IV ${r.backIV}%</span></div>
      </div>

      <div class="verify-note">Live data via Yahoo Finance · Thresholds: slope ≤ -0.00406 · IV/RV ≥ 1.25 · vol ≥ 1.5M</div>
    </div>`;
  }

  function toggleCard(ticker) {
    if(expandedCards.has(ticker)) expandedCards.delete(ticker);
    else expandedCards.add(ticker);
    applyFilter();
  }

  // ── UI states ─────────────────────────────────────────────────────────────
  function showLoading() {
    document.getElementById("loading").style.display = "";
    document.getElementById("results").style.display = "none";
    document.getElementById("error-box").style.display = "none";
    document.getElementById("status-bar").style.display = "none";
    document.getElementById("empty").style.display = "none";
  }

  function hideLoading() {
    document.getElementById("loading").style.display = "none";
  }

  function showError(msg) {
    hideLoading();
    const el = document.getElementById("error-box");
    el.style.display = "";
    el.innerHTML = `<strong>Connection Error</strong><br><br>${msg}<br><br>
      <small style="color:var(--muted)">Make sure your backend is deployed and API_BASE is set correctly in the HTML file.</small>`;
  }

  function showEmpty() {
    document.getElementById("empty").style.display = "";
    document.getElementById("results").style.display = "none";
  }

  function showStatusBar(data) {
    const el = document.getElementById("status-bar");
    el.style.display = "";
    const refreshing = data.isRefreshing
      ? ` · <span style='color:var(--yellow)'>Refreshing... ${data.progress?.done||0}/${data.progress?.total||0}</span>`
      : "";
    el.innerHTML = `<strong style="color:var(--bright)">${data.count}</strong> upcoming earnings events · Universe: <strong style="color:var(--bright)">${data.universe}</strong> tickers scanned<br>
      Last updated: ${data.scannedAt || "—"} (${data.ageMinutes}min ago)${refreshing}<br>
      Thresholds: slope ≤ -0.00406 · IV/RV ≥ 1.25 · vol ≥ 1.5M shares`;
  }
</script>

</body>
</html>