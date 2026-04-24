// Datalogger V2 — SPA vanilla JS.
//
// Estructura (wire-up en init()):
//   - tabs, header (reloj, status, tema)
//   - panel signals: render incremental con flash al cambiar
//   - panel events: filtros + paginado + auto-refresh
//   - panel counts, sysevents
//
// Cambios clave vs. versión anterior:
//   - Render incremental en señales (preserva foco/selección, permite flash).
//   - Polling pausa cuando la tab del browser está hidden o fuera del panel correspondiente.
//   - Auto-refresh opcional en eventos (tail -f de la tabla).
//   - Presets de fecha, atajos de teclado, export XLSX/CSV, hash URL, loading, timeout.

// ---------- config ----------
const FETCH_TIMEOUT_MS = 5000;
const POLL_STATUS_MS = 2000;
const POLL_SIGNALS_MS = 1000;
const POLL_EVENTS_TAIL_MS = 2000;   // cuando auto-refresh activo
const FLASH_MS = 450;
const SEARCH_DEBOUNCE_MS = 300;

// ---------- estado global ----------
const state = {
  variables: [],           // último snapshot /api/variables (por address)
  signalFilter: 'on',      // 'all' | 'on' | 'off'
  currentTab: 'signals',
  ev: {
    page: 1,
    pageSize: 50,
    sortBy: 'id',
    sortOrder: 'desc',
    autoRefresh: true,          // por default queremos "live tail" al entrar al panel
    maxIdSeen: 0,               // para marcar con flash las filas recién aparecidas
  },
  counts: {
    data: [],
    sortBy: 'total',
    sortOrder: 'desc',
  },
  evTagOptionsInjected: false,
};

// Nodos DOM persistentes por address — evitan re-crear en cada poll.
const signalNodes = new Map();  // address -> { root, pill, desc, currentState }

// Timers
const timers = { status: null, signals: null, eventsTail: null };
const debouncers = { sig: null, ev: null, cnt: null };

// ---------- utilidades ----------
const $ = (id) => document.getElementById(id);

function fmtDateTime(iso) {
  if (!iso) return '—';
  const d = new Date(iso);
  if (isNaN(d)) return iso;
  const pad = (n) => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} `
       + `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}

function fmtClock() {
  const d = new Date();
  const pad = (n) => String(n).padStart(2, '0');
  return `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}

function qs(p) {
  const s = new URLSearchParams();
  for (const [k, v] of Object.entries(p)) if (v !== '' && v != null) s.set(k, v);
  return s.toString();
}

// Escapa antes de meter a innerHTML — evita XSS si una descripción trae <script>.
function esc(s) {
  if (s == null) return '';
  return String(s).replace(/[&<>"']/g, (c) =>
    ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}

// fetch con timeout vía AbortController — evita tabs colgadas si la red se cae.
async function api(path) {
  const t0 = performance.now();
  const ctrl = new AbortController();
  const to = setTimeout(() => ctrl.abort(), FETCH_TIMEOUT_MS);
  try {
    const r = await fetch(path, { credentials: 'same-origin', signal: ctrl.signal });
    const ms = Math.round(performance.now() - t0);
    if (r.status === 401) { window.location.replace('/login'); throw new Error('unauth'); }
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const data = await r.json();
    return { data, ms };
  } finally {
    clearTimeout(to);
  }
}

// ---------- tabs ----------
function switchTab(name) {
  state.currentTab = name;
  document.querySelectorAll('.tab').forEach((t) => {
    const active = t.dataset.tab === name;
    t.classList.toggle('active', active);
    t.setAttribute('aria-selected', active ? 'true' : 'false');
  });
  document.querySelectorAll('.panel').forEach((p) => {
    const active = p.id === `panel-${name}`;
    p.classList.toggle('active', active);
    p.hidden = !active;
  });
  // Carga datos sólo al entrar al panel, y reconfigura el polling.
  if (name === 'events')    loadEvents();
  if (name === 'counts')    loadCounts();
  if (name === 'sysevents') loadSysEvents();
  reconfigureTimers();
}

// ---------- reloj + status ----------
function tickClock() {
  $('clock').textContent = fmtClock();
}

async function pollStatus() {
  try {
    const { data, ms } = await api('/api/status');
    $('latency').textContent = `srv ${ms}ms`;
    const el = $('plc-status');
    el.classList.remove('connected', 'disconnected', 'connecting');
    if (data.link.connected) {
      el.classList.add('connected');
      const cy = data.link.last_cycle_ms != null ? ` · ${Math.round(data.link.last_cycle_ms)}ms` : '';
      el.textContent = `● PLC CONECTADO${cy}`;
      el.removeAttribute('title');
    } else if (data.link.last_error) {
      el.classList.add('disconnected');
      el.textContent = '✕ PLC DESCONECTADO';
      el.title = data.link.last_error;
    } else {
      el.classList.add('connecting');
      el.textContent = '◌ CONECTANDO...';
    }
  } catch (e) {
    const el = $('plc-status');
    el.classList.remove('connected', 'connecting');
    el.classList.add('disconnected');
    el.textContent = '✕ SIN CONEXIÓN';
    el.title = e.message || 'network';
  }
}

// ---------- signals (render incremental + flash) ----------
async function pollVariables() {
  try {
    const { data } = await api('/api/variables');
    state.variables = data;
    renderSignals();
    populateTagOptions();
  } catch (e) {
    // No silenciamos — si falla en loop, al menos marcamos la tarjeta.
    $('signal-grid').setAttribute('aria-busy', 'true');
  }
}

function populateTagOptions() {
  if (state.evTagOptionsInjected || !state.variables.length) return;
  const sel = $('ev-tag');
  const seen = new Set();
  const opts = state.variables
    .map((v) => v.symbol)
    .filter((s) => !seen.has(s) && (seen.add(s), true))
    .sort();
  for (const s of opts) {
    const o = document.createElement('option');
    o.value = s;
    o.textContent = s;
    sel.appendChild(o);
  }
  state.evTagOptionsInjected = true;
}

function setSignalFilter(m) {
  state.signalFilter = m;
  for (const k of ['all', 'on', 'off']) {
    const b = $(`btn-${k}`);
    b.classList.remove('active', 'active-on', 'active-off');
    if (m === k) {
      b.classList.add(k === 'on' ? 'active-on' : (k === 'off' ? 'active-off' : 'active'));
    }
  }
  renderSignals();
}

// Construye (o reutiliza) el nodo DOM de una señal. Vuelve a settear text
// sólo cuando cambió. Si el state cambió, aplica flash (CSS class temporal).
function upsertSignalNode(grid, v) {
  let entry = signalNodes.get(v.address);
  if (!entry) {
    const root = document.createElement('div');
    root.className = 'signal';
    root.setAttribute('role', 'listitem');
    root.innerHTML = `
      <div class="tag">
        <span class="sym"></span>
        <span class="state-pill" aria-label="estado">—</span>
      </div>
      <div class="addr"></div>
      <div class="desc"></div>`;
    grid.appendChild(root);
    entry = {
      root,
      sym: root.querySelector('.sym'),
      pill: root.querySelector('.state-pill'),
      addr: root.querySelector('.addr'),
      desc: root.querySelector('.desc'),
      currentState: undefined,
    };
    entry.sym.textContent = v.symbol;
    entry.addr.textContent = `${v.address} · ${v.type}`;
    entry.desc.textContent = v.description || '—';
    entry.desc.title = v.description || '';
    signalNodes.set(v.address, entry);
  }

  const newCls = v.state === 1 ? 'on' : (v.state == null ? 'unknown' : 'off');
  if (entry.currentState !== v.state) {
    // Clase base (sin flash).
    entry.root.classList.remove('on', 'unknown', 'off');
    entry.root.classList.add(newCls);
    entry.pill.textContent = v.state === 1 ? 'ON' : (v.state === 0 ? 'OFF' : '—');
    entry.pill.setAttribute('aria-label', `estado: ${entry.pill.textContent}`);
    // Flash sólo si no es la primera vez (initial != undefined).
    if (entry.currentState !== undefined) {
      entry.root.classList.remove('flash-on', 'flash-off');
      // Forzamos reflow para reiniciar la animación si cambia seguido.
      void entry.root.offsetWidth;
      entry.root.classList.add(v.state === 1 ? 'flash-on' : 'flash-off');
      setTimeout(() => entry.root.classList.remove('flash-on', 'flash-off'), FLASH_MS);
    }
    entry.currentState = v.state;
  }
  return entry.root;
}

function renderSignals() {
  const grid = $('signal-grid');
  const q = ($('sig-search').value || '').toLowerCase().trim();
  const filter = state.signalFilter;

  let on = 0, off = 0;
  let visible = 0;

  // Index por address de los que deberían ser visibles.
  const visibleAddrs = new Set();

  for (const v of state.variables) {
    if (v.state === 1) on++; else if (v.state === 0) off++;
    if (filter === 'on'  && v.state !== 1) continue;
    if (filter === 'off' && v.state !== 0) continue;
    if (q) {
      const hay = `${v.symbol} ${v.address} ${v.description || ''}`.toLowerCase();
      if (!hay.includes(q)) continue;
    }
    visibleAddrs.add(v.address);
    visible++;
  }

  // Fase 1: upsert + actualizar visibilidad.
  for (const v of state.variables) {
    const node = upsertSignalNode(grid, v);
    node.hidden = !visibleAddrs.has(v.address);
  }

  // Fase 2: si el catálogo bajó de tamaño (no debería), remover huérfanos.
  if (signalNodes.size > state.variables.length) {
    const keep = new Set(state.variables.map((v) => v.address));
    for (const [addr, entry] of signalNodes.entries()) {
      if (!keep.has(addr)) {
        entry.root.remove();
        signalNodes.delete(addr);
      }
    }
  }

  $('stat-total').textContent = state.variables.length;
  $('stat-on').textContent = on;
  $('stat-off').textContent = off;
  $('stat-visible').textContent = visible;
  grid.setAttribute('aria-busy', 'false');
}

// ---------- events ----------
function currentEventFilters() {
  const dtFrom = $('date-from').value;
  const dtTo   = $('date-to').value;
  return {
    search:  $('ev-search').value.trim(),
    state:   $('ev-state').value,
    symbol:  $('ev-tag').value,
    ts_from: dtFrom ? new Date(dtFrom).toISOString() : '',
    ts_to:   dtTo   ? new Date(dtTo).toISOString()   : '',
    sort_by: state.ev.sortBy,
    order:   state.ev.sortOrder,
  };
}

function setLoading(overlayId, on) {
  const el = $(overlayId);
  if (el) el.hidden = !on;
}

async function loadEvents() {
  const f = currentEventFilters();
  const params = { ...f, limit: state.ev.pageSize, offset: (state.ev.page - 1) * state.ev.pageSize };
  setLoading('events-loading', true);
  try {
    const { data } = await api('/api/events?' + qs(params));
    renderEvents(data);
    writeHash();
  } catch (e) {
    $('events-body').innerHTML = '';
    const empty = $('ev-empty');
    empty.hidden = false;
    empty.textContent = 'Error: ' + (e.message || 'fetch');
  } finally {
    setLoading('events-loading', false);
  }
}

function renderEvents({ items, total }) {
  const tb = $('events-body');
  const empty = $('ev-empty');
  if (!items.length) {
    tb.innerHTML = '';
    empty.hidden = false;
    empty.textContent = 'Sin resultados';
  } else {
    empty.hidden = true;
    // Marcamos como "nuevas" las filas cuyo id supere el máximo visto antes.
    // Primera carga: maxIdSeen=0 → ninguna fila flashea (evitamos spam inicial).
    const prevMax = state.ev.maxIdSeen;
    const firstLoad = prevMax === 0;
    let newMax = prevMax;
    tb.innerHTML = items.map((e) => {
      if (e.id > newMax) newMax = e.id;
      const flashCls = (!firstLoad && e.id > prevMax) ? ' flash-new' : '';
      return `
      <tr class="${flashCls.trim()}">
        <td class="id">${e.id}</td>
        <td class="addr">${esc(e.address)}</td>
        <td class="tag">${esc(e.symbol)}</td>
        <td><span class="state-badge ${e.state ? 'on' : 'off'}" aria-label="estado: ${e.state ? 'ON' : 'OFF'}">${e.state ? 'ON' : 'OFF'}</span></td>
        <td class="desc">${esc(e.description || '')}</td>
        <td class="ts">${esc(fmtDateTime(e.ts))}</td>
      </tr>`;
    }).join('');
    state.ev.maxIdSeen = newMax;
  }
  const from = total === 0 ? 0 : (state.ev.page - 1) * state.ev.pageSize + 1;
  const to   = Math.min(state.ev.page * state.ev.pageSize, total);
  $('pg-from').textContent = from;
  $('pg-to').textContent = to;
  $('pg-total').textContent = total;
  renderPageControls(total);
  updateLiveIndicator();
}

function renderPageControls(total) {
  const pages = Math.max(1, Math.ceil(total / state.ev.pageSize));
  const ctrls = $('page-controls');
  state.ev.page = Math.min(state.ev.page, pages);
  const btn = (label, page, extra = {}) => {
    const b = document.createElement('button');
    b.type = 'button';
    b.className = 'page-btn' + (extra.current ? ' current' : '');
    b.textContent = label;
    if (extra.disabled) b.disabled = true;
    else b.addEventListener('click', () => { state.ev.page = page; loadEvents(); });
    if (extra.current) b.setAttribute('aria-current', 'page');
    return b;
  };
  ctrls.innerHTML = '';
  ctrls.appendChild(btn('«', 1, { disabled: state.ev.page === 1 }));
  ctrls.appendChild(btn('‹', state.ev.page - 1, { disabled: state.ev.page === 1 }));
  const start = Math.max(1, state.ev.page - 2);
  const end   = Math.min(pages, start + 4);
  if (start > 1) {
    const d = document.createElement('span');
    d.className = 'page-dots'; d.textContent = '…';
    ctrls.appendChild(d);
  }
  for (let p = start; p <= end; p++) {
    ctrls.appendChild(btn(String(p), p, { current: p === state.ev.page }));
  }
  if (end < pages) {
    const d = document.createElement('span');
    d.className = 'page-dots'; d.textContent = '…';
    ctrls.appendChild(d);
  }
  ctrls.appendChild(btn('›', state.ev.page + 1, { disabled: state.ev.page === pages }));
  ctrls.appendChild(btn('»', pages, { disabled: state.ev.page === pages }));
}

function sortEvents(field) {
  if (state.ev.sortBy === field) {
    state.ev.sortOrder = state.ev.sortOrder === 'asc' ? 'desc' : 'asc';
  } else {
    state.ev.sortBy = field;
    state.ev.sortOrder = 'desc';
  }
  for (const f of ['id', 'address', 'tag', 'state', 'timestamp']) {
    const el = $(`sort-${f}`);
    if (!el) continue;
    el.textContent = (f === state.ev.sortBy) ? (state.ev.sortOrder === 'asc' ? '↑' : '↓') : '↕';
  }
  state.ev.maxIdSeen = 0;  // cambio de orden/columna → no flashear al repintar
  loadEvents();
}

// Presets de fecha: escriben en los inputs datetime-local y recargan.
function applyDatePreset(preset) {
  const to = new Date();
  let from = new Date(to);
  if (preset === '15m')   from.setMinutes(to.getMinutes() - 15);
  else if (preset === '1h')  from.setHours(to.getHours() - 1);
  else if (preset === 'today') { from = new Date(to); from.setHours(0, 0, 0, 0); }
  else if (preset === '24h') from.setDate(to.getDate() - 1);
  else if (preset === '7d')  from.setDate(to.getDate() - 7);
  $('date-from').value = toDatetimeLocal(from);
  $('date-to').value   = toDatetimeLocal(to);
  state.ev.page = 1;
  state.ev.maxIdSeen = 0;
  loadEvents();
}

function toDatetimeLocal(d) {
  const pad = (n) => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`;
}

function clearFilters() {
  $('ev-search').value = '';
  $('ev-state').value = '';
  $('ev-tag').value = '';
  $('date-from').value = '';
  $('date-to').value = '';
  state.ev.page = 1;
  state.ev.maxIdSeen = 0;
  loadEvents();
}

function exportEvents(kind) {
  const f = currentEventFilters();
  const limit = kind === 'xlsx' ? 100000 : 1000000;
  window.location.href = `/api/export.${kind}?` + qs({ ...f, limit });
}

// Auto-refresh: sólo tiene sentido en orden descendente por id y página 1 (tail).
function eventsTailEligible() {
  return state.ev.autoRefresh
      && state.currentTab === 'events'
      && state.ev.sortBy === 'id'
      && state.ev.sortOrder === 'desc'
      && state.ev.page === 1
      && !document.hidden;
}

// Pausa si el usuario está seleccionando texto (evita pisarle la selección que
// está por copiar). Chequea toda la página pero con foco en el scope del tbody.
function userIsSelecting() {
  const sel = window.getSelection && window.getSelection();
  if (!sel || sel.rangeCount === 0 || sel.isCollapsed) return false;
  const tb = $('events-body');
  if (!tb) return false;
  // True si el foco de la selección toca algo dentro de la tabla de eventos.
  return tb.contains(sel.anchorNode) || tb.contains(sel.focusNode);
}

async function tailTick() {
  if (!eventsTailEligible()) { updateLiveIndicator(); return; }
  if (userIsSelecting())     { updateLiveIndicator('paused'); return; }
  await loadEvents();
}

// Indicador visual del estado del live tail. Sólo se oculta fuera del panel
// eventos; dentro siempre muestra algo:
//   - 'live'   : tail activo y actualizando (verde, pulsa)
//   - 'paused' : pausado por el usuario o condiciones (amarillo, estático)
function updateLiveIndicator(forceState) {
  const el = $('live-indicator');
  if (!el) return;
  if (state.currentTab !== 'events') {
    el.hidden = true;
    return;
  }
  el.hidden = false;
  const eligible = eventsTailEligible();
  const mode = forceState || (state.ev.autoRefresh && eligible ? 'live' : 'paused');
  el.classList.toggle('paused', mode === 'paused');
  el.textContent = mode === 'live' ? '● LIVE' : '⏸ PAUSED';
  if (mode === 'live') {
    el.title = 'Actualizando cada 2s';
  } else if (!state.ev.autoRefresh) {
    el.title = 'Auto-refresh desactivado';
  } else if (!eligible) {
    el.title = 'Pausado: cambiá a orden id desc y página 1 para reanudar';
  } else {
    el.title = 'Pausado mientras seleccionás texto';
  }
}

// ---------- counts ----------
async function loadCounts() {
  setLoading('counts-loading', true);
  try {
    const { data } = await api('/api/stats');
    state.counts.data = data;
    renderCounts();
  } catch (e) {
    $('counts-body').innerHTML = '';
    const empty = $('cnt-empty');
    empty.hidden = false;
    empty.textContent = 'Error: ' + (e.message || 'fetch');
  } finally {
    setLoading('counts-loading', false);
  }
}

function sortCounts(field) {
  if (state.counts.sortBy === field) {
    state.counts.sortOrder = state.counts.sortOrder === 'asc' ? 'desc' : 'asc';
  } else {
    state.counts.sortBy = field;
    state.counts.sortOrder = field === 'tag' ? 'asc' : 'desc';
  }
  for (const f of ['tag', 'total', 'total_on', 'total_off', 'last_event']) {
    const el = $(`csort-${f}`);
    if (!el) continue;
    el.textContent = (f === state.counts.sortBy) ? (state.counts.sortOrder === 'asc' ? '↑' : '↓') : '↕';
  }
  renderCounts();
}

function renderCounts() {
  const q = ($('cnt-search').value || '').toLowerCase().trim();
  let rows = state.counts.data.filter((r) => {
    if (!q) return true;
    const hay = `${r.symbol} ${r.address} ${r.description || ''}`.toLowerCase();
    return hay.includes(q);
  });
  const key = state.counts.sortBy === 'tag' ? 'symbol' : state.counts.sortBy;
  const dir = state.counts.sortOrder === 'asc' ? 1 : -1;
  rows.sort((a, b) => {
    const va = a[key], vb = b[key];
    if (va == null && vb == null) return 0;
    if (va == null) return 1;
    if (vb == null) return -1;
    if (typeof va === 'number') return (va - vb) * dir;
    return String(va).localeCompare(String(vb)) * dir;
  });
  const tb = $('counts-body');
  const empty = $('cnt-empty');
  if (!rows.length) {
    tb.innerHTML = '';
    empty.hidden = false;
    empty.textContent = 'Sin datos aún';
    return;
  }
  empty.hidden = true;
  tb.innerHTML = rows.map((r) => `
    <tr>
      <td class="tag">${esc(r.symbol)} <span class="addr">${esc(r.address)}</span></td>
      <td class="id">${r.total}</td>
      <td><span class="state-badge on num" aria-label="total ON: ${r.total_on}">${r.total_on}</span></td>
      <td><span class="state-badge off num" aria-label="total OFF: ${r.total_off}">${r.total_off}</span></td>
      <td class="ts">${r.last_event ? esc(fmtDateTime(r.last_event)) : '—'}</td>
      <td class="desc">${esc(r.description || '')}</td>
    </tr>`).join('');
}

// ---------- sysevents ----------
async function loadSysEvents() {
  setLoading('sys-loading', true);
  try {
    const { data } = await api('/api/sysevents');
    const tb = $('sysev-body');
    const empty = $('sysev-empty');
    if (!data.length) {
      tb.innerHTML = '';
      empty.hidden = false;
      return;
    }
    empty.hidden = true;
    tb.innerHTML = data.map((e) => `
      <tr>
        <td><span class="sys-type ${esc(e.type)}">${esc(e.type)}</span></td>
        <td class="desc">${esc(e.description || '')}</td>
        <td class="ts">${esc(fmtDateTime(e.ts))}</td>
      </tr>`).join('');
  } catch (e) {
    // Sys events no es crítico; mostramos placeholder si falla.
    $('sysev-body').innerHTML = '';
    const empty = $('sysev-empty');
    empty.hidden = false;
    empty.textContent = 'Error: ' + (e.message || 'fetch');
  } finally {
    setLoading('sys-loading', false);
  }
}

// ---------- tema claro/oscuro ----------
function applyTheme(theme) {
  document.documentElement.dataset.theme = theme;
  const btn = $('btn-theme');
  if (btn) btn.textContent = theme === 'dark' ? '☀' : '☾';
  try { localStorage.setItem('theme', theme); } catch (_) { /* sandbox */ }
}

function initTheme() {
  let pref = null;
  try { pref = localStorage.getItem('theme'); } catch (_) {}
  if (!pref) {
    pref = window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
  }
  applyTheme(pref);
}

function toggleTheme() {
  const cur = document.documentElement.dataset.theme || 'light';
  applyTheme(cur === 'dark' ? 'light' : 'dark');
}

// ---------- hash URL (persistencia de filtros de eventos) ----------
function writeHash() {
  if (state.currentTab !== 'events') return;
  const f = currentEventFilters();
  // Sólo los que tienen valor — para no llenar la URL.
  const nonEmpty = {};
  for (const [k, v] of Object.entries(f)) {
    if (v !== '' && v != null) nonEmpty[k] = v;
  }
  nonEmpty.page = state.ev.page;
  nonEmpty.pageSize = state.ev.pageSize;
  const h = qs(nonEmpty);
  const newHash = h ? `#events?${h}` : '#events';
  if (window.location.hash !== newHash) {
    // replaceState para no ensuciar el historial en cada filtro.
    history.replaceState(null, '', newHash);
  }
}

function readHash() {
  const m = window.location.hash.match(/^#(\w+)(?:\?(.*))?$/);
  if (!m) return;
  const tab = m[1];
  if (['signals', 'events', 'counts', 'sysevents'].includes(tab)) {
    switchTab(tab);
  }
  if (tab === 'events' && m[2]) {
    const p = new URLSearchParams(m[2]);
    if (p.has('search')) $('ev-search').value = p.get('search');
    if (p.has('state'))  $('ev-state').value  = p.get('state');
    if (p.has('symbol')) $('ev-tag').value    = p.get('symbol');
    if (p.has('ts_from')) {
      const d = new Date(p.get('ts_from'));
      if (!isNaN(d)) $('date-from').value = toDatetimeLocal(d);
    }
    if (p.has('ts_to')) {
      const d = new Date(p.get('ts_to'));
      if (!isNaN(d)) $('date-to').value = toDatetimeLocal(d);
    }
    if (p.has('sort_by')) state.ev.sortBy = p.get('sort_by');
    if (p.has('order'))   state.ev.sortOrder = p.get('order');
    if (p.has('page'))     state.ev.page     = parseInt(p.get('page'), 10) || 1;
    if (p.has('pageSize')) {
      state.ev.pageSize = parseInt(p.get('pageSize'), 10) || 50;
      $('page-size').value = state.ev.pageSize;
    }
    sortEvents(state.ev.sortBy); // sincroniza flechas (llama loadEvents)
  }
}

// ---------- timers (pausan según visibilidad + tab activa) ----------
function clearTimers() {
  for (const k of Object.keys(timers)) {
    if (timers[k]) { clearInterval(timers[k]); timers[k] = null; }
  }
}

function reconfigureTimers() {
  clearTimers();
  if (document.hidden) { updateLiveIndicator(); return; }
  // status: siempre (salvo hidden) — es barato y queremos saber si el PLC se cayó.
  timers.status = setInterval(pollStatus, POLL_STATUS_MS);
  // signals: sólo cuando estamos en ese panel.
  if (state.currentTab === 'signals') {
    timers.signals = setInterval(pollVariables, POLL_SIGNALS_MS);
  }
  // events tail: sólo si el toggle está prendido y las condiciones se cumplen.
  if (state.ev.autoRefresh && state.currentTab === 'events') {
    timers.eventsTail = setInterval(tailTick, POLL_EVENTS_TAIL_MS);
  }
  updateLiveIndicator();
}

// ---------- wire-up ----------
function wireEvents() {
  // Tabs
  document.querySelectorAll('.tab').forEach((t) => {
    t.addEventListener('click', () => switchTab(t.dataset.tab));
  });

  // Tema
  $('btn-theme').addEventListener('click', toggleTheme);

  // Signals
  $('sig-search').addEventListener('input', () => {
    clearTimeout(debouncers.sig);
    debouncers.sig = setTimeout(renderSignals, SEARCH_DEBOUNCE_MS);
  });
  for (const f of ['all', 'on', 'off']) {
    $(`btn-${f}`).addEventListener('click', () => setSignalFilter(f));
  }

  // Events — filtros. Cada cambio de filtro resetea maxIdSeen (las filas del
  // siguiente render no son "nuevas", son distintas).
  const resetAndReload = () => { state.ev.page = 1; state.ev.maxIdSeen = 0; loadEvents(); };
  $('ev-search').addEventListener('input', () => {
    clearTimeout(debouncers.ev);
    debouncers.ev = setTimeout(resetAndReload, SEARCH_DEBOUNCE_MS);
  });
  $('ev-state').addEventListener('change', resetAndReload);
  $('ev-tag').addEventListener('change', resetAndReload);
  $('page-size').addEventListener('change', () => {
    state.ev.pageSize = parseInt($('page-size').value, 10) || 50;
    resetAndReload();
  });
  $('auto-refresh').addEventListener('change', (ev) => {
    state.ev.autoRefresh = ev.target.checked;
    reconfigureTimers();
    if (state.ev.autoRefresh) tailTick();
  });

  // Fechas: presets + manuales
  document.querySelectorAll('.preset').forEach((b) => {
    b.addEventListener('click', () => applyDatePreset(b.dataset.preset));
  });
  $('date-from').addEventListener('change', resetAndReload);
  $('date-to').addEventListener('change',   resetAndReload);

  // Botones
  $('btn-clear-filters').addEventListener('click', clearFilters);
  $('btn-export-xlsx').addEventListener('click', () => exportEvents('xlsx'));
  $('btn-export-csv').addEventListener('click',  () => exportEvents('csv'));

  // Sort (eventos)
  document.querySelectorAll('[data-sort]').forEach((th) => {
    th.addEventListener('click', () => sortEvents(th.dataset.sort));
  });

  // Counts
  $('cnt-search').addEventListener('input', () => {
    clearTimeout(debouncers.cnt);
    debouncers.cnt = setTimeout(renderCounts, SEARCH_DEBOUNCE_MS);
  });
  $('btn-reload-counts').addEventListener('click', loadCounts);
  document.querySelectorAll('[data-csort]').forEach((th) => {
    th.addEventListener('click', () => sortCounts(th.dataset.csort));
  });

  // Sys events
  $('btn-reload-sys').addEventListener('click', loadSysEvents);
}

function wireKeyboard() {
  document.addEventListener('keydown', (e) => {
    // Dentro de inputs no interceptamos nada salvo Esc.
    const tag = (e.target.tagName || '').toUpperCase();
    const inInput = tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT';

    if (e.key === 'Escape') {
      if (state.currentTab === 'events') clearFilters();
      if (inInput) e.target.blur();
      return;
    }

    if (inInput) return;

    if (e.key >= '1' && e.key <= '4') {
      const map = { '1': 'signals', '2': 'events', '3': 'counts', '4': 'sysevents' };
      switchTab(map[e.key]);
      e.preventDefault();
      return;
    }
    if (e.key === '/') {
      const inputId = (
        state.currentTab === 'signals' ? 'sig-search' :
        state.currentTab === 'events'  ? 'ev-search'  :
        state.currentTab === 'counts'  ? 'cnt-search' : null
      );
      if (inputId) {
        $(inputId).focus();
        e.preventDefault();
      }
      return;
    }
    if (e.key === 't' || e.key === 'T') {
      toggleTheme();
    }
  });
}

function wireVisibility() {
  document.addEventListener('visibilitychange', reconfigureTimers);
  window.addEventListener('pagehide', clearTimers);
}

// ---------- init ----------
function init() {
  initTheme();
  wireEvents();
  wireKeyboard();
  wireVisibility();

  tickClock();
  setInterval(tickClock, 1000);

  // Primer snapshot inmediato (sin esperar intervalo).
  pollStatus();
  pollVariables();

  // Si el hash trae tab/filtros, los aplicamos ahora.
  readHash();

  // Timers configurados según visibilidad + tab activa.
  reconfigureTimers();
  window.addEventListener('hashchange', readHash);
}

document.addEventListener('DOMContentLoaded', init);
