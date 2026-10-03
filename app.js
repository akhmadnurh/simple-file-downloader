'use strict';

const express = require('express');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const { spawn } = require('child_process');
const { Readable, PassThrough } = require('stream');
const { pipeline } = require('stream/promises');

const PORT = Number(process.env.PORT) || 5000;
// Whitelist direktori tujuan. Ubah lewat env DOWNLOAD_DIRS (pisah koma).
const DIRS = (process.env.DOWNLOAD_DIRS || '/mnt/jellyfin').split(',').map(s => s.trim()).filter(Boolean);

// Riwayat terminal ditulis ke JSONL agar tetap ada setelah server restart.
const HISTORY_FILE = process.env.HISTORY_FILE || path.join(__dirname, 'data', 'history.jsonl');
const HISTORY_LIMIT = 200;

const downloads = new Map();
const history = new Map(); // snapshot terminal per id — sumber data halaman riwayat
const TERMINAL = new Set(['done', 'error', 'canceled']);
const STATE_LABEL = {
  pending: 'Menunggu',
  downloading: 'Mengunduh',
  extracting: 'Mengekstrak',
  done: 'Selesai',
  error: 'Gagal',
  canceled: 'Dibatalkan',
};

try {
  for (const line of fs.readFileSync(HISTORY_FILE, 'utf8').split('\n')) {
    if (!line.trim()) continue;
    try {
      const s = JSON.parse(line);
      if (s && s.id) history.set(s.id, s);
    } catch (_) { /* baris rusak di-skip */ }
  }
} catch (_) { /* file belum ada */ }

// Diserialkan: append dan rewrite tidak boleh saling tabrakan.
let histQ = Promise.resolve();
function histEnqueue(job) {
  histQ = histQ.then(job).catch(err => log('history-io-error', null, { error: String(err) }));
}
function histAppend(snap) {
  histEnqueue(() => fs.promises.mkdir(path.dirname(HISTORY_FILE), { recursive: true })
    .then(() => fs.promises.appendFile(HISTORY_FILE, JSON.stringify(snap) + '\n')));
}
function histRewrite() {
  const data = Array.from(history.values()).map(s => JSON.stringify(s)).join('\n');
  histEnqueue(() => fs.promises.mkdir(path.dirname(HISTORY_FILE), { recursive: true })
    .then(() => fs.promises.writeFile(HISTORY_FILE, data ? data + '\n' : '')));
}
function persistHistory(rec) {
  if (!TERMINAL.has(rec.state) || rec.histSaved) return;
  rec.histSaved = true;
  const snap = snapshot(rec);
  history.set(snap.id, snap);
  histAppend(snap);
}

const app = express();
app.disable('x-powered-by');
app.use(express.json());

function log(ev, rec, extra) {
  console.log(JSON.stringify(Object.assign(
    { t: new Date().toISOString(), ev, id: rec && rec.id },
    extra || {}
  )));
}

function snapshot(rec) {
  let percentage = null;
  if (rec.state === 'done') percentage = 100;
  else if (rec.total) percentage = Math.min(100, Math.floor((rec.downloaded / rec.total) * 100));
  return {
    id: rec.id,
    url: rec.url,
    filename: rec.filename,
    directory: rec.directory,
    state: rec.state,
    percentage,
    downloaded: rec.downloaded,
    total: rec.total,
    speed: Math.round(rec.speed),
    eta: rec.eta,
    error: rec.error,
    createdAt: rec.createdAt,
  };
}

function broadcast(rec) {
  if (!rec.sse.size) return;
  const msg = 'data: ' + JSON.stringify(snapshot(rec)) + '\n\n';
  for (const res of rec.sse) {
    try { res.write(msg); } catch (_) { rec.sse.delete(res); }
  }
  if (TERMINAL.has(rec.state)) {
    for (const res of rec.sse) { try { res.end(); } catch (_) {} }
    rec.sse.clear();
  }
}

function stopTicking(rec) {
  if (rec.timer) clearInterval(rec.timer);
  rec.timer = null;
  rec.speed = 0;
  rec.eta = null;
}

function finish(rec) {
  stopTicking(rec);
  broadcast(rec);
  persistHistory(rec);
}

function startTicking(rec) {
  rec.lastBytes = 0;
  rec.lastAt = Date.now();
  rec.timer = setInterval(() => {
    const now = Date.now();
    const dt = (now - rec.lastAt) / 1000;
    if (dt > 0) {
      rec.speed = (rec.downloaded - rec.lastBytes) / dt;
      rec.lastBytes = rec.downloaded;
      rec.lastAt = now;
      rec.eta = rec.total && rec.speed > 0
        ? Math.max(0, Math.round((rec.total - rec.downloaded) / rec.speed))
        : null;
    }
    broadcast(rec);
  }, 1000);
}

function cleanupPart(rec) {
  if (rec.partPath) fs.promises.unlink(rec.partPath).catch(() => {});
}

function fail(rec, message) {
  rec.state = 'error';
  rec.error = message;
  log('error', rec, { error: message });
  finish(rec);
}

// Batas sanitasi nama file: tanpa path separator (cegah traversal), tanpa kontrol.
function safeName(name) {
  let base = String(name).replace(/[\x00-\x1f<>:"\\|?*]/g, '_').trim();
  if (!base || base === '.' || base === '..') return 'download';
  return base.slice(0, 200);
}

function fromDisposition(cd) {
  if (!cd) return null;
  let m = /filename\*\s*=\s*(?:UTF-8'')?"?([^";]+)"?/i.exec(cd);
  if (m) {
    try { return decodeURIComponent(m[1].trim()); } catch (_) { /* fallthrough */ }
  }
  m = /filename\s*=\s*"?([^";]+)"?/i.exec(cd);
  return m ? m[1].trim() : null;
}

function basenameFromUrl(rawUrl) {
  try {
    const p = decodeURIComponent(new URL(rawUrl).pathname);
    const base = p.split('/').filter(Boolean).pop();
    return base || null;
  } catch (_) {
    return null;
  }
}

async function resolveTarget(dir, name) {
  if (!fs.existsSync(path.join(dir, name))) return path.join(dir, name);
  const ext = path.extname(name);
  const stem = ext ? name.slice(0, -ext.length) : name;
  for (let i = 1; ; i++) {
    const cand = path.join(dir, `${stem} (${i})${ext}`);
    if (!fs.existsSync(cand)) return cand;
  }
}

function extractZip(rec, zipPath) {
  return new Promise(resolve => {
    const child = spawn('unzip', ['-o', '-q', zipPath, '-d', path.dirname(zipPath)]);
    let stderr = '';
    child.stderr.on('data', d => { stderr += d; });
    child.on('error', err => {
      fail(rec, `Perintah unzip tidak tersedia: ${err.message}`);
      resolve();
    });
    child.on('close', async code => {
      if (TERMINAL.has(rec.state)) { resolve(); return; } // sudah di-cancel
      if (code === 0) {
        if (rec.deleteAfter) await fs.promises.unlink(zipPath).catch(() => {});
        rec.state = 'done';
        log('done', rec, { extracted: true });
        finish(rec);
      } else {
        fail(rec, `Unzip gagal (kode ${code})${stderr ? ': ' + stderr.trim().slice(0, 200) : ''}`);
      }
      resolve();
    });
  });
}

async function runDownload(rec) {
  try {
    const res = await fetch(rec.url, { signal: rec.controller.signal });
    if (res.status >= 400) throw new Error(`Server merespons HTTP ${res.status}`);
    const ct = (res.headers.get('content-type') || '').toLowerCase();
    if (ct.includes('text/html')) throw new Error('URL bukan file download (server merespons HTML)');
    if (!res.body) throw new Error('Server tidak mengirim isi file');

    if (!rec.manualName) {
      rec.filename = safeName(
        fromDisposition(res.headers.get('content-disposition')) ||
        basenameFromUrl(rec.url) ||
        'download'
      );
    } else {
      rec.filename = safeName(rec.manualName);
    }
    const len = Number(res.headers.get('content-length'));
    rec.total = Number.isFinite(len) && len > 0 ? len : null;

    await fs.promises.mkdir(rec.directory, { recursive: true });
    const target = await resolveTarget(rec.directory, rec.filename);
    rec.filename = path.basename(target);
    rec.partPath = target + '.part';

    rec.state = 'downloading';
    startTicking(rec);
    log('start', rec, { url: rec.url, target });
    broadcast(rec);

    // Byte counting via meter stream; pipeline menangani backpressure + error disk.
    const meter = new PassThrough();
    meter.on('data', chunk => { rec.downloaded += chunk.length; });
    await pipeline(Readable.fromWeb(res.body), meter, fs.createWriteStream(rec.partPath));

    stopTicking(rec);
    if (TERMINAL.has(rec.state)) { cleanupPart(rec); return; }
    await fs.promises.rename(rec.partPath, target);
    rec.partPath = null;

    if (path.extname(rec.filename).toLowerCase() === '.zip') {
      rec.state = 'extracting';
      broadcast(rec);
      await extractZip(rec, target);
    } else {
      rec.state = 'done';
      log('done', rec, { bytes: rec.downloaded });
      finish(rec);
    }
  } catch (err) {
    cleanupPart(rec);
    if (TERMINAL.has(rec.state)) return; // sudah di-cancel via endpoint
    if (rec.controller.signal.aborted) {
      rec.state = 'canceled';
      log('canceled', rec);
    } else {
      rec.state = 'error';
      rec.error = String(err.message || err);
      log('error', rec, { error: rec.error });
    }
    finish(rec);
  }
}

app.get('/', (req, res) => {
  res.type('html').send(PAGE);
});

app.post('/download', (req, res) => {
  const body = req.body || {};
  const url = typeof body.url === 'string' ? body.url.trim() : '';
  if (!url) return res.status(400).json({ error: 'URL wajib diisi' });
  let parsed;
  try { parsed = new URL(url); } catch (_) {
    return res.status(400).json({ error: 'URL tidak valid' });
  }
  if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') {
    return res.status(400).json({ error: 'URL harus http atau https' });
  }
  const dir = body.directory || DIRS[0];
  if (!DIRS.includes(dir)) {
    return res.status(400).json({ error: 'Direktori tidak diizinkan' });
  }
  const rec = {
    id: crypto.randomUUID(),
    url: parsed.toString(),
    directory: dir,
    manualName: typeof body.filename === 'string' && body.filename.trim() ? body.filename.trim() : null,
    deleteAfter: body.deleteAfter !== false,
    filename: null,
    state: 'pending',
    downloaded: 0,
    total: null,
    speed: 0,
    eta: null,
    error: null,
    createdAt: Date.now(),
    controller: new AbortController(),
    timer: null,
    partPath: null,
    sse: new Set(),
  };
  downloads.set(rec.id, rec);
  runDownload(rec).catch(err => console.error('runDownload fatal:', err));
  res.json({ downloadId: rec.id });
});

app.get('/downloads', (req, res) => {
  const list = [];
  downloads.forEach(rec => { if (!rec.hidden) list.push(snapshot(rec)); });
  list.sort((a, b) => b.createdAt - a.createdAt);
  res.json(list);
});

// Sembunyikan record terminal dari layar utama (tetap ada di riwayat).
app.post('/downloads/:id/hide', (req, res) => {
  const rec = downloads.get(req.params.id);
  if (!rec) return res.status(404).json({ error: 'Download tidak ditemukan' });
  if (!TERMINAL.has(rec.state)) return res.status(400).json({ error: 'Masih berjalan, batalkan dulu' });
  rec.hidden = true;
  res.json({ ok: true });
});

// Riwayat: gabungan record terminal di memory + yang sudah di-persist ke JSONL.
app.get('/history-data', (req, res) => {
  const seen = new Set();
  const list = [];
  downloads.forEach(rec => {
    if (!TERMINAL.has(rec.state)) return;
    seen.add(rec.id);
    list.push(snapshot(rec));
  });
  history.forEach((snap, id) => {
    if (!seen.has(id)) list.push(snap);
  });

  const q = String(req.query.q || '').toLowerCase();
  const state = String(req.query.state || '');
  const from = req.query.from ? Date.parse(req.query.from + 'T00:00:00') : NaN;
  const to = req.query.to ? Date.parse(req.query.to + 'T23:59:59.999') : NaN;

  let out = list;
  if (q) out = out.filter(s => (s.filename || '').toLowerCase().includes(q) || s.url.toLowerCase().includes(q));
  if (state) out = out.filter(s => s.state === state);
  if (req.query.from) out = Number.isFinite(from) ? out.filter(s => s.createdAt >= from) : [];
  if (req.query.to) out = Number.isFinite(to) ? out.filter(s => s.createdAt <= to) : [];

  out.sort((a, b) => b.createdAt - a.createdAt);
  res.json(out.slice(0, HISTORY_LIMIT));
});

app.get('/history', (req, res) => {
  res.type('html').send(PAGE_HISTORY);
});

app.get('/status/:id', (req, res) => {
  const rec = downloads.get(req.params.id);
  if (!rec) return res.status(404).json({ error: 'Download tidak ditemukan' });
  res.json(snapshot(rec));
});

app.get('/progress/:id', (req, res) => {
  const rec = downloads.get(req.params.id);
  if (!rec) return res.status(404).json({ error: 'Download tidak ditemukan' });
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    Connection: 'keep-alive',
  });
  res.write('retry: 3000\n\n');
  res.write('data: ' + JSON.stringify(snapshot(rec)) + '\n\n');
  if (TERMINAL.has(rec.state)) {
    res.end();
    return;
  }
  rec.sse.add(res);
  res.on('error', () => rec.sse.delete(res));
  req.on('close', () => rec.sse.delete(res));
});

app.post('/cancel/:id', (req, res) => {
  const rec = downloads.get(req.params.id);
  if (!rec) return res.status(404).json({ error: 'Download tidak ditemukan' });
  if (TERMINAL.has(rec.state)) return res.status(400).json({ error: 'Download sudah berakhir' });
  rec.state = 'canceled';
  rec.controller.abort();
  log('cancel-request', rec);
  finish(rec);
  res.json({ ok: true });
});

app.delete('/downloads/:id', (req, res) => {
  const rec = downloads.get(req.params.id);
  if (rec && !TERMINAL.has(rec.state)) {
    return res.status(400).json({ error: 'Masih berjalan, batalkan dulu' });
  }
  const hadRuntime = downloads.delete(req.params.id);
  const hadHist = history.delete(req.params.id);
  if (!hadRuntime && !hadHist) {
    return res.status(404).json({ error: 'Download tidak ditemukan' });
  }
  if (hadHist) histRewrite();
  res.json({ ok: true });
});

const COMMON_CSS = `* { box-sizing: border-box; }
body { margin: 0; font-family: system-ui, -apple-system, "Segoe UI", Roboto, sans-serif;
  background: #f4f5f7; color: #1f2937; font-size: 16px; line-height: 1.45; }
.wrap { max-width: 640px; margin: 0 auto; padding: 20px 16px 48px; }
h1 { font-size: 1.35rem; margin: 0 0 16px; font-weight: 650; }
.head { display: flex; justify-content: space-between; align-items: baseline; margin-bottom: 16px; }
.head h1 { margin: 0; }
.navlink { color: #2563eb; font-weight: 600; text-decoration: none; font-size: .95rem; }
.card { background: #fff; border: 1px solid #e4e6eb; border-radius: 12px; padding: 14px 16px;
  margin-bottom: 12px; box-shadow: 0 1px 2px rgba(0,0,0,.04); }
input, select { width: 100%; padding: 10px 12px; font-size: 16px; border: 1px solid #d1d5db;
  border-radius: 8px; background: #fff; color: inherit; font-family: inherit; }
input:focus, select:focus { outline: 2px solid rgba(37,99,235,.35); border-color: #2563eb; }
.row { display: flex; justify-content: space-between; align-items: flex-start; gap: 8px; }
.fname { font-weight: 600; word-break: break-all; }
.url { font-size: .78rem; color: #9ca3af; word-break: break-all; margin-top: 2px; }
.badge { flex-shrink: 0; font-size: .7rem; font-weight: 700; padding: 3px 9px; border-radius: 99px;
  text-transform: uppercase; letter-spacing: .03em; }
.badge.pending { background: #f3f4f6; color: #4b5563; }
.badge.downloading { background: #dbeafe; color: #1d4ed8; }
.badge.extracting { background: #fef3c7; color: #b45309; }
.badge.done { background: #dcfce7; color: #15803d; }
.badge.error { background: #fee2e2; color: #b91c1c; }
.badge.canceled { background: #f3f4f6; color: #6b7280; }
.meta { font-size: .85rem; color: #6b7280; font-variant-numeric: tabular-nums; }
.err { color: #b91c1c; font-size: .87rem; margin-top: 8px; word-break: break-word; }
.acts { display: flex; gap: 8px; margin-top: 10px; }
.acts button { flex: 1; padding: 9px; font-size: .9rem; border-radius: 8px; border: 1px solid #d1d5db;
  background: #fff; cursor: pointer; font-family: inherit; color: #374151; }
.acts button.danger { color: #b91c1c; border-color: #fecaca; }
.empty { color: #9ca3af; text-align: center; padding: 32px 0; font-size: .95rem; }
.list-head { font-size: .8rem; font-weight: 700; text-transform: uppercase; letter-spacing: .05em;
  color: #6b7280; margin: 24px 0 10px; }
dialog { border: 1px solid #e4e6eb; border-radius: 12px; padding: 18px 18px 14px; max-width: 340px;
  width: calc(100% - 32px); font: inherit; color: inherit; box-shadow: 0 10px 30px rgba(0,0,0,.2); }
dialog::backdrop { background: rgba(17,24,39,.45); }
#confirmMsg { margin: 0 0 16px; font-weight: 600; }
.dlg-acts { display: flex; gap: 8px; margin: 0; }
.dlg-acts button { flex: 1; padding: 10px; font-size: .95rem; border-radius: 8px; border: 1px solid #d1d5db;
  background: #fff; cursor: pointer; font-family: inherit; color: #374151; }
.dlg-acts button[value="ok"] { color: #b91c1c; border-color: #fecaca; background: #fef2f2; font-weight: 600; }
@media (max-width: 480px) { .wrap { padding: 14px 12px 40px; } }
`;

const PAGE = `<!doctype html>
<html lang="id">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Download Manager</title>
<style>
${COMMON_CSS}
.field { margin-bottom: 12px; }
label { display: block; font-size: .85rem; font-weight: 600; color: #4b5563; margin-bottom: 4px; }
label.check { display: flex; align-items: center; gap: 8px; font-weight: 500; margin: 12px 0 16px; }
label.check input { width: auto; }
button.primary { width: 100%; padding: 12px; font-size: 16px; font-weight: 600; color: #fff;
  background: #2563eb; border: 0; border-radius: 8px; cursor: pointer; font-family: inherit; }
button.primary:active { background: #1d4ed8; }
button.primary:disabled { opacity: .6; }
.error-box { margin-top: 10px; padding: 10px 12px; background: #fef2f2; border: 1px solid #fecaca;
  color: #b91c1c; border-radius: 8px; font-size: .9rem; }
.bar { height: 10px; background: #e5e7eb; border-radius: 99px; overflow: hidden; margin: 10px 0 6px; }
.bar .fill { height: 100%; width: 0%; background: #2563eb; border-radius: 99px; transition: width .5s ease; }
.bar .fill.s-done { background: #16a34a; }
.bar .fill.s-error { background: #dc2626; }
.bar .fill.s-canceled { background: #9ca3af; }
.bar.indeterminate .fill { width: 40% !important; animation: slide 1.2s ease-in-out infinite; }
@keyframes slide { 0% { transform: translateX(-100%); } 100% { transform: translateX(270%); } }
</style>
</head>
<body>
<div class="wrap">
  <div class="head">
    <h1>Download Manager</h1>
    <a class="navlink" href="/history">Riwayat</a>
  </div>
  <form id="form" class="card">
    <div class="field">
      <label for="url">URL file</label>
      <input id="url" name="url" type="url" placeholder="https://example.com/file.zip" required
        autocomplete="off" spellcheck="false">
    </div>
    <div class="field">
      <label for="name">Nama file (opsional, kosongkan untuk auto)</label>
      <input id="name" name="filename" type="text" placeholder="otomatis dari URL" autocomplete="off">
    </div>
    <div class="field">
      <label for="dir">Direktori tujuan</label>
      <select id="dir" name="directory"></select>
    </div>
    <label class="check">
      <input id="delzip" type="checkbox" checked>
      Hapus zip setelah di-extract
    </label>
    <button id="go" class="primary" type="submit">Download</button>
    <div id="formErr" class="error-box" hidden></div>
  </form>
  <div id="empty" class="empty">Belum ada download.</div>
  <div id="activeHead" class="list-head" hidden>Antrean</div>
  <div id="activeList"></div>
  <div id="doneHead" class="list-head" hidden>Selesai</div>
  <div id="doneList"></div>
</div>

<dialog id="confirmDlg">
  <p id="confirmMsg"></p>
  <form method="dialog" class="dlg-acts">
    <button id="dlgCancel" value="cancel">Lanjutkan</button>
    <button id="dlgOk" value="ok">Ya, Batalkan</button>
  </form>
</dialog>

<script>
var STATE = ${JSON.stringify(STATE_LABEL)};
var DIRS = ${JSON.stringify(DIRS)};
var cards = new Map();
var activeList = document.getElementById('activeList');
var doneList = document.getElementById('doneList');
var activeHead = document.getElementById('activeHead');
var doneHead = document.getElementById('doneHead');
var emptyBox = document.getElementById('empty');
var confirmDlg = document.getElementById('confirmDlg');
var confirmMsg = document.getElementById('confirmMsg');

var dirSel = document.getElementById('dir');
DIRS.forEach(function (d) {
  var opt = document.createElement('option');
  opt.value = d;
  opt.textContent = d;
  dirSel.appendChild(opt);
});

function el(tag, cls, text) {
  var e = document.createElement(tag);
  if (cls) e.className = cls;
  if (text != null) e.textContent = text;
  return e;
}

function fmtBytes(n) {
  if (n == null) return '—';
  var u = ['B', 'KB', 'MB', 'GB', 'TB'], i = 0, v = n;
  while (v >= 1024 && i < u.length - 1) { v /= 1024; i++; }
  return (i === 0 ? Math.round(v) : v.toFixed(1)) + ' ' + u[i];
}

function fmtEta(s) {
  if (s == null) return '';
  s = Math.max(0, Math.round(s));
  if (s < 60) return s + ' dtk';
  var m = Math.floor(s / 60);
  if (m < 60) return m + ' mnt';
  return Math.floor(m / 60) + ' j ' + (m % 60) + ' mnt';
}

function isTerminal(state) {
  return state === 'done' || state === 'error' || state === 'canceled';
}

function checkEmpty() {
  var a = activeList.children.length;
  var d = doneList.children.length;
  emptyBox.hidden = a + d > 0;
  activeHead.hidden = a === 0;
  doneHead.hidden = d === 0;
}

// Modal konfirmasi generik. resolve(true) jika user memilih aksi berbahaya.
function askConfirm(msg, okLabel) {
  return new Promise(function (resolve) {
    confirmMsg.textContent = msg;
    document.getElementById('dlgOk').textContent = okLabel || 'Ya, Batalkan';
    if (typeof confirmDlg.showModal !== 'function') {
      resolve(window.confirm(msg));
      return;
    }
    confirmDlg.returnValue = 'cancel';
    function onClose() {
      confirmDlg.removeEventListener('close', onClose);
      resolve(confirmDlg.returnValue === 'ok');
    }
    confirmDlg.addEventListener('close', onClose);
    confirmDlg.showModal();
  });
}

function updateCard(c, rec) {
  var r = c.refs;
  r.name.textContent = rec.filename || '…';
  r.badge.className = 'badge ' + rec.state;
  r.badge.textContent = STATE[rec.state] || rec.state;
  r.url.textContent = rec.url;
  var pct = rec.percentage;
  if (pct == null && (rec.state === 'downloading' || rec.state === 'extracting')) {
    r.bar.classList.add('indeterminate');
  } else {
    r.bar.classList.remove('indeterminate');
    r.fill.style.width = (pct == null ? 0 : pct) + '%';
  }
  r.fill.className = 'fill s-' + rec.state;
  var parts = [];
  if (rec.state === 'extracting') {
    parts.push('Mengekstrak file…');
  } else if (rec.state === 'done') {
    parts.push('Selesai' + (rec.total != null ? ' · ' + fmtBytes(rec.total) : ''));
  } else if (rec.state === 'canceled') {
    parts.push('Dibatalkan');
  } else {
    if (rec.total != null) {
      parts.push(fmtBytes(rec.downloaded) + ' / ' + fmtBytes(rec.total) +
        (pct != null ? ' (' + pct + '%)' : ''));
    } else if (rec.downloaded) {
      parts.push(fmtBytes(rec.downloaded) + ' terunduh');
    }
    if (rec.state === 'downloading') {
      if (rec.speed > 0) parts.push(fmtBytes(rec.speed) + '/s');
      if (rec.eta != null) parts.push('sisa ' + fmtEta(rec.eta));
    }
  }
  r.meta.textContent = parts.join(' · ');
  r.err.hidden = !rec.error;
  r.err.textContent = rec.error || '';
  var active = rec.state === 'pending' || rec.state === 'downloading';
  r.cancel.hidden = !active;
  r.hide.hidden = !isTerminal(rec.state);
  r.acts.style.display = (r.cancel.hidden && r.hide.hidden) ? 'none' : '';

  // Kartu terminal pindah dari Antrean ke bagian Selesai (sekali saja).
  if (isTerminal(rec.state) && c.el.parentNode === activeList) {
    doneList.prepend(c.el);
    checkEmpty();
  }
}

function attachSSE(c, id) {
  if (c.es) return;
  var es = new EventSource('/progress/' + id);
  c.es = es;
  es.onmessage = function (ev) {
    var data;
    try { data = JSON.parse(ev.data); } catch (_) { return; }
    updateCard(c, data);
    if (isTerminal(data.state)) {
      es.close();
      c.es = null;
    }
  };
  es.onerror = function () {
    // reconnect otomatis oleh EventSource; tutup kalau entri sudah hilang
    fetch('/status/' + id).then(function (r) {
      if (!r.ok) { es.close(); c.es = null; }
    }).catch(function () {});
  };
}

function addOrUpdate(rec) {
  var c = cards.get(rec.id);
  if (!c) {
    var root = el('div', 'card');
    var row = el('div', 'row');
    var name = el('div', 'fname', rec.filename || '…');
    var badge = el('span', 'badge ' + rec.state, STATE[rec.state] || rec.state);
    row.appendChild(name);
    row.appendChild(badge);
    var url = el('div', 'url', rec.url);
    var bar = el('div', 'bar');
    var fill = el('div', 'fill');
    bar.appendChild(fill);
    var meta = el('div', 'meta', '');
    var err = el('div', 'err');
    err.hidden = true;
    var acts = el('div', 'acts');
    var cancelBtn = el('button', 'danger', 'Batalkan');
    cancelBtn.type = 'button';
    var hideBtn = el('button', '', 'Tutup');
    hideBtn.type = 'button';
    acts.appendChild(cancelBtn);
    acts.appendChild(hideBtn);
    root.appendChild(row);
    root.appendChild(url);
    root.appendChild(bar);
    root.appendChild(meta);
    root.appendChild(err);
    root.appendChild(acts);
    c = {
      el: root,
      es: null,
      refs: { name: name, badge: badge, url: url, bar: bar, fill: fill, meta: meta,
        err: err, acts: acts, cancel: cancelBtn, hide: hideBtn },
    };
    cancelBtn.onclick = function () {
      askConfirm('Batalkan download ' + (name.textContent || '') + '? File parsial akan dihapus.')
        .then(function (ok) {
          if (!ok) return;
          cancelBtn.disabled = true;
          fetch('/cancel/' + rec.id, { method: 'POST' }).then(function (resp) {
            if (!resp.ok) cancelBtn.disabled = false;
          }).catch(function () {
            cancelBtn.disabled = false;
          });
        });
    };
    hideBtn.onclick = function () {
      hideBtn.disabled = true;
      fetch('/downloads/' + rec.id + '/hide', { method: 'POST' })
        .then(function (resp) {
          if (!resp.ok) throw new Error('HTTP ' + resp.status);
          if (c.es) { c.es.close(); c.es = null; }
          root.remove();
          cards.delete(rec.id);
          checkEmpty();
        })
        .catch(function () { hideBtn.disabled = false; });
    };
    cards.set(rec.id, c);
    (isTerminal(rec.state) ? doneList : activeList).prepend(root);
    checkEmpty();
  }
  updateCard(c, rec);
  if (!isTerminal(rec.state)) attachSSE(c, rec.id);
}

document.getElementById('form').addEventListener('submit', async function (e) {
  e.preventDefault();
  var errBox = document.getElementById('formErr');
  errBox.hidden = true;
  var btn = document.getElementById('go');
  btn.disabled = true;
  try {
    var body = {
      url: document.getElementById('url').value.trim(),
      filename: document.getElementById('name').value.trim() || null,
      directory: document.getElementById('dir').value,
      deleteAfter: document.getElementById('delzip').checked,
    };
    var r = await fetch('/download', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    var d = await r.json();
    if (!r.ok) {
      errBox.textContent = d.error || 'Gagal memulai download';
      errBox.hidden = false;
      return;
    }
    var rec = await (await fetch('/status/' + d.downloadId)).json();
    addOrUpdate(rec);
    document.getElementById('url').value = '';
    document.getElementById('name').value = '';
  } catch (e) {
    console.error('submit error:', e);
    errBox.textContent = e instanceof TypeError
      ? 'Tidak bisa terhubung ke server'
      : 'Terjadi kesalahan pada halaman: ' + e.message;
    errBox.hidden = false;
  } finally {
    btn.disabled = false;
  }
});

(async function init() {
  try {
    var list = await (await fetch('/downloads')).json();
    // Server kirim terbaru-dulu; prepend tiap kartu → balik jadi terbaru-di-atas per bagian.
    list.slice().reverse().forEach(addOrUpdate);
  } catch (_) {}
  checkEmpty();
})();
</script>
</body>
</html>`;

const PAGE_HISTORY = `<!doctype html>
<html lang="id">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Riwayat Download</title>
<style>
${COMMON_CSS}
.toolbar { display: grid; gap: 8px; grid-template-columns: 1fr; margin-bottom: 16px; }
@media (min-width: 560px) { .toolbar { grid-template-columns: 2fr 1fr 1fr 1fr; } }
.toolbar .search { grid-column: 1 / -1; }
@media (min-width: 560px) { .toolbar .search { grid-column: auto; } }
.hitem-meta { font-size: .85rem; color: #6b7280; font-variant-numeric: tabular-nums; margin-top: 6px; }
</style>
</head>
<body>
<div class="wrap">
  <div class="head">
    <h1>Riwayat Download</h1>
    <a class="navlink" href="/">← Kembali</a>
  </div>
  <div class="toolbar">
    <input class="search" id="q" type="search" placeholder="Cari nama file atau URL…">
    <select id="state">
      <option value="">Semua status</option>
      <option value="done">Selesai</option>
      <option value="error">Gagal</option>
      <option value="canceled">Dibatalkan</option>
    </select>
    <input id="from" type="date" title="Dari tanggal">
    <input id="to" type="date" title="Sampai tanggal">
  </div>
  <div id="empty" class="empty">Tidak ada riwayat.</div>
  <div id="list"></div>
</div>

<dialog id="confirmDlg">
  <p id="confirmMsg"></p>
  <form method="dialog" class="dlg-acts">
    <button id="dlgCancel" value="cancel">Batal</button>
    <button id="dlgOk" value="ok">Ya, Hapus</button>
  </form>
</dialog>

<script>
var STATE = ${JSON.stringify(STATE_LABEL)};
var listEl = document.getElementById('list');
var emptyEl = document.getElementById('empty');
var confirmDlg = document.getElementById('confirmDlg');
var timer = null;

function isTerminal(s) { return s === 'done' || s === 'error' || s === 'canceled'; }

function askConfirm(msg, okLabel) {
  return new Promise(function (resolve) {
    document.getElementById('confirmMsg').textContent = msg;
    document.getElementById('dlgOk').textContent = okLabel || 'Ya, Hapus';
    if (typeof confirmDlg.showModal !== 'function') {
      resolve(window.confirm(msg));
      return;
    }
    confirmDlg.returnValue = 'cancel';
    function onClose() {
      confirmDlg.removeEventListener('close', onClose);
      resolve(confirmDlg.returnValue === 'ok');
    }
    confirmDlg.addEventListener('close', onClose);
    confirmDlg.showModal();
  });
}

function fmtBytes(n) {
  if (n == null) return '';
  if (n < 1024) return n + ' B';
  if (n < 1048576) return (n / 1024).toFixed(1) + ' KB';
  if (n < 1073741824) return (n / 1048576).toFixed(1) + ' MB';
  return (n / 1073741824).toFixed(2) + ' GB';
}

function fmtDate(ms) {
  return new Date(ms).toLocaleString('id-ID', {
    year: 'numeric', month: 'short', day: 'numeric',
    hour: '2-digit', minute: '2-digit',
  });
}

function renderCard(s) {
  var root = document.createElement('div');
  root.className = 'card';
  var row = document.createElement('div');
  row.className = 'row';
  var name = document.createElement('div');
  name.className = 'fname';
  name.textContent = s.filename || s.url;
  name.title = s.filename || '';
  var badge = document.createElement('span');
  badge.className = 'badge ' + s.state;
  badge.textContent = STATE[s.state] || s.state;
  row.appendChild(name);
  row.appendChild(badge);
  var url = document.createElement('div');
  url.className = 'url';
  url.textContent = s.url;
  var meta = document.createElement('div');
  meta.className = 'hitem-meta';
  var bits = [fmtDate(s.createdAt)];
  if (s.total != null) bits.push(fmtBytes(s.total));
  if (s.directory) bits.push(s.directory);
  meta.textContent = bits.join(' · ');
  root.appendChild(row);
  root.appendChild(url);
  root.appendChild(meta);
  if (s.error) {
    var err = document.createElement('div');
    err.className = 'err';
    err.textContent = s.error;
    root.appendChild(err);
  }
  var acts = document.createElement('div');
  acts.className = 'acts';
  var delBtn = document.createElement('button');
  delBtn.type = 'button';
  delBtn.className = 'danger';
  delBtn.textContent = 'Hapus';
  delBtn.onclick = function () {
    askConfirm('Hapus "' + (s.filename || s.url) + '" dari riwayat?', 'Ya, Hapus')
      .then(function (ok) {
        if (!ok) return;
        delBtn.disabled = true;
        fetch('/downloads/' + s.id, { method: 'DELETE' })
          .then(function (r) { if (r.ok) root.remove(); else delBtn.disabled = false; checkEmpty(); })
          .catch(function () { delBtn.disabled = false; });
      });
  };
  acts.appendChild(delBtn);
  root.appendChild(acts);
  return root;
}

function checkEmpty() {
  emptyEl.hidden = listEl.children.length > 0;
}

async function refresh() {
  var p = new URLSearchParams();
  var q = document.getElementById('q').value.trim();
  var st = document.getElementById('state').value;
  var from = document.getElementById('from').value;
  var to = document.getElementById('to').value;
  if (q) p.set('q', q);
  if (st) p.set('state', st);
  if (from) p.set('from', from);
  if (to) p.set('to', to);
  try {
    var data = await (await fetch('/history-data?' + p.toString())).json();
    listEl.textContent = '';
    data.forEach(function (s) { listEl.appendChild(renderCard(s)); });
    checkEmpty();
  } catch (_) {}
}

['q', 'state', 'from', 'to'].forEach(function (id) {
  document.getElementById(id).addEventListener('input', function () {
    clearTimeout(timer);
    timer = setTimeout(refresh, 300);
  });
});

refresh();
</script>
</body>
</html>`;

const server = app.listen(PORT, () => log('server', null, { port: server.address().port, dirs: DIRS }));
