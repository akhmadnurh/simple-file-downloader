'use strict';

// Self-check: jalankan dengan `npm test`. Butuh Node >= 18.
// Fixture server lokal menyediakan: /file (2MB), /html, /nolength, /missing (404),
// /file.zip (zip valid), /noslow (server tanpa file). Tidak butuh internet.

const assert = require('assert');
const http = require('http');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { spawn, spawnSync } = require('child_process');

const TMP = fs.mkdtempSync(path.join(os.tmpdir(), 'dlmgr-test-'));
const DL_DIR = path.join(TMP, 'downloads');
fs.mkdirSync(DL_DIR);

const FILE_BYTES = 2 * 1024 * 1024;
const FILE_BODY = Buffer.alloc(FILE_BYTES, 0x61);

function crc32(buf) {
  let c, table = crc32._t;
  if (!table) {
    table = crc32._t = new Int32Array(256);
    for (let n = 0; n < 256; n++) {
      c = n;
      for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
      table[n] = c;
    }
  }
  c = -1;
  for (let i = 0; i < buf.length; i++) c = table[(c ^ buf[i]) & 0xff] ^ (c >>> 8);
  return (c ^ -1) >>> 0;
}

// ZIP stored (tanpa kompresi), satu file: entri minimal valid untuk `unzip`.
function makeStoredZip(name, content) {
  const data = Buffer.from(content);
  const nameB = Buffer.from(name);
  const crc = crc32(data);
  const local = Buffer.alloc(30);
  local.writeUInt32LE(0x04034b50, 0);
  local.writeUInt16LE(20, 4); // version needed
  local.writeUInt16LE(0, 8);  // method = stored
  local.writeUInt32LE(crc, 14);
  local.writeUInt32LE(data.length, 18);
  local.writeUInt32LE(data.length, 22);
  local.writeUInt16LE(nameB.length, 26);
  const central = Buffer.alloc(46);
  central.writeUInt32LE(0x02014b50, 0);
  central.writeUInt16LE(20, 4);
  central.writeUInt16LE(20, 6);
  central.writeUInt16LE(0, 10); // method
  central.writeUInt32LE(crc, 16);
  central.writeUInt32LE(data.length, 20);
  central.writeUInt32LE(data.length, 24);
  central.writeUInt16LE(nameB.length, 28);
  central.writeUInt32LE(0, 42); // local header offset
  const eocd = Buffer.alloc(22);
  eocd.writeUInt32LE(0x06054b50, 0);
  eocd.writeUInt16LE(1, 8);
  eocd.writeUInt16LE(1, 10);
  eocd.writeUInt32LE(46 + nameB.length, 12);
  eocd.writeUInt32LE(30 + nameB.length + data.length, 16);
  return Buffer.concat([local, nameB, data, central, nameB, eocd]);
}

const ZIP_BODY = makeStoredZip('hello.txt', 'hello from zip\n');

function startFixture() {
  return new Promise(resolve => {
    const srv = http.createServer((req, res) => {
      const url = req.url.split('?')[0];
      if (url === '/file') {
        res.writeHead(200, {
          'Content-Type': 'application/octet-stream',
          'Content-Length': FILE_BYTES,
          'Content-Disposition': 'attachment; filename="server-name.bin"',
        });
        res.end(FILE_BODY);
      } else if (url === '/html') {
        res.writeHead(200, { 'Content-Type': 'text/html' });
        res.end('<html>not a file</html>');
      } else if (url === '/nolength') {
        res.writeHead(200, { 'Content-Type': 'application/octet-stream' });
        res.write(Buffer.alloc(64 * 1024, 0x62));
        setTimeout(() => res.end(Buffer.alloc(64 * 1024, 0x62)), 1100);
      } else if (url === '/archive.zip') {
        res.writeHead(200, {
          'Content-Type': 'application/zip',
          'Content-Length': ZIP_BODY.length,
        });
        res.end(ZIP_BODY);
      } else {
        res.writeHead(404, { 'Content-Type': 'text/plain' });
        res.end('not found');
      }
    });
    srv.listen(0, '127.0.0.1', () => resolve(srv));
  });
}

const sleep = ms => new Promise(r => setTimeout(r, ms));

async function waitFor(fn, timeoutMs, label) {
  const start = Date.now();
  let last;
  while (Date.now() - start < timeoutMs) {
    last = await fn();
    if (last) return last;
    await sleep(150);
  }
  throw new Error('timeout: ' + label + ' (last=' + JSON.stringify(last) + ')');
}

async function main() {
  const fixture = await startFixture();
  const fxBase = `http://127.0.0.1:${fixture.address().port}`;
  const appPort = 20000 + Math.floor(Math.random() * 20000);

  const child = spawn(process.execPath, ['app.js'], {
    env: { ...process.env, PORT: String(appPort), DOWNLOAD_DIRS: DL_DIR },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let childLog = '';
  child.stdout.on('data', d => { childLog += d; });
  child.stderr.on('data', d => { childLog += d; });

  const base = `http://127.0.0.1:${appPort}`;
  let passed = 0;
  const ok = name => { passed++; console.log('  ok ' + name); };

  try {
    await waitFor(async () => {
      try { return (await fetch(base + '/')).ok; } catch (_) { return false; }
    }, 10000, 'server start');
    ok('server start');

    // --- Task 1: form load
    const home = await (await fetch(base + '/')).text();
    assert(home.includes('Download Manager'), 'form contains title');
    assert(home.includes('id="url"'), 'form has url input');
    assert(home.includes(JSON.stringify(DL_DIR)), 'form lists allowed directory');
    ok('GET / renders form with directory');

    // --- Task 7: URL validation
    let r = await fetch(base + '/download', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ url: 'ftp://x/y.zip', directory: DL_DIR }),
    });
    assert.strictEqual(r.status, 400);
    assert((await r.json()).error);
    ok('POST /download rejects non-http URL (400)');

    r = await fetch(base + '/download', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ url: 'not a url', directory: DL_DIR }),
    });
    assert.strictEqual(r.status, 400);
    ok('POST /download rejects malformed URL (400)');

    r = await fetch(base + '/download', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ url: fxBase + '/file', directory: '/etc' }),
    });
    assert.strictEqual(r.status, 400);
    ok('POST /download rejects unknown directory (400)');

    // --- HTML response → error state
    r = await fetch(base + '/download', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ url: fxBase + '/html', directory: DL_DIR }),
    });
    assert.strictEqual(r.status, 200);
    const htmlId = (await r.json()).downloadId;
    const htmlSt = await waitFor(async () => {
      const s = await (await fetch(base + '/status/' + htmlId)).json();
      return s.state === 'error' ? s : null;
    }, 10000, 'html → error');
    assert.match(htmlSt.error, /HTML/);
    ok('html URL → state error with message');

    // --- 404 → error state
    r = await fetch(base + '/download', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ url: fxBase + '/missing', directory: DL_DIR }),
    });
    const missingId = (await r.json()).downloadId;
    const missSt = await waitFor(async () => {
      const s = await (await fetch(base + '/status/' + missingId)).json();
      return s.state === 'error' ? s : null;
    }, 10000, '404 → error');
    assert.match(missSt.error, /404/);
    ok('404 URL → state error with HTTP status');

    // --- Task 2+3+4: happy path + SSE
    r = await fetch(base + '/download', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ url: fxBase + '/file', directory: DL_DIR }),
    });
    assert.strictEqual(r.status, 200);
    const t0 = Date.now();
    const dl = await r.json();
    assert(Date.now() - t0 < 1000, 'POST /download returns immediately');
    assert(dl.downloadId, 'returns downloadId');
    ok('POST /download returns {downloadId} non-blocking');

    const id = dl.downloadId;
    const res = await fetch(base + '/progress/' + id);
    assert.strictEqual(res.headers.get('content-type'), 'text/event-stream');
    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buf = '';
    const events = [];
    while (events.length < 2) {
      const { done, value } = await reader.read();
      if (done) break;
      buf += decoder.decode(value, { stream: true });
      let idx;
      while ((idx = buf.indexOf('\n\n')) !== -1) {
        const frame = buf.slice(0, idx);
        buf = buf.slice(idx + 2);
        const line = frame.split('\n').find(l => l.startsWith('data: '));
        if (line) events.push(JSON.parse(line.slice(6)));
      }
    }
    await reader.cancel().catch(() => {});
    assert(events.length >= 1, 'got SSE events');
    const ev = events[0];
    assert.strictEqual(ev.id, id);
    assert(ev.state === 'pending' || ev.state === 'downloading', 'sse state: ' + ev.state);
    for (const k of ['percentage', 'downloaded', 'total', 'speed', 'eta', 'state']) {
      assert(k in ev, 'sse payload has ' + k);
    }
    ok('SSE emits {percentage,downloaded,total,speed,eta,state}');

    const st = await waitFor(async () => {
      const s = await (await fetch(base + '/status/' + id)).json();
      return s.state === 'done' ? s : null;
    }, 15000, 'download done');
    assert.strictEqual(st.percentage, 100);
    assert.strictEqual(st.filename, 'server-name.bin', 'filename from content-disposition');
    assert.strictEqual(st.total, FILE_BYTES);
    const written = fs.statSync(path.join(DL_DIR, 'server-name.bin'));
    assert.strictEqual(written.size, FILE_BYTES, 'file size matches');
    assert(!fs.readdirSync(DL_DIR).some(f => f.endsWith('.part')), 'no leftover .part');
    ok('download completes: size, filename, percentage=100, no .part');

    // --- manual filename override
    r = await fetch(base + '/download', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ url: fxBase + '/file', filename: 'custom.dat', directory: DL_DIR }),
    });
    const customId = (await r.json()).downloadId;
    const cst = await waitFor(async () => {
      const s = await (await fetch(base + '/status/' + customId)).json();
      return s.state === 'done' ? s : null;
    }, 15000, 'custom name done');
    assert.strictEqual(cst.filename, 'custom.dat');
    assert(fs.existsSync(path.join(DL_DIR, 'custom.dat')));
    ok('manual filename override respected');

    // --- total=null (chunked tanpa content-length)
    r = await fetch(base + '/download', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ url: fxBase + '/nolength', directory: DL_DIR }),
    });
    const nlId = (await r.json()).downloadId;
    const nlSt = await waitFor(async () => {
      const s = await (await fetch(base + '/status/' + nlId)).json();
      return s.state === 'done' ? s : null;
    }, 15000, 'nolength done');
    assert.strictEqual(nlSt.total, null);
    assert.strictEqual(nlSt.percentage, 100, 'done forces 100%');
    ok('no content-length → total=null, done=100%');

    // --- Task 5: auto-unzip (conditional: unzip binary)
    const hasUnzip = spawnSync('unzip', ['-v'], { stdio: 'ignore' }).status === 0;
    r = await fetch(base + '/download', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ url: fxBase + '/archive.zip', directory: DL_DIR }),
    });
    const zipId = (await r.json()).downloadId;
    const zSt = await waitFor(async () => {
      const s = await (await fetch(base + '/status/' + zipId)).json();
      return (s.state === 'done' || s.state === 'error') ? s : null;
    }, 15000, 'zip done/error');
    if (hasUnzip) {
      assert.strictEqual(zSt.state, 'done', zSt.error || '');
      assert(!fs.existsSync(path.join(DL_DIR, 'archive.zip')), 'zip deleted after extract');
      assert.strictEqual(
        fs.readFileSync(path.join(DL_DIR, 'hello.txt'), 'utf8'),
        'hello from zip\n'
      );
      ok('auto-unzip extracts and deletes zip');
    } else {
      assert.strictEqual(zSt.state, 'error');
      assert.match(zSt.error, /unzip/);
      ok('unzip missing → error message (binary tidak ada di host ini)');
    }

    // --- Task 5b: duplicate filename → " (1)" suffix, tidak menimpa
    r = await fetch(base + '/download', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ url: fxBase + '/file', directory: DL_DIR }),
    });
    const dupId = (await r.json()).downloadId;
    const dupSt = await waitFor(async () => {
      const s = await (await fetch(base + '/status/' + dupId)).json();
      return s.state === 'done' ? s : null;
    }, 15000, 'dup done');
    assert.strictEqual(dupSt.filename, 'server-name (1).bin');
    ok('duplicate filename gets (1) suffix, no overwrite');

    // --- GET /downloads (list untuk refresh)
    const list = await (await fetch(base + '/downloads')).json();
    assert(Array.isArray(list) && list.length >= 6, 'list has entries');
    assert(list.every(x => 'id' in x && 'state' in x && 'percentage' in x));
    ok('GET /downloads returns all entries');

    // --- unknown id → 404
    r = await fetch(base + '/status/nope');
    assert.strictEqual(r.status, 404);
    r = await fetch(base + '/progress/nope');
    assert.strictEqual(r.status, 404);
    ok('unknown id → 404');

    // --- DELETE entry
    r = await fetch(base + '/downloads/' + dupId, { method: 'DELETE' });
    assert.strictEqual(r.status, 200);
    const list2 = await (await fetch(base + '/downloads')).json();
    assert(!list2.some(x => x.id === dupId));
    ok('DELETE /downloads/:id removes terminal entry');

    // --- cancel flow: download aktif lalu cancel
    r = await fetch(base + '/download', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ url: fxBase + '/nolength', directory: DL_DIR }),
    });
    const cId = (await r.json()).downloadId;
    await sleep(300);
    r = await fetch(base + '/cancel/' + cId, { method: 'POST' });
    assert.strictEqual(r.status, 200);
    const cSt = await waitFor(async () => {
      const s = await (await fetch(base + '/status/' + cId)).json();
      return s.state === 'canceled' ? s : null;
    }, 5000, 'cancel');
    await sleep(300);
    assert(!fs.readdirSync(DL_DIR).some(f => f.endsWith('.part')), 'cancel removes .part');
    r = await fetch(base + '/cancel/' + cId, { method: 'POST' });
    assert.strictEqual(r.status, 400, 'cancel again → 400');
    ok('cancel: state=canceled, .part removed, double-cancel=400');

    console.log('\nALL ' + passed + ' CHECKS PASSED');
  } catch (err) {
    console.error('\nFAILED: ' + err.message);
    console.error('--- server log ---\n' + childLog);
    process.exitCode = 1;
  } finally {
    child.kill();
    fixture.close();
    fs.rmSync(TMP, { recursive: true, force: true });
  }
}

main();
