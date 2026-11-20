// HTTP API + static UI. Zero dependencies.
import http from 'node:http';
import { readFileSync, existsSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { Engine } from './engine.js';
import { Store } from './store.js';

const __dirname = dirname(fileURLToPath(import.meta.url));
const DATA_FILE = process.env.DATA_FILE ?? join(__dirname, '..', 'data', 'checkpoint.json');
const PORT = Number(process.env.PORT ?? 8080);

const store = new Store(DATA_FILE);
const engine = store.load() ?? new Engine();
let restored = existsSync(DATA_FILE);

function persist() {
  store.save(engine);
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    let data = '';
    req.on('data', (c) => { data += c; if (data.length > 1e6) req.destroy(); });
    req.on('end', () => {
      try { resolve(data ? JSON.parse(data) : {}); } catch (e) { reject(e); }
    });
    req.on('error', reject);
  });
}

function json(res, code, obj) {
  res.writeHead(code, { 'content-type': 'application/json' });
  res.end(JSON.stringify(obj));
}

export function createServer(eng = engine, onMutate = persist) {
  return http.createServer(async (req, res) => {
    const url = new URL(req.url, 'http://x');
    const path = url.pathname;
    try {
      if (path === '/' || path === '/index.html') {
        res.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
        res.end(readFileSync(join(__dirname, 'public', 'index.html'), 'utf8'));
        return;
      }
      if (path === '/api/state' && req.method === 'GET') {
        json(res, 200, {
          config: eng.config,
          watermark: eng.watermark,
          maxEventTime: eng.maxEventTime,
          windows: eng.listWindows(),
          revisions: eng.revisions,
          restoredFromCheckpoint: restored,
        });
        return;
      }
      if (path.startsWith('/api/windows/') && req.method === 'GET') {
        const detail = eng.windowDetail(decodeURIComponent(path.slice('/api/windows/'.length)));
        if (!detail) return json(res, 404, { error: 'window not found' });
        json(res, 200, detail);
        return;
      }
      if (path === '/api/events' && req.method === 'POST') {
        const body = await readBody(req);
        const result = Array.isArray(body) ? eng.ingestMany(body) : eng.ingest(body);
        onMutate();
        json(res, 200, { result });
        return;
      }
      if (path === '/api/config' && req.method === 'POST') {
        const body = await readBody(req);
        const cfg = eng.updateConfig(body);
        onMutate();
        json(res, 200, { config: cfg });
        return;
      }
      const revMatch = path.match(/^\/api\/revisions\/(\d+)\/(apply|discard)$/);
      if (revMatch && req.method === 'POST') {
        const r = revMatch[2] === 'apply'
          ? eng.applyRevision(Number(revMatch[1]))
          : eng.discardRevision(Number(revMatch[1]));
        onMutate();
        json(res, r.status === 'error' ? 400 : 200, r);
        return;
      }
      const reopenMatch = path.match(/^\/api\/windows\/(.+)\/reopen$/);
      if (reopenMatch && req.method === 'POST') {
        const r = eng.reopenWindow(decodeURIComponent(reopenMatch[1]));
        onMutate();
        json(res, r.status === 'error' ? 400 : 200, r);
        return;
      }
      if (path === '/api/checkpoint' && req.method === 'POST') {
        onMutate();
        json(res, 200, { ok: true, file: DATA_FILE });
        return;
      }
      json(res, 404, { error: 'not found' });
    } catch (e) {
      json(res, 500, { error: String(e.message ?? e) });
    }
  });
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  createServer().listen(PORT, () => {
    console.log(`event-agg listening on http://localhost:${PORT}`);
    console.log(`checkpoint file: ${DATA_FILE}${restored ? ' (restored)' : ''}`);
  });
}
