import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Engine } from '../src/engine.js';
import { Store } from '../src/store.js';
import { createServer } from '../src/server.js';

const B = Date.UTC(2026, 0, 1, 0, 0, 0);
const cfg = {
  windowSizeMs: 60_000, allowedLatenessMs: 30_000,
  watermark: { strategy: 'bounded-out-of-orderness', maxOutOfOrdernessMs: 10_000 },
};

test('task recovery: checkpoint and restore preserves full state', () => {
  const dir = mkdtempSync(join(tmpdir(), 'evt-'));
  const store = new Store(join(dir, 'cp.json'));
  try {
    const e = new Engine(cfg);
    e.ingest({ eventId: 'k1', key: 'a', eventTime: B + 1_000, value: 5 });
    e.ingest({ eventId: 'k2', key: 'a', eventTime: B + 130_000, value: 7 }); // closes window 1
    e.ingest({ eventId: 'k3', key: 'a', eventTime: B + 2_000, value: 9 });  // -> revision queue
    e.ingest({ type: 'retract', retractOf: 'k2' });
    store.save(e);

    // simulate crash + restart
    const r = store.load();
    assert.ok(r);
    assert.deepEqual(r.listWindows(), e.listWindows());
    assert.equal(r.watermark, e.watermark);
    assert.equal(r.revisions.length, 1);
    assert.equal(r.revisions[0].status, 'pending');

    // dedupe survives restart
    assert.equal(r.ingest({ eventId: 'k1', key: 'a', eventTime: B + 1_000, value: 5 }).status, 'duplicate');

    // revision queue still actionable after restart
    const applied = r.applyRevision(1);
    assert.equal(applied.status, 'applied');
    assert.equal(r.windowDetail(`a|${B}`).value, 14);

    // retraction state survived (k2 was retracted before crash)
    assert.equal(r.windowDetail(`a|${B + 120_000}`).value, 0);

    // processing continues seamlessly
    const n = r.ingest({ eventId: 'k4', key: 'a', eventTime: B + 140_000, value: 1 });
    assert.equal(n.status, 'applied');
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('server smoke: ingest via HTTP, read state, config update', async () => {
  const eng = new Engine(cfg);
  const server = createServer(eng, () => {});
  await new Promise((res) => server.listen(0, res));
  const port = server.address().port;
  try {
    const post = (p, body) => fetch(`http://127.0.0.1:${port}${p}`, {
      method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body),
    }).then((r) => r.json());
    const r1 = await post('/api/events', { eventId: 's1', key: 'a', eventTime: B + 1_000, value: 3 });
    assert.equal(r1.result.status, 'applied');
    const r2 = await post('/api/events', { eventId: 's1', key: 'a', eventTime: B + 1_000, value: 3 });
    assert.equal(r2.result.status, 'duplicate');
    await post('/api/config', { allowedLatenessMs: 5_000 });
    const state = await fetch(`http://127.0.0.1:${port}/api/state`).then((r) => r.json());
    assert.equal(state.config.allowedLatenessMs, 5_000);
    assert.equal(state.windows.length, 1);
    assert.equal(state.windows[0].value, 3);
    const html = await fetch(`http://127.0.0.1:${port}/`).then((r) => r.text());
    assert.ok(html.includes('事件时间聚合'));
  } finally {
    server.close();
  }
});
