import { test } from 'node:test';
import assert from 'node:assert/strict';
import { Engine, REASON, REVISION_REASON } from '../src/engine.js';

const B = Date.UTC(2026, 0, 1, 0, 0, 0); // aligned base
const mk = (cfg = {}) => new Engine({
  windowSizeMs: 60_000, allowedLatenessMs: 30_000,
  watermark: { strategy: 'bounded-out-of-orderness', maxOutOfOrdernessMs: 10_000 },
  ...cfg,
});
const add = (key, t, value, id) => ({ eventId: id ?? `e-${key}-${t}-${value}`, key, eventTime: t, value });

test('duplicate events are counted once', () => {
  const e = mk();
  const ev = add('a', B + 5_000, 7, 'dup-1');
  assert.equal(e.ingest(ev).status, 'applied');
  assert.equal(e.ingest(ev).status, 'duplicate');
  assert.equal(e.ingest({ ...ev }).status, 'duplicate'); // same id, new object
  const w = e.listWindows()[0];
  assert.equal(w.value, 7);
  assert.equal(w.count, 1);
});

test('out-of-order event within allowed lateness updates window with LATE_EVENT reason', () => {
  const e = mk();
  e.ingest(add('a', B + 5_000, 1));
  e.ingest(add('a', B + 100_000, 10)); // advances watermark to B+90s
  const r = e.ingest(add('a', B + 85_000, 2)); // late (<= wm) but window still open
  assert.equal(r.status, 'late-applied');
  const w = e.windowDetail(r.windowId);
  assert.equal(w.value, 12);
  assert.equal(w.versions.at(-1).reason, REASON.LATE_EVENT);
  assert.equal(w.versions.at(-1).diff, 2);
});

test('extremely late event after window close goes to revision queue, value untouched', () => {
  const e = mk();
  e.ingest(add('a', B + 5_000, 1));
  e.ingest(add('a', B + 130_000, 5)); // wm = B+120s -> window [B,B+60s) closed (end+lateness=B+90s)
  const w1 = `${'a'}|${B}`;
  assert.equal(e.windowDetail(w1).state, 'CLOSED');
  const r = e.ingest(add('a', B + 10_000, 99, 'too-late-1'));
  assert.equal(r.status, 'queued-revision');
  assert.equal(e.windowDetail(w1).value, 1); // not silently applied
  const rev = e.revisions.find((x) => x.id === r.revisionId);
  assert.equal(rev.reason, REVISION_REASON.TOO_LATE);
  assert.equal(rev.status, 'pending');
});

test('applying a revision reopens the closed window, recomputes, then re-closes', () => {
  const e = mk();
  e.ingest(add('a', B + 5_000, 1));
  e.ingest(add('a', B + 130_000, 5));
  const r = e.ingest(add('a', B + 10_000, 99, 'too-late-2'));
  const applied = e.applyRevision(r.revisionId);
  const w = e.windowDetail(applied.windowId);
  assert.equal(w.value, 100);
  assert.equal(w.state, 'CLOSED'); // watermark still past -> re-closed
  const reasons = w.versions.map((v) => v.reason);
  assert.ok(reasons.includes(REASON.REOPEN));
  assert.ok(reasons.includes(REASON.REVISION_APPLIED));
  assert.equal(w.versions.at(-1).reason, REASON.WINDOW_CLOSE);
  assert.equal(e.revisions.find((x) => x.id === r.revisionId).status, 'applied');
});

test('retraction removes contribution and records RETRACTION reason', () => {
  const e = mk();
  e.ingest(add('a', B + 1_000, 5, 'r1'));
  e.ingest(add('a', B + 2_000, 3, 'r2'));
  const r = e.ingest({ type: 'retract', retractOf: 'r1' });
  assert.equal(r.status, 'retract-applied');
  const w = e.windowDetail(r.windowId);
  assert.equal(w.value, 3);
  assert.equal(w.count, 1);
  assert.equal(w.versions.at(-1).reason, REASON.RETRACTION);
  assert.equal(w.versions.at(-1).diff, -5);
});

test('retraction targeting a closed window goes to revision queue; applying it reopens', () => {
  const e = mk();
  e.ingest(add('a', B + 1_000, 5, 'rc1'));
  e.ingest(add('a', B + 130_000, 1)); // close first window
  const r = e.ingest({ type: 'retract', retractOf: 'rc1' });
  assert.equal(r.status, 'queued-revision');
  assert.equal(e.revisions[0].reason, REVISION_REASON.RETRACT_OF_CLOSED);
  const applied = e.applyRevision(r.revisionId);
  assert.equal(e.windowDetail(applied.windowId).value, 0);
});

test('duplicate retraction is deduped', () => {
  const e = mk();
  e.ingest(add('a', B + 1_000, 5, 'rd1'));
  assert.equal(e.ingest({ type: 'retract', eventId: 'rt-1', retractOf: 'rd1' }).status, 'retract-applied');
  assert.equal(e.ingest({ type: 'retract', eventId: 'rt-1', retractOf: 'rd1' }).status, 'duplicate');
  assert.equal(e.listWindows()[0].value, 0);
});

test('manual window reopen', () => {
  const e = mk();
  e.ingest(add('a', B + 1_000, 5));
  e.ingest(add('a', B + 130_000, 1));
  const id = `a|${B}`;
  assert.equal(e.reopenWindow(id).status, 'reopened');
  assert.equal(e.windowDetail(id).state, 'REOPENED');
  assert.equal(e.windowDetail(id).versions.at(-1).reason, REASON.MANUAL_REOPEN);
});

test('recompute boundary: window vs key cascade of running totals', () => {
  // boundary = window: later windows keep stale running totals
  const e1 = mk({ recomputeBoundary: 'window' });
  e1.ingest(add('a', B + 1_000, 1));
  e1.ingest(add('a', B + 61_000, 2));
  e1.ingest(add('a', B + 95_000, 10)); // late into window 2, wm = B+85s
  const w2 = e1.windowDetail(`a|${B + 60_000}`);
  assert.equal(w2.value, 12);
  // now a late event lands in window 1 (still open: closes at wm>=B+90s)
  e1.ingest(add('a', B + 2_000, 100));
  const w2After = e1.windowDetail(`a|${B + 60_000}`);
  assert.equal(w2After.stale, true);
  assert.equal(w2After.runningTotal, 13); // 1 + 12, frozen before the late event

  // boundary = key: cascade updates running totals with CASCADE versions
  const e2 = mk({ recomputeBoundary: 'key' });
  e2.ingest(add('a', B + 1_000, 1));
  e2.ingest(add('a', B + 61_000, 2));
  e2.ingest(add('a', B + 95_000, 10));
  e2.ingest(add('a', B + 2_000, 100));
  const w2b = e2.windowDetail(`a|${B + 60_000}`);
  assert.equal(w2b.stale, false);
  assert.equal(w2b.runningTotal, 113); // 101 + 12
  assert.ok(w2b.versions.some((v) => v.reason === REASON.CASCADE));
});

test('punctuated watermark strategy', () => {
  const e = mk({ watermark: { strategy: 'punctuated' } });
  e.ingest(add('a', B + 1_000, 1));
  e.ingest(add('a', B + 500_000, 1)); // watermark does NOT move with events
  assert.equal(e.watermark, null);
  e.ingest({ type: 'watermark', value: B + 91_000 });
  assert.equal(e.windowDetail(`a|${B}`).state, 'CLOSED');
});

test('watermark is monotonic', () => {
  const e = mk();
  e.ingest(add('a', B + 200_000, 1));
  const wm = e.watermark;
  e.ingest(add('a', B + 50_000, 1)); // old event must not move watermark back
  assert.equal(e.watermark, wm);
});
