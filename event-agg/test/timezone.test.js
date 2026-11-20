import { test } from 'node:test';
import assert from 'node:assert/strict';
import { Engine, windowRange, tzOffsetMs } from '../src/engine.js';

const DAY = 86_400_000;

test('timezone offsets incl. DST', () => {
  const winter = Date.UTC(2026, 0, 15);
  const summer = Date.UTC(2026, 6, 15);
  assert.equal(tzOffsetMs('UTC', winter), 0);
  assert.equal(tzOffsetMs('Asia/Shanghai', winter), 8 * 3_600_000);
  assert.equal(tzOffsetMs('America/New_York', winter), -5 * 3_600_000); // EST
  assert.equal(tzOffsetMs('America/New_York', summer), -4 * 3_600_000); // EDT
});

test('same instant falls into different daily windows under different timezones', () => {
  const t = Date.UTC(2026, 10, 20, 16, 30); // 2026-11-21 00:30 in Shanghai
  const utc = windowRange('UTC', DAY, t);
  const sh = windowRange('Asia/Shanghai', DAY, t);
  assert.equal(utc.start, Date.UTC(2026, 10, 20));
  assert.equal(sh.start, Date.UTC(2026, 10, 20, 16)); // local midnight 11-21 +08
  assert.notEqual(utc.start, sh.start);
});

test('DST transition makes a "day" window 25 hours long', () => {
  // 2026-11-01: US DST ends (fall back)
  const t = Date.UTC(2026, 10, 1, 12);
  const w = windowRange('America/New_York', DAY, t);
  assert.equal(w.end - w.start, 25 * 3_600_000);
});

test('switching timezone re-buckets new events, keeps historical windows intact', () => {
  const e = new Engine({
    windowSizeMs: DAY, allowedLatenessMs: DAY,
    timezone: 'UTC',
    watermark: { strategy: 'bounded-out-of-orderness', maxOutOfOrdernessMs: 0 },
  });
  const t = Date.UTC(2026, 10, 20, 16, 30);
  e.ingest({ eventId: 'tz-1', key: 'k', eventTime: t, value: 1 });
  assert.equal(e.listWindows()[0].start, Date.UTC(2026, 10, 20));

  e.updateConfig({ timezone: 'Asia/Shanghai' });
  e.ingest({ eventId: 'tz-2', key: 'k', eventTime: t + 1000, value: 2 });

  const wins = e.listWindows();
  assert.equal(wins.length, 2); // same instant range, two different buckets
  const starts = wins.map((w) => w.start).sort();
  assert.deepEqual(starts, [Date.UTC(2026, 10, 20), Date.UTC(2026, 10, 20, 16)].sort());
  // historical window kept its own timezone for display
  assert.equal(wins.find((w) => w.start === Date.UTC(2026, 10, 20)).timezone, 'UTC');
  assert.equal(wins.find((w) => w.start === Date.UTC(2026, 10, 20, 16)).timezone, 'Asia/Shanghai');
});
