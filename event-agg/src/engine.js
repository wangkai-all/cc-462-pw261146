// Event-time aggregation engine.
// Handles: out-of-order events, late data, retractions, window close/reopen,
// configurable watermark strategy / allowed lateness / recompute boundary,
// and a revision queue for extremely-late events (never silently dropped).

export const REASON = {
  ON_TIME: 'ON_TIME',
  LATE_EVENT: 'LATE_EVENT',
  RETRACTION: 'RETRACTION',
  WINDOW_CLOSE: 'WINDOW_CLOSE',
  REOPEN: 'REOPEN',
  MANUAL_REOPEN: 'MANUAL_REOPEN',
  REVISION_APPLIED: 'REVISION_APPLIED',
  CASCADE: 'CASCADE',
};

export const REVISION_REASON = {
  TOO_LATE: 'TOO_LATE_WINDOW_CLOSED',
  RETRACT_OF_CLOSED: 'RETRACT_OF_CLOSED_WINDOW',
  RETRACT_UNKNOWN: 'RETRACT_UNKNOWN_EVENT',
};

// ---------- timezone helpers (no external deps) ----------

const dtfCache = new Map();
function dtf(tz) {
  let f = dtfCache.get(tz);
  if (!f) {
    f = new Intl.DateTimeFormat('en-US', {
      timeZone: tz, hourCycle: 'h23',
      year: 'numeric', month: '2-digit', day: '2-digit',
      hour: '2-digit', minute: '2-digit', second: '2-digit',
    });
    dtfCache.set(tz, f);
  }
  return f;
}

/** Offset of `timeZone` from UTC (ms) at the given instant. Handles DST. */
export function tzOffsetMs(timeZone, epochMs) {
  const parts = dtf(timeZone).formatToParts(new Date(epochMs));
  const p = {};
  for (const { type, value } of parts) p[type] = value;
  const asUtc = Date.UTC(+p.year, +p.month - 1, +p.day, +p.hour, +p.minute, +p.second);
  return asUtc - (epochMs - (epochMs % 1000));
}

/**
 * Window [start, end) in epoch ms containing `t`, aligned to wall-clock
 * boundaries in `timeZone`. Across DST transitions a "day" window may be
 * 23h or 25h long — that is intentional.
 */
export function windowRange(timeZone, sizeMs, t) {
  const off = tzOffsetMs(timeZone, t);
  const localStart = Math.floor((t + off) / sizeMs) * sizeMs;
  const start = localStart - tzOffsetMs(timeZone, localStart - off);
  const localEnd = localStart + sizeMs;
  const end = localEnd - tzOffsetMs(timeZone, localEnd - off);
  return { start, end };
}

// ---------- engine ----------

const DEFAULTS = {
  windowSizeMs: 60_000,
  allowedLatenessMs: 30_000,
  timezone: 'UTC',
  recomputeBoundary: 'window', // 'window' | 'key'
  watermark: { strategy: 'bounded-out-of-orderness', maxOutOfOrdernessMs: 10_000 },
};

export class Engine {
  constructor(config = {}, opts = {}) {
    this.config = {
      ...DEFAULTS,
      ...config,
      watermark: { ...DEFAULTS.watermark, ...(config.watermark || {}) },
    };
    this.now = opts.now ?? (() => Date.now());
    this.watermark = null;       // epoch ms, monotonic
    this.maxEventTime = null;
    this.windows = new Map();    // id -> window
    this.seen = new Map();       // eventId -> windowId (dedupe + retraction lookup)
    this.seenRetractions = new Set();
    this.revisions = [];         // revision queue
    this.revSeq = 0;
  }

  // ----- ingestion -----

  /**
   * event: { eventId, key, eventTime, value, type?: 'add'|'retract'|'watermark',
   *          retractOf? }
   */
  ingest(event) {
    const type = event.type ?? 'add';
    if (type === 'watermark') return this._ingestWatermark(event);
    if (type === 'retract') return this._ingestRetract(event);
    return this._ingestAdd(event);
  }

  ingestMany(events) {
    return events.map((e) => this.ingest(e));
  }

  _ingestWatermark(event) {
    if (this.config.watermark.strategy !== 'punctuated') {
      return { status: 'ignored', reason: 'strategy is not punctuated' };
    }
    const v = Number(event.value);
    if (this.watermark === null || v > this.watermark) {
      this.watermark = v;
      this._closeCheck();
    }
    return { status: 'watermark-advanced', watermark: this.watermark };
  }

  _ingestAdd(event) {
    const { eventId, key, eventTime, value } = event;
    if (!eventId || key == null || eventTime == null || typeof value !== 'number') {
      return { status: 'rejected', reason: 'need eventId, key, eventTime, numeric value' };
    }
    if (this.seen.has(eventId)) {
      return { status: 'duplicate', eventId };
    }

    // Extremely late: the window it belongs to is already past
    // watermark + allowed lateness (i.e. closed) -> revision queue,
    // never silently dropped.
    const { start, end } = windowRange(this.config.timezone, this.config.windowSizeMs, eventTime);
    const tooLate =
      this.watermark !== null &&
      this.watermark >= end + this.config.allowedLatenessMs;
    const id = `${key}|${start}`;
    const existing = this.windows.get(id);
    if (tooLate || (existing && existing.state === 'CLOSED')) {
      this.seen.set(eventId, null); // still dedupe the id
      const rev = {
        id: ++this.revSeq,
        event: { ...event, type: 'add' },
        reason: REVISION_REASON.TOO_LATE,
        windowId: id,
        status: 'pending',
        enqueuedAt: this.now(),
      };
      this.revisions.push(rev);
      return { status: 'queued-revision', revisionId: rev.id, windowId: id };
    }

    const win = existing ?? this._createWindow(id, key, start, end);
    const late = this.watermark !== null && eventTime <= this.watermark;
    win.events.set(eventId, { eventTime, value, retracted: false });
    this.seen.set(eventId, id);
    this._recompute(win, late ? REASON.LATE_EVENT : REASON.ON_TIME);
    this._advanceWatermark(eventTime);
    return { status: late ? 'late-applied' : 'applied', windowId: id, value: win.value };
  }

  _ingestRetract(event) {
    const { retractOf } = event;
    if (!retractOf) return { status: 'rejected', reason: 'retract needs retractOf' };
    if (event.eventId) {
      if (this.seenRetractions.has(event.eventId)) return { status: 'duplicate', eventId: event.eventId };
      this.seenRetractions.add(event.eventId);
    }
    const windowId = this.seen.get(retractOf);
    const win = windowId ? this.windows.get(windowId) : null;
    if (!win || !win.events.has(retractOf)) {
      const rev = {
        id: ++this.revSeq,
        event: { ...event, type: 'retract' },
        reason: REVISION_REASON.RETRACT_UNKNOWN,
        windowId: windowId ?? null,
        status: 'pending',
        enqueuedAt: this.now(),
      };
      this.revisions.push(rev);
      return { status: 'queued-revision', revisionId: rev.id };
    }
    if (win.state === 'CLOSED') {
      const rev = {
        id: ++this.revSeq,
        event: { ...event, type: 'retract' },
        reason: REVISION_REASON.RETRACT_OF_CLOSED,
        windowId,
        status: 'pending',
        enqueuedAt: this.now(),
      };
      this.revisions.push(rev);
      return { status: 'queued-revision', revisionId: rev.id, windowId };
    }
    win.events.get(retractOf).retracted = true;
    this._recompute(win, REASON.RETRACTION);
    return { status: 'retract-applied', windowId, value: win.value };
  }

  // ----- windows -----

  _createWindow(id, key, start, end) {
    const win = {
      id, key, start, end,
      timezone: this.config.timezone,
      state: 'OPEN',
      events: new Map(),
      versions: [],
      value: 0,
      count: 0,
      runningTotal: this._prevRunningTotal(key, start),
      stale: false,
    };
    this.windows.set(id, win);
    return win;
  }

  _prevRunningTotal(key, start) {
    let prev = 0;
    for (const w of this.windows.values()) {
      if (w.key === key && w.start < start) prev = Math.max(prev, w.runningTotal);
    }
    return prev;
  }

  _recompute(win, reason) {
    let value = 0, count = 0;
    for (const e of win.events.values()) {
      if (!e.retracted) { value += e.value; count += 1; }
    }
    const prev = win.versions[win.versions.length - 1];
    win.value = value;
    win.count = count;
    win.runningTotal = this._prevRunningTotal(win.key, win.start) + value;
    if (!prev || prev.value !== value || prev.count !== count || reason === REASON.WINDOW_CLOSE) {
      win.versions.push({
        v: win.versions.length + 1,
        value, count,
        diff: value - (prev ? prev.value : 0),
        reason,
        at: this.now(),
      });
    }
    this._propagate(win);
  }

  /** Recompute boundary: 'window' stops at the changed window; 'key' cascades. */
  _propagate(win) {
    const later = [...this.windows.values()]
      .filter((w) => w.key === win.key && w.start > win.start)
      .sort((a, b) => a.start - b.start);
    if (this.config.recomputeBoundary !== 'key') {
      for (const w of later) w.stale = true; // running total no longer trustworthy
      return;
    }
    let base = win.runningTotal;
    for (const w of later) {
      const rt = base + w.value;
      if (rt !== w.runningTotal) {
        w.runningTotal = rt;
        w.stale = false;
        w.versions.push({
          v: w.versions.length + 1,
          value: w.value, count: w.count, diff: 0,
          reason: REASON.CASCADE, at: this.now(),
        });
      }
      base = w.runningTotal;
    }
  }

  _advanceWatermark(eventTime) {
    if (this.maxEventTime === null || eventTime > this.maxEventTime) {
      this.maxEventTime = eventTime;
    }
    if (this.config.watermark.strategy === 'bounded-out-of-orderness') {
      const wm = this.maxEventTime - this.config.watermark.maxOutOfOrdernessMs;
      if (this.watermark === null || wm > this.watermark) {
        this.watermark = wm;
        this._closeCheck();
      }
    }
  }

  _closeCheck() {
    const limit = this.config.allowedLatenessMs;
    for (const win of this.windows.values()) {
      if (win.state !== 'CLOSED' && this.watermark !== null && this.watermark >= win.end + limit) {
        win.state = 'CLOSED';
        this._recompute(win, REASON.WINDOW_CLOSE); // finalize
      }
    }
  }

  // ----- revision queue & reopening -----

  applyRevision(revisionId) {
    const rev = this.revisions.find((r) => r.id === revisionId);
    if (!rev) return { status: 'error', reason: 'revision not found' };
    if (rev.status !== 'pending') return { status: 'error', reason: `already ${rev.status}` };
    const e = rev.event;
    let result;
    if (e.type === 'retract') {
      const win = rev.windowId ? this.windows.get(rev.windowId) : null;
      if (!win || !win.events.has(e.retractOf)) {
        rev.status = 'discarded';
        return { status: 'error', reason: 'target event not found' };
      }
      if (win.state === 'CLOSED') this._reopen(win, REASON.REOPEN);
      win.events.get(e.retractOf).retracted = true;
      this._recompute(win, REASON.REVISION_APPLIED);
      result = { status: 'applied', windowId: win.id, value: win.value };
    } else {
      const win = this.windows.get(rev.windowId)
        ?? this._createWindow(rev.windowId, e.key,
          windowRange(this.config.timezone, this.config.windowSizeMs, e.eventTime).start,
          windowRange(this.config.timezone, this.config.windowSizeMs, e.eventTime).end);
      if (win.state === 'CLOSED') this._reopen(win, REASON.REOPEN);
      if (!win.events.has(e.eventId)) {
        win.events.set(e.eventId, { eventTime: e.eventTime, value: e.value, retracted: false });
      }
      this.seen.set(e.eventId, win.id);
      this._recompute(win, REASON.REVISION_APPLIED);
      result = { status: 'applied', windowId: win.id, value: win.value };
    }
    rev.status = 'applied';
    this._closeCheck(); // a reopened window usually re-closes immediately
    return result;
  }

  discardRevision(revisionId) {
    const rev = this.revisions.find((r) => r.id === revisionId);
    if (!rev) return { status: 'error', reason: 'revision not found' };
    rev.status = 'discarded';
    return { status: 'discarded' };
  }

  /** Manual window reopening (operator action). */
  reopenWindow(windowId) {
    const win = this.windows.get(windowId);
    if (!win) return { status: 'error', reason: 'window not found' };
    if (win.state !== 'CLOSED') return { status: 'error', reason: `window is ${win.state}` };
    this._reopen(win, REASON.MANUAL_REOPEN);
    return { status: 'reopened', windowId };
  }

  _reopen(win, reason) {
    win.state = 'REOPENED';
    win.versions.push({
      v: win.versions.length + 1,
      value: win.value, count: win.count, diff: 0,
      reason, at: this.now(),
    });
  }

  // ----- config -----

  updateConfig(partial) {
    const before = this.config;
    const next = {
      ...before,
      ...partial,
      watermark: { ...before.watermark, ...(partial.watermark || {}) },
    };
    this.config = next;
    // strategy change: recompute watermark from what we already know
    if (next.watermark.strategy === 'bounded-out-of-orderness' && this.maxEventTime !== null) {
      const wm = this.maxEventTime - next.watermark.maxOutOfOrdernessMs;
      if (this.watermark === null || wm > this.watermark) {
        this.watermark = wm;
        this._closeCheck();
      }
    }
    return this.config;
  }

  // ----- views / persistence -----

  listWindows() {
    return [...this.windows.values()]
      .sort((a, b) => a.key.localeCompare(b.key) || a.start - b.start)
      .map((w) => ({
        id: w.id, key: w.key, start: w.start, end: w.end, timezone: w.timezone,
        state: w.state, value: w.value, count: w.count,
        runningTotal: w.runningTotal, stale: w.stale,
        versionCount: w.versions.length,
      }));
  }

  windowDetail(id) {
    const w = this.windows.get(id);
    if (!w) return null;
    return {
      ...this.listWindows().find((x) => x.id === id),
      versions: w.versions,
      events: [...w.events.entries()].map(([eventId, e]) => ({ eventId, ...e })),
    };
  }

  snapshot() {
    return {
      config: this.config,
      watermark: this.watermark,
      maxEventTime: this.maxEventTime,
      revSeq: this.revSeq,
      windows: [...this.windows.values()].map((w) => ({
        ...w,
        events: [...w.events.entries()],
      })),
      seen: [...this.seen.entries()],
      seenRetractions: [...this.seenRetractions],
      revisions: this.revisions,
    };
  }

  static restore(snap, opts = {}) {
    const eng = new Engine(snap.config, opts);
    eng.watermark = snap.watermark;
    eng.maxEventTime = snap.maxEventTime;
    eng.revSeq = snap.revSeq;
    for (const w of snap.windows) {
      eng.windows.set(w.id, { ...w, events: new Map(w.events) });
    }
    eng.seen = new Map(snap.seen);
    eng.seenRetractions = new Set(snap.seenRetractions);
    eng.revisions = snap.revisions;
    return eng;
  }
}
