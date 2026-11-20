// Checkpoint persistence: JSON snapshot on disk for task recovery.
import { mkdirSync, readFileSync, writeFileSync, renameSync, existsSync } from 'node:fs';
import { dirname } from 'node:path';
import { Engine } from './engine.js';

export class Store {
  constructor(file) {
    this.file = file;
  }

  save(engine) {
    mkdirSync(dirname(this.file), { recursive: true });
    const tmp = this.file + '.tmp';
    writeFileSync(tmp, JSON.stringify(engine.snapshot()));
    renameSync(tmp, this.file); // atomic-ish
  }

  load(opts = {}) {
    if (!existsSync(this.file)) return null;
    const snap = JSON.parse(readFileSync(this.file, 'utf8'));
    return Engine.restore(snap, opts);
  }
}
