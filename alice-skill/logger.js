'use strict';

const fs = require('fs');
const path = require('path');

// --- Log Levels ---
const LEVELS = { DEBUG: 0, INFO: 1, WARN: 2, ERROR: 3 };
const LEVEL_NAMES = ['DEBUG', 'INFO ', 'WARN ', 'ERROR'];
const LEVEL_PAD = { DEBUG: 'DEBUG', INFO: 'INFO ', WARN: 'WARN ', ERROR: 'ERROR' };

function parseLevel(str) {
  const up = (str || 'info').toUpperCase();
  return LEVELS[up] !== undefined ? LEVELS[up] : LEVELS.INFO;
}

// --- Ring Buffer ---
class RingBuffer {
  constructor(size) {
    this.buf = new Array(size);
    this.size = size;
    this.head = 0;
    this.count = 0;
  }
  push(item) {
    this.buf[this.head] = item;
    this.head = (this.head + 1) % this.size;
    if (this.count < this.size) this.count++;
  }
  toArray() {
    if (this.count < this.size) return this.buf.slice(0, this.count);
    // wrap: oldest is at head, newest is at head-1
    return this.buf.slice(this.head).concat(this.buf.slice(0, this.head));
  }
}

// --- File Writer with Rotation ---
class FileWriter {
  constructor(filePath, maxBytes, maxRotated) {
    this.filePath = filePath;
    this.maxBytes = maxBytes;
    this.maxRotated = maxRotated;
    this.fd = -1;
    this.currentSize = 0;
    this._open();
  }

  _open() {
    try {
      const dir = path.dirname(this.filePath);
      if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
      this.fd = fs.openSync(this.filePath, 'a');
      try {
        this.currentSize = fs.fstatSync(this.fd).size;
      } catch (e) {
        this.currentSize = 0;
      }
    } catch (e) {
      this.fd = -1;
    }
  }

  write(line) {
    if (this.fd === -1) return;
    const buf = Buffer.from(line + '\n', 'utf-8');
    try {
      if (this.currentSize + buf.length > this.maxBytes) {
        this._rotate();
      }
      fs.writeSync(this.fd, buf);
      this.currentSize += buf.length;
    } catch (e) {
      // silently ignore write errors (disk full, etc.)
    }
  }

  _rotate() {
    try {
      fs.closeSync(this.fd);
    } catch (e) {}
    // Rotate: .log -> .log.1 (overwrite old .1)
    for (let i = this.maxRotated; i >= 1; i--) {
      const src = i === 1 ? this.filePath : `${this.filePath}.${i - 1}`;
      const dst = `${this.filePath}.${i}`;
      try { fs.renameSync(src, dst); } catch (e) {}
    }
    this.fd = fs.openSync(this.filePath, 'w');
    this.currentSize = 0;
  }

  close() {
    if (this.fd !== -1) {
      try { fs.closeSync(this.fd); } catch (e) {}
      this.fd = -1;
    }
  }
}

// --- Metrics Accumulator ---
class Metrics {
  constructor() {
    this.data = {};
  }
  record(key, value) {
    let m = this.data[key];
    if (!m) {
      m = { last: value, min: value, max: value, sum: value, count: 1, lastTs: new Date().toISOString() };
      this.data[key] = m;
    } else {
      m.last = value;
      if (value < m.min) m.min = value;
      if (value > m.max) m.max = value;
      m.sum += value;
      m.count++;
      m.lastTs = new Date().toISOString();
    }
  }
  get(key) {
    return this.data[key] || null;
  }
  getAll() {
    const out = {};
    for (const [k, m] of Object.entries(this.data)) {
      out[k] = { ...m, avg: +(m.sum / m.count).toFixed(1) };
    }
    return out;
  }
  reset() {
    this.data = {};
  }
}

// --- Logger Factory ---
function createLogger(options = {}) {
  const name = options.name || 'app';
  const logFile = options.logFile || null;
  const maxFileBytes = options.maxFileBytes || 2 * 1024 * 1024;
  const maxRotated = options.maxRotated || 1;
  const ringSize = options.ringSize || 200;
  const level = parseLevel(options.defaultLevel || process.env.LOG_LEVEL || 'info');
  const snapshotIntervalMs = options.snapshotIntervalMs || 30000;

  const ring = new RingBuffer(ringSize);
  const errorRing = new RingBuffer(50);
  const metrics = new Metrics();
  const startTime = Date.now();

  let fileWriter = null;
  if (logFile) {
    fileWriter = new FileWriter(logFile, maxFileBytes, maxRotated);
  }

  let snapshotFn = null;
  let snapshotTimer = null;

  // --- Core write ---
  function _write(lvl, tag, msg, ctx, requestId) {
    if (lvl < level) return;

    const ts = new Date().toISOString();
    const levelStr = LEVEL_PAD[LEVEL_NAMES[lvl].trim()] || 'INFO ';

    // Build tag string
    const tagStr = requestId ? `${tag}:${requestId}` : tag;

    // Build context string (serialization can throw on circular refs / BigInts —
    // never let logging itself blow up the caller).
    let ctxStr = '';
    if (ctx && typeof ctx === 'object' && Object.keys(ctx).length > 0) {
      try {
        ctxStr = ' ' + JSON.stringify(ctx);
      } catch (e) {
        ctxStr = ' [ctx serialization error]';
      }
    }

    // Format line
    const line = `${ts} ${levelStr} [${tagStr}] ${msg}${ctxStr}`;

    // Console output
    if (lvl >= LEVELS.ERROR) {
      process.stderr.write(line + '\n');
    } else {
      process.stdout.write(line + '\n');
    }

    // File output
    if (fileWriter) fileWriter.write(line);

    // Ring buffer entry
    const entry = { ts, level: LEVEL_NAMES[lvl].trim(), tag: tagStr, msg };
    if (ctx && Object.keys(ctx).length > 0) entry.ctx = ctx;
    ring.push(entry);

    // Error ring
    if (lvl >= LEVELS.ERROR) {
      const errEntry = { ...entry };
      if (!errEntry.ctx) errEntry.ctx = {};
      // Auto-capture stack if not provided
      if (!errEntry.ctx.stack) {
        const err = new Error();
        const stack = err.stack.split('\n').slice(3, 6).map(s => s.trim()).join(' | ');
        errEntry.ctx.stack = stack;
      }
      errorRing.push(errEntry);
    }
  }

  // --- Public logging methods ---
  function debug(tag, msg, ctx) { _write(LEVELS.DEBUG, tag, msg, ctx); }
  function info(tag, msg, ctx) { _write(LEVELS.INFO, tag, msg, ctx); }
  function warn(tag, msg, ctx) { _write(LEVELS.WARN, tag, msg, ctx); }
  function error(tag, msg, ctx) { _write(LEVELS.ERROR, tag, msg, ctx); }

  // --- Child logger (request tracing) ---
  function child(opts) {
    const rid = opts.requestId || '';
    return {
      debug: (tag, msg, ctx) => _write(LEVELS.DEBUG, tag, msg, ctx, rid),
      info:  (tag, msg, ctx) => _write(LEVELS.INFO, tag, msg, ctx, rid),
      warn:  (tag, msg, ctx) => _write(LEVELS.WARN, tag, msg, ctx, rid),
      error: (tag, msg, ctx) => _write(LEVELS.ERROR, tag, msg, ctx, rid),
      startTimer: (tag, op) => _startTimer(tag, op, rid),
    };
  }

  // --- Timer ---
  function _startTimer(tag, opName, requestId) {
    const start = Date.now();
    return {
      end(ctx) {
        const elapsed = Date.now() - start;
        const merged = { ...ctx, elapsed_ms: elapsed, op: opName };
        _write(LEVELS.DEBUG, tag, `${opName} completed`, merged, requestId);
        metrics.record(`${tag}:${opName}`, elapsed);
        return elapsed;
      }
    };
  }

  function startTimer(tag, opName) {
    return _startTimer(tag, opName, undefined);
  }

  // --- Metrics ---
  function metric(tag, metricName, value, unit) {
    const key = `${tag}:${metricName}`;
    metrics.record(key, value);
    const unitStr = unit ? ` ${unit}` : '';
    _write(LEVELS.DEBUG, tag, `${metricName}=${value}${unitStr}`);
  }

  // --- State snapshots ---
  function setSnapshotFn(fn) {
    snapshotFn = fn;
    // Start/restart snapshot timer
    if (snapshotTimer) clearInterval(snapshotTimer);
    if (level <= LEVELS.DEBUG && snapshotIntervalMs > 0) {
      snapshotTimer = setInterval(() => {
        snapshot();
      }, snapshotIntervalMs);
      snapshotTimer.unref();
    }
  }

  function snapshot() {
    if (!snapshotFn) return null;
    try {
      const state = snapshotFn();
      _write(LEVELS.DEBUG, 'SNAPSHOT', 'State dump', state);
      return state;
    } catch (e) {
      _write(LEVELS.ERROR, 'SNAPSHOT', 'Failed to capture state', { error: e.message });
      return null;
    }
  }

  // --- Diagnostics (for /debug endpoint) ---
  function getDiagnostics() {
    const mem = process.memoryUsage();
    const state = snapshotFn ? (() => { try { return snapshotFn(); } catch (e) { return { error: e.message }; } })() : {};

    // Filter errors to last hour
    const oneHourAgo = new Date(Date.now() - 3600000).toISOString();
    const recentErrors = errorRing.toArray().filter(e => e.ts >= oneHourAgo);

    return {
      name,
      uptime_sec: Math.floor((Date.now() - startTime) / 1000),
      timestamp: new Date().toISOString(),
      log_level: LEVEL_NAMES[level].trim().toLowerCase(),
      memory: {
        rss_mb: +(mem.rss / 1048576).toFixed(1),
        heapUsed_mb: +(mem.heapUsed / 1048576).toFixed(1),
        heapTotal_mb: +(mem.heapTotal / 1048576).toFixed(1),
        external_mb: +((mem.external || 0) / 1048576).toFixed(1),
      },
      state,
      metrics: metrics.getAll(),
      recent_logs: ring.toArray(),
      errors_last_hour: recentErrors,
    };
  }

  // --- Lifecycle ---
  function close() {
    if (snapshotTimer) clearInterval(snapshotTimer);
    if (fileWriter) fileWriter.close();
  }

  // Log startup
  _write(LEVELS.INFO, 'LOGGER', `Initialized`, {
    name,
    level: LEVEL_NAMES[level].trim().toLowerCase(),
    logFile: logFile || '(none)',
  });

  return {
    debug, info, warn, error,
    child,
    startTimer,
    metric,
    setSnapshotFn,
    snapshot,
    getDiagnostics,
    close,
    // Expose for advanced use
    getMetrics: () => metrics.getAll(),
    getRingBuffer: () => ring.toArray(),
    getLevel: () => level,
    LEVELS,
  };
}

module.exports = createLogger;
