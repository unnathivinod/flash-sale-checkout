// Flash sale API (Node.js + Fastify).
//
// The API is STATELESS: everything shared lives in Redis or Postgres, so we can
// run many copies behind a load balancer. The checkout path never touches the
// database: it runs one atomic Redis script and replies 202 Accepted.
//
// Event-loop safety: every I/O call (Redis, Postgres, SQS) is non-blocking, and
// nothing CPU-heavy runs on the request path, so one Node process can hold
// thousands of requests at once. node:cluster starts one event loop per CPU core.
import cluster from 'node:cluster';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import Fastify from 'fastify';
import Redis from 'ioredis';
import pg from 'pg';
import * as C from './config.js';
import * as sqs from './sqs.js';

const here = path.dirname(fileURLToPath(import.meta.url));
const LUA = fs.readFileSync(path.join(here, 'reserve.lua'), 'utf8');
const USER_RE = /^[A-Za-z0-9_-]{1,64}$/;

if (cluster.isPrimary && C.WEB_PROCESSES > 1) {
  for (let i = 0; i < C.WEB_PROCESSES; i++) cluster.fork();
  cluster.on('exit', (w, code) => { if (code !== 0) cluster.fork(); });   // replace a crashed process
} else {
  await main();
}

async function main() {
  const rdb = new Redis(C.REDIS_URL, { maxRetriesPerRequest: 2, enableOfflineQueue: true });
  rdb.on('error', (e) => console.warn('redis:', e.message));   // logged; ioredis reconnects by itself
  rdb.defineCommand('reserve', { numberOfKeys: 7, lua: LUA });
  const db = new pg.Pool({ connectionString: C.DATABASE_URL, max: C.DB_POOL_MAX });
  db.on('error', (e) => console.warn('postgres:', e.message));
  const sqsClient = C.QUEUE_BACKEND === 'sqs' ? sqs.client() : null;

  await bootstrap(rdb, db);

  const app = Fastify({ logger: false, bodyLimit: 1024 });
  const sseClients = new Set();
  let snapshot = '{}';

  const send = (reply, code, body, headers = {}) =>
    reply.code(code).headers({ 'cache-control': 'no-store', ...headers }).send(body);

  // ---------------------------------------------------------------- live updates (SSE)
  async function buildSnapshot() {
    const [[, startStock], [, confirmed], [, feed]] = await rdb.multi()
      .mget(C.K.START, C.K.STOCK).scard(C.K.CONFIRMED).lrange(C.K.FEED, 0, 5).exec();
    const start = Number(startStock[0] || 0), stock = Number(startStock[1] || 0);
    const t = Date.now();
    const status = t < start ? 'UPCOMING' : (stock <= 0 ? 'SOLD_OUT' : 'LIVE');
    const items = feed.map((f) => {
      const [oid, ts] = f.split('|');
      return { order: `${oid.slice(0, 8)}…${oid.slice(-4)}`, at: Number(ts || 0) };
    });
    return { status, startsAt: start, serverTime: t, stockLeft: stock, totalStock: C.TOTAL_STOCK, confirmed, feed: items };
  }

  // One Redis read per second per process, pushed to every connected browser.
  // 100,000 viewers do NOT mean 100,000 Redis reads.
  setInterval(async () => {
    try {
      snapshot = JSON.stringify(await buildSnapshot());
      for (const res of sseClients) res.write(`data: ${snapshot}\n\n`);
    } catch { /* Redis briefly unavailable: keep the last snapshot */ }
  }, 1000).unref();

  app.get('/api/events', (req, reply) => {
    reply.hijack();
    const res = reply.raw;
    res.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-cache', 'x-accel-buffering': 'no', connection: 'keep-alive' });
    res.write(`retry: 3000\n\ndata: ${snapshot}\n\n`);
    sseClients.add(res);
    const ping = setInterval(() => res.write(': keep-alive\n\n'), 15000);
    req.raw.on('close', () => { clearInterval(ping); sseClients.delete(res); });
  });

  // ---------------------------------------------------------------- public API
  app.get('/healthz', async () => ({ ok: true }));

  app.get('/api/sale', async (req, reply) => send(reply, 200, await buildSnapshot()));

  // The hot path: validate, ONE atomic Redis script, reply. No database.
  app.post('/api/checkout', async (req, reply) => {
    const user = req.body?.userId;
    if (typeof user !== 'string' || !USER_RE.test(user)) return send(reply, 400, { error: 'invalid userId' });

    if (C.RATE_LIMIT_PER_SEC > 0 && !(await allowed(req))) {
      return send(reply, 429, { error: 'too many requests' }, { 'retry-after': '1' });
    }

    // One round trip: start-time check, stock check, decrement, one-per-customer
    // and enqueue all happen inside the Lua script.
    const t = Date.now();
    let code, oid, left;
    try {
      [code, oid, left] = await rdb.reserve(
        C.K.STOCK, C.K.BUYERS, C.K.STATUS, C.K.OUTBOX, C.K.FEED, C.K.METRICS + Math.floor(t / 1000), C.K.START,
        user, `ord_${crypto.randomBytes(8).toString('hex')}`, C.PRODUCT_ID, t);
    } catch {
      return send(reply, 503, { error: 'busy, try again' }, { 'retry-after': '1' });
    }

    if (code === 3) return send(reply, 425, { status: 'NOT_STARTED', startsAt: Number(oid) });
    if (code === 1 || code === 2) {
      const url = `/api/orders/${oid}`;
      return send(reply, 202, { status: 'QUEUED', orderId: oid, trackUrl: url, duplicate: code === 2, stockLeft: left }, { location: url });
    }
    return send(reply, 409, { status: 'SOLD_OUT' });
  });

  app.get('/api/orders/:id', async (req, reply) => {
    let st;
    try { st = await rdb.hget(C.K.STATUS, req.params.id); } catch { return send(reply, 503, { error: 'busy, try again' }); }
    if (st === null) return send(reply, 404, { error: 'unknown order' });
    return send(reply, 200, { orderId: req.params.id, status: st });
  });

  // The ORIGINAL buggy design, kept on purpose to prove the bug. It reads the
  // stock, then writes back a value computed from that read. Two requests can
  // read the same number and both succeed (lost update).
  app.post('/api/naive/checkout', async (req, reply) => {
    const user = req.body?.userId;
    if (typeof user !== 'string' || !USER_RE.test(user)) return send(reply, 400, { error: 'invalid userId' });
    const c = await db.connect();
    try {
      const { rows } = await c.query('SELECT stock FROM naive_inventory WHERE product_id=$1', [C.PRODUCT_ID]);
      const stock = rows[0].stock;
      if (stock <= 0) return send(reply, 409, { status: 'SOLD_OUT' });
      await C.sleep(2);                                        // pretend to validate payment
      await c.query('UPDATE naive_inventory SET stock=$1 WHERE product_id=$2', [stock - 1, C.PRODUCT_ID]);
      await c.query('INSERT INTO naive_orders(user_id) VALUES($1)', [user]);
      return send(reply, 200, { status: 'CONFIRMED' });
    } finally {
      c.release();
    }
  });

  // ---------------------------------------------------------------- monitoring
  app.get('/api/stats', async (req, reply) => {
    const group = C.QUEUE_BACKEND === 'sqs' ? C.G_RELAY : C.G_WORKERS;
    let stock = 0, reserved = 0, outboxPending = 0;
    try {
      const r = await rdb.multi().get(C.K.STOCK).hlen(C.K.BUYERS).xpending(C.K.OUTBOX, group).exec();
      stock = Number(r[0][1] || 0); reserved = Number(r[1][1] || 0);
      outboxPending = Array.isArray(r[2][1]) ? Number(r[2][1][0] || 0) : 0;
    } catch { /* keep zeros */ }

    const q = async (sql) => (await db.query(sql, sql.includes('$1') ? [C.PRODUCT_ID] : [])).rows[0];
    const dbStock = (await q('SELECT stock FROM inventory WHERE product_id=$1')).stock;
    const confirmed = Number((await q('SELECT count(*) AS n FROM orders')).n);
    const nStock = (await q('SELECT stock FROM naive_inventory WHERE product_id=$1')).stock;
    const nOrders = Number((await q('SELECT count(*) AS n FROM naive_orders')).n);

    const queue = { backend: C.QUEUE_BACKEND, outboxPending };
    if (sqsClient) {
      try { queue.sqs = await sqs.depth(sqsClient, await sqs.queueUrl(sqsClient)); } catch { queue.sqs = null; }
    }
    const inQueue = outboxPending + (queue.sqs ? queue.sqs.visible + queue.sqs.inFlight : 0);

    return send(reply, 200, {
      totalStock: C.TOTAL_STOCK,
      redisStockLeft: stock, reserved, dbStockLeft: dbStock, confirmedOrders: confirmed,
      oversold: confirmed > C.TOTAL_STOCK, queuePending: inQueue, queue,
      naive: { dbStockLeft: nStock, orders: nOrders, oversold: nOrders > C.TOTAL_STOCK, oversoldBy: Math.max(0, nOrders - C.TOTAL_STOCK) },
    });
  });

  // Results per second for the last 60 seconds (dashboard chart).
  app.get('/api/metrics', async (req, reply) => {
    const now = Math.floor(Date.now() / 1000);
    const secs = Array.from({ length: 60 }, (_, i) => now - 59 + i);
    const p = rdb.pipeline();
    secs.forEach((s) => p.hgetall(C.K.METRICS + s));
    const rows = (await p.exec()).map(([, r]) => r || {});
    return send(reply, 200, {
      seconds: secs,
      series: rows.map((r) => ({ accepted: Number(r.accepted || 0), soldOut: Number(r.sold_out || 0), duplicate: Number(r.duplicate || 0) })),
    });
  });

  app.post('/api/admin/reset', async (req, reply) => {
    if (req.headers['x-admin-token'] !== C.ADMIN_TOKEN) return send(reply, 403, { error: 'forbidden' });
    const delay = Math.max(0, parseInt(req.query.delay ?? C.SALE_DELAY_SEC, 10) || 0);
    const c = await db.connect();
    try {
      await c.query('BEGIN');
      await c.query('TRUNCATE orders, naive_orders');
      await c.query('UPDATE inventory SET stock=$1 WHERE product_id=$2', [C.TOTAL_STOCK, C.PRODUCT_ID]);
      await c.query('UPDATE naive_inventory SET stock=$1 WHERE product_id=$2', [C.TOTAL_STOCK, C.PRODUCT_ID]);
      await c.query('COMMIT');
    } catch (e) {
      await c.query('ROLLBACK');
      throw e;
    } finally {
      c.release();
    }
    if (sqsClient) {
      try {
        const { PurgeQueueCommand } = await import('@aws-sdk/client-sqs');
        await sqsClient.send(new PurgeQueueCommand({ QueueUrl: await sqs.queueUrl(sqsClient) }));
      } catch { /* queue may not exist yet */ }
    }
    const start = Date.now() + delay * 1000;
    await rdb.multi()
      .del(C.K.BUYERS, C.K.STATUS, C.K.OUTBOX, C.K.FEED, C.K.CONFIRMED)
      .set(C.K.STOCK, C.TOTAL_STOCK).set(C.K.START, start).exec();
    await ensureGroup(rdb);
    return send(reply, 200, { ok: true, stock: C.TOTAL_STOCK, startsAt: start });
  });

  // Per-IP limit shared by all replicas (counters in Redis). Used on AWS,
  // where there is no NGINX in front.
  async function allowed(req) {
    if (req.headers['x-loadtest-key'] === C.LOADTEST_KEY) return true;
    const ua = (req.headers['user-agent'] || '').toLowerCase();
    if (!ua || ['python-requests', 'scrapy', 'aiohttp', 'bot', 'spider', 'crawler'].some((b) => ua.includes(b))) return false;
    const xff = req.headers['x-forwarded-for'];
    const ip = xff ? xff.split(',').pop().trim() : req.ip;     // last entry = added by the load balancer
    const key = `rl:${ip}:${Math.floor(Date.now() / 1000)}`;
    const [[, n]] = await rdb.multi().incr(key).expire(key, 2).exec();
    return n <= C.RATE_LIMIT_PER_SEC;
  }

  // On AWS the API also serves the two pages (no NGINX there).
  if (C.STATIC_DIR) {
    const files = { '/': 'index.html', '/index.html': 'index.html', '/stats.html': 'stats.html' };
    for (const [route, file] of Object.entries(files)) {
      app.get(route, (req, reply) => reply.type('text/html').header('cache-control', 'no-cache')
        .send(fs.readFileSync(path.join(C.STATIC_DIR, file))));
    }
  }

  const shutdown = async () => { await app.close(); await db.end(); rdb.disconnect(); process.exit(0); };
  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);

  await app.listen({ host: '0.0.0.0', port: C.PORT });
  console.log(`api ${process.pid} listening on :${C.PORT}`);
}

// Postgres is the source of truth. If Redis has no stock (fresh start or data
// loss) copy it from the database. SET NX = only if missing, so several
// replicas starting together cannot overwrite each other.
async function bootstrap(rdb, db) {
  for (let i = 0; ; i++) {
    try { await rdb.ping(); await db.query('SELECT 1'); break; } catch (e) {
      if (i > 60) throw e;
      await C.sleep(1000);
    }
  }
  const { rows } = await db.query('SELECT stock FROM inventory WHERE product_id=$1', [C.PRODUCT_ID]);
  await rdb.set(C.K.STOCK, rows[0].stock, 'NX');
  await rdb.set(C.K.START, Date.now() + C.SALE_DELAY_SEC * 1000, 'NX');
  await ensureGroup(rdb);
}

async function ensureGroup(rdb) {
  const group = C.QUEUE_BACKEND === 'sqs' ? C.G_RELAY : C.G_WORKERS;
  try { await rdb.xgroup('CREATE', C.K.OUTBOX, group, '0', 'MKSTREAM'); } catch (e) {
    if (!String(e).includes('BUSYGROUP')) throw e;
  }
}
