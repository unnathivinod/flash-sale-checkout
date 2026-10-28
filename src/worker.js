// Order worker: takes reserved orders off the queue and saves them to Postgres,
// outside the HTTP request. Users never wait on the database.
//
// Database safety layers (they hold even if Redis were wrong):
//   1. UPDATE ... WHERE stock > 0      -> stock never goes below zero
//   2. CHECK (stock >= 0)              -> the database refuses negative stock
//   3. UNIQUE (user_id, product_id)    -> one phone per customer
//   4. order_id PRIMARY KEY            -> a message delivered twice is saved once
import os from 'node:os';
import Redis from 'ioredis';
import pg from 'pg';
import { ReceiveMessageCommand, DeleteMessageBatchCommand } from '@aws-sdk/client-sqs';
import * as C from './config.js';
import * as sqs from './sqs.js';

const rdb = new Redis(C.REDIS_URL);
rdb.on('error', (e) => console.warn('redis:', e.message));   // ioredis reconnects by itself
const db = new pg.Pool({ connectionString: C.DATABASE_URL, max: C.WORKER_CONCURRENCY + 2 });
db.on('error', (e) => console.warn('postgres:', e.message));
const host = os.hostname();

// Save one order in ONE transaction. Returns the final status.
async function persist({ order_id: oid, user_id: uid, product_id: pid }) {
  const c = await db.connect();
  try {
    await c.query('BEGIN');
    const done = await c.query('SELECT 1 FROM orders WHERE order_id=$1', [oid]);
    if (done.rowCount) { await c.query('COMMIT'); return 'CONFIRMED'; }        // already saved earlier

    // THE key statement from the case study: decrement only if stock > 0.
    const upd = await c.query('UPDATE inventory SET stock = stock - 1 WHERE product_id = $1 AND stock > 0', [Number(pid)]);
    if (upd.rowCount === 0) { await c.query('COMMIT'); return 'FAILED_SOLD_OUT'; }  // never oversell

    const ins = await c.query(
      'INSERT INTO orders(order_id, user_id, product_id) VALUES($1,$2,$3) ON CONFLICT (user_id, product_id) DO NOTHING',
      [oid, uid, Number(pid)]);
    if (ins.rowCount === 0) { await c.query('ROLLBACK'); return 'REJECTED_DUPLICATE'; }  // undo the decrement
    await c.query('COMMIT');
    return 'CONFIRMED';
  } catch (e) {
    await c.query('ROLLBACK').catch(() => {});
    throw e;
  } finally {
    c.release();
  }
}

async function handle(order) {
  const status = await persist(order);
  const m = rdb.multi().hset(C.K.STATUS, order.order_id, status);
  if (status === 'CONFIRMED') m.sadd(C.K.CONFIRMED, order.order_id);   // a set: a retry is never counted twice
  if (status === 'REJECTED_DUPLICATE') m.incr(C.K.STOCK);              // give the token back
  await m.exec();
}

// ---------------------------------------------------------------- SQS mode
async function runSqs() {
  const client = sqs.client();
  let url;
  while (!url) {
    try { url = await sqs.ensureQueues(client); } catch (e) { console.log('waiting for sqs:', e.message); await C.sleep(2000); }
  }
  const loop = async () => {
    for (;;) {
      try {
        const res = await client.send(new ReceiveMessageCommand({
          QueueUrl: url, MaxNumberOfMessages: 10, WaitTimeSeconds: 2, VisibilityTimeout: 30,
        }));
        const done = [];
        for (const msg of res.Messages || []) {
          try {
            await handle(JSON.parse(msg.Body));
            done.push({ Id: String(done.length), ReceiptHandle: msg.ReceiptHandle });
          } catch (e) {
            // Not deleted -> SQS shows it again after 30 s. After 5 tries -> DLQ.
            console.warn('order failed, will retry:', e.message);
          }
        }
        if (done.length) await client.send(new DeleteMessageBatchCommand({ QueueUrl: url, Entries: done }));
      } catch (e) {
        console.warn('sqs loop error:', e.message);
        await C.sleep(1000);
      }
    }
  };
  await Promise.all(Array.from({ length: C.WORKER_CONCURRENCY }, loop));
}

// ---------------------------------------------------------------- Redis-stream mode
const toObj = (flat) => Object.fromEntries(Array.from({ length: flat.length / 2 }, (_, i) => [flat[2 * i], flat[2 * i + 1]]));

async function runRedis() {
  const loop = async (i) => {
    const name = `${host}-${process.pid}-${i}`;
    const r = rdb.duplicate();                     // own connection: XREADGROUP BLOCK holds it
    r.on('error', () => {});
    const process_ = async (entries) => {
      for (const [id, fields] of entries) {
        try {
          await handle(toObj(fields));
          await r.xack(C.K.OUTBOX, C.G_WORKERS, id);      // only after the DB commit
        } catch (e) { console.warn('order failed, will retry:', e.message); }
      }
    };
    for (;;) {
      try {
        try { await r.xgroup('CREATE', C.K.OUTBOX, C.G_WORKERS, '0', 'MKSTREAM'); } catch (e) {
          if (!String(e).includes('BUSYGROUP')) throw e;
        }
        const [, claimed] = await r.xautoclaim(C.K.OUTBOX, C.G_WORKERS, name, 30000, '0', 'COUNT', 50);
        await process_(claimed);
        const resp = await r.xreadgroup('GROUP', C.G_WORKERS, name, 'COUNT', 50, 'BLOCK', 2000, 'STREAMS', C.K.OUTBOX, '>');
        for (const [, entries] of resp || []) await process_(entries);
      } catch (e) {
        const m = String(e);
        if (!m.includes('NOGROUP') && !m.includes('UNBLOCKED')) console.warn('stream loop error:', e.message);
        await C.sleep(500);
      }
    }
  };
  await Promise.all(Array.from({ length: C.WORKER_CONCURRENCY }, (_, i) => loop(i)));
}

async function main() {
  for (let i = 0; ; i++) {
    try { await rdb.ping(); await db.query('SELECT 1'); break; } catch (e) {
      if (i > 60) throw e;
      console.log('waiting for redis/postgres:', e.message);
      await C.sleep(2000);
    }
  }
  console.log(`worker ${host} started: backend=${C.QUEUE_BACKEND} consumers=${C.WORKER_CONCURRENCY}`);
  if (C.QUEUE_BACKEND === 'sqs') await runSqs(); else await runRedis();
}

main();
