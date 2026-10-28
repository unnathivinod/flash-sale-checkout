// Outbox relay: moves reservations from the Redis outbox stream to Amazon SQS.
//
// Why not send to SQS directly from the API? Because then "take stock in Redis"
// and "send to SQS" are two separate writes. If the API crashed between them we
// would lose an order (or queue one that has no stock). Writing to the outbox
// inside the same Lua script makes both happen together; this relay then
// delivers them, and only acknowledges a message after SQS has accepted it.
import os from 'node:os';
import Redis from 'ioredis';
import { SendMessageBatchCommand } from '@aws-sdk/client-sqs';
import * as C from './config.js';
import * as sqs from './sqs.js';

const rdb = new Redis(C.REDIS_URL);
rdb.on('error', (e) => console.warn('redis:', e.message));   // ioredis reconnects by itself
const client = sqs.client();
const name = `${os.hostname()}-${process.pid}`;

// ioredis returns stream fields as a flat list [k1, v1, k2, v2, ...]
const toObj = (flat) => Object.fromEntries(Array.from({ length: flat.length / 2 }, (_, i) => [flat[2 * i], flat[2 * i + 1]]));

async function ensureGroup() {
  try { await rdb.xgroup('CREATE', C.K.OUTBOX, C.G_RELAY, '0', 'MKSTREAM'); } catch (e) {
    if (!String(e).includes('BUSYGROUP')) throw e;
  }
}

async function deliver(url, entries) {
  if (!entries.length) return;
  const res = await client.send(new SendMessageBatchCommand({
    QueueUrl: url,
    Entries: entries.map(([, fields], i) => ({ Id: String(i), MessageBody: JSON.stringify(toObj(fields)) })),
  }));
  const ok = (res.Successful || []).map((s) => entries[Number(s.Id)][0]);
  if (ok.length) await rdb.xack(C.K.OUTBOX, C.G_RELAY, ...ok);       // only after SQS accepted them
  if (res.Failed?.length) console.warn(`${res.Failed.length} messages not accepted by SQS, will retry`);
}

async function main() {
  let url;
  while (!url) {
    try { await rdb.ping(); url = await sqs.ensureQueues(client); } catch (e) {
      console.log('waiting for redis/sqs:', e.message);
      await C.sleep(2000);
    }
  }
  console.log(`relay ${name} -> ${url}`);

  for (;;) {
    try {
      await ensureGroup();
      // Take over messages another relay read but never delivered (crash).
      const [, claimed] = await rdb.xautoclaim(C.K.OUTBOX, C.G_RELAY, name, 15000, '0', 'COUNT', 50);
      for (let i = 0; i < claimed.length; i += 10) await deliver(url, claimed.slice(i, i + 10));

      const resp = await rdb.xreadgroup('GROUP', C.G_RELAY, name, 'COUNT', 10, 'BLOCK', 1000, 'STREAMS', C.K.OUTBOX, '>');
      for (const [, entries] of resp || []) await deliver(url, entries);
    } catch (e) {
      const m = String(e);
      if (!m.includes('NOGROUP') && !m.includes('UNBLOCKED')) console.warn('relay error:', e.message);
      await C.sleep(500);
    }
  }
}

main();
