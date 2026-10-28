"""Order worker: takes reserved orders off the queue and saves them to Postgres,
outside the HTTP request. Users never wait on the database.

Database safety layers (they hold even if Redis were wrong):
  1. UPDATE ... WHERE stock > 0      -> stock never goes below zero
  2. CHECK (stock >= 0)              -> the database refuses negative stock
  3. UNIQUE (user_id, product_id)    -> one phone per customer
  4. order_id PRIMARY KEY            -> a message delivered twice is saved once
"""
import asyncio
import json
import logging
import os
import socket

import asyncpg
import redis.asyncio as aioredis

from . import config as C
from . import sqs

log = logging.getLogger("worker")


async def persist(db: asyncpg.Pool, order_id: str, user_id: str, product_id: int) -> tuple[str, bool]:
    """Save one order in one transaction. Returns (status, newly_saved)."""
    async with db.acquire() as c:
        async with c.transaction():
            if await c.fetchval("SELECT 1 FROM orders WHERE order_id=$1", order_id):
                return "CONFIRMED", False                        # already done earlier
            r = await c.execute(
                "UPDATE inventory SET stock = stock - 1 WHERE product_id = $1 AND stock > 0", product_id)
            if r.endswith(" 0"):
                return "FAILED_SOLD_OUT", False                  # Redis and DB disagreed: never oversell
            r = await c.execute(
                "INSERT INTO orders(order_id, user_id, product_id) VALUES($1,$2,$3) "
                "ON CONFLICT (user_id, product_id) DO NOTHING", order_id, user_id, product_id)
            if r.endswith(" 0"):
                raise _Duplicate()                               # roll back the decrement
            return "CONFIRMED", True


class _Duplicate(Exception):
    pass


async def handle(rdb, db, fields: dict) -> None:
    oid, uid, pid = fields["order_id"], fields["user_id"], int(fields["product_id"])
    try:
        status, _ = await persist(db, oid, uid, pid)
    except _Duplicate:
        status = "REJECTED_DUPLICATE"
    p = rdb.pipeline(transaction=True)
    p.hset(C.K_STATUS, oid, status)
    if status == "CONFIRMED":
        p.sadd(C.K_CONFIRMED, oid)       # a set, so a retried message is never counted twice
    if status == "REJECTED_DUPLICATE":
        p.incr(C.K_STOCK)                                        # give the token back
    await p.execute()


# ---------------------------------------------------------------- SQS mode
async def run_sqs(rdb, db, n: int):
    cl, url = sqs.client(), None
    while url is None:
        try:
            url = await asyncio.to_thread(sqs.ensure_queues, cl)
        except Exception as e:
            log.info("waiting for sqs: %s", e)
            await asyncio.sleep(2)

    async def loop(i):
        while True:
            try:
                res = await asyncio.to_thread(cl.receive_message, QueueUrl=url, MaxNumberOfMessages=10,
                                              WaitTimeSeconds=2, VisibilityTimeout=30)
                done = []
                for m in res.get("Messages", []):
                    try:
                        await handle(rdb, db, json.loads(m["Body"]))
                        done.append({"Id": str(len(done)), "ReceiptHandle": m["ReceiptHandle"]})
                    except Exception as e:
                        # Not deleted -> SQS shows it again after 30 s. After 5 tries -> DLQ.
                        log.warning("order failed, will retry: %s", e)
                if done:
                    await asyncio.to_thread(cl.delete_message_batch, QueueUrl=url, Entries=done)
            except Exception as e:
                log.warning("sqs loop error: %s", e)
                await asyncio.sleep(1)

    await asyncio.gather(*(loop(i) for i in range(n)))


# ---------------------------------------------------------------- Redis-stream mode
async def run_redis(rdb, db, n: int):
    host = socket.gethostname()

    async def ensure():
        try:
            await rdb.xgroup_create(C.K_OUTBOX, C.G_WORKERS, id="0", mkstream=True)
        except aioredis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

    async def process(name, entries):
        for mid, fields in entries:
            try:
                await handle(rdb, db, fields)
                await rdb.xack(C.K_OUTBOX, C.G_WORKERS, mid)     # only after the DB commit
            except Exception as e:
                log.warning("order failed, will retry: %s", e)

    async def loop(i):
        name = f"{host}-{os.getpid()}-{i}"
        while True:
            try:
                await ensure()
                _, claimed, *_ = await rdb.xautoclaim(C.K_OUTBOX, C.G_WORKERS, name, min_idle_time=30000, count=50)
                await process(name, claimed)
                resp = await rdb.xreadgroup(C.G_WORKERS, name, {C.K_OUTBOX: ">"}, count=50, block=2000)
                for _, entries in resp or []:
                    await process(name, entries)
            except Exception as e:
                if "NOGROUP" not in str(e) and "UNBLOCKED" not in str(e):
                    log.warning("stream loop error: %s", e)
                await asyncio.sleep(0.5)

    await asyncio.gather(*(loop(i) for i in range(n)))


async def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s worker %(message)s")
    rdb = aioredis.from_url(C.REDIS_URL, decode_responses=True)
    db = None
    while db is None:
        try:
            await rdb.ping()
            db = await asyncpg.create_pool(C.DATABASE_URL, min_size=1, max_size=C.WORKER_CONCURRENCY + 2)
        except Exception as e:
            log.info("waiting for redis/postgres: %s", e)
            await asyncio.sleep(2)
    log.info("worker %s started: backend=%s consumers=%d", socket.gethostname(), C.QUEUE_BACKEND, C.WORKER_CONCURRENCY)
    if C.QUEUE_BACKEND == "sqs":
        await run_sqs(rdb, db, C.WORKER_CONCURRENCY)
    else:
        await run_redis(rdb, db, C.WORKER_CONCURRENCY)


if __name__ == "__main__":
    asyncio.run(main())
