"""Outbox relay: moves reservations from the Redis outbox stream to Amazon SQS.

Why not send to SQS directly from the API? Because then "take stock in Redis"
and "send to SQS" are two separate writes. If the API crashed between them we
would lose an order (or queue one that has no stock). Writing to the outbox
inside the same Lua script makes both happen together; this relay then
delivers them, and only acknowledges a message after SQS has accepted it.
"""
import asyncio
import json
import logging
import os
import socket

import redis.asyncio as aioredis

from . import config as C
from . import sqs

log = logging.getLogger("relay")


async def ensure_group(rdb):
    try:
        await rdb.xgroup_create(C.K_OUTBOX, C.G_RELAY, id="0", mkstream=True)
    except aioredis.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise


async def send(cl, url, rdb, entries):
    batch = [{"Id": str(i), "MessageBody": json.dumps(fields)} for i, (_, fields) in enumerate(entries)]
    res = await asyncio.to_thread(cl.send_message_batch, QueueUrl=url, Entries=batch)
    ok_ids = [entries[int(s["Id"])][0] for s in res.get("Successful", [])]
    if ok_ids:
        await rdb.xack(C.K_OUTBOX, C.G_RELAY, *ok_ids)   # only after SQS accepted them
    if res.get("Failed"):
        log.warning("%d messages not accepted by SQS, will retry", len(res["Failed"]))


async def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s relay %(message)s")
    rdb = aioredis.from_url(C.REDIS_URL, decode_responses=True)
    name = f"{socket.gethostname()}-{os.getpid()}"
    cl, url = None, None
    while url is None:
        try:
            await rdb.ping()
            cl = sqs.client()
            url = await asyncio.to_thread(sqs.ensure_queues, cl)
        except Exception as e:
            log.info("waiting for redis/sqs: %s", e)
            await asyncio.sleep(2)
    log.info("relay %s -> %s", name, url)

    while True:
        try:
            await ensure_group(rdb)
            # Take over messages another relay read but never delivered (crash).
            _, claimed, *_ = await rdb.xautoclaim(C.K_OUTBOX, C.G_RELAY, name, min_idle_time=15000, count=50)
            for i in range(0, len(claimed), 10):
                await send(cl, url, rdb, claimed[i:i + 10])

            resp = await rdb.xreadgroup(C.G_RELAY, name, {C.K_OUTBOX: ">"}, count=10, block=1000)
            for _, entries in resp or []:
                if entries:
                    await send(cl, url, rdb, entries)
        except Exception as e:
            if "NOGROUP" not in str(e) and "UNBLOCKED" not in str(e):
                log.warning("relay error: %s", e)
            await asyncio.sleep(0.5)


if __name__ == "__main__":
    asyncio.run(main())
