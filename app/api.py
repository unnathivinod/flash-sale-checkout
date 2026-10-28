"""Flash sale API (FastAPI).

The API is STATELESS: everything shared lives in Redis or Postgres, so we can
run many copies behind a load balancer. The checkout path never touches the
database. It runs one atomic Redis script and replies 202 Accepted.
"""
import asyncio
import os
import re
import secrets
import time
from contextlib import asynccontextmanager
from pathlib import Path

import asyncpg
import orjson
import redis.asyncio as aioredis
from fastapi import FastAPI, Request
from fastapi.responses import ORJSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from . import config as C
from . import sqs

USER_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
LUA = (Path(__file__).parent / "reserve.lua").read_text()


class State:
    rdb: aioredis.Redis
    db: asyncpg.Pool
    reserve = None
    sse_clients: set = set()
    snapshot: bytes = b"{}"


S = State()


# ------------------------------------------------------------------ startup
async def bootstrap():
    """Postgres is the source of truth. If Redis has no stock (fresh start or
    data loss) copy it from the database. SETNX = only if missing, so several
    replicas starting together cannot overwrite each other."""
    for _ in range(60):
        try:
            await S.rdb.ping()
            break
        except Exception:
            await asyncio.sleep(1)
    db_stock = await S.db.fetchval("SELECT stock FROM inventory WHERE product_id=$1", C.PRODUCT_ID)
    await S.rdb.set(C.K_STOCK, db_stock, nx=True)
    await S.rdb.set(C.K_START, int((time.time() + C.SALE_DELAY_SEC) * 1000), nx=True)
    await ensure_groups()


async def ensure_groups():
    group = C.G_RELAY if C.QUEUE_BACKEND == "sqs" else C.G_WORKERS
    try:
        await S.rdb.xgroup_create(C.K_OUTBOX, group, id="0", mkstream=True)
    except aioredis.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise


async def connect_db():
    for _ in range(60):
        try:
            return await asyncpg.create_pool(C.DATABASE_URL, min_size=2, max_size=C.DB_POOL_MAX)
        except Exception:
            await asyncio.sleep(1)
    raise RuntimeError("postgres not reachable")


@asynccontextmanager
async def lifespan(_app):
    S.rdb = aioredis.from_url(C.REDIS_URL, decode_responses=True, max_connections=200,
                              health_check_interval=10, retry_on_timeout=True)
    S.db = await connect_db()
    S.reserve = S.rdb.register_script(LUA)
    await bootstrap()
    task = asyncio.create_task(broadcaster())
    yield
    task.cancel()
    await S.db.close()
    await S.rdb.aclose()


app = FastAPI(lifespan=lifespan, default_response_class=ORJSONResponse, docs_url=None, redoc_url=None)


def j(status: int, body: dict, headers: dict | None = None):
    h = {"Cache-Control": "no-store"}
    if headers:
        h.update(headers)
    return ORJSONResponse(body, status_code=status, headers=h)


def now_ms() -> int:
    return int(time.time() * 1000)


# ------------------------------------------------------------------ live updates (SSE)
async def build_snapshot() -> dict:
    p = S.rdb.pipeline(transaction=False)
    p.mget(C.K_START, C.K_STOCK)
    p.scard(C.K_CONFIRMED)
    p.lrange(C.K_FEED, 0, 5)
    (start, stock), confirmed, feed = await p.execute()
    start, stock, confirmed = int(start or 0), int(stock or 0), int(confirmed or 0)
    t = now_ms()
    status = "UPCOMING" if t < start else ("SOLD_OUT" if stock <= 0 else "LIVE")
    items = []
    for f in feed:
        oid, _, ts = f.partition("|")
        items.append({"order": oid[:8] + "…" + oid[-4:], "at": int(ts or 0)})
    return {"status": status, "startsAt": start, "serverTime": t, "stockLeft": stock,
            "totalStock": C.TOTAL_STOCK, "confirmed": confirmed, "feed": items}


async def broadcaster():
    """One Redis read per second per process, pushed to every connected
    browser. 100,000 viewers do NOT mean 100,000 Redis reads."""
    while True:
        try:
            snap = await build_snapshot()
            S.snapshot = orjson.dumps(snap)
            for q in list(S.sse_clients):
                if q.full():
                    q.get_nowait()          # slow client: drop the old update
                q.put_nowait(S.snapshot)
        except Exception:
            pass
        await asyncio.sleep(1)


@app.get("/api/events")
async def events(request: Request):
    q: asyncio.Queue = asyncio.Queue(maxsize=1)
    S.sse_clients.add(q)
    q.put_nowait(S.snapshot)

    async def stream():
        try:
            yield b"retry: 3000\n\n"
            while not await request.is_disconnected():
                try:
                    data = await asyncio.wait_for(q.get(), timeout=15)
                    yield b"data: " + data + b"\n\n"
                except asyncio.TimeoutError:
                    yield b": keep-alive\n\n"
        finally:
            S.sse_clients.discard(q)

    return StreamingResponse(stream(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


# ------------------------------------------------------------------ public API
@app.get("/healthz")
async def healthz():
    return {"ok": True}


@app.get("/api/sale")
async def sale():
    return j(200, await build_snapshot())


@app.post("/api/checkout")
async def checkout(request: Request):
    """The hot path: validate, one atomic Redis script, reply. No database."""
    try:
        body = orjson.loads(await request.body())
        user = body.get("userId", "")
    except Exception:
        user = ""
    if not isinstance(user, str) or not USER_RE.match(user):
        return j(400, {"error": "invalid userId"})

    if C.RATE_LIMIT_PER_SEC > 0 and not await allowed(request):
        return j(429, {"error": "too many requests"}, {"Retry-After": "1"})

    # ONE round trip to Redis: start-time check, stock check, decrement,
    # one-per-customer and enqueue all happen inside the Lua script.
    t = now_ms()
    try:
        code, oid, left = await S.reserve(
            keys=[C.K_STOCK, C.K_BUYERS, C.K_STATUS, C.K_OUTBOX, C.K_FEED,
                  C.K_METRICS + str(t // 1000), C.K_START],
            args=[user, "ord_" + secrets.token_hex(8), C.PRODUCT_ID, t])
    except Exception:
        return j(503, {"error": "busy, try again"}, {"Retry-After": "1"})

    if code == 3:
        return j(425, {"status": "NOT_STARTED", "startsAt": int(oid)})

    if code in (1, 2):
        url = f"/api/orders/{oid}"
        return j(202, {"status": "QUEUED", "orderId": oid, "trackUrl": url,
                       "duplicate": code == 2, "stockLeft": left}, {"Location": url})
    return j(409, {"status": "SOLD_OUT"})


@app.get("/api/orders/{order_id}")
async def order_status(order_id: str):
    try:
        st = await S.rdb.hget(C.K_STATUS, order_id)
    except Exception:
        return j(503, {"error": "busy, try again"})
    if st is None:
        return j(404, {"error": "unknown order"})
    return j(200, {"orderId": order_id, "status": st})


@app.post("/api/naive/checkout")
async def naive_checkout(request: Request):
    """The ORIGINAL buggy design, kept on purpose to prove the bug.
    It reads the stock, then writes back a value computed from that read.
    Two requests can read the same number and both succeed (lost update)."""
    try:
        user = orjson.loads(await request.body()).get("userId", "")
    except Exception:
        user = ""
    if not isinstance(user, str) or not USER_RE.match(user):
        return j(400, {"error": "invalid userId"})
    async with S.db.acquire() as c:
        stock = await c.fetchval("SELECT stock FROM naive_inventory WHERE product_id=$1", C.PRODUCT_ID)
        if stock <= 0:
            return j(409, {"status": "SOLD_OUT"})
        await asyncio.sleep(0.002)  # pretend to validate payment
        await c.execute("UPDATE naive_inventory SET stock=$1 WHERE product_id=$2", stock - 1, C.PRODUCT_ID)
        await c.execute("INSERT INTO naive_orders(user_id) VALUES($1)", user)
    return j(200, {"status": "CONFIRMED"})


# ------------------------------------------------------------------ monitoring
@app.get("/api/stats")
async def stats():
    p = S.rdb.pipeline(transaction=False)
    p.get(C.K_STOCK)
    p.hlen(C.K_BUYERS)
    p.xlen(C.K_OUTBOX)
    group = C.G_RELAY if C.QUEUE_BACKEND == "sqs" else C.G_WORKERS
    p.xpending(C.K_OUTBOX, group)
    try:
        stock, reserved, _, pend = await p.execute(raise_on_error=False)
    except Exception:
        stock, reserved, pend = 0, 0, {}
    outbox_pending = pend.get("pending", 0) if isinstance(pend, dict) else 0

    async with S.db.acquire() as c:
        db_stock = await c.fetchval("SELECT stock FROM inventory WHERE product_id=$1", C.PRODUCT_ID)
        confirmed = await c.fetchval("SELECT count(*) FROM orders")
        n_stock = await c.fetchval("SELECT stock FROM naive_inventory WHERE product_id=$1", C.PRODUCT_ID)
        n_orders = await c.fetchval("SELECT count(*) FROM naive_orders")

    queue = {"backend": C.QUEUE_BACKEND, "outboxPending": outbox_pending}
    if C.QUEUE_BACKEND == "sqs":
        try:
            def _depth():
                cl = sqs.client()
                return sqs.depth(cl, cl.get_queue_url(QueueName=C.SQS_QUEUE_NAME)["QueueUrl"])
            queue["sqs"] = await asyncio.to_thread(_depth)
        except Exception:
            queue["sqs"] = None
    in_queue = outbox_pending + ((queue.get("sqs") or {}).get("visible", 0) + (queue.get("sqs") or {}).get("inFlight", 0))

    total = C.TOTAL_STOCK
    return j(200, {
        "totalStock": total,
        "redisStockLeft": int(stock or 0), "reserved": int(reserved or 0),
        "dbStockLeft": db_stock, "confirmedOrders": confirmed, "oversold": confirmed > total,
        "queuePending": in_queue, "queue": queue,
        "naive": {"dbStockLeft": n_stock, "orders": n_orders,
                  "oversold": n_orders > total, "oversoldBy": max(0, n_orders - total)},
    })


@app.get("/api/metrics")
async def metrics():
    """Results per second for the last 60 seconds (for the dashboard charts)."""
    now = int(time.time())
    secs = list(range(now - 59, now + 1))
    p = S.rdb.pipeline(transaction=False)
    for s in secs:
        p.hgetall(C.K_METRICS + str(s))
    rows = await p.execute()
    return j(200, {"seconds": secs, "series": [
        {"accepted": int(r.get("accepted", 0)), "soldOut": int(r.get("sold_out", 0)),
         "duplicate": int(r.get("duplicate", 0))} for r in rows]})


@app.post("/api/admin/reset")
async def reset(request: Request, delay: int = C.SALE_DELAY_SEC):
    if request.headers.get("x-admin-token") != C.ADMIN_TOKEN:
        return j(403, {"error": "forbidden"})
    total = C.TOTAL_STOCK
    async with S.db.acquire() as c:
        async with c.transaction():
            await c.execute("TRUNCATE orders, naive_orders")
            await c.execute("UPDATE inventory SET stock=$1 WHERE product_id=$2", total, C.PRODUCT_ID)
            await c.execute("UPDATE naive_inventory SET stock=$1 WHERE product_id=$2", total, C.PRODUCT_ID)
    if C.QUEUE_BACKEND == "sqs":
        def _purge():
            cl = sqs.client()
            cl.purge_queue(QueueUrl=cl.get_queue_url(QueueName=C.SQS_QUEUE_NAME)["QueueUrl"])
        try:
            await asyncio.to_thread(_purge)
        except Exception:
            pass
    start = now_ms() + max(0, delay) * 1000
    p = S.rdb.pipeline(transaction=True)
    p.delete(C.K_BUYERS, C.K_STATUS, C.K_OUTBOX, C.K_FEED, C.K_CONFIRMED)
    p.set(C.K_STOCK, total)
    p.set(C.K_START, start)
    await p.execute()
    await ensure_groups()
    return j(200, {"ok": True, "stock": total, "startsAt": start})


# ------------------------------------------------------------------ helpers
async def allowed(request: Request) -> bool:
    """Per-IP limit shared by all replicas (counters in Redis). Used on AWS,
    where there is no NGINX in front."""
    if request.headers.get("x-loadtest-key") == C.LOADTEST_KEY:
        return True
    ua = (request.headers.get("user-agent") or "").lower()
    if not ua or any(b in ua for b in ("python-requests", "scrapy", "aiohttp", "bot", "spider", "crawler")):
        return False
    xff = request.headers.get("x-forwarded-for")
    ip = xff.split(",")[-1].strip() if xff else (request.client.host if request.client else "?")
    key = f"rl:{ip}:{int(time.time())}"
    p = S.rdb.pipeline(transaction=False)
    p.incr(key)
    p.expire(key, 2)
    n, _ = await p.execute()
    return n <= C.RATE_LIMIT_PER_SEC


if C.STATIC_DIR and os.path.isdir(C.STATIC_DIR):
    app.mount("/", StaticFiles(directory=C.STATIC_DIR, html=True), name="static")
