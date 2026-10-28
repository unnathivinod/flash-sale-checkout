# Flash Sale Checkout: 500 phones, 100k requests, zero overselling

A re-architected checkout for a festival flash sale. The original design confirmed **540 orders for 500 phones**. This version sells **exactly 500**, keeps the database off the request path, and keeps working when parts of the system fail mid-sale.

**Stack:** Node.js 22 (Fastify) · Redis (Lua, Streams) · Amazon SQS (LocalStack locally) · PostgreSQL · NGINX / AWS ALB · Docker · k6

## How it works

```
Browser ──► NGINX / AWS ALB ──► Node.js x5 ──► Redis Lua script (one atomic step):
 click lock    rate limit,         (stateless)     sale open? already bought? stock > 0?
 live SSE      bot block                           take token + write order to OUTBOX
                                     │
                       202 Accepted + tracking URL (no database call)
                                                        │
                       Relay ──► Amazon SQS (+ dead-letter queue) ──► Workers ──► PostgreSQL
                       (outbox → SQS, ack after send)                 UPDATE ... WHERE stock > 0
```

1. The button locks on the first click, so one user sends one request.
2. NGINX (AWS ALB in the cloud) blocks bots, rate-limits per IP and spreads load over 5 API servers.
3. The API runs **one Lua script** in Redis: it checks the sale has started, returns the same order if this user already reserved, checks stock, takes a token and writes the order to an **outbox stream**, all atomically.
4. The API replies **202 Accepted** with a tracking URL. No database call on the hot path.
5. A **relay** moves orders from the outbox to **Amazon SQS** and only acknowledges them after SQS accepts them (transactional outbox pattern).
6. **Workers** take orders from SQS and save each one in a single transaction with `UPDATE inventory SET stock = stock - 1 WHERE product_id = $1 AND stock > 0`. Failed messages are retried and then moved to a dead-letter queue.
7. The page gets live stock updates over **Server-Sent Events** and shows the order as confirmed.

## What makes it different

| | |
|---|---|
| **Bug reproduced** | The original read-then-update design is kept at `/api/naive/checkout`, so you can watch it oversell next to the fix |
| **Chaos tested** | Workers killed and Redis restarted mid-sale: still exactly 500 sold, nothing lost |
| **Transactional outbox** | Taking stock and queueing the order happen in one atomic step, then delivery to SQS is retried until it succeeds |
| **Four layers against overselling** | Redis Lua token, conditional UPDATE, `CHECK (stock >= 0)`, `UNIQUE (user_id, product_id)` + `order_id` primary key |
| **Idempotent** | Double clicks and network retries return the same order, never a second phone |
| **Live UI** | 500-dot stock grid, order progress steps and a live feed over SSE; dashboard with per-second charts and queue depth |

## Run it on Windows

Needs **Docker Desktop** running. Node.js does not need to be installed.

```powershell
docker compose up --build
```

First build takes 3 to 5 minutes. Then open:

| URL | What |
|---|---|
| http://localhost:8080 | Shop page (sale opens 30 s after startup) |
| http://localhost:8080/stats.html | Live monitor, charts and **Reset sale** |

Stop with `Ctrl+C`, then `docker compose down`.

> If LocalStack (the local SQS) will not start on your machine, set `QUEUE_BACKEND: redis` in `docker-compose.yml`. Workers then read the Redis outbox directly and everything else stays the same.

## Load tests

In a **second** PowerShell window, from the project folder:

```powershell
# 1. Original design (read then update). Expect MORE than 500 sold.
docker run --rm --network flashsale-net -v "${PWD}/loadtest:/scripts" grafana/k6 run /scripts/naive.js

# 2. New design: 1,000 users, 20,000 requests. Expect EXACTLY 500 sold.
docker run --rm --network flashsale-net -v "${PWD}/loadtest:/scripts" -e RESET=0 grafana/k6 run /scripts/flash-sale.js
```

Keep `stats.html` open while they run.

## Chaos test

Window 1 starts a 40-second rush:

```powershell
docker run --rm --network flashsale-net -v "${PWD}/loadtest:/scripts" grafana/k6 run /scripts/chaos.js
```

As soon as it prints *"Sale is live"*, run these in window 2, one at a time:

```powershell
docker compose kill worker        # all workers die; orders pile up in the queue
docker compose restart redis      # Redis restarts mid-sale
docker compose up -d worker       # workers come back and drain the queue
```

Expected: `CHAOS RESULT` shows **Confirmed in DB: 500**, stock 0, queue empty, **not oversold**. On the dashboard you can watch the queue fill while workers are down, then drain.

## Project structure

```
src/api.js           Node.js API (Fastify): checkout, order status, SSE, stats, naive endpoint, rate limiter
src/reserve.lua      The atomic reservation script (runs inside Redis)
src/relay.js         Outbox relay: Redis stream -> Amazon SQS
src/worker.js        Workers: SQS (or Redis stream) -> PostgreSQL, retries, crash recovery
src/sqs.js           SQS queue + dead-letter queue setup
src/config.js        Settings and Redis key names
db/init.sql          Tables and safety constraints
frontend/            Shop page (~9 KB, no framework) and live dashboard
nginx/nginx.conf     Load balancer, bot blocking, rate limiting, SSE, static caching
loadtest/            k6 tests: naive.js, flash-sale.js, chaos.js
deploy/aws/          AWS Application Load Balancer deployment guide and files
```

## Event-loop safety

Each API container runs two Node.js event loops (`node:cluster`, one per CPU core). Every call on the request path (Redis, PostgreSQL, SQS) is non-blocking, nothing CPU-heavy runs there, and the API keeps no shared state in memory, so there are no locks to get wrong and one process can hold thousands of open requests.

## API

| Method | Path | Response |
|---|---|---|
| GET | `/api/sale` | Sale status, start time, stock left, live feed |
| GET | `/api/events` | Server-Sent Events stream of the same data, once a second |
| POST | `/api/checkout` `{"userId":"..."}` | **202** reserved + `trackUrl` · **409** sold out · **425** not open yet · **429** slow down · **503** retry |
| GET | `/api/orders/{id}` | `QUEUED` → `CONFIRMED` |
| POST | `/api/naive/checkout` | Original broken design (comparison only) |
| GET | `/api/stats`, `/api/metrics` | Counters and per-second results for the dashboard |
| POST | `/api/admin/reset?delay=10` | Reset to 500 (header `X-Admin-Token`) |
| GET | `/healthz` | Load balancer health check |

## Results

Measured on a Windows laptop with Docker Desktop (the test and every server on one machine). Fill in from your own run:

| Measure | Original design | New design |
|---|---|---|
| Users / requests | 300 / 3,000 | 1,000 / 20,000 |
| Phones sold (stock 500) | ___ | ___ |
| Failed requests | ___ | ___ |
| p95 latency | ___ | ___ |
| Requests per second | ___ | ___ |
| Chaos test (workers killed, Redis restarted mid-sale) | — | ___ |

## Deploy on AWS

See [`deploy/aws/AWS_DEPLOY.md`](deploy/aws/AWS_DEPLOY.md): two EC2 API servers behind an **Application Load Balancer**, plus one data server.

## Troubleshooting

| Problem | Fix |
|---|---|
| `port is already allocated` | Change `"8080:80"` in `docker-compose.yml` to `"8081:80"` |
| `network flashsale-net not found` | Start the app first with `docker compose up` |
| `localstack` stays unhealthy | Set `QUEUE_BACKEND: redis` in `docker-compose.yml` and run `docker compose up -d` |
| Sale never starts | On `stats.html`, set "Opens in" to 0 and press **Reset sale** |
| Code changed but nothing changed | `docker compose up --build` |
