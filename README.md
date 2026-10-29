# Flash Sale Checkout

A checkout service for a festival flash sale that sells exactly 500 phones to a burst of concurrent buyers without overselling, built with Node.js, Redis, Amazon SQS and PostgreSQL.

**Candidate:** Unnathi Vinod · **Role:** Full-Stack Software Engineer (campus placement case study)
**Full write-up:** [`docs/Flash_Sale_Case_Study_Unnathi_Vinod.docx`](docs/Flash_Sale_Case_Study_Unnathi_Vinod.docx)

## Summary

The brief: 500 phones go on sale at 8:00 PM and more than 100,000 checkout requests arrive in the first ten seconds. Earlier trial runs timed out, locked up the inventory row and confirmed 540 orders for 500 phones.

My approach takes the database off the request path. A Lua script running inside Redis decides who gets a phone in one atomic step, the API replies `202 Accepted` straight away, and background workers save the winning orders to PostgreSQL through a queue.

| Result (measured on one Windows laptop with Docker) | Original design | This project |
|---|---|---|
| Orders confirmed against 500 units | 3,000 | **500** |
| Load applied | 300 users, 3,000 requests | 1,000 users, 20,000 requests |
| Failed requests | 0% | 0.00% |
| 95th percentile response time | not measured | 736 ms |
| Throughput | not measured | 1,644 requests/s |
| Workers killed and Redis restarted mid-sale | not tested | 500 confirmed, nothing lost |

The 100,000-request peak (about 10,000 requests per second) has **not** been demonstrated; see [Performance](#performance-and-scalability).

## Contents

1. [The problem](#the-problem)
2. [Features](#features)
3. [Architecture](#architecture)
4. [Checkout flow](#checkout-flow)
5. [Technology stack](#technology-stack)
6. [Repository structure](#repository-structure)
7. [Getting started](#getting-started)
8. [Configuration](#configuration)
9. [API](#api)
10. [How overselling is prevented](#how-overselling-is-prevented)
11. [Error handling and reliability](#error-handling-and-reliability)
12. [Testing](#testing)
13. [Performance and scalability](#performance-and-scalability)
14. [Design decisions](#design-decisions)
15. [Limitations and future work](#limitations-and-future-work)
16. [Screenshots](#screenshots)

## The problem

The original checkout did three things wrong:

- **Check-then-act race.** It read the stock in one query and wrote it back in another. Two requests could both read "1 left" and both succeed, which is how 540 orders happened.
- **Hot-row locking.** Every request updated the same inventory row, so PostgreSQL's row lock forced 100,000 requests through one at a time.
- **Database work inside the request (N+1 round trips).** Each checkout made several queries before replying, so users waited on the slowest part of the system.

Only 500 of those requests can ever succeed, so the database should only ever see 500 writes.

## Features

**Implemented**

- Purchase button that locks on the first click, so one user sends one request
- Atomic reservation in Redis: sale-start check, stock check, decrement, one-per-customer and queueing in a single Lua script
- Idempotent checkout: a repeat request from the same user returns the same order ID
- Asynchronous persistence: Redis outbox → relay → Amazon SQS (LocalStack locally) → workers → PostgreSQL
- Conditional SQL update (`WHERE stock > 0`), `CHECK (stock >= 0)` and unique constraints as a database-level safety net
- Order status tracking through `GET /api/orders/{id}`, with live sale updates over Server-Sent Events
- Crash recovery: unacknowledged outbox entries and SQS messages are retried; failing messages go to a dead-letter queue
- NGINX load balancing across 5 API replicas, per-IP rate limiting and basic bot filtering
- The original (buggy) checkout kept at `/api/naive/checkout` so both designs can be compared under the same load
- Live monitor page with per-second results and queue depth
- k6 load test, comparison test and chaos test

**Designed but not implemented** (described in the case study document)

- CloudFront CDN, AWS WAF bot control, auto scaling on AWS
- Payment step with reservation expiry
- Automated reconciliation that re-queues orders stuck in `QUEUED`

## Architecture

This is what runs locally with `docker compose up`:

```mermaid
flowchart TB
    B["Browser<br/>shop page + monitor"] -->|static files| N
    B -->|"POST /api/checkout"| N["NGINX<br/>rate limit, bot filter, least-connections"]
    N --> A1["API replica 1"]
    N --> A2["API replica ..."]
    N --> A5["API replica 5"]
    A1 & A2 & A5 -->|"one Lua script"| R[("Redis<br/>stock, buyers, status, outbox")]
    R -->|outbox stream| L["Relay"]
    L -->|"send, then acknowledge"| Q["SQS via LocalStack<br/>+ dead-letter queue"]
    Q --> W["Workers x2"]
    W -->|"one transaction per order"| P[("PostgreSQL")]
    W -->|"status = CONFIRMED"| R
```

| Component | What it does |
|---|---|
| Shop page (`frontend/index.html`) | Locks the button, shows progress, receives live stock updates |
| NGINX | Serves the static pages, rate-limits each IP to 5 checkout requests per second (burst 10), blocks scripted User-Agents, balances across API replicas |
| API (`src/api.js`) | Stateless Fastify server. Validates the request, runs the reservation script, replies `202` or `409`. Two event loops per container via `node:cluster` |
| Redis | Holds the stock counter, the buyer list, order statuses and the outbox stream |
| Relay (`src/relay.js`) | Moves outbox entries to SQS and acknowledges them only after SQS accepts them |
| SQS | Durable queue between the API and the database, with a dead-letter queue after 5 failed attempts |
| Workers (`src/worker.js`) | Save each order in one PostgreSQL transaction and update its status in Redis |
| PostgreSQL | Source of truth for stock and orders |

## Checkout flow

1. The user clicks **Reserve my phone**. The button locks immediately and later clicks are ignored.
2. NGINX checks the User-Agent and the per-IP rate limit, then forwards the request to the least busy API replica.
3. The API checks the body (1 KB limit) and the user ID format.
4. The API runs `src/reserve.lua` in Redis. In one atomic step it checks that the sale has started, returns the existing order if this user already reserved, checks stock, decrements it, records the buyer and writes the order to the outbox.
5. The API replies `202 Accepted` with the order ID and a tracking URL. No database call has happened yet.
6. The relay sends the outbox entry to SQS. A worker receives it and runs one transaction: conditional stock decrement, then order insert.
7. The worker marks the order `CONFIRMED` in Redis and deletes the SQS message.
8. The page polls the tracking URL until it shows **Confirmed**, **Sold out** or **Failed**.

## Technology stack

| Technology | Used for | Why I chose it |
|---|---|---|
| Node.js 22 + Fastify 5 | API, relay, workers | The checkout path is almost entirely waiting on Redis, which suits a non-blocking event loop. The brief also asks for Node/Go event-loop safety |
| Redis 7 (Lua, Streams) | Stock token, buyer list, outbox | Runs a multi-step script atomically in microseconds without locks. A plain `DECR` alone could not enforce one-per-customer or queue the order in the same step |
| Amazon SQS (LocalStack locally) | Queue between API and database | Managed and durable, with visibility timeouts and dead-letter queues built in. RabbitMQ would also work; Kafka is more than 500 orders need |
| PostgreSQL 16 | Orders and inventory | ACID transactions and constraints give a final guarantee that does not depend on Redis being right |
| NGINX 1.27 | Load balancing, rate limiting, static files | Stands in for an AWS Application Load Balancer locally |
| Plain HTML, CSS and JavaScript | Shop page and monitor | The sale page needs about 9 KB; a framework would add more than that on its own |
| Docker Compose | Running everything | One command starts all 8 services |
| Grafana k6 | Load and chaos tests | Scripts concurrent users in JavaScript and reports percentiles |

## Repository structure

```
flash-sale-checkout/
├── src/
│   ├── api.js            API server: checkout, order status, SSE, stats, naive endpoint, rate limiter
│   ├── reserve.lua       Atomic reservation script that runs inside Redis
│   ├── relay.js          Moves the Redis outbox to SQS
│   ├── worker.js         Saves orders to PostgreSQL; SQS or Redis-stream mode
│   ├── sqs.js            Creates the queue and its dead-letter queue
│   └── config.js         Environment variables and Redis key names
├── frontend/
│   ├── index.html        Shop page
│   └── stats.html        Live monitor
├── db/init.sql           Tables, constraints and starting stock
├── nginx/nginx.conf      Load balancer, rate limits, bot filter, SSE, caching
├── loadtest/
│   ├── naive.js          Load test against the original design
│   ├── flash-sale.js     Load test against this design
│   └── chaos.js          Steady load for the failure-injection test
├── deploy/aws/           Guide and compose files for an AWS ALB + EC2 deployment (not yet deployed)
├── docs/                 Case study document and screenshots
├── Dockerfile
├── docker-compose.yml
├── package.json
└── package-lock.json
```

## Getting started

### Prerequisites

- **Docker Desktop** with Docker Compose v2 (tested on Windows 11)
- About 2 GB of free disk space for the images
- Node.js is **not** needed on your machine; it runs inside the containers

### Run it

```bash
git clone https://github.com/unnathivinod/flash-sale-checkout.git
cd flash-sale-checkout
docker compose up --build -d
```

The first build takes 5 to 10 minutes, mostly downloading LocalStack. PostgreSQL creates its tables from `db/init.sql` on first start; there are no separate migrations.

| Page | URL |
|---|---|
| Shop | http://localhost:8080 |
| Monitor | http://localhost:8080/stats.html |

The sale opens 30 seconds after startup. To reopen it at any time, set **Opens in (s)** to `0` on the monitor page and press **Reset sale**.

To stop:

```bash
docker compose down        # add -v to also delete the database volume
```

> If LocalStack will not start on your machine, set `QUEUE_BACKEND: redis` in `docker-compose.yml` and run `docker compose up -d`. The workers then read the Redis outbox directly and everything else behaves the same.

## Configuration

All settings are environment variables read in `src/config.js`. `docker-compose.yml` already sets working local values, so nothing needs to be configured to run the project.

| Variable | Purpose | Local value |
|---|---|---|
| `TOTAL_STOCK` | Units on sale | `500` |
| `SALE_DELAY_SEC` | Seconds after startup before the sale opens | `30` |
| `REDIS_URL` | Redis connection | `redis://redis:6379/0` |
| `DATABASE_URL` | PostgreSQL connection | `postgresql://flash:flash@postgres:5432/flashsale` |
| `DB_POOL_MAX` | PostgreSQL connections per API process | `20` |
| `QUEUE_BACKEND` | `sqs` (outbox → SQS) or `redis` (workers read the outbox directly) | `sqs` |
| `SQS_ENDPOINT` | SQS endpoint; leave empty for real AWS | `http://localstack:4566` |
| `SQS_QUEUE_NAME` | Queue name (the DLQ is `<name>-dlq`) | `flashsale-orders` |
| `AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | AWS client settings (LocalStack accepts any key) | `ap-south-1`, `test`, `test` |
| `ADMIN_TOKEN` | Required header for the reset endpoint | `demo-admin-token` |
| `LOADTEST_KEY` | Header that lets the load tests skip the per-IP limit | `local-loadtest-only` |
| `RATE_LIMIT_PER_SEC` | In-app per-IP limit; `0` because NGINX does this locally | `0` |
| `WEB_PROCESSES` | Event loops per API container | `2` |
| `WORKER_CONCURRENCY` | Parallel consumers per worker container | `4` |
| `STATIC_DIR` | Lets the API serve the pages itself (used on AWS, where there is no NGINX) | unset |

These values are for local development only. Change `ADMIN_TOKEN`, `LOADTEST_KEY` and the database password before deploying anywhere public.

## API

| Method | Endpoint | Purpose | Responses |
|---|---|---|---|
| `POST` | `/api/checkout` | Reserve a phone | `202` reserved · `409` sold out · `425` sale not open · `429` too many requests · `400` invalid user ID · `503` Redis busy, retry |
| `GET` | `/api/orders/{id}` | Order status | `200` with `QUEUED`, `CONFIRMED` or `FAILED_SOLD_OUT` · `404` unknown order |
| `GET` | `/api/sale` | Sale status, start time and stock left | `200` |
| `GET` | `/api/events` | Same data as a Server-Sent Events stream, once a second | `200 text/event-stream` |
| `GET` | `/api/stats`, `/api/metrics` | Counters and per-second results for the monitor | `200` |
| `POST` | `/api/naive/checkout` | The original read-then-update design, for comparison only | `200` · `409` |
| `POST` | `/api/admin/reset?delay=10` | Reset stock to 500 (header `X-Admin-Token`) | `200` · `403` |
| `GET` | `/healthz` | Health check | `200` |

Example checkout:

```http
POST /api/checkout
Content-Type: application/json

{ "userId": "guest-uzftl2y2" }
```

```http
HTTP/1.1 202 Accepted
Location: /api/orders/ord_3bd47fbf35d34d1a

{ "status": "QUEUED", "orderId": "ord_3bd47fbf35d34d1a",
  "trackUrl": "/api/orders/ord_3bd47fbf35d34d1a", "duplicate": false, "stockLeft": 499 }
```

`202` means the phone is **reserved** and the order is **queued**, not that it is confirmed. The order becomes `CONFIRMED` only after a worker commits it to PostgreSQL, normally within a second.

## How overselling is prevented

Three different states matter here:

| State | Where it lives | Meaning |
|---|---|---|
| Reserved | Redis (stock decremented, buyer recorded) | The user won a phone |
| Accepted | Redis outbox, then SQS | The order is durably queued; the user got `202` |
| Confirmed | PostgreSQL `orders` table | The order is saved and stock is reduced in the database |

**Reservation.** Every checkout runs `src/reserve.lua`. Redis runs a script from start to finish before any other command, so two requests can never read the same stock value. The script, in order:

1. Rejects the request if the sale has not started.
2. Returns the existing order if this user already has one, without taking stock.
3. Returns sold out if stock is 0.
4. Otherwise decrements stock, records the buyer, marks the order `QUEUED` and appends it to the outbox stream.

Because the outbox entry is written in the same atomic step as the decrement, there is never a reservation without a queued order, or the other way round.

**Persistence.** The worker saves each order in one transaction:

```sql
BEGIN;
SELECT 1 FROM orders WHERE order_id = $1;                       -- already saved? stop here
UPDATE inventory SET stock = stock - 1
 WHERE product_id = $2 AND stock > 0;                           -- 0 rows: FAILED_SOLD_OUT
INSERT INTO orders (order_id, user_id, product_id) VALUES ($1, $3, $2)
ON CONFLICT (user_id, product_id) DO NOTHING;                   -- 0 rows: roll back
COMMIT;
```

**Why the database guarantee holds even if Redis is wrong.** `inventory.stock` starts at 500 and has a `CHECK (stock >= 0)` constraint, and orders are only created by this transaction. A conditional `UPDATE` re-checks `stock > 0` against the latest committed value, so two workers cannot both take the last unit. PostgreSQL alone therefore never confirms more than 500 orders, even if Redis were to over-reserve (for example after losing data); the extra orders would be marked `FAILED_SOLD_OUT`.

**Duplicates.** A repeat click or client retry gets the same order ID from Redis. A message delivered twice by SQS is caught by the `order_id` primary key check. A second order for the same user is blocked by `UNIQUE (user_id, product_id)`.

## Error handling and reliability

**Implemented**

| Situation | What happens |
|---|---|
| Invalid body or user ID | `400` |
| Too many requests from one IP | `429` with `Retry-After`; the page retries with backoff (about 1, 2, 4 and 8 seconds) |
| Scripted User-Agent | `403` |
| Redis unavailable | `503`; the page retries; the same user gets the same order once Redis is back |
| Relay crashes before acknowledging | Another relay claims the outbox entry after 15 seconds |
| Worker crashes mid-order | The transaction rolls back; SQS shows the message again after 30 seconds |
| Worker crashes after commit, before deleting the message | The retry finds the order already saved and skips it |
| Message fails 5 times | Moved to the dead-letter queue |
| PostgreSQL down | Orders wait in the queue and are confirmed when it returns |
| Redis restarts | Data is written to disk on every change (`appendfsync always`); if the stock key is missing, the API reloads it from PostgreSQL |

**Known gaps**

- LocalStack keeps its queue in memory. During testing, restarting the LocalStack container discarded 500 queued orders. Real SQS stores messages durably, but there is no automated job yet that re-queues orders stuck in `QUEUED`.
- Reconciliation between Redis and PostgreSQL is visible on the monitor page but not automated.

## Testing

Testing is done with **Grafana k6** load scripts run in Docker, plus manual checks with PowerShell and Chrome Lighthouse. There are no unit tests yet.

**Load test against the original design** (expect more than 500 sold):

```bash
docker run --rm --network flashsale-net -v "${PWD}/loadtest:/scripts" grafana/k6 run /scripts/naive.js
```

**Load test against this design** (1,000 users, 20,000 requests, 5% double clicks):

```bash
docker run --rm --network flashsale-net -v "${PWD}/loadtest:/scripts" -e RESET=0 grafana/k6 run /scripts/flash-sale.js
```

It passes when confirmed orders equal 500, PostgreSQL and Redis stock are both 0, the queue is empty, every double click returns the same order, fewer than 0.1% of requests fail and the 95th percentile stays under 1 second.

**Chaos test.** Stop the workers, start a 40-second rush, restart Redis while requests are arriving, then bring the workers back:

```bash
docker compose kill worker
docker run --rm --network flashsale-net -v "${PWD}/loadtest:/scripts" grafana/k6 run /scripts/chaos.js
# in a second terminal, once the test prints "Sale is live":
docker compose restart redis
docker compose up -d --no-deps worker
```

Use `--no-deps` so Docker does not restart LocalStack and clear its in-memory queue.

**Results**

| Test | Outcome |
|---|---|
| Original design, 300 users, 3,000 requests | 3,000 orders for 500 phones; database still showed 474 left (lost updates) |
| This design, 1,000 users, 20,000 requests | 500 confirmed, 19,500 sold out, 0.00% failed, p95 736 ms, 1,644 req/s, 25/25 double clicks returned the same order |
| Chaos test | 500 orders waited in SQS while no worker ran; 105 requests got `503` during the Redis restart and succeeded on retry; 500 confirmed, 0.00% failed |
| 20 rapid requests from one client | `429` after the burst allowance |
| Scripted User-Agent | `403` |
| Lighthouse (desktop) | Performance 100, Accessibility 95, Best Practices 100, SEO 91 |

## Performance and scalability

Concurrent users, open connections, requests per second and total requests are different measurements. My tests used 1,000 concurrent virtual users sending 20,000 requests in total, which averaged about 1,644 requests per second.

| Objective | How it is measured | Target | Result |
|---|---|---|---|
| No more than 500 orders | Final database count | ≤ 500 | 500 in every run |
| No lost requests | k6 `http_req_failed` | < 0.1% | 0.00% |
| Peak load | k6 requests per second | ~10,000 req/s | **1,644 req/s on one laptop; not yet demonstrated** |
| Response time | k6 95th percentile | < 1 s on one laptop | 736 ms |
| Page size | File size | < 200 KB | about 9 KB |
| Time to interactive | Chrome / Lighthouse | < 1.5 s | Largest content paint 0.16 s |

The laptop ran the test tool and all eight services at once, so these numbers reflect that machine rather than the design's ceiling. The API holds no state, so capacity grows by adding replicas, and each checkout is a single Redis call. My estimate is that 30 to 40 API event loops on dedicated machines would be needed for 10,000 requests per second, but that has to be confirmed with a distributed load test. The remaining bottlenecks are the single Redis instance and the single inventory row, which could be split across several keys for much larger sales.

## Design decisions

- **Decide in Redis, persist asynchronously.** Writing every order synchronously would recreate the hot-row bottleneck. Queuing every request without deciding would leave users without an answer. Deciding in Redis and persisting later gives an immediate answer and a protected database.
- **Lua script rather than row locks.** `SELECT ... FOR UPDATE` is correct but makes every request wait for one lock. Optimistic version checks are correct but nearly every attempt conflicts and retries. The Lua script has no gap between reading and writing and holds no locks.
- **Transactional outbox.** Redis and SQS cannot share a transaction, so the order is written to a Redis stream in the same atomic step as the decrement, and a relay delivers it to SQS.
- **Server-Sent Events plus polling.** SSE pushes sale status without the per-connection state WebSockets need; polling remains as a fallback and for order status.
- **Refuse rather than guess.** If Redis is unavailable, checkout returns `503` instead of falling back to an unsafe path.

## Limitations and future work

- 10,000 requests per second has not been demonstrated; a distributed load test is needed.
- Bot protection is limited to User-Agent filtering and per-IP limits. AWS WAF Bot Control and a CAPTCHA during the countdown are designed but not built.
- No payment step or reservation expiry.
- No automated reconciliation or re-queue job for orders stuck in `QUEUED`.
- No unit tests; testing is load-based.
- The AWS deployment files in `deploy/aws/` have not been run against a live account.
- Single Redis instance; production would use a replicated, managed Redis.

## Screenshots

**Shop page after a confirmed order**

![Shop page](docs/screenshots/shop.png)

**Monitor after both load tests: this design confirmed 500, the original design accepted 3,000**

![Original versus new design](docs/screenshots/compare.png)

**k6 load test result**

![k6 result](docs/screenshots/k6-result.png)

**Chaos test: 500 orders waiting in SQS while all workers are stopped**

![Chaos test, orders waiting](docs/screenshots/chaos-waiting.png)

**Chaos test result after the workers come back**

![Chaos test result](docs/screenshots/chaos-result.png)

**Rate limiting (429) and bot filtering (403)**

![Rate limit and bot filter](docs/screenshots/rate-limit.png)

**Lighthouse report**

![Lighthouse](docs/screenshots/lighthouse.png)
