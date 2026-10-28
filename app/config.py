"""Settings and shared names. Everything is read from environment variables
so the same code runs on a laptop (Docker Compose) and on AWS."""
import os


def env(key: str, default: str) -> str:
    return os.getenv(key) or default


PRODUCT_ID = 1
TOTAL_STOCK = int(env("TOTAL_STOCK", "500"))
SALE_DELAY_SEC = int(env("SALE_DELAY_SEC", "30"))

REDIS_URL = env("REDIS_URL", "redis://localhost:6379/0")
DATABASE_URL = env("DATABASE_URL", "postgresql://flash:flash@localhost:5432/flashsale")
DB_POOL_MAX = int(env("DB_POOL_MAX", "20"))

# "sqs"   = reservations go Redis outbox -> relay -> Amazon SQS -> workers
# "redis" = workers read the Redis outbox stream directly (no SQS needed)
QUEUE_BACKEND = env("QUEUE_BACKEND", "redis")
SQS_ENDPOINT = os.getenv("SQS_ENDPOINT") or None  # e.g. http://localstack:4566 ; empty = real AWS
SQS_QUEUE_NAME = env("SQS_QUEUE_NAME", "flashsale-orders")
AWS_REGION = env("AWS_REGION", "ap-south-1")

ADMIN_TOKEN = env("ADMIN_TOKEN", "change-me")
LOADTEST_KEY = env("LOADTEST_KEY", "local-loadtest-only")
RATE_LIMIT_PER_SEC = int(env("RATE_LIMIT_PER_SEC", "0"))  # 0 = off (NGINX limits locally)
STATIC_DIR = os.getenv("STATIC_DIR")  # set on AWS, where there is no NGINX

WORKER_CONCURRENCY = int(env("WORKER_CONCURRENCY", "4"))

# ---- Redis key names (API, relay and worker must agree) ----
K_STOCK = "sale:stock"            # tokens (phones) left
K_START = "sale:start_ms"         # sale start time, unix ms
K_BUYERS = "sale:buyers"          # hash userId -> orderId (one per customer)
K_STATUS = "sale:order_status"    # hash orderId -> QUEUED / CONFIRMED / FAILED_*
K_OUTBOX = "sale:outbox"          # stream: reservations waiting to be delivered
K_FEED = "sale:feed"              # list: latest reservations (for the live feed)
K_CONFIRMED = "sale:confirmed"    # set of order ids saved in Postgres
K_METRICS = "sale:metrics:"       # + unix second -> hash of result counts

G_RELAY = "relay"                 # consumer group used by the SQS relay
G_WORKERS = "workers"             # consumer group used by workers in "redis" mode
