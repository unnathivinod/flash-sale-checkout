// Settings and shared names. Everything comes from environment variables,
// so the same code runs on a laptop (Docker Compose) and on AWS.
const env = (key, def) => process.env[key] || def;
const int = (key, def) => {
  const n = parseInt(process.env[key] ?? '', 10);
  return Number.isNaN(n) ? def : n;
};

export const PRODUCT_ID = 1;
export const TOTAL_STOCK = int('TOTAL_STOCK', 500);
export const SALE_DELAY_SEC = int('SALE_DELAY_SEC', 30);

export const REDIS_URL = env('REDIS_URL', 'redis://localhost:6379/0');
export const DATABASE_URL = env('DATABASE_URL', 'postgresql://flash:flash@localhost:5432/flashsale');
export const DB_POOL_MAX = int('DB_POOL_MAX', 20);

// "sqs"   = Redis outbox -> relay -> Amazon SQS -> workers
// "redis" = workers read the Redis outbox stream directly (no SQS needed)
export const QUEUE_BACKEND = env('QUEUE_BACKEND', 'redis');
export const SQS_ENDPOINT = process.env.SQS_ENDPOINT || undefined;   // empty = real AWS
export const SQS_QUEUE_NAME = env('SQS_QUEUE_NAME', 'flashsale-orders');
export const AWS_REGION = env('AWS_REGION', 'ap-south-1');

export const ADMIN_TOKEN = env('ADMIN_TOKEN', 'change-me');
export const LOADTEST_KEY = env('LOADTEST_KEY', 'local-loadtest-only');
export const RATE_LIMIT_PER_SEC = int('RATE_LIMIT_PER_SEC', 0);       // 0 = off (NGINX limits locally)
export const STATIC_DIR = process.env.STATIC_DIR;                     // set on AWS (no NGINX there)
export const PORT = int('PORT', 8080);
export const WEB_PROCESSES = int('WEB_PROCESSES', 2);                 // event loops per container
export const WORKER_CONCURRENCY = int('WORKER_CONCURRENCY', 4);

// ---- Redis key names (API, relay and worker must agree) ----
export const K = {
  STOCK: 'sale:stock',             // tokens (phones) left
  START: 'sale:start_ms',          // sale start time, unix ms
  BUYERS: 'sale:buyers',           // hash userId -> orderId (one per customer)
  STATUS: 'sale:order_status',     // hash orderId -> QUEUED / CONFIRMED / FAILED_*
  OUTBOX: 'sale:outbox',           // stream: reservations waiting to be delivered
  FEED: 'sale:feed',               // list: latest reservations
  CONFIRMED: 'sale:confirmed',     // set of order ids saved in Postgres
  METRICS: 'sale:metrics:',        // + unix second -> hash of result counts
};
export const G_RELAY = 'relay';      // consumer group used by the SQS relay
export const G_WORKERS = 'workers';  // consumer group used by workers in "redis" mode

export const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
