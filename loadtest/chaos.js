// Chaos test: the same rush as flash-sale.js, but you break things while it runs
// (kill the workers, restart Redis). See README "Chaos test".
// During the ~1 s Redis restart some requests get 503 "busy, try again",
// which the real page retries automatically. The test counts them, and at the
// end checks that EXACTLY 500 phones were sold.
//
// Run (PowerShell, project folder):
//   docker run --rm --network flashsale-net -v "${PWD}/loadtest:/scripts" grafana/k6 run /scripts/chaos.js
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter } from 'k6/metrics';

const BASE = __ENV.BASE || 'http://nginx';
const HEADERS = { 'Content-Type': 'application/json', 'X-Loadtest-Key': __ENV.LOADTEST_KEY || 'local-loadtest-only' };
const accepted = new Counter('accepted_202');
const soldOut = new Counter('sold_out_409');
const busy = new Counter('busy_503_retried');

export const options = {
  scenarios: {
    // slower, steady rush lasting ~40 s so there is time to break things
    rush: { executor: 'constant-arrival-rate', rate: Number(__ENV.RATE || 300), timeUnit: '1s',
            duration: __ENV.DURATION || '40s', preAllocatedVUs: 300, maxVUs: 1000 },
  },
};
http.setResponseCallback(http.expectedStatuses(202, 409, 503));

export function setup() {
  http.post(`${BASE}/api/admin/reset?delay=0`, null, { headers: { 'X-Admin-Token': __ENV.ADMIN || 'demo-admin-token' } });
  sleep(1);
  console.log('>>> Sale is live. Now run the chaos commands in another window. <<<');
}

export default function () {
  const body = JSON.stringify({ userId: `c${__VU}_${__ITER}` });
  let r = http.post(`${BASE}/api/checkout`, body, { headers: HEADERS });
  if (r.status === 503) { busy.add(1); sleep(1); r = http.post(`${BASE}/api/checkout`, body, { headers: HEADERS }); }
  if (r.status === 202) accepted.add(1);
  else if (r.status === 409) soldOut.add(1);
  check(r, { 'answered (202/409/503)': (x) => [202, 409, 503].includes(x.status) });
}

export function teardown() {
  // wait for workers to come back and drain the queue
  let s;
  for (let i = 0; i < 60; i++) {
    s = http.get(`${BASE}/api/stats`, { responseCallback: http.expectedStatuses(200) }).json();
    if (s.queuePending === 0 && s.confirmedOrders >= s.reserved) break;
    sleep(1);
  }
  console.log('================ CHAOS RESULT ================');
  console.log(`Stock:              ${s.totalStock}`);
  console.log(`Confirmed in DB:    ${s.confirmedOrders}`);
  console.log(`DB stock left:      ${s.dbStockLeft}`);
  console.log(`Redis tokens left:  ${s.redisStockLeft}`);
  console.log(`Still in queue:     ${s.queuePending}`);
  console.log(`OVERSOLD?           ${s.oversold ? 'YES  <-- bug' : 'NO  (exactly ' + s.confirmedOrders + ')'}`);
  console.log('==============================================');
}
