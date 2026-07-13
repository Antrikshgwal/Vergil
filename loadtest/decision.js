// k6 load test for the decision api. Records latency percentiles for
// POST /v1/transactions under a ramped concurrent load.
//
//   k6 run --summary-trend-stats="avg,min,med,p(90),p(95),p(99),max" loadtest/decision.js
import http from 'k6/http';
import { check } from 'k6';

export const options = {
  scenarios: {
    ramp: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '10s', target: 50 }, // ramp up
        { duration: '20s', target: 50 }, // hold
        { duration: '5s', target: 0 },   // ramp down
      ],
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
  },
};

const currencies = ['USD', 'USD', 'USD', 'EUR', 'XRP'];

export default function () {
  const payload = JSON.stringify({
    txn_id: `${__VU}-${__ITER}-${Date.now()}`,
    user_id: `u-${__VU % 20}`, // 20 users, so velocity actually accumulates
    amount: Math.floor(Math.random() * 5000) + 1,
    currency: currencies[Math.floor(Math.random() * currencies.length)],
  });
  const res = http.post('http://localhost:8080/v1/transactions', payload, {
    headers: { 'Content-Type': 'application/json' },
  });
  check(res, { 'status is 200': (r) => r.status === 200 });
}
