import http from 'k6/http';
import { check } from 'k6';

export const options = {
  vus: 50,
  iterations: 50,
};

export default function () {
  const url = 'http://localhost:8080/payments';
  const payload = JSON.stringify({
    account_id: 'acc_stress_01',
    amount: 500.00,
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
      'X-Idempotency-Key': 'race-condition-key-007',
    },
  };

  const res = http.post(url, payload, params);
  check(res, {
    'status is 202': (r) => r.status === 202,
  });
}
