// k6 load test: kiểm chứng con số latency thay vì tin lời hứa trong tài liệu.
//   k6 run loadtest/order-load.js
import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

const rejected = new Counter('orders_rejected');
const rateLimited = new Counter('orders_rate_limited');

export const options = {
  stages: [
    { duration: '30s', target: 50 },   // tăng dần
    { duration: '1m',  target: 200 },  // giữ tải - vượt RateLimiter 200/s để xem 429
    { duration: '30s', target: 0 },
  ],
  thresholds: {
    // Ngưỡng FAIL build nếu vi phạm - biến hiệu năng thành hợp đồng, không phải kỳ vọng
    'http_req_duration{expected_response:true}': ['p(95)<200', 'p(99)<500'],
    'http_req_failed': ['rate<0.05'],
  },
};

const SYMBOLS = ['VN30F2607', 'HPG', 'VCB', 'FPT'];

export default function () {
  const symbol = SYMBOLS[Math.floor(Math.random() * SYMBOLS.length)];
  // clientOrderId duy nhất mỗi request: đo đường đặt lệnh thật,
  // không phải đo tốc độ Redis trả cache.
  const clientOrderId = `k6-${__VU}-${__ITER}-${Date.now()}`;

  const res = http.post(
    'http://localhost:8083/api/v1/orders',
    JSON.stringify({
      clientOrderId,
      symbol,
      side: Math.random() > 0.5 ? 'BUY' : 'SELL',
      price: 28500,
      quantity: 100,
      accountId: `ACC${__VU}`,
    }),
    { headers: { 'Content-Type': 'application/json', 'X-API-KEY': 'demo-key' } },
  );

  if (res.status === 429) rateLimited.add(1);
  if (res.status === 422) rejected.add(1);

  check(res, {
    'khong loi 5xx': (r) => r.status < 500,
  });
}
