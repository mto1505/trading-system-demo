# Load test

```bash
docker compose up -d --build
sleep 45                      # chờ feed mô phỏng bơm đủ tick để cache enriched có dữ liệu
k6 run loadtest/order-load.js
```

## Đọc kết quả thế nào
- **422 nhiều** ở đầu bài test là *đúng*: cache enriched chưa đủ dữ liệu → pre-trade
  từ chối vì `NO_FRESH_MARKET_DATA`. Chờ feed chạy trước rồi mới bắn tải.
- **429 xuất hiện khi vượt 200 req/s** là *đúng*: RateLimiter đang làm việc.
  Nếu không thấy 429 mà thấy latency tăng vọt → rate limiter chưa hiệu lực.
- **p99 tăng theo bậc** thường là DB pool cạn (`maximum-pool-size: 20`) hoặc
  outbox relay đang giữ transaction lâu. Xem panel *Outbox & Matching* trong Grafana.

## Cảnh báo về ý nghĩa của con số
Chạy trên một máy dev có Docker: Kafka, Postgres, 4 service và cả k6 đều tranh
nhau CPU. Số đo ở đây dùng để **so sánh trước/sau khi thay đổi code**, không
dùng để cam kết SLA. Muốn số thật thì cần môi trường tách riêng, dữ liệu tick
thật, và chạy đủ dài để thấy ảnh hưởng của GC.
