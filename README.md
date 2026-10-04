# Trading Stock Microservices — Spring Boot + Kafka + Monitoring + Kubernetes

## Kiến trúc

```
External Feed ──► market-gateway (8081)
                       │ produce (acks=1, lz4, linger 5ms)
                       ▼
              Kafka topic: stock.ticks (6 partitions, key = symbol)
                       │ batch consume (max.poll.records=1000)
                       ▼
              pricing-risk-service (8082)  ── flush 5s ──► PostgreSQL (market_snapshot)
                       │ tính EMA / volatility / VaR95 / riskLevel (in-memory, O(1)/tick)
                       ▼
              Kafka topic: stock.enriched
                       │
                       ▼
              order-service (8083) — local cache giá/rủi ro
                       │ POST /api/v1/orders → pre-trade check (staleness, risk, fat-finger)
                       ▼
              Kafka topic: stock.orders (acks=all, idempotent) → hệ thống khớp lệnh

Monitoring: mỗi service expose /actuator/prometheus → Prometheus (10s) → Grafana
```

## Chạy local (Docker Compose)

```bash
docker compose up -d --build

# Xem giá + rủi ro realtime
curl http://localhost:8082/api/v1/marketdata/VN30F2607

# Đặt lệnh (cần X-API-KEY + clientOrderId để idempotent)
curl -X POST http://localhost:8083/api/v1/orders \
  -H "Content-Type: application/json" -H "X-API-KEY: demo-key" \
  -d '{"clientOrderId":"c-001","symbol":"VN30F2607","side":"BUY","price":1350.5,"quantity":2,"accountId":"ACC001"}'

# Gọi lại y hệt (retry) -> trả đúng kết quả cũ, KHÔNG tạo lệnh trùng
# Lệnh bị chặn fat-finger (giá lệch >5%)
curl -X POST http://localhost:8083/api/v1/orders \
  -H "Content-Type: application/json" -H "X-API-KEY: demo-key" \
  -d '{"clientOrderId":"c-002","symbol":"VN30F2607","side":"BUY","price":2000,"quantity":2,"accountId":"ACC001"}'

# Monitoring & Tracing
# Prometheus: http://localhost:9090   Grafana: http://localhost:3000 (admin/admin)
# Zipkin:     http://localhost:9411   (xem trace gateway -> pricing -> order)
```

Metrics đáng xem trên Prometheus:
- `pricing_e2e_latency_seconds{quantile="0.99"}` — latency tick → tính xong (p99)
- `pricing_batch_process_seconds` — thời gian xử lý mỗi batch
- `gateway_ticks_sent_total`, `orders_accepted_total`, `orders_rejected_total`

## Deploy Kubernetes

Ba môi trường tách biệt bằng Kustomize overlay — chi tiết ở **`k8s/README.md`**.

```bash
# Build image (minikube: eval $(minikube docker-env) trước)
for m in market-gateway pricing-risk-service order-service matching-engine; do
  docker build --build-arg MODULE=$m -t trading/$m:dev .
done

cp k8s/overlays/dev/secrets.env.example k8s/overlays/dev/secrets.env
kubectl apply -k k8s/overlays/dev          # staging/prod: đổi overlay
kubectl -n trading-dev get pods -w

# Test qua port-forward
kubectl -n trading-dev port-forward svc/order-service 8083:8083
kubectl -n trading-dev port-forward svc/prometheus 9090:9090
```

## Cấu trúc project

```
trading-system/
├── pom.xml                     # parent POM (Spring Boot 3.2, Java 17)
├── Dockerfile                  # multi-stage, dùng chung (ARG MODULE)
├── docker-compose.yml          # Kafka KRaft + Postgres + Prometheus + Grafana + 3 services
├── common-contracts/           # DTO/contract Kafka dùng chung (single source of truth)
├── market-gateway/             # nhận feed ngoài → Kafka
├── pricing-risk-service/       # tính giá + rủi ro, lưu DB, publish enriched
├── order-service/              # pre-trade check + đặt lệnh
├── monitoring/                 # prometheus.yml, alert-rules.yml, grafana datasource
├── k8s/                        # Kustomize: base + overlays dev/staging/prod (xem k8s/README.md)
└── .github/workflows/          # CI build+test, validate 3 overlay Kubernetes
```

Giải thích thiết kế & ưu nhược điểm chi tiết: xem file `DESIGN-NOTES.md`.


---

# V3 — Cập nhật

## Thành phần mới
| Thành phần | Port (host) | Vai trò |
|---|---|---|
| matching-engine | 8084 | Idempotent consumer, khớp lệnh giả lập |
| schema-registry | 8085 | Quản lý + cưỡng chế Avro schema (stock.ticks) |
| redis | 6379 | Cache idempotency tập trung |

## Chạy toàn hệ
```bash
docker compose up -d --build
```

## Verify luồng distributed transaction (Outbox → Idempotent Consumer)
```bash
# 1. Đặt lệnh (cần API key + clientOrderId)
curl -s -X POST localhost:8083/api/v1/orders \
  -H 'Content-Type: application/json' -H 'X-API-KEY: demo-key' \
  -d '{"clientOrderId":"demo-001","symbol":"HPG","side":"BUY","price":28500,"quantity":100,"accountId":"ACC1"}'

# 2. Gửi LẠI y hệt -> nhận đúng kết quả cũ (idempotent, không tạo lệnh mới)

# 3. Xem lệnh đã được khớp (đi qua outbox -> Kafka -> matching-engine)
curl -s localhost:8084/api/v1/executions/demo-001
# -> đúng 1 execution dù bạn có retry bao nhiêu lần
```

## Chạy integration test (cần Docker trên máy)
```bash
mvn verify
# TickToMarketDataIT: tick Avro -> risk -> REST (Kafka + Postgres thật)
# IdempotentConsumerIT: gửi trùng 3 lần -> đúng 1 execution
```

## Grafana
Mở http://localhost:3000 (admin/admin) → folder **Trading** → dashboard **Trading System Overview** đã được provision sẵn.

## Schema Registry
```bash
curl -s localhost:8085/subjects            # danh sách schema
curl -s localhost:8085/subjects/stock.ticks-value/versions/1
```

---

# V4 — Cập nhật

## Vòng đời lệnh trọn vẹn (saga)
```
POST /api/v1/orders → ACCEPTED → stock.orders → matching-engine
                                       ↓
        FILLED | REJ_MATCH ← stock.executions ← (outbox)
```

## Verify saga
```bash
docker compose up -d --build && sleep 45

# Lệnh hợp lệ -> sẽ chuyển sang FILLED
curl -s -X POST localhost:8083/api/v1/orders \
  -H 'Content-Type: application/json' -H 'X-API-KEY: demo-key' \
  -d '{"clientOrderId":"saga-1","symbol":"HPG","side":"BUY","price":28500,"quantity":100,"accountId":"ACC1"}'

sleep 3
curl -s localhost:8084/api/v1/executions/saga-1     # đúng 1 execution
docker compose exec postgres psql -U trading -d trading \
  -c "SELECT client_order_id, status, filled_qty, avg_fill_price FROM ordersvc.orders WHERE client_order_id='saga-1';"
# -> status = FILLED (order-service đã nhận execution report)
```

Lệnh vi phạm lô chẵn (`quantity: 150`) sẽ đi hết vòng và về `REJ_MATCH` với lý do `INVALID_LOT_SIZE`.

## Debezium CDC thay polling relay
```bash
docker compose -f docker-compose.yml -f docker-compose.cdc.yml up -d --build
sleep 30 && ./cdc/register-connectors.sh
```
Chi tiết và ưu/nhược: `cdc/README.md`.

## Load test
```bash
k6 run loadtest/order-load.js     # threshold p95<200ms, p99<500ms
```

## Bảo mật Kafka (production)
`security/README.md` — SASL/SCRAM + TLS + ACL đặc quyền tối thiểu, kèm `create-acls.sh`.

## Test
```bash
mvn verify   # 3 integration test, cần Docker
# TickToMarketDataIT   - tick Avro -> risk -> REST
# IdempotentConsumerIT - gửi trùng 3 lần -> đúng 1 execution
# SagaRejectionIT      - lệnh ngoài biên độ -> report REJECTED, không có execution
```
