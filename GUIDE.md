# GUIDE — Hướng dẫn thực hành cho dự án trading-system

> Tài liệu này trả lời **"làm thế nào"**: chạy ở đâu, code nằm chỗ nào, sửa thế nào, hỏng thì tra ở đâu.
> Muốn biết **"vì sao thiết kế như vậy"** và ưu/nhược từng quyết định → đọc `DESIGN-NOTES.md`.
> Muốn xem nhanh lệnh curl demo → `README_ARC.md`.

---

## 1. Hệ thống này làm gì

Mô phỏng một pipeline giao dịch chứng khoán end-to-end: nhận tick giá từ thị trường → tính rủi ro
realtime → nhận lệnh của khách, kiểm tra pre-trade → khớp lệnh → báo kết quả ngược về.

```
                         ┌──────────────────── Schema Registry (8085) ─ enforce Avro
                         ▼
  SimulatedFeed ──► market-gateway (8081) ──[Avro, acks=1, lz4]──► stock.ticks (12 part, key=symbol)
   hoặc POST /api/v1/ticks                                                  │
                                                                            ▼  batch 1000 tick/poll
                                                     pricing-risk-service (8082)
                                                     EMA / volatility / VaR95 / riskLevel  (O(1)/tick, in-RAM)
                                                       │                    └─ flush 5s ──► Postgres market_snapshot
                                                       ▼ [JSON, acks=1]
                                                  stock.enriched
                                                       │
                                                       ▼  giữ bản mới nhất trong ConcurrentHashMap
  Client ──POST /api/v1/orders──►  order-service (8083)
            X-API-KEY + clientOrderId    │ pre-trade: staleness → riskLevel → fat-finger
                                         │ 1 transaction: INSERT orders + INSERT outbox_event
                                         ▼ OutboxRelay poll 500ms [acks=all, idempotent]
                                    stock.orders
                                         │
                                         ▼
                              matching-engine (8084)
                              dedupe (processed_event) → TradingRules → INSERT execution + outbox
                                         │
                                         ▼
                                  stock.executions
                                         │
                                         ▼
                        order-service: ACCEPTED → FILLED | REJ_MATCH  (state machine + optimistic lock)

  Quan sát: mọi service → /actuator/prometheus → Prometheus (10s) → Grafana (3000)
            trace xuyên suốt REST + Kafka → Zipkin (9411), sampling 10%
```

**Cấu trúc thư mục**

| Module | Vai trò | Port |
|---|---|---|
| `common-contracts` | DTO dùng chung + Avro schema `.avsc` (sinh class lúc build) | — |
| `outbox-support` | Thư viện: `OutboxRelay` + 2 interface port `OutboxStore`/`OutboxRecord` | — |
| `market-gateway` | Nhận feed → publish Avro vào `stock.ticks` | 8081 |
| `pricing-risk-service` | Tính giá/rủi ro, write-behind DB, publish `stock.enriched` | 8082 |
| `order-service` | Pre-trade check, đặt lệnh, theo dõi vòng đời lệnh | 8083 |
| `matching-engine` | Idempotent consumer, luật sàn, khớp lệnh, báo execution | 8084 |
| `monitoring/` `k8s/` `cdc/` `security/` `loadtest/` | Hạ tầng vận hành | — |

Hạ tầng phụ trợ: Kafka KRaft 9092 · Postgres 5432 · Schema Registry 8085 · Redis 6379 · Zipkin 9411 · Prometheus 9090 · Grafana 3000.

---

## 2. Chạy lần đầu

**Cần có:** Docker Desktop (bật), JDK 17, Maven 3.9+. Test tích hợp cần Docker vì dùng Testcontainers.

```bash
cd trading-system
docker compose up -d --build     # lần đầu ~5-10 phút do build 4 image
docker compose ps                # chờ tất cả healthy
```

Kiểm tra pipeline sống:

```bash
# 1. Feed mô phỏng đang bơm tick (4 mã x 5 tick/s)
curl -s localhost:8082/api/v1/marketdata/HPG

# 2. Đặt lệnh hợp lệ -> 201 ACCEPTED
curl -s -X POST localhost:8083/api/v1/orders \
  -H 'Content-Type: application/json' -H 'X-API-KEY: demo-key' \
  -d '{"clientOrderId":"g-1","symbol":"HPG","side":"BUY","price":28500,"quantity":100,"accountId":"ACC1"}'

# 3. Sau ~1-2s, lệnh đã đi hết vòng saga
curl -s localhost:8084/api/v1/executions/g-1
docker compose exec postgres psql -U trading -d trading \
  -c "SELECT client_order_id,status,filled_qty,avg_fill_price FROM ordersvc.orders WHERE client_order_id='g-1';"
# -> FILLED
```

> ⏱ **Chạy `curl` đặt lệnh ngay sau khi `up`** thường bị `422 NO_FRESH_MARKET_DATA` — đó là hành vi
> đúng, không phải lỗi: cache enriched của order-service chưa có dữ liệu. Chờ ~30-45s rồi thử lại.

**Chạy 1 service trong IDE** (debug tiện hơn): tắt service đó trong compose, các cổng đã map sẵn ra host
nên chỉ cần `mvn -pl order-service spring-boot:run` — default trong `application.yml` trỏ về `localhost`.

**Build/test:**
```bash
mvn -q clean package -DskipTests     # build tất cả module
mvn verify                           # + 3 integration test (cần Docker, mỗi test spin Kafka+Postgres thật)
mvn -pl order-service -am package    # build 1 module kèm dependency
```

---

## 3. Đi theo một lệnh qua toàn bộ code

Đây là đường đi quan trọng nhất của hệ; nắm được nó là nắm được 80% dự án.

| # | Bước | File |
|---|---|---|
| 1 | Nhận HTTP, validate bean, rate-limit 200 req/s/pod | `order-service/.../controller/OrderController.java` |
| 2 | Chặn request thiếu `X-API-KEY` | `order-service/.../config/ApiKeyFilter.java` |
| 3 | Tra Redis theo `idem:<clientOrderId>` — trùng thì trả kết quả cũ, không tạo lệnh mới | `OrderService.java:75` |
| 4 | Pre-trade 3 lớp: **staleness** (>10s) → **riskLevel=HIGH** → **fat-finger** (lệch >5%) | `OrderService.java:109` |
| 5 | 1 transaction: `INSERT orders` + `INSERT outbox_event(topic=stock.orders)` | `OrderPersistence.java:38` |
| 6 | Trùng `clientOrderId` lọt qua Redis → `DataIntegrityViolationException` → trả lệnh cũ | `OrderService.java:75` (catch) |
| 7 | Scheduler 500ms: `SELECT ... FOR UPDATE SKIP LOCKED` → gửi Kafka đồng bộ → mark published | `outbox-support/.../OutboxRelay.java:52` |
| 8 | matching-engine nhận, `INSERT ... ON CONFLICT DO NOTHING` vào `processed_event` để dedupe | `MatchingService.java:61` |
| 9 | Luật sàn: lô chẵn 100 → biên độ 7% → (tuỳ chọn) giờ giao dịch | `TradingRules.java:40` |
| 10 | `INSERT execution` + outbox `ExecutionReport` — vẫn trong cùng transaction | `MatchingService.java:97` |
| 11 | order-service nhận report, kiểm tra chuyển trạng thái hợp lệ rồi cập nhật | `OrderStatusService.java:36`, `OrderStateMachine.java` |
| 12 | Xung đột optimistic lock → retry tối đa 3 lần | `ExecutionReportConsumer.java` |

Luồng tick (song song, độc lập): `SimulatedFeed` → `TickProducer` → `TickConsumer.java:52` (batch listener)
→ `RiskCalculator.java:35` (EMA α=0.1, cửa sổ 120 tick, VaR z=1.645) → `SnapshotStore` (RAM + flush 5s).

---

## 4. Bảng tra nhanh: pattern → nơi cài đặt

| Pattern | Cài ở đâu | Điểm cần nhớ khi sửa |
|---|---|---|
| **Transactional Outbox** | `OutboxRelay` + `JpaOutboxStore` ở mỗi service | Ghi outbox **phải** cùng transaction với ghi nghiệp vụ, nếu không mất tính nguyên tử |
| **Idempotent Consumer** | `processed_event` (PK = `clientOrderId`), `ProcessedEventRepository.insertIfAbsent` | Outbox là at-least-once → consumer *bắt buộc* dedupe |
| **Idempotency API** | Redis `idem:*` TTL 10 phút + UNIQUE constraint DB | Redis là cache tăng tốc, **DB constraint mới là chốt chặn thật** |
| **Saga choreography** | `stock.orders` → `stock.executions`, không có orchestrator | Chiều về đứt = lệnh treo mãi ở ACCEPTED → alert `SagaStuckOrders` |
| **State machine** | `OrderStateMachine.ALLOWED` | Chỉ `ACCEPTED → FILLED \| REJ_MATCH`; chuyển sai bị bỏ qua lặng lẽ (tăng `orders.report.ignored`) |
| **Optimistic locking** | `@Version` trên `OrderEntity` (cột `version`, V2 migration) | Retry 3 lần ở consumer, quá thì ném lên và Kafka retry |
| **CQRS read model** | `MarketDataCache` (subscribe `stock.enriched`) | Không gọi REST sang pricing; đổi lại phải có staleness check |
| **Write-behind cache** | `SnapshotStore.flushToDatabase()` (5s) | Pod chết mất tối đa 5s snapshot; Kafka giữ raw tick để replay |
| **DLQ** | `DefaultErrorHandler` + `stock.ticks.DLT` (chỉ pricing) | Retry 3 lần cách 1s rồi đẩy DLT — không im lặng nuốt lỗi |
| **Circuit breaker / Rate limiter** | Resilience4j, khai báo trong `order-service/application.yml` | Breaker mở → `SYSTEM_UNAVAILABLE`, fail-safe là *từ chối* lệnh |
| **Avro + Schema Registry** | `common-contracts/src/main/avro/TickEventAvro.avsc` | Chỉ topic `stock.ticks` dùng Avro; các topic khác vẫn JSON |

---

## 5. Cấu hình hay phải chỉnh

Tất cả đều override được bằng biến môi trường (xem `docker-compose.yml`).

| Thuộc tính | Mặc định | Ở đâu | Ý nghĩa |
|---|---|---|---|
| `order.max-data-age-ms` | 10000 | order-service | Dữ liệu cũ hơn → từ chối lệnh |
| `order.max-price-deviation-pct` | 5 | order-service | Chặn fat-finger |
| `matching.price-band-pct` | 7 | matching-engine | Biên độ sàn (HOSE 7, HNX 10) |
| `matching.lot-size` | 100 | matching-engine | Lô chẵn; phái sinh đặt 1 |
| `matching.session-check-enabled` | false | matching-engine | Bật → từ chối lệnh ngoài giờ 9:00–11:30, 13:00–14:45 |
| `outbox.relay.enabled` | true | order + matching | **Đặt false khi bật CDC**, nếu không publish nhân đôi |
| `security.api-key` (`ORDER_API_KEY`) | `demo-key` | order-service | Production lấy từ Secret/Vault |
| `TRACING_PROBABILITY` | 0.1 | mọi service | Nâng lên 1.0 khi điều tra sự cố |
| `feed.simulated.enabled` (`SIMULATED_FEED`) | true | market-gateway | Tắt khi cắm feed thật |
| `resilience4j.ratelimiter...limit-for-period` | 200/s | order-service | Vượt → HTTP 429 |

**Quy tắc DB:** `ddl-auto: validate` ở cả 3 service có DB — Hibernate **không bao giờ** tự sửa schema.
Muốn đổi bảng: thêm file `V<n>__ten.sql` vào `src/main/resources/db/migration/`, Flyway chạy lúc khởi động.
`order-service` dùng schema `ordersvc`, `matching-engine` dùng `matchsvc`, `pricing-risk-service` dùng `public`
— chung một Postgres instance nhưng không đụng bảng của nhau.

---

## 6. Quan sát & gỡ lỗi

**Metric tự định nghĩa** (query trên Prometheus `localhost:9090`):

| Metric | Nghĩa |
|---|---|
| `pricing_e2e_latency_seconds{quantile="0.99"}` | KPI số 1: tick sinh ra → tính xong. Nhìn p99, đừng nhìn average |
| `pricing_batch_process_seconds` | Hot path chậm dần = GC hoặc lock contention |
| `gateway_ticks_sent_total` / `gateway_ticks_error_total` | Feed còn chạy không |
| `orders_accepted_total` / `orders_rejected_total` / `orders_duplicate_total` | Sức khoẻ cửa nhận lệnh |
| `orders_filled_total` / `orders_rejected_by_matching_total` | Chiều về của saga |
| `orders_report_ignored_total` | Report đến sai trạng thái hoặc lệnh không tồn tại |
| `outbox_published_total` / `outbox_publish_failed_total` | Outbox có ra được Kafka không |
| `matching_executed_total` / `matching_rejected_total` / `matching_duplicate_skipped_total` | Bên khớp lệnh |

- **Grafana** `localhost:3000` (admin/admin) → folder **Trading** → dashboard đã provision sẵn.
- **Alert mẫu** trong `monitoring/alert-rules.yml` — nghiêm trọng nhất là `SagaStuckOrders`
  (lệnh ACCEPTED không bao giờ nhận được execution report).
- **Zipkin** `localhost:9411`: xem một trace đi xuyên gateway → pricing → order. Log cũng in
  `[service,traceId,spanId]` nên có thể copy traceId từ log sang Zipkin.

**Kiểm tra bằng SQL khi nghi ngờ:**
```bash
docker compose exec postgres psql -U trading -d trading
```
```sql
SELECT status, count(*) FROM ordersvc.orders GROUP BY status;          -- lệnh treo ở ACCEPTED?
SELECT count(*) FROM ordersvc.outbox_event WHERE published_at IS NULL; -- outbox tắc?
SELECT * FROM matchsvc.processed_event ORDER BY processed_at DESC LIMIT 5;
```

---

## 7. Triệu chứng → nguyên nhân

| Triệu chứng | Nguyên nhân thường gặp |
|---|---|
| `422 NO_FRESH_MARKET_DATA` | Feed chưa chạy đủ lâu, hoặc pricing-risk chết, hoặc `stock.enriched` tắc. Kiểm tra `curl localhost:8082/api/v1/marketdata` |
| `422 RISK_LEVEL_HIGH` | Volatility mô phỏng vọt lên — bình thường; chờ vài giây hoặc nới ngưỡng trong `RiskCalculator` |
| `401` khi POST order | Thiếu header `X-API-KEY: demo-key` |
| `429` | Rate limiter 200 req/s/pod đang làm việc — đúng như thiết kế |
| Lệnh kẹt mãi ở `ACCEPTED` | `outbox_publish_failed_total` > 0, matching-engine chết, hoặc consumer lag. Xem mục 6 |
| Lệnh về `REJ_MATCH / INVALID_LOT_SIZE` | `quantity` không phải bội số 100 |
| Event ra Kafka **hai lần** | Bật CDC mà quên `OUTBOX_RELAY_ENABLED=false` |
| App không khởi động, lỗi schema validate | Thiếu migration Flyway; `ddl-auto: validate` cố ý không tự sửa |
| Compose CDC báo trùng cổng **8083** | `docker-compose.cdc.yml` map Kafka Connect vào 8083 trùng order-service — đổi một trong hai trước khi chạy overlay |
| Deserialize lỗi ở consumer | Contract lệch giữa các service → sửa ở `common-contracts` rồi build lại **tất cả** module |
| Tick không được tiêu thụ | Schema Registry chưa lên (`curl localhost:8085/subjects`) |

---

## 8. Công thức sửa đổi thường dùng

**Thêm field vào contract JSON** (`OrderResult`, `EnrichedMarketData`, `ExecutionReport`)
→ sửa record trong `common-contracts`, `mvn clean install` toàn repo, rebuild mọi image. Compiler bắt lỗi giúp.

**Đổi Avro `TickEventAvro`** → sửa `.avsc`; muốn tương thích BACKWARD thì **chỉ thêm field có `default`**,
không xoá/đổi kiểu field cũ. Schema Registry sẽ từ chối schema vi phạm lúc producer đăng ký.

**Thêm luật sàn mới** → thêm nhánh vào `TradingRules.validate()` trả về mã lý do; lý do sẽ tự chảy
về order-service qua `ExecutionReport.reason` và vào cột `orders.reason`.

**Thêm một topic mới** → khai `@Bean NewTopic` trong `KafkaConfig` của service *producer*
(12 partition là chuẩn hiện tại), key nên là `symbol` để giữ thứ tự theo mã.

**Thêm cột DB** → file migration mới `V<n>__...sql` + sửa `@Entity`. Không bao giờ sửa file migration cũ.

**Thêm service mới có outbox** → thêm dependency `outbox-support`, tạo `@Entity` implement `OutboxRecord`,
một `@Component` implement `OutboxStore` bằng query `FOR UPDATE SKIP LOCKED`, và một bean
`KafkaTemplate<String,String>` tên `outboxKafkaTemplate` (acks=all + idempotence). `OutboxRelay` tự hoạt động.

---

## 9. Các chế độ mở rộng (tuỳ chọn)

```bash
# Debezium CDC thay polling relay — đọc WAL, độ trễ ~ms  (xem cdc/README_ARC.md)
docker compose -f docker-compose.yml -f docker-compose.cdc.yml up -d --build
sleep 30 && ./cdc/register-connectors.sh
#   Lưu ý: phải canh replication slot, connector chết lâu -> WAL không dọn -> đầy đĩa Postgres

# Load test k6 — threshold p95<200ms, p99<500ms  (xem loadtest/README_ARC.md)
k6 run loadtest/order-load.js

# Kubernetes
docker build --build-arg MODULE=order-service -t trading/order-service:dev .   # lặp cho 4 module
kubectl apply -k k8s/overlays/dev && kubectl -n trading-dev get pods -w
#   Kustomize: base + overlays dev/staging/prod. Khác nhau ở số partition, số replica,
#   autoscale (KEDA), nguồn secret, TLS. Toàn bộ bảng so sánh ở k8s/README_ARC.md.

# Bảo mật Kafka production: SASL/SCRAM + TLS + ACL đặc quyền tối thiểu
cat security/README_ARC.md && cat security/create-acls.sh
```

---

## 10. Giới hạn cần biết trước khi dùng thật

- **Không phải HFT.** Mỗi hop qua Kafka tốn 1–10ms, E2E ~10–30ms. HFT cần single-process + ring buffer.
- **`matching-engine` là mô phỏng**: không có sổ lệnh (order book), không khớp đối ứng, không khớp
  một phần — fill toàn bộ tại `refPrice`. Đây là chỗ cần thay bằng engine thật.
- **Không có quản lý tài khoản/số dư**: chưa chặn bán khống hay thiếu tiền.
- **Một Postgres instance dùng chung** (tách bằng schema) — production mỗi service một DB riêng.
- **API key tĩnh** là sàn bảo mật tối thiểu, không phải trần: production cần OAuth2/JWT + mTLS.
- **Replica factor = 1** ở mọi topic; mất broker là mất dữ liệu. Production tối thiểu 3.
- **Số đo load test trên máy dev chỉ để so sánh trước/sau**, không dùng cam kết SLA.

---

## 11. Đọc tiếp

| Muốn biết | Đọc |
|---|---|
| Vì sao chia service như vậy, ưu/nhược mỗi quyết định, lộ trình V1→V4 | `DESIGN-NOTES.md` |
| Lệnh curl demo nhanh theo từng phiên bản | `README_ARC.md` |
| CDC: đánh đổi polling vs WAL, rủi ro replication slot | `cdc/README.md` |
| Kafka SASL/SCRAM + TLS + ACL | `security/README.md` |
| Cách đọc kết quả load test | `loadtest/README.md` |
