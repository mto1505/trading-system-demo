# Trading System v4 — Bộ Tài Liệu Kiến Trúc & Vận Hành Toàn Diện

> **Phiên bản tài liệu:** 2.0 — viết lại toàn bộ, đối chiếu trực tiếp với mã nguồn tại commit hiện tại.
> **Đối tượng:** kỹ sư cần nắm bản chất hệ thống để vận hành thực chiến, mở rộng tính năng, hoặc trả lời phỏng vấn cấp Senior / Lead / Architect.
> **Nguyên tắc biên soạn:** mọi khẳng định trong bộ tài liệu này đều chỉ ra được dòng mã tương ứng. Không có khẳng định nào dựa trên "thường thì người ta làm thế".

---

## 0. Hệ thống này là gì — trong 10 dòng

`trading-system` là một mô phỏng **sàn giao dịch chứng khoán end-to-end**, xây bằng Spring Boot 3.2.5 / Java 17, gồm 4 microservice nghiệp vụ nối với nhau **chỉ bằng Kafka** — không service nào gọi REST sang service nào.

Hệ thống nhận luồng giá thị trường (tick), tính chỉ số rủi ro thời gian thực, nhận lệnh của khách qua REST, thẩm định trước giao dịch, khớp lệnh theo luật sàn, rồi trả kết quả ngược về để đóng vòng đời lệnh.

Giá trị của repo này **không nằm ở nghiệp vụ chứng khoán** — nghiệp vụ được rút gọn tối đa: khớp toàn bộ tại giá tham chiếu, không có sổ lệnh (limit order book), không có khớp một phần. Giá trị nằm ở chỗ nó cài đặt **đầy đủ và đúng** một bộ pattern phân tán khó:

| Pattern | Cài đặt ở đâu |
|---|---|
| Event-Driven Architecture + CQRS read model | `order-service/consumer/MarketDataCache.java` |
| Transactional Outbox (2 service, 1 relay dùng chung) | `outbox-support/`, `*/entity/OutboxEventEntity.java` |
| Idempotent Consumer (effectively-once) | `matching-engine/repository/ProcessedEventRepository.java` |
| Saga Choreography (vòng đời lệnh khép kín) | `OrderService` → `MatchingService` → `OrderStatusService` |
| Write-behind cache (hot path / cold path) | `pricing-risk-service/service/SnapshotStore.java` |
| Dead Letter Topic | `pricing-risk-service/config/KafkaConfig.java:74-81` |
| Optimistic Locking | `order-service/entity/OrderEntity.java:70-80` |
| Schema Registry + Avro evolution | `common-contracts/src/main/avro/TickEventAvro.avsc` |
| Autoscale theo Kafka lag (KEDA) | `k8s/components/keda/` |
| Zero-trust network (default-deny) | `k8s/base/networkpolicy-default-deny.yaml` |

---

## 1. Bản đồ kiến trúc

### 1.1. Luồng dữ liệu chính

```
                   ┌───────────────────────────────────────────────────┐
                   │ Nguồn ngoài: exchange feed / FIX / REST             │
                   │ (demo: SimulatedFeed random walk, 4 mã x 5 tick/s)  │
                   └──────────────────────┬────────────────────────────┘
                                          │ POST /api/v1/ticks
                                          ▼
                   ┌───────────────────────────────────────────────────┐
                   │ market-gateway  :8081                              │
                   │ • Không có nghiệp vụ. Chỉ map + produce.            │
                   │ • Avro + Schema Registry                           │
                   │ • acks=1, linger=5ms, lz4, batch 64KB              │
                   └──────────────────────┬────────────────────────────┘
                                          │ key = symbol
                                          ▼
                     ═══════════ stock.ticks (12 partition) ═══════════
                                          │
                                          ▼ batch tối đa 1000 bản ghi
                   ┌───────────────────────────────────────────────────┐
                   │ pricing-risk-service  :8082                        │
                   │ HOT PATH (mỗi tick, ~µs):                          │
                   │   EMA → log-return → variance O(1)                 │
                   │   → volatility → VaR95 → riskLevel                 │
                   │   → ConcurrentHashMap.put                          │
                   │   → send(stock.enriched) bất đồng bộ               │
                   │ COLD PATH (mỗi 5s): saveAll → PostgreSQL           │
                   │ Lỗi xử lý → retry 3 lần → stock.ticks.DLT          │
                   └──────────────────────┬────────────────────────────┘
                                          │ JSON, acks=1, key = symbol
                                          ▼
                    ═══════════ stock.enriched (12 partition) ═══════════
                                          │
 Client ──POST /api/v1/orders──┐          │ (local materialized view)
          X-API-KEY            ▼          ▼
                   ┌───────────────────────────────────────────────────┐
                   │ order-service  :8083                               │
                   │ 1. Redis GET idem:{clientOrderId}                  │
                   │ 2. Pre-trade check — bảo vệ KHÁCH HÀNG:            │
                   │      • staleness > 10s  → NO_FRESH_MARKET_DATA     │
                   │      • riskLevel = HIGH → RISK_LEVEL_HIGH          │
                   │      • |Δgiá| > 5%      → PRICE_DEVIATION          │
                   │ 3. MỘT TRANSACTION: INSERT orders                  │
                   │                   + INSERT outbox_event            │
                   │    UNIQUE(client_order_id) = chốt chặn cuối        │
                   │ 4. Redis SET kết quả, TTL 10 phút                  │
                   │ RateLimiter 200/s · CircuitBreaker orders-db       │
                   └──────────────────────┬────────────────────────────┘
                                          │ OutboxRelay @Scheduled 500ms
                                          │ SELECT … FOR UPDATE SKIP LOCKED
                                          │ acks=all + enable.idempotence
                                          ▼
                     ═══════════ stock.orders (12 partition) ═══════════
                                          │
                                          ▼
                   ┌───────────────────────────────────────────────────┐
                   │ matching-engine  :8084                             │
                   │ MỘT TRANSACTION gồm 3 việc:                        │
                   │  (1) INSERT processed_event ON CONFLICT DO NOTHING │
                   │      → 0 row = đã xử lý → bỏ qua                   │
                   │  (2) TradingRules — luật của SÀN:                  │
                   │        • lô chẵn 100                               │
                   │        • biên độ ±7%                               │
                   │        • phiên giao dịch (tắt mặc định)            │
                   │      vi phạm → ghi report REJECTED                 │
                   │  (3) INSERT execution + INSERT outbox_event        │
                   └──────────────────────┬────────────────────────────┘
                                          │ OutboxRelay, key = symbol
                                          ▼
                   ═══════════ stock.executions (12 partition) ═══════════
                                          │
                                          ▼
                   ┌───────────────────────────────────────────────────┐
                   │ order-service  :8083   (reply leg của saga)        │
                   │ OrderStateMachine.canTransition()                  │
                   │   ACCEPTED → FILLED | REJ_MATCH                    │
                   │   trạng thái cuối không có đường ra                │
                   │   ⇒ report trùng tự động bị bỏ qua                 │
                   │ @Version → optimistic lock → retry 3 lần           │
                   └───────────────────────────────────────────────────┘
```

### 1.2. Hạ tầng phụ trợ và hành vi khi hỏng

| Thành phần | Cổng local | Vai trò | Hỏng thì sao? |
|---|:--:|---|---|
| Kafka (KRaft, không Zookeeper) | 9092 | Xương sống truyền thông | Toàn hệ ngừng luân chuyển. Outbox **giữ lại event trong DB**, không mất dữ liệu; relay gửi lại khi Kafka trở lại. |
| PostgreSQL 16 | 5432 | Nguồn sự thật của `orders`, `execution`, `outbox_event` | order/matching không nhận lệnh mới (CircuitBreaker mở → trả `SYSTEM_UNAVAILABLE`). pricing **vẫn chạy** nhờ write-behind. |
| Schema Registry | 8085→8081 | Cưỡng chế tương thích schema Avro | Gateway không produce được schema mới; service đã cache schema cũ vẫn chạy tiếp. |
| Redis 7 | 6379 | **Chỉ là cache tăng tốc** idempotency | Hệ vẫn **đúng**, chỉ chậm hơn — DB UNIQUE constraint mới là chốt chặn thật. `management.health.redis.enabled: false` cố ý để Redis chết không làm readiness fail. |
| Zipkin | 9411 | Distributed tracing | Mất khả năng quan sát; nghiệp vụ không ảnh hưởng. |
| Prometheus | 9090 | Pull `/actuator/prometheus` mỗi 10s | Mất metric và alert — tức là mất khả năng **biết** hệ đang hỏng. |
| Grafana | 3000 | Dashboard (admin/admin) | — |

---

## 2. Danh mục chương

Mỗi chương độc lập, đọc riêng được. Cấu trúc thống nhất: **bản chất → vai trò → cơ chế (kèm mã) → vấn đề được giải quyết → đánh đổi → phương án thay thế → núm vặn cấu hình → cách kiểm chứng → hạn chế đã biết → câu hỏi phỏng vấn → bảng tổng kết**.

### Phần I — Hiểu hệ thống

| # | Chương | Trọng tâm |
|:--:|---|---|
| **00** | [Đường đi của một lệnh — end-to-end](./00-duong-di-cua-mot-lenh-end-to-end.md) | Bám theo đúng một lệnh `BUY HPG 100 @ 28.500` qua **từng dòng mã**, từ `curl` tới khi `status = FILLED`. **Đọc chương này trước tiên.** |
| **01** | [Kiến trúc tổng thể & Microservices Boundary](./01-kien-truc-tong-the-va-microservices-boundary.md) | Vì sao chia đúng 4 service; tiêu chí chia theo *nhịp scale* chứ không theo bảng DB; CQRS local read model; khi nào kiến trúc này **sai** (HFT). |
| **02** | [Kafka Messaging & Schema Registry](./02-kafka-messaging-va-schema-registry.md) | KRaft; 4 topic + 4 DLT; `key = symbol` là **ràng buộc đúng đắn** chứ không phải tối ưu hoá; hai profile producer `acks=1` vs `acks=all`; Avro BACKWARD compatibility. |

### Phần II — Bốn service nghiệp vụ

| # | Chương | Trọng tâm |
|:--:|---|---|
| **03** | [Pricing, Real-time Risk & Write-Behind Cache](./03-pricing-realtime-risk-va-write-behind-cache.md) | Thuật toán $O(1)$ (EMA, sliding-window variance, VaR 95%); vì sao thứ tự tick bắt buộc; hot path vs cold path; DLQ chống poison pill. |
| **04** | [Order Service, Pre-trade Risk & Resilience](./04-order-service-pretrade-risk-va-resilience.md) | 3 lớp pre-trade check; triết lý fail-safe; RateLimiter / CircuitBreaker; Redis degrade gracefully; phân trang là bắt buộc. |
| **07** | [Matching Engine, Trading Rules & Saga Choreography](./07-matching-engine-va-saga-choreography.md) | Luật sàn (lô 100, biên độ 7%, phiên); một transaction ba việc; state machine; alert kẹt saga. |

### Phần III — Dữ liệu & tính đúng đắn phân tán

| # | Chương | Trọng tâm |
|:--:|---|---|
| **05** | [Distributed Transactions, Outbox Pattern & CDC](./05-distributed-transactions-outbox-va-cdc.md) | Thảm hoạ dual-write; vì sao không dùng XA/2PC; `FOR UPDATE SKIP LOCKED`; Debezium đọc WAL; một property đổi cả cơ chế. |
| **06** | [Idempotency & Consumer Deduplication](./06-idempotency-va-consumer-deduplication.md) | At-least-once là định mệnh chứ không phải lựa chọn; 3 lớp dedupe; `ON CONFLICT DO NOTHING`; hợp đồng ngầm giữa retention topic và bảng `processed_event`. |
| **13** | [Persistence, Flyway & Mô hình dữ liệu](./13-persistence-flyway-va-mo-hinh-du-lieu.md) | `ddl-auto: validate`; schema-per-service trong một instance; partial index; optimistic lock; tắt OSIV; DTO thay vì trả entity. |

### Phần IV — Vận hành

| # | Chương | Trọng tâm |
|:--:|---|---|
| **08** | [Observability: Metrics, Tracing & Alerting](./08-observability-metrics-tracing-va-alerting.md) | Pull model; custom metric nghiệp vụ; p99 vs trung bình; sampling 10% trên hot path; 6 alert rule và ý nghĩa thật của từng cái. |
| **09** | [Triển khai Local & Kiểm thử tích hợp](./09-trien-khai-local-va-integration-testing.md) | 11 container; chế độ hybrid để debug IDE; Testcontainers Kafka/Postgres **thật**; k6 biến hiệu năng thành hợp đồng. |
| **10** | [Kubernetes: Kustomize & KEDA](./10-trien-khai-kubernetes-kustomize-va-keda.md) | Một manifest ba môi trường; công thức **trần pod = partition / 3**; vì sao CPU là tín hiệu sai; CI cưỡng chế ràng buộc liên-file. |
| **11** | [Helm & Hạ tầng Production](./11-trien-khai-helm-va-ha-tang-production.md) | Values phân tầng; staging in-cluster vs prod managed (RDS, MSK); ESO; rollback tức thì; cert-manager tự ký. |
| **12** | [Bảo mật hệ thống](./12-bao-mat-he-thong.md) | API key là sàn không phải trần; SASL_SSL + SCRAM + ACL đặc quyền tối thiểu; zero-trust NetworkPolicy; hardening container. |
| **15** | [Runbook vận hành & xử lý sự cố](./15-runbook-van-hanh-va-su-co.md) | Triệu chứng → nguyên nhân → thao tác. Replay DLT, gỡ saga kẹt, xoay mật khẩu, tăng partition. |

### Phần V — Sự thật trần trụi

| # | Chương | Trọng tâm |
|:--:|---|---|
| **14** | [Hạn chế đã biết & Lộ trình](./14-han-che-da-biet-va-lo-trinh.md) | 22 vấn đề đã xác định trong chính repo này (Bug / Gap / Doc / Ops), vị trí chính xác, hướng sửa, mức độ nghiêm trọng. **Chương quan trọng nhất khi phỏng vấn.** |

---

## 3. Lộ trình đọc gợi ý

| Bạn là | Đọc theo thứ tự |
|---|---|
| Có 30 phút, muốn nắm ý chính | `00` → mục 1 của README này → `14` |
| Chuẩn bị phỏng vấn kiến trúc | `00` → `01` → `05` → `06` → `07` → `14` |
| Sắp vận hành hệ thống | `09` → `10` → `08` → `15` → `12` |
| Sắp viết tính năng mới | `00` → `02` → chương của service liên quan → `13` |
| Muốn đào thuật toán định giá | `03` → `DEEP-DIVE.md` mục 2 |

---

## 4. Bảng tra cứu nhanh: pattern → dòng mã

| Bạn muốn xem | Mở file | Dòng |
|---|---|:--:|
| Tick được sinh ra | `market-gateway/feed/SimulatedFeed.java` | 38-51 |
| Map domain → Avro | `market-gateway/kafka/TickProducer.java` | 28-40 |
| Tuning producer tốc độ | `market-gateway/config/KafkaProducerConfig.java` | 52-58 |
| Thuật toán EMA / volatility / VaR | `pricing-risk-service/service/RiskCalculator.java` | 42-89 |
| Hot path xử lý batch tick | `pricing-risk-service/consumer/TickConsumer.java` | 51-68 |
| Write-behind flush | `pricing-risk-service/service/SnapshotStore.java` | 48-68 |
| Dead Letter Topic | `pricing-risk-service/config/KafkaConfig.java` | 74-81 |
| CooperativeStickyAssignor | `pricing-risk-service/config/KafkaConfig.java` | 47-49 |
| 4 lớp chống trùng khi đặt lệnh | `order-service/service/OrderService.java` | 75-107 |
| 3 luật pre-trade | `order-service/service/OrderService.java` | 109-128 |
| Fail-safe fallback | `order-service/service/OrderService.java` | 146-152 |
| **2 INSERT 1 transaction (Outbox)** | `order-service/service/OrderPersistence.java` | 37-66 |
| `FOR UPDATE SKIP LOCKED` | `order-service/repository/OutboxEventRepository.java` | 16-23 |
| Relay publish đồng bộ + rollback batch | `outbox-support/OutboxRelay.java` | 50-71 |
| **Idempotent consumer** | `matching-engine/repository/ProcessedEventRepository.java` | 14-23 |
| Một transaction ba việc | `matching-engine/service/MatchingService.java` | 60-95 |
| Luật sàn | `matching-engine/service/TradingRules.java` | 37-62 |
| State machine = lớp idempotency miễn phí | `order-service/service/OrderStateMachine.java` | 16-37 |
| Optimistic lock | `order-service/entity/OrderEntity.java` | 70-80 |
| Retry optimistic lock | `order-service/consumer/ExecutionReportConsumer.java` | 23-41 |
| Test chứng minh effectively-once | `matching-engine/src/test/.../IdempotentConsumerIT.java` | 56-83 |
| Test saga chiều về | `matching-engine/src/test/.../SagaRejectionIT.java` | 57-85 |
| Alert kẹt saga | `monitoring/alert-rules.yml` | 25-36 |
| Trần pod = partition / 3 | `k8s/components/keda/scaledobject-pricing-risk.yaml` | 11-15 |
| CI cưỡng chế ràng buộc KEDA | `.github/workflows/k8s-validate.yml` | 59-78 |

---

## 5. Tài liệu nguồn trong repo

Bộ tài liệu này thay thế vai trò "tài liệu ôn tập", nhưng không thay thế các tài liệu gốc — chúng vẫn là nguồn đối chiếu:

| File | Nội dung | Khi nào đọc |
|---|---|---|
| [`README.md`](../README.md) | Tổng quan + lệnh chạy nhanh + cURL xác thực | Lần đầu clone repo |
| [`GUIDE.md`](../GUIDE.md) | Cẩm nang thực hành, bảng tra `file:dòng`, triệu chứng → nguyên nhân | Khi đang gỡ lỗi |
| [`DESIGN-NOTES.md`](../DESIGN-NOTES.md) | Lý do lựa chọn thiết kế (bản ghi quyết định) | Khi muốn hiểu "vì sao không làm cách khác" |
| [`DEEP-DIVE.md`](../DEEP-DIVE.md) | Phân tích sâu 11 chủ đề + bảng 22 điểm nên xem lại | Khi muốn đào tận đáy một cơ chế |
| [`DEPLOY-LOCAL.md`](../DEPLOY-LOCAL.md) | Thiết lập Docker Desktop, xung đột cổng | Khi dựng môi trường cá nhân |
| [`HELM-DEPLOY.md`](../HELM-DEPLOY.md) | Phát hành Helm cho staging/production | Khi deploy lên cluster |
| [`k8s/README.md`](../k8s/README.md) | Quy chuẩn Kustomize + runbook | Khi sửa manifest |

---

## 6. Quy ước trong bộ tài liệu

- `file.java:12-34` — tham chiếu dòng mã, đã kiểm chứng tại thời điểm viết.
- ⚠️ — hạn chế hoặc cạm bẫy **đã được xác nhận trong chính repo này**, không phải lo ngại lý thuyết.
- 🔑 — điểm mấu chốt; mất nó là mất tính đúng đắn.
- 💡 — mẹo vận hành.
- **Bug / Gap / Doc / Ops** — phân loại vấn đề theo chương 14: *Bug* = sai hành vi; *Gap* = cơ chế không phủ hết tình huống nó định phủ; *Doc* = tài liệu lệch mã; *Ops* = vấn đề tích luỹ theo thời gian vận hành.
