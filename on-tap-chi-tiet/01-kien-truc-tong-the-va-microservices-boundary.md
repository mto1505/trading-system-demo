# Chương 01 — Kiến Trúc Tổng Thể & Microservices Boundary

> **Câu hỏi trung tâm:** vì sao hệ thống này chia thành **đúng bốn** service, và vì sao ranh giới lại nằm ở đúng những chỗ đó chứ không phải chỗ khác?
>
> **Tài liệu đối chiếu:** `README.md`, `GUIDE.md` mục 1 & 3, `DESIGN-NOTES.md` mục 1, `DEEP-DIVE.md` mục 1-2.

---

## 1. Nó là gì?

`trading-system` là một hệ thống phân tán **hướng sự kiện (Event-Driven Architecture)** kết hợp **CQRS**, mô phỏng vòng đời giao dịch chứng khoán từ lúc giá về tới lúc lệnh đóng.

### 1.1. Sáu module Maven

```
trading-system (pom, Spring Boot 3.2.5 parent, Java 17)
├── common-contracts        ← DTO dùng chung + Avro schema. KHÔNG có logic.
├── outbox-support          ← OutboxRelay + 2 interface. KHÔNG có entity.
├── market-gateway      :8081   I/O-bound
├── pricing-risk-service :8082  CPU-bound
├── order-service       :8083   Latency-critical + ACID
└── matching-engine     :8084   State-heavy + rule enforcement
```

Hai module đầu là **thư viện**, bốn module sau là **ứng dụng Spring Boot độc lập**.

### 1.2. Bốn service nghiệp vụ

| Service | Cổng | Trách nhiệm duy nhất | Có DB? | Có REST vào? |
|---|:--:|---|:--:|:--:|
| `market-gateway` | 8081 | Nhận tick từ nguồn ngoài → produce `stock.ticks`. **Không có một dòng nghiệp vụ nào.** | Không | Có (`/api/v1/ticks`) |
| `pricing-risk-service` | 8082 | Consume tick → tính EMA/volatility/VaR/riskLevel → produce `stock.enriched` + ghi snapshot | Có (`public`) | Có (đọc, `/api/v1/marketdata`) |
| `order-service` | 8083 | Nhận lệnh REST → pre-trade check → ghi `orders` + outbox → đóng vòng đời lệnh khi nhận report | Có (`ordersvc`) | Có (`/api/v1/orders`) |
| `matching-engine` | 8084 | Consume lệnh → dedupe → luật sàn → ghi `execution` + outbox report | Có (`matchsvc`) | Có (đọc, `/api/v1/executions`) |

🔑 **Không có mũi tên REST nào giữa bốn service.** Toàn bộ giao tiếp liên-service đi qua Kafka. Bằng chứng cứng trong manifest — `k8s/base/apps/apps-networkpolicy.yaml`:

```yaml
# Không có rule nào cho service-gọi-service vì thiết kế không có: 4 service
# chỉ nói chuyện qua Kafka.
```

NetworkPolicy egress của nhóm app chỉ mở tới Kafka (9092), Postgres (5432), Redis (6379), Schema Registry (8081), Zipkin (9411) — **không mở tới nhau**. Nếu ai đó lén thêm một `RestTemplate` gọi sang service khác, nó sẽ bị chặn ở tầng mạng, không phải ở code review.

### 1.3. Hai module thư viện và ranh giới của chúng

**`common-contracts`** — chỉ chứa 4 `record` và 1 file `.avsc`:

```java
// common-contracts/src/main/java/com/trading/common/model/TickEvent.java
/**
 * CONTRACT DÙNG CHUNG giữa các service (single source of truth).
 * Trước đây mỗi service tự định nghĩa record giống hệt nhau -> "contract ngầm",
 * đổi 1 field ở gateway mà quên sửa ở pricing là lỗi runtime khó tìm.
 * Giờ đổi schema = đổi 1 chỗ, compiler bắt lỗi ở mọi service.
 */
public record TickEvent(
        String symbol,       // Mã CK: VN30F2607, HPG, ...
        BigDecimal price,    // BigDecimal, KHÔNG dùng double cho tiền tệ
        long volume,
        long eventTime       // epoch millis tại nguồn -> đo latency E2E
) {}
```

⚠️ **Đây là một đánh đổi gây tranh cãi và cần nói thẳng.** Chia sẻ DTO giữa các microservice tạo **coupling ở tầng build**: đổi `TickEvent` là phải rebuild cả bốn service. Trường phái thuần tuý sẽ nói mỗi service nên có bản sao riêng của contract.

Lập luận bảo vệ lựa chọn này, theo đúng comment trong mã:
- Repo là **monorepo một đội**, không phải nhiều đội nhiều ngôn ngữ. Chi phí rebuild thấp, lợi ích compiler bắt lỗi cao.
- Bước tiến hoá tiếp theo đã được chỉ rõ: Avro/Protobuf + Schema Registry, nơi **registry** cưỡng chế tương thích thay cho compiler. `stock.ticks` đã đi bước đó rồi.

Nghĩa là: dùng chung DTO ở đây là **điểm dừng có ý thức trên một lộ trình**, không phải thiếu hiểu biết. Khi nào nên rời điểm dừng này? Khi có đội thứ hai, hoặc có service viết bằng ngôn ngữ khác.

**`outbox-support`** — chia sẻ **hành vi**, không chia sẻ **cấu trúc dữ liệu**:

```java
// outbox-support/src/main/java/com/trading/outbox/OutboxRecord.java
/**
 * Vì sao KHÔNG dùng chung một @Entity JPA giữa các service?
 * Shared entity kéo theo ràng buộc schema, JPA config, và vòng đời migration
 * chung giữa các service - đúng loại coupling mà microservices muốn tránh.
 */
public interface OutboxRecord {
    Long getId();
    String getTopic();
    String getMsgKey();
    String getPayload();
    void setPublishedAt(Instant publishedAt);
}
```

So sánh hai module:

| | `common-contracts` | `outbox-support` |
|---|---|---|
| Chia sẻ | Cấu trúc dữ liệu (DTO) | Hành vi (relay logic) |
| Coupling tạo ra | Build-time, cao | Rất thấp (chỉ 2 interface 5 method) |
| Khi đổi | Rebuild 4 service | Mỗi service tự quyết bảng của mình |
| Có nên bắt chước? | Chỉ trong monorepo một đội | Luôn luôn — đây là mẫu mực |

---

## 2. Vai trò trong hệ thống — bốn hồ sơ tài nguyên khác nhau

🔑 **Tiêu chí chia service của repo này: chia theo *nhịp scale và nhịp thay đổi*, không chia theo bảng dữ liệu.**

| Service | Nút thắt tài nguyên | Khi tải tăng 10× thì cần gì | Nhịp thay đổi mã |
|---|---|---|---|
| `market-gateway` | **Network I/O** — giữ socket/FIX session dài | Nhiều kết nối hơn, **không** nhiều CPU. Và với feed FIX một phiên, **không scale ngang được** | Rất chậm (chỉ đổi khi sàn đổi giao thức) |
| `pricing-risk-service` | **CPU + RAM** — tính toán thống kê trên mọi tick | Nhiều pod consumer hơn (tới trần = partition/3) | Nhanh (đội quant chỉnh công thức liên tục) |
| `order-service` | **Latency + ACID** — p99 phải thấp, không được mất lệnh | Nhiều pod + connection pool DB lớn hơn | Trung bình (đổi khi luật rủi ro đổi) |
| `matching-engine` | **State + rule enforcement** | Nhiều pod + throughput DB | Chậm–trung bình (đổi khi quy định sàn đổi) |

Bốn hồ sơ này **không thể tối ưu đồng thời trong một process**. Một ví dụ cụ thể: `pricing-risk-service` có `maximum-pool-size: 10` còn `order-service` có `20` — vì pricing chỉ dùng DB ở cold path (scheduler flush + API lịch sử), còn order mở một transaction ngắn cho **mỗi lệnh**. Gộp hai service lại thì phải chọn một con số cho cả hai, và con số nào cũng sai cho một bên.

Tương tự với heap: `k8s/base/apps/pricing-risk-service.yaml` xin `1536Mi` (giữ `SymbolState` + sliding window cho mọi mã), còn `order-service.yaml` xin `1Gi`. Gộp lại thì phải provision theo cái lớn hơn cho toàn bộ số pod.

### 2.1. Vì sao đúng con số 4, không phải 2 hay 8?

**Vì sao không gộp `market-gateway` vào `pricing-risk-service`?**
Vì chúng scale ngược chiều nhau. Gateway với feed FIX **không scale ngang được** — mỗi phiên FIX là một kết nối có trạng thái, hai pod cùng đọc một phiên sẽ nhân đôi tick. Bằng chứng trong repo: `k8s/overlays/prod/kustomization.yaml` đặt `market-gateway: count 1` kèm comment "xem cảnh báo scale ở base". Trong khi pricing phải scale lên 4 pod khi lag tăng. Gộp lại = một trong hai phải chịu con số sai.

**Vì sao không gộp `order-service` vào `matching-engine`?**
Đây là câu hỏi hay nhất, vì chúng *có vẻ* cùng là "xử lý lệnh". Ba lý do tách:
1. **Ranh giới tin cậy khác nhau.** `order-service` là cổng hướng ra client, `matching-engine` là nội bộ sàn. Gộp lại thì client có đường tới nơi thi hành luật sàn.
2. **Vai trò pháp lý khác nhau.** Pre-trade check bảo vệ *khách hàng*; trading rules bảo vệ *sàn*. Gộp lại thì không còn hai tầng kiểm tra độc lập (xem [chương 07](./07-matching-engine-va-saga-choreography.md) mục 5).
3. **Nhịp thay đổi khác nhau.** Luật sàn đổi theo quy định cơ quan quản lý; luật rủi ro đổi theo khẩu vị của chính công ty chứng khoán.

**Vì sao không chia nhỏ hơn (8 service)?**
Chống chỉ định rõ ràng: *đừng tạo `UserService`, `AccountService`, `OrderService` chỉ vì có 3 bảng DB*. Mỗi ranh giới service thêm vào là thêm một lần serialize, một network hop, một điểm hỏng, một consumer group, một manifest, một dashboard. Ví dụ: tách `RiskCalculator` thành service riêng khỏi `pricing-risk-service` sẽ thêm 2-5ms vào mỗi tick và chẳng giải quyết vấn đề gì — cả hai cùng CPU-bound, cùng nhịp scale, cùng nhịp đổi mã.

> **Quy tắc rút ra:** chỉ tách khi hai phần có **nhịp scale khác nhau**, **nhịp thay đổi khác nhau**, hoặc **ranh giới tin cậy khác nhau**. Thiếu cả ba thì tách là tự gây thương tích.

---

## 3. Cơ chế hoạt động

### 3.1. CQRS — Local Materialized View

Thay vì truy vấn đồng bộ, `order-service` **chủ động giữ bản sao** của read model mà nó cần:

```java
// order-service/src/main/java/com/trading/order/consumer/MarketDataCache.java:16-29
@Component
public class MarketDataCache {

    private final Map<String, EnrichedMarketData> cache = new ConcurrentHashMap<>();

    @KafkaListener(topics = "stock.enriched",
                   groupId = "order-service-group",
                   containerFactory = "enrichedListenerFactory")
    public void onEnriched(EnrichedMarketData data) {
        cache.put(data.symbol(), data);
    }

    public Optional<EnrichedMarketData> get(String symbol) {
        return Optional.ofNullable(cache.get(symbol));
    }
}
```

Lý do thiết kế, ghi ngay trong `OrderServiceApplication.java:10-19`:

```java
/**
 * Điểm thiết kế quan trọng: order-service KHÔNG gọi REST sang pricing-risk-service
 * mỗi lần đặt lệnh (chậm + coupling). Thay vào đó nó SUBSCRIBE topic "stock.enriched"
 * và giữ LOCAL CACHE giá/rủi ro mới nhất.
 *
 * -> Pre-trade risk check chỉ là 1 lần đọc ConcurrentHashMap: micro-giây.
 * -> pricing-risk-service chết, order-service vẫn hoạt động (dữ liệu hơi cũ - trade-off).
 */
```

So sánh ba cách lấy dữ liệu giá:

| Cách | Độ trễ | pricing chậm 100ms | pricing chết | Bộ nhớ |
|---|---|---|---|---|
| REST đồng bộ | 1-5ms | order chậm 100ms | **order chết** | 0 |
| REST + cache có TTL | 0 hoặc 1-5ms | thỉnh thoảng chậm | order chết khi cache hết hạn | O(symbol) |
| **Kafka + local view** | **~0** | **không ảnh hưởng** | **order vẫn chạy, dữ liệu cũ dần** | O(symbol) |

🔑 Lựa chọn thứ ba **bắt buộc đi kèm** staleness check — nếu không, "dữ liệu cũ dần" biến thành "khách đặt lệnh theo giá 10 phút trước". Cặp *cache + staleness check* phải đi cùng nhau; tách ra là nguy hiểm.

💡 Bộ nhớ không phình: `cache.put` **ghi đè** theo key, nên kích thước là O(số mã) chứ không phải O(số tick). 1000 mã × ~200 byte ≈ 200KB — không đáng kể so với heap 1Gi.

### 3.2. Kafka làm xương sống, không phải hàng đợi RPC

Bốn service nối với nhau bằng bốn topic:

| Topic | Producer | Consumer | Serialization | `auto.offset.reset` |
|---|---|---|---|---|
| `stock.ticks` | market-gateway | pricing-risk-service | **Avro** | `latest` |
| `stock.enriched` | pricing-risk-service | order-service | JSON | `latest` |
| `stock.orders` | order-service (qua outbox) | matching-engine | JSON | **`earliest`** |
| `stock.executions` | matching-engine (qua outbox) | order-service | JSON | **`earliest`** |

🔑 **Sự khác biệt `latest` vs `earliest` là một quyết định nghiệp vụ, không phải mặc định.**

```java
// pricing-risk-service/config/KafkaConfig.java:57
props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
```
```java
// matching-engine/config/KafkaConfig.java:40-41
// KHÁC với cache enriched (latest): LỆNH không được bỏ sót -> earliest
props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
```

Với dữ liệu giá: consumer group mới khởi động mà đọc từ đầu topic sẽ xử lý hàng triệu tick cũ — vô nghĩa (giá 3 ngày trước không giúp gì cho EMA hiện tại) và còn gây lag giả. `latest` là đúng.
Với lệnh: bỏ sót một lệnh là mất tiền của khách. `earliest` là bắt buộc.

### 3.3. Database per service — nhưng chung một instance

```sql
-- order-service/src/main/resources/db/migration/V1__orders_and_outbox.sql
-- Schema riêng "ordersvc": nhiều service dùng chung 1 Postgres instance (demo)
-- nhưng KHÔNG đụng bảng của nhau, mỗi service 1 flyway_schema_history riêng.
```

```yaml
# order-service/application.yml
spring:
  flyway:
    schemas: ordersvc
    default-schema: ordersvc
```
```yaml
# matching-engine/application.yml
spring:
  flyway:
    schemas: matchsvc
    default-schema: matchsvc
```

| | Database riêng (lý tưởng) | **Schema riêng, instance chung (repo này)** | Bảng chung (chống chỉ định) |
|---|---|---|---|
| Cô lập logic | Hoàn toàn | **Hoàn toàn** | Không |
| Cô lập tài nguyên | Hoàn toàn | **Không** — chung connection, chung disk, chung WAL | Không |
| Chi phí vận hành | 3 instance | **1 instance** | 1 instance |
| Migration độc lập | Có | **Có** (mỗi schema một `flyway_schema_history`) | Không |
| Nguy cơ | — | Một service làm nghẽn DB ⇒ ảnh hưởng service kia | Thay đổi schema phá service khác |

Đây là điểm dừng hợp lý cho demo và cho staging. Prod tách thành RDS instance riêng là bước nâng cấp tự nhiên, **không cần đổi một dòng mã nào** — chỉ đổi `DB_HOST` trong ConfigMap, vì mỗi service đã đọc biến môi trường riêng.

---

## 4. Vấn đề được giải quyết

### 4.1. Nghẽn cổ chai dây chuyền (cascading bottleneck)

**Kịch bản monolith:** phiên ATC, thị trường sụp đổ, hàng trăm nghìn tick/giây đổ về. `RiskCalculator` chiếm 100% CPU, JVM vào Full GC 2 giây. Trong một process duy nhất, **luồng nhận lệnh của khách bị block cứng** — khách không cắt lỗ được, không đặt mua được. Hậu quả: khiếu nại, kiện tụng, phạt từ cơ quan quản lý.

**Kịch bản hiện tại:** `pricing-risk-service` nghẽn ⇒ lag `stock.ticks` tăng ⇒ KEDA scale lên 4 pod ⇒ `order-service` **hoàn toàn không biết chuyện đó đang xảy ra**, vẫn phục vụ lệnh bình thường với giá trong cache. Nếu nghẽn kéo dài quá 10 giây, staleness check bắt đầu từ chối lệnh — một sự suy giảm *có kiểm soát và giải thích được*, thay vì treo cứng.

### 4.2. Độc lập vòng đời triển khai

Đội quant chỉnh hệ số `EMA_ALPHA` từ `0.1` xuống `0.05`: sửa một hằng số trong `RiskCalculator.java:31`, build, rolling update **chỉ pod pricing**. `order-service` không restart, không mất một lệnh nào. Với monolith, cùng thay đổi đó đòi hỏi restart toàn hệ thống giữa phiên giao dịch.

### 4.3. Cô lập lỗi

Bảng kiểm chứng (có thể tự thử bằng `docker compose stop <service>`):

| Tắt service | Hậu quả |
|---|---|
| `market-gateway` | Không có tick mới. Sau 10s, mọi lệnh bị từ chối `NO_FRESH_MARKET_DATA`. Lệnh đang trong saga **vẫn hoàn tất**. |
| `pricing-risk-service` | Như trên. Snapshot đã ghi vẫn truy vấn được qua `/history`. |
| `order-service` | Không nhận lệnh mới. Lệnh đã trong `stock.orders` **vẫn được matching khớp**; execution report nằm chờ trong `stock.executions`, được xử lý khi order-service trở lại. |
| `matching-engine` | Lệnh vào `stock.orders` và **nằm chờ**, không mất. Lệnh ở trạng thái `ACCEPTED`. Alert `SagaStuckOrders` kêu sau 2 phút. |
| `redis` | Hệ **vẫn đúng**, chỉ chậm hơn. `management.health.redis.enabled: false` để readiness không fail. |
| `postgres` | order/matching ngừng nhận việc mới (CircuitBreaker mở → `SYSTEM_UNAVAILABLE`). pricing vẫn tính toán và vẫn trả API realtime. |
| `kafka` | Toàn hệ ngừng luân chuyển. **Không mất dữ liệu** — outbox giữ event trong DB, relay gửi lại khi Kafka trở lại. |
| `zipkin` | Không ảnh hưởng nghiệp vụ. |

🔑 Dòng cuối cùng là giá trị thực của Outbox: **Kafka chết không làm mất lệnh**, chỉ làm trễ.

---

## 5. Đánh đổi

### 5.1. Ưu điểm

1. **Co giãn độc lập.** KEDA scale pricing theo lag `stock.ticks`, matching theo lag `stock.orders`, hai trigger khác nhau với ngưỡng khác nhau (1000 vs 500 — "một lệnh chờ khớp nhạy cảm hơn một tick giá trễ").
2. **Cô lập lỗi** — bảng mục 4.3.
3. **Độc lập triển khai** — mục 4.2.
4. **Đọc giá gần như tức thì** nhờ CQRS local view.
5. **Mỗi service có cấu hình riêng phù hợp bản chất của nó** — pool size, heap, probe, shutdown timeout, sampling rate.

### 5.2. Nhược điểm — nói thẳng

1. **Độ trễ end-to-end.** Gọi hàm in-process < 1µs. Ở đây: `Client → REST → DB → Outbox(≤500ms) → Kafka → Matching → DB → Outbox(≤500ms) → Kafka → Order`. Tổng saga **~0,5-1,1 giây** (bảng chi tiết ở [chương 00](./00-duong-di-cua-mot-lenh-end-to-end.md) mục 8). Hoàn toàn không dùng được cho HFT.

2. **Eventual consistency.** Giá ở `order-service` luôn trễ 1-5ms so với thực tế, và có thể trễ **vô hạn** nếu luồng đứt. Bắt buộc có staleness check — không phải tuỳ chọn.

3. **Chi phí vận hành tăng vọt.** Thay vì một file JAR: 4 service + Kafka + Postgres + Schema Registry + Redis + Prometheus + Grafana + Zipkin = **11 container** ở local, cộng Kustomize/Helm/KEDA/NetworkPolicy/ESO/cert-manager ở cluster.

4. **Gỡ lỗi khó.** Một giao dịch qua 4 service. Không có distributed tracing thì về cơ bản không điều tra được. Đó là lý do mọi service đều nhúng traceId vào pattern log.

5. **Tính đúng đắn phải tự xây.** Monolith có `@Transactional` bao cả luồng. Ở đây ta phải tự dựng Outbox + Idempotent Consumer + Saga + State Machine + Optimistic Lock — **5 cơ chế** chỉ để đạt được thứ mà một transaction local cho không.

6. **Ràng buộc liên-file không ai cưỡng chế.** `maxReplicaCount` của KEDA phải khớp `KAFKA_PARTITIONS / setConcurrency(3)` — ba con số ở ba file khác nhau. Repo đã bịt cặp này bằng CI (`.github/workflows/k8s-validate.yml:59-78`), nhưng còn hai cặp nữa vẫn dựa vào trí nhớ (xem [chương 14](./14-han-che-da-biet-va-lo-trinh.md)).

### 5.3. Cân đo công bằng

| Tiêu chí | Monolith | Kiến trúc này |
|---|---|---|
| Latency tốt nhất | **< 1ms** | ~10-30ms (không kể polling) |
| Throughput tick | Giới hạn bởi 1 process | Scale ngang tới partition/3 |
| Mất lệnh khi crash | Transaction lo | Outbox lo (phải tự xây) |
| Thời gian lên tính năng mới | **Nhanh** | Chậm hơn (phải nghĩ về contract, idempotency) |
| Số người vận hành được | 1 | 2-3 |
| Phù hợp | HFT, MVP, đội < 5 người | Retail brokerage, đội nhiều nhóm |

---

## 6. Phương án thay thế & khi nào nên chọn

### 6.1. Monolith module hoá (modular monolith)

Một process, nhiều module Java với ranh giới rõ ràng, giao tiếp qua in-process event bus.

**Chọn khi:** đội < 5 người, chưa biết ranh giới nghiệp vụ nằm ở đâu, cần ra sản phẩm nhanh.
**Ưu:** latency < 1ms, một transaction bao cả luồng, một lần deploy, một dashboard.
**Nhược:** không scale riêng được; một Full GC đóng băng mọi thứ; một bug tràn heap giết cả hệ.
**Đường nâng cấp:** nếu module hoá tốt (không gọi chéo trực tiếp, chỉ qua event bus), tách thành microservice sau này là cơ học. Đây thường là lộ trình đúng.

### 6.2. HFT / Market Making — single process trên bare metal

**Chọn khi:** yêu cầu latency dưới micro-giây.
**Kiến trúc:** C++20/Rust, hoặc Java zero-allocation. **LMAX Disruptor** (ring buffer lock-free, mechanical sympathy), shared memory / HugePages, kernel bypass (DPDK/Solarflare), CPU pinning, triệt tiêu GC hoàn toàn.
**Vì sao kiến trúc hiện tại không dùng được:** chỉ riêng một lần serialize JSON + một network hop Kafka đã tốn hơn **toàn bộ ngân sách latency** của HFT. Thêm nữa, `BigDecimal` cấp phát object trên heap — điều cấm kỵ.

### 6.3. Orchestration Saga thay cho Choreography

Hệ hiện tại dùng **choreography**: mỗi service tự phản ứng với event, không ai điều phối.

| | Choreography (repo này) | Orchestration |
|---|---|---|
| Logic saga nằm ở | Phân tán trong các service | Tập trung ở orchestrator |
| Thêm bước mới | Sửa nhiều service | Sửa orchestrator |
| Nhìn thấy toàn cảnh | Khó — phải ghép từ log/trace | Dễ — orchestrator biết hết |
| Điểm hỏng tập trung | Không | Có |
| Phù hợp | 2-4 bước | **> 5 bước**, có bồi hoàn phức tạp |

Với vòng `order → matching → order` chỉ 2 bước, choreography là lựa chọn đúng. Nếu mở rộng thành `order → risk-limit → funding → matching → settlement → clearing`, nên chuyển sang orchestration (Temporal, Camunda, hoặc một `SagaOrchestrator` tự viết).

### 6.4. Event Sourcing

Repo lưu **trạng thái hiện tại** (`orders.status = FILLED`). Event sourcing lưu **chuỗi sự kiện** và tái dựng trạng thái bằng cách phát lại.

**Chọn khi:** cần audit trail tuyệt đối, cần "trạng thái tại thời điểm T", cần nhiều projection khác nhau từ cùng một nguồn.
**Nhược:** phức tạp lớn; cần snapshot để không phát lại hàng triệu event; schema evolution của event cũ là ác mộng.
**Ghi chú:** repo đã có **một nửa** lợi ích rồi — `outbox_event` là một event log bất biến, và `orders` giữ cả lệnh REJECTED. Đủ cho audit mà không gánh chi phí event sourcing đầy đủ.

---

## 7. Núm vặn cấu hình

| Tham số | File | Mặc định | Ý nghĩa |
|---|---|---|---|
| `feed.simulated.enabled` | `market-gateway/application.yml` | `true` | Tắt khi cắm feed thật |
| `spring.datasource.hikari.maximum-pool-size` | mỗi service | 10 (pricing) / 20 (order) | Phải khớp bản chất tải của service |
| `spring.jpa.open-in-view` | order, pricing, matching | `false` | 🔑 Tắt OSIV — xem [chương 13](./13-persistence-flyway-va-mo-hinh-du-lieu.md) |
| `server.shutdown` | order, matching | `graceful` | ⚠️ gateway và pricing **thiếu** (Gap #20) |
| `spring.lifecycle.timeout-per-shutdown-phase` | order, matching | `30s` | Phải ≤ `terminationGracePeriodSeconds` − `preStop` |
| `KAFKA_PARTITIONS` | `k8s/base/config/trading.env` | 6 (dev 3 / prod 12) | Quyết định trần scale |
| `TRACING_PROBABILITY` | như trên | 0.1 (dev 1.0 / prod 0.05) | Sampling rate |

---

## 8. Cách kiểm chứng

```bash
# 1. Bốn service đều sống và sẵn sàng?
for p in 8081 8082 8083 8084; do
  echo -n "$p: "; curl -s http://localhost:$p/actuator/health | jq -r .status
done

# 2. Chứng minh KHÔNG có REST liên service: bắt mọi kết nối ra của order-service
docker compose exec order-service sh -c 'cat /proc/net/tcp' | head
#    hoặc trong cluster:
kubectl -n trading exec deploy/order-service -- sh -c 'netstat -tn 2>/dev/null | grep ESTABLISHED'
#    -> chỉ thấy kafka:9092, postgres:5432, redis:6379, zipkin:9411

# 3. Chứng minh cô lập lỗi: tắt pricing, order vẫn trả lời
docker compose stop pricing-risk-service
curl -s http://localhost:8083/actuator/health | jq -r .status      # vẫn UP
sleep 11
# đặt lệnh -> 422 NO_FRESH_MARKET_DATA (suy giảm có kiểm soát, không sập)
docker compose start pricing-risk-service

# 4. Chứng minh cache là local, không phải gọi mạng: đo latency đặt lệnh
curl -s -o /dev/null -w 'total=%{time_total}s\n' -X POST \
  http://localhost:8083/api/v1/orders -H 'X-API-KEY: demo-key' \
  -H 'Content-Type: application/json' \
  -d '{"clientOrderId":"'$RANDOM'","symbol":"HPG","side":"BUY",
       "price":28500,"quantity":100,"accountId":"ACC001"}'
#    -> thường 5-15ms, trong đó phần lớn là INSERT DB, không phải đọc giá
```

---

## 9. Hạn chế đã biết của kiến trúc này

| # | Loại | Vấn đề | Chi tiết |
|:--:|---|---|---|
| — | Thiết kế | `common-contracts` tạo build-time coupling | Mục 1.3. Chấp nhận có ý thức trong monorepo một đội. |
| — | Thiết kế | Một Postgres instance cho 3 schema | Mục 3.3. Cô lập logic đủ, cô lập tài nguyên chưa. |
| 20 | Gap | `market-gateway` và `pricing-risk-service` thiếu `server.shutdown: graceful` | Tomcat đóng ngay khi nhận SIGTERM ⇒ request REST đang bay bị cắt, vô hiệu hoá `preStop: sleep 5`. |
| 22 | Gap | Vùng phủ của môi trường dev | dev có 3 partition / 1 pod / concurrency 3 ⇒ mỗi consumer thread ôm đúng 1 partition. Mọi bug phụ thuộc việc một thread xử lý **xen kẽ hai partition** (batch 1000 bản ghi trộn hai nhóm symbol) **không bao giờ lộ ra ở dev**. |
| 16 | Gap | `.replicas(1)` + 1 broker | `acks=all` không có bản sao nào để chờ ⇒ độ bền của outbox producer chỉ là danh nghĩa. |

Danh sách đầy đủ 22 vấn đề: [chương 14](./14-han-che-da-biet-va-lo-trinh.md).

---

## 10. Câu hỏi phỏng vấn

**Q: Hãy biện hộ cho việc chia đúng 4 service. Nếu tôi là CTO và bảo bạn gộp lại thành 2 thì sao?**

A: Tôi sẽ hỏi gộp cặp nào. `market-gateway` + `pricing-risk-service`: không gộp được vì chúng scale ngược chiều — gateway với feed FIX không scale ngang được (một phiên có trạng thái), pricing phải scale tới 4 pod theo lag. `order-service` + `matching-engine`: gộp được về mặt kỹ thuật, nhưng mất hai tầng kiểm tra độc lập và đưa client vào cùng process với nơi thi hành luật sàn — một vấn đề ranh giới tin cậy, không phải vấn đề kỹ thuật. Nếu CTO vẫn muốn gộp vì chi phí vận hành, tôi sẽ đề xuất gộp thành **modular monolith có ranh giới module rõ ràng** thay vì gộp nửa vời — và giữ nguyên Outbox ở ranh giới DB↔Kafka, vì vấn đề đó không biến mất khi gộp process.

**Q: `common-contracts` dùng chung DTO giữa các microservice — đó chẳng phải anti-pattern sao?**

A: Có, trong bối cảnh nhiều đội nhiều ngôn ngữ. Ở đây là monorepo một đội, nên compiler bắt lỗi contract drift là lợi ích thực, còn chi phí rebuild bốn module là nhỏ. Quan trọng hơn: lộ trình thoát đã được vạch và đã bắt đầu đi — `stock.ticks` chuyển sang Avro + Schema Registry, nơi *registry* cưỡng chế tương thích BACKWARD thay cho compiler. Khi có đội thứ hai hoặc service viết bằng ngôn ngữ khác, chuyển nốt ba topic còn lại. Tôi phân biệt rõ hai thứ trong repo: `common-contracts` chia sẻ **cấu trúc dữ liệu** (coupling cao, chấp nhận tạm), còn `outbox-support` chia sẻ **hành vi** qua 2 interface (coupling tối thiểu, đúng mẫu mực).

**Q: Order-service cache giá trong RAM. Nếu cache sai thì sao?**

A: "Sai" ở đây có hai nghĩa. *Cũ* — xử lý bằng staleness check 10 giây, và đó là lý do bắt buộc phải có nó. *Lệch giữa các pod* — hoàn toàn có thể: 3 pod order-service mỗi pod có cache riêng, pod này nhận tick trước pod kia vài ms. Điều này **chấp nhận được** vì pre-trade check là bộ lọc sơ bộ, không phải nguồn sự thật; `matching-engine` kiểm lại bằng `refPrice` đóng băng theo lệnh. Cái **không** chấp nhận được sẽ là dùng cache này làm cơ sở tính tiền hay tính vị thế — và hệ thống không làm thế.

**Q: Vì sao không dùng Kafka Streams hoặc ksqlDB cho phần pricing?**

A: Kafka Streams hợp khi cần join nhiều stream, windowed aggregation phức tạp, hoặc state store có fault tolerance tự động (changelog topic). Ở đây phép tính là **incremental thuần tuý trên một stream** — EMA và sliding-window variance, $O(1)$ mỗi tick, state chỉ vài trăm byte mỗi mã. Dùng Kafka Streams sẽ thêm RocksDB, thêm changelog topic, thêm một mô hình lập trình mới, để đổi lấy thứ mà 60 dòng Java đã làm nhanh hơn. Điểm đáng cân nhắc lại: state hiện tại **mất khi pod restart** và phải hâm nóng lại 120 tick — Kafka Streams sẽ khôi phục state từ changelog. Với WINDOW = 120 tick ≈ 24 giây ở feed này, hâm nóng lại rẻ hơn nhiều so với chi phí vận hành RocksDB.

---

## 11. Bảng tổng kết

| Khía cạnh | Quyết định | Lý do cốt lõi | Cái giá |
|---|---|---|---|
| Số service | 4 | Bốn hồ sơ tài nguyên + nhịp thay đổi khác nhau | 11 container, 4 dashboard |
| Giao tiếp liên service | **Chỉ Kafka** | Tách thời gian (temporal decoupling); service chết không kéo nhau | Eventual consistency |
| Đọc giá khi đặt lệnh | CQRS local materialized view | ~0ms; pricing chết order vẫn chạy | Bắt buộc có staleness check |
| Dữ liệu | Schema per service, một instance | Cô lập logic đủ dùng, rẻ | Không cô lập tài nguyên |
| DTO dùng chung | `common-contracts` | Compiler bắt contract drift | Build-time coupling |
| Relay dùng chung | `outbox-support` (interface) | Chia sẻ hành vi không chia sẻ cấu trúc | ~0 |
| Saga | Choreography | Chỉ 2 bước; không cần orchestrator | Khó nhìn toàn cảnh |
| Lưu trạng thái | Current state (không event sourcing) | Đủ cho audit nhờ `orders` giữ cả REJECTED + `outbox_event` bất biến | Không tái dựng được trạng thái tại thời điểm T |
| `auto.offset.reset` | `latest` cho giá, `earliest` cho lệnh | Giá cũ vô nghĩa; lệnh sót là mất tiền | — |

> **Đọc tiếp:** [Chương 02 — Kafka Messaging & Schema Registry](./02-kafka-messaging-va-schema-registry.md) để hiểu tầng vận chuyển mà toàn bộ kiến trúc này dựa vào.
