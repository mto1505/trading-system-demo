# Chương 05 — Distributed Transactions, Transactional Outbox & CDC

> **Câu hỏi trung tâm:** làm sao ghi vào database **và** publish lên Kafka một cách nguyên tử, khi hai hệ thống đó không chia sẻ transaction?
>
> Đây là bài toán khó nhất của microservices, và chương này là **phần quan trọng nhất** của toàn bộ repo.
>
> **Tài liệu đối chiếu:** `DESIGN-NOTES.md` mục 9, `DEEP-DIVE.md` mục 8 & 9.5-9.6, `cdc/README.md`.

---

## 1. Bài toán: thảm hoạ dual-write

### 1.1. Cách viết "tự nhiên" và vì sao nó sai

```java
// ❌ DUAL-WRITE — sai, nhưng là thứ ai cũng viết lần đầu
@Transactional
public void placeOrder(OrderRequest req) {
    orderRepository.save(order);                       // ghi PostgreSQL
    kafkaTemplate.send("stock.orders", result);        // publish Kafka
}
```

`@Transactional` ở đây **chỉ bao PostgreSQL**. Kafka hoàn toàn nằm ngoài. Bốn kịch bản hỏng:

| # | Thời điểm sự cố | Trạng thái để lại | Hậu quả nghiệp vụ |
|:--:|---|---|---|
| 1 | `save` xong, `send` thất bại (hoặc pod crash) | Lệnh có trong DB, Kafka không có event | 🔴 **Lệnh treo `ACCEPTED` vĩnh viễn.** matching-engine không bao giờ biết lệnh tồn tại. Tiền của khách bị giữ cho một giao dịch không bao giờ kết thúc. |
| 2 | `send` thành công, rồi transaction rollback | Kafka có event, DB không có lệnh | 🔴 **Matching khớp một lệnh không tồn tại.** Có `execution` mà không có `order` tương ứng. Sổ sách lệch. |
| 3 | `send` timeout nhưng Kafka **đã** nhận | Cả hai đều có, nhưng ứng dụng tưởng thất bại ⇒ retry | 🔴 **Lệnh trùng.** Khách mua hai lần. |
| 4 | Kafka tạm thời chết | `send` ném exception ⇒ transaction rollback | 🟡 **Lệnh bị từ chối dù DB hoàn toàn khoẻ.** Khả dụng của hệ bị ràng buộc vào khả dụng của Kafka. |

🔑 **Điểm mấu chốt của kịch bản 1 và 2: không có cơ chế nào tự chữa.** Dữ liệu ở hai nơi mâu thuẫn và sẽ mâu thuẫn mãi mãi. Phát hiện ra thì phải đối soát thủ công.

### 1.2. Vì sao không dùng XA / 2PC

Hai-giai-đoạn-commit (Two-Phase Commit) là lời giải "sách giáo khoa" cho transaction phân tán:

```
Coordinator → [prepare] → DB,  Kafka
            ← [ready]   ← DB,  Kafka
Coordinator → [commit]  → DB,  Kafka
```

Ba lý do nó không dùng được ở đây:

1. **Kafka không hỗ trợ XA.** Kết thúc tranh luận. Kafka có transaction *nội bộ* (`transactional.id`, exactly-once semantics) nhưng đó không phải XA và không bao gồm được một resource bên ngoài như PostgreSQL.

2. **2PC là blocking protocol.** Nếu coordinator chết giữa `prepare` và `commit`, mọi participant giữ khoá và **chờ vô thời hạn**. Trong một hệ thống giao dịch, điều đó nghĩa là bảng `orders` bị khoá giữa phiên.

3. **Throughput sụp.** Mỗi transaction cần ít nhất 4 lần đi-về mạng cộng với ghi log bền vững của coordinator. So với một local commit, cái giá là hàng chục lần.

4. (Thực tế) **Hầu như không ai dùng nữa.** XA trong Java (JTA + Atomikos/Narayana) tồn tại nhưng hệ sinh thái cloud-native đã bỏ hẳn. Nói "dùng XA" trong một buổi phỏng vấn kiến trúc hiện đại là tín hiệu xấu.

---

## 2. Lời giải: Transactional Outbox

### 2.1. 🔑 Mẹo cốt lõi — biến bài toán phân tán thành bài toán cục bộ

> **Không ghi hai hệ thống nữa. Ghi *một* hệ thống, hai bảng, trong một local ACID transaction.**

```java
// order-service/src/main/java/com/trading/order/entity/OutboxEventEntity.java:11-24
/**
 * TRANSACTIONAL OUTBOX - lời giải cho distributed transaction giữa DB và Kafka.
 *
 * Vấn đề dual-write: ghi DB xong publish Kafka fail (hoặc ngược lại)
 * -> hai hệ thống lệch nhau vĩnh viễn. XA/2PC không dùng được:
 * Kafka không hỗ trợ XA, và 2PC blocking giết throughput.
 *
 * Giải pháp: KHÔNG ghi 2 hệ thống. Bảng orders + bảng outbox_event được insert
 * trong CÙNG MỘT local ACID transaction của Postgres (nguyên tử tuyệt đối).
 * OutboxRelay sau đó đọc outbox -> publish Kafka -> đánh dấu published_at.
 * Crash ở bất kỳ điểm nào: event chưa mark thì relay gửi lại -> AT-LEAST-ONCE,
 * và idempotent consumer ở matching-engine biến nó thành effectively-once.
 */
```

```java
// order-service/src/main/java/com/trading/order/service/OrderPersistence.java:32-66
/**
 * TRÁI TIM CỦA OUTBOX PATTERN: 2 INSERT, 1 transaction, 1 database.
 * Hoặc cả lệnh lẫn event cùng tồn tại, hoặc không cái nào - không có
 * trạng thái "lệnh có mà event mất" hay ngược lại.
 */
@Transactional
public void saveWithOutbox(OrderRequest req, OrderResult result) {
    OrderEntity order = new OrderEntity();
    order.setOrderId(result.orderId());
    order.setClientOrderId(result.clientOrderId());
    order.setStatus(result.status());
    order.setReason(result.reason());
    order.setSymbol(result.symbol());
    order.setSide(result.side());
    order.setPrice(result.price());
    order.setQuantity(result.quantity());
    order.setRefPrice(result.refPrice());
    order.setRiskLevel(result.riskLevel());
    order.setAccountId(req.accountId());
    order.setCreatedAt(Instant.now());
    orderRepository.save(order);

    // Chỉ lệnh ACCEPTED mới sinh event cho hệ khớp lệnh
    if ("ACCEPTED".equals(result.status())) {
        OutboxEventEntity event = new OutboxEventEntity();
        event.setAggregateType("ORDER");
        event.setAggregateId(result.orderId());
        event.setEventType("ORDER_ACCEPTED");
        event.setTopic("stock.orders");
        event.setMsgKey(result.symbol());   // key = symbol giữ thứ tự theo mã
        event.setPayload(toJson(result));
        event.setCreatedAt(Instant.now());
        outboxRepository.save(event);
    }
}
```

Bốn kịch bản hỏng của dual-write được giải quyết thế nào:

| # | Kịch bản dual-write | Với Outbox |
|:--:|---|---|
| 1 | Lệnh có, event mất | **Không thể xảy ra** — hai INSERT cùng commit hoặc cùng rollback |
| 2 | Event có, lệnh mất | **Không thể xảy ra** — cùng lý do |
| 3 | Lệnh trùng do retry | Relay có thể gửi lại ⇒ at-least-once ⇒ **idempotent consumer lo** |
| 4 | Kafka chết ⇒ từ chối lệnh | **Lệnh vẫn được nhận.** Event nằm trong DB, relay gửi khi Kafka trở lại |

🔑 Kịch bản 4 là lợi ích ít được nói tới nhưng lớn nhất về mặt vận hành: **khả dụng của luồng nhận lệnh không còn ràng buộc vào khả dụng của Kafka.**

### 2.2. Lược đồ bảng — và vì sao từng cột tồn tại

```sql
-- order-service/src/main/resources/db/migration/V1__orders_and_outbox.sql
CREATE TABLE outbox_event (
    id             BIGSERIAL PRIMARY KEY,
    aggregate_type VARCHAR(30) NOT NULL,
    aggregate_id   VARCHAR(64) NOT NULL,
    event_type     VARCHAR(30) NOT NULL,
    topic          VARCHAR(60) NOT NULL,
    msg_key        VARCHAR(64) NOT NULL,
    payload        TEXT        NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL,
    published_at   TIMESTAMPTZ
);

-- Partial index: relay chỉ quét event chưa publish -> index nhỏ, quét nhanh
CREATE INDEX idx_outbox_unpublished ON outbox_event (id) WHERE published_at IS NULL;
```

| Cột | Ai đọc | Vì sao cần |
|---|---|---|
| `id BIGSERIAL` | Relay (`ORDER BY id`) | 🔑 Thứ tự publish. Tăng đơn điệu theo thứ tự insert. |
| `aggregate_type` | Con người, Debezium | `ORDER` — phân loại khi một bảng chở nhiều loại aggregate |
| `aggregate_id` | Debezium `EventRouter` | Dùng làm khoá mặc định nếu không chỉ định `msg_key` |
| `event_type` | Consumer (nếu cần phân nhánh) | `ORDER_ACCEPTED` |
| **`topic`** | 🔑 Relay | **Đích đến nằm trong dữ liệu, không nằm trong mã relay.** Nhờ vậy `OutboxRelay` hoàn toàn vô tri — một bảng chở được nhiều topic. |
| **`msg_key`** | 🔑 Relay | **Khoá Kafka được quyết định lúc ghi DB**, trong cùng transaction với dữ liệu nghiệp vụ |
| `payload TEXT` | Relay (gửi nguyên văn) | JSON đã serialize sẵn |
| `created_at` | Con người, job dọn dẹp | Đo độ trễ relay: `published_at − created_at` |
| **`published_at`** | 🔑 Relay | `NULL` = chưa gửi. Đây là toàn bộ máy trạng thái của outbox. |

🔑 **Partial index là chi tiết đáng học nhất về mặt hiệu năng.**

Bảng `outbox_event` chỉ tăng — sau một tháng có thể hàng triệu row. Nhưng số row `published_at IS NULL` tại mọi thời điểm chỉ vài chục (relay dọn mỗi 500ms). Một index thường trên `(id)` sẽ bao cả triệu row đã publish: tốn RAM trong shared_buffers, chậm khi insert, và vô dụng cho query của relay.

```sql
CREATE INDEX idx_outbox_unpublished ON outbox_event (id) WHERE published_at IS NULL;
```

Index này **chỉ chứa** các row chưa publish. Nó luôn nhỏ bất kể bảng lớn tới đâu, và Postgres planner dùng nó cho đúng query của relay (`WHERE published_at IS NULL ORDER BY id`). Khi một row được đánh dấu published, nó **tự động bị xoá khỏi index**.

💡 Chi phí: mỗi `UPDATE SET published_at = now()` phải cập nhật index (xoá entry). Nhưng đó chính là điều ta muốn — index luôn gọn.

### 2.3. OutboxRelay — một relay dùng chung cho mọi service

```java
// outbox-support/src/main/java/com/trading/outbox/OutboxRelay.java:19-28
/**
 * POLLING PUBLISHER dùng chung cho mọi service có outbox.
 *
 * @ConditionalOnProperty outbox.relay.enabled (mặc định true):
 * đặt false khi chuyển sang Debezium CDC - CDC đọc thẳng WAL của Postgres,
 * lúc đó relay polling phải TẮT nếu không hai đường sẽ publish trùng.
 * Cùng một bảng outbox phục vụ được cả hai cơ chế, đổi bằng 1 property.
 */
@Component
@ConditionalOnProperty(name = "outbox.relay.enabled", havingValue = "true", matchIfMissing = true)
public class OutboxRelay {
```

```java
// outbox-support/src/main/java/com/trading/outbox/OutboxRelay.java:50-71
@Scheduled(fixedDelayString = "${outbox.relay.interval-ms:500}")
@Transactional
public void relay() {
    List<? extends OutboxRecord> batch = store.lockUnpublishedBatch(batchSize);
    for (OutboxRecord event : batch) {
        try {
            // Gửi ĐỒNG BỘ có chủ đích: chắc chắn Kafka nhận (acks=all) trước khi
            // mark published, và giữ đúng thứ tự id tăng dần.
            kafkaTemplate.send(event.getTopic(), event.getMsgKey(), event.getPayload())
                    .get(5, TimeUnit.SECONDS);
            event.setPublishedAt(Instant.now());
            published.increment();
        } catch (Exception ex) {
            failed.increment();
            log.error("Publish outbox id={} that bai, rollback ca batch de giu thu tu",
                    event.getId(), ex);
            // Rollback: không mark published cho event nào trong batch.
            // Event đã lên Kafka có thể gửi lại = at-least-once, consumer dedupe lo.
            throw new IllegalStateException("Outbox publish failed", ex);
        }
    }
}
```

**Năm quyết định thiết kế trong 20 dòng này:**

#### (a) `@Transactional` bao cả vòng lặp, và dirty checking

`event.setPublishedAt(...)` **không** gọi `save()`. Entity đang nằm trong persistence context (được nạp bởi `lockUnpublishedBatch`), nên Hibernate phát hiện thay đổi và flush `UPDATE` khi transaction commit. Nghĩa là `published_at` chỉ được ghi khi **cả batch** thành công.

💡 Đây cũng là lý do `lockUnpublishedBatch` phải trả về **entity được quản lý** (managed entity), không phải DTO projection. Nếu dùng projection, `setPublishedAt` sẽ không có tác dụng gì và relay sẽ gửi lại vô hạn — một bug im lặng rất khó tìm.

#### (b) Gửi **đồng bộ** `.get(5, TimeUnit.SECONDS)`

Đối lập hoàn toàn với `TickProducer` (bất đồng bộ, fire-and-forget).

| | Nếu gửi bất đồng bộ rồi mark ngay | **Gửi đồng bộ** (lựa chọn hiện tại) |
|---|---|---|
| Thông lượng | Cao hơn | Thấp hơn |
| Rủi ro | "Đã mark published mà Kafka chưa nhận" ⇒ **mất lệnh vĩnh viễn** | Chỉ có nguy cơ gửi trùng |
| Thứ tự | Có thể đảo | Giữ đúng `id` tăng dần |

🔑 **Mất lệnh tệ hơn gửi trùng rất nhiều.** Gửi trùng thì consumer dedupe lo; mất thì không gì cứu được. Đây là nguyên tắc chi phối toàn bộ thiết kế này.

Timeout 5 giây: đủ cho `acks=all` ở tải bình thường; nếu vượt thì nhiều khả năng Kafka đang có vấn đề thật và nên rollback chứ không nên chờ tiếp.

#### (c) Rollback **cả batch** khi một event lỗi

Giữ thứ tự `id` tăng dần. Nếu bỏ qua event lỗi và đi tiếp, event thứ 51 có thể lên Kafka trước event thứ 50 — và với `key = symbol`, hai event cùng mã bị đảo thứ tự.

⚠️ **Doc #17 — quy mô của hành vi này ít được nói rõ.** Nếu event thứ 50 trong batch 100 thất bại, thì **49 event đã lên Kafka rồi sẽ được gửi lại** ở lần relay sau, không phải một. Hành vi là cố ý (ưu tiên thứ tự hơn số lần gửi trùng), nhưng cần hiểu đúng khi đọc metric: `outbox_published_total` có thể vượt xa số event thực tế.

#### (d) Hai counter cho quan sát

```java
// OutboxRelay.java:41-48
this.published = Counter.builder("outbox.published").register(registry);
this.failed = Counter.builder("outbox.publish.failed").register(registry);
```

`outbox_publish_failed_total` là cơ sở của một trong những alert quan trọng nhất:

```yaml
# monitoring/alert-rules.yml:38-45
- alert: OutboxPublishFailing
  expr: rate(outbox_publish_failed_total[5m]) > 0
  for: 1m
  labels: { severity: critical }
  annotations:
    summary: "OutboxRelay dang that bai khi publish"
```

💡 Đây là mẫu đúng mà `SnapshotStore.flushToDatabase()` **thiếu** (xem [chương 03](./03-pricing-realtime-risk-va-write-behind-cache.md)): mỗi cơ chế chịu lỗi phải đi kèm một cơ chế phát hiện.

#### (e) `@ConditionalOnProperty` — một property đổi cả cơ chế

```yaml
# order-service/application.yml  &  matching-engine/application.yml
outbox:
  relay:
    # true = polling relay (mac dinh). Dat false khi dung Debezium CDC,
    # neu khong hai duong se publish trung.
    enabled: ${OUTBOX_RELAY_ENABLED:true}
    interval-ms: 500
    batch-size: 100
```

🔑 Cùng một bảng `outbox_event` phục vụ được **cả hai** cơ chế vận chuyển. Chuyển từ polling sang CDC là đổi một biến môi trường, **không sửa một dòng mã nghiệp vụ nào**. Đây chính là giá trị kiến trúc thật của Outbox: nó **tách "ghi sự kiện" khỏi "cách chuyển sự kiện đi"**.

### 2.4. `FOR UPDATE SKIP LOCKED` — nhiều pod relay song song

```java
// order-service/src/main/java/com/trading/order/repository/OutboxEventRepository.java:12-23
/**
 * FOR UPDATE SKIP LOCKED: nhiều pod relay chạy song song, mỗi pod khoá
 * một lô event khác nhau, KHÔNG chờ nhau, KHÔNG gửi trùng lô của nhau.
 * (Trùng vẫn có thể xảy ra khi crash giữa chừng - at-least-once by design.)
 */
@Query(value = """
        SELECT * FROM ordersvc.outbox_event
        WHERE published_at IS NULL
        ORDER BY id
        LIMIT :n
        FOR UPDATE SKIP LOCKED
        """, nativeQuery = true)
List<OutboxEventEntity> lockUnpublishedBatch(@Param("n") int n);
```

Ba cách viết, ba kết quả hoàn toàn khác nhau khi chạy 3 pod:

| Cách viết | Hành vi |
|---|---|
| `SELECT ... LIMIT 100` (không khoá) | Cả 3 pod lấy **cùng 100 row** ⇒ mỗi event publish 3 lần. Vô nghĩa. |
| `SELECT ... FOR UPDATE` | Pod 1 khoá row 1-100; pod 2 và 3 **nằm chờ** tới khi pod 1 commit ⇒ relay bị tuần tự hoá, throughput bằng đúng 1 pod |
| **`SELECT ... FOR UPDATE SKIP LOCKED`** | Pod 1 lấy 1-100, pod 2 **bỏ qua** row bị khoá và lấy 101-200, pod 3 lấy 201-300 ⇒ **song song thật** |

🔑 `SKIP LOCKED` biến một bảng SQL thành **hàng đợi công việc đa tiêu thụ** mà không cần message broker. Đây là một trong những tính năng hữu dụng nhất và ít được biết nhất của PostgreSQL (có từ 9.5; Oracle và MySQL 8 cũng hỗ trợ).

**Nó vẫn không loại trừ gửi trùng.** Kịch bản:
1. Pod 1 khoá event #42, gửi lên Kafka thành công.
2. Pod 1 **crash trước khi commit** `published_at`.
3. Transaction rollback ⇒ khoá được giải phóng ⇒ `published_at` vẫn `NULL`.
4. Pod 2 ở vòng tiếp theo lấy event #42 và gửi lại.

⇒ Event #42 lên Kafka **hai lần**. Đây là **at-least-once by design**, và là lý do bắt buộc phải có idempotent consumer ở đầu kia ([chương 06](./06-idempotency-va-consumer-deduplication.md)).

### 2.5. Producer của relay

```java
// order-service/src/main/java/com/trading/order/config/KafkaConfig.java:57-74
// ============ PRODUCER cho OutboxRelay ============
// Payload đã là JSON string (serialize lúc ghi outbox) -> StringSerializer.
// Lệnh không được mất/trùng do producer retry: acks=all + idempotence.
@Bean
public KafkaTemplate<String, String> outboxKafkaTemplate() {
    Map<String, Object> props = new HashMap<>();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
    var template = new KafkaTemplate<>(new DefaultKafkaProducerFactory<String, String>(props));
    template.setObservationEnabled(true);
    return template;
}
```

💡 **`StringSerializer` cho value, không phải `JsonSerializer`.** Payload đã được serialize thành JSON **lúc ghi outbox** (`OrderPersistence.toJson()`). Nếu dùng `JsonSerializer` ở đây, chuỗi JSON sẽ bị serialize **lần hai** thành một JSON string có escape (`"{\"orderId\":...}"`) và consumer sẽ không parse được. Đây là lỗi hay gặp khi mới cài outbox.

`template.setObservationEnabled(true)` — nối trace từ transaction ghi lệnh sang span publish Kafka, để Zipkin hiển thị được toàn bộ hành trình.

⚠️ **Gap #16:** mọi `TopicBuilder` trong repo khai `.replicas(1)` và cluster demo có 1 broker. Với một bản sao, `acks=all` ≡ `acks=1` — **không có replica nào để chờ**. Độ bền ở đây hiện chỉ là *danh nghĩa*. Để `acks=all` có ý nghĩa thật cần multi-broker + `min.insync.replicas=2`.

### 2.6. Module dùng chung: chia sẻ hành vi, không chia sẻ cấu trúc

```java
// outbox-support/src/main/java/com/trading/outbox/OutboxRecord.java:5-20
/**
 * PORT: relay chỉ cần biết ngần này về một outbox event.
 *
 * Vì sao KHÔNG dùng chung một @Entity JPA giữa các service?
 * Shared entity kéo theo ràng buộc schema, JPA config, và vòng đời migration
 * chung giữa các service - đúng loại coupling mà microservices muốn tránh.
 * Mỗi service tự sở hữu bảng outbox của mình (schema ordersvc / matchsvc),
 * chỉ chia sẻ HÀNH VI (relay) chứ không chia sẻ CẤU TRÚC DỮ LIỆU.
 */
public interface OutboxRecord {
    Long getId();
    String getTopic();
    String getMsgKey();
    String getPayload();
    void setPublishedAt(Instant publishedAt);
}
```

```java
// outbox-support/src/main/java/com/trading/outbox/OutboxStore.java:5-12
/**
 * ADAPTER do mỗi service tự cài đặt: khoá một lô event chưa publish.
 * Cài đặt chuẩn dùng SELECT ... FOR UPDATE SKIP LOCKED để nhiều pod relay
 * chạy song song mà không giẫm chân nhau.
 */
public interface OutboxStore {
    List<? extends OutboxRecord> lockUnpublishedBatch(int limit);
}
```

Mỗi service cài adapter của mình — 24 dòng, không có logic:

```java
// order-service/src/main/java/com/trading/order/service/JpaOutboxStore.java
/** ADAPTER nối bảng outbox của ordersvc vào relay dùng chung. */
@Component
public class JpaOutboxStore implements OutboxStore {

    private final OutboxEventRepository repository;

    public JpaOutboxStore(OutboxEventRepository repository) {
        this.repository = repository;
    }

    @Override
    public List<OutboxEventEntity> lockUnpublishedBatch(int limit) {
        return repository.lockUnpublishedBatch(limit);
    }
}
```

Và entity tự implement interface:

```java
// order-service/entity/OutboxEventEntity.java — có thêm aggregate_type
@Entity
@Table(name = "outbox_event", schema = "ordersvc")
public class OutboxEventEntity implements OutboxRecord { ... }
```
```java
// matching-engine/entity/OutboxEventEntity.java — KHÔNG có aggregate_type
@Entity
@Table(name = "outbox_event", schema = "matchsvc")
public class OutboxEventEntity implements OutboxRecord { ... }
```

🔑 **Hai bảng có lược đồ khác nhau** (`ordersvc.outbox_event` có `aggregate_type`, `matchsvc.outbox_event` không có) mà **vẫn dùng chung một relay**. Đó là bằng chứng rằng ranh giới interface được vạch đúng: relay chỉ cần 5 method, mọi thứ khác là chuyện riêng của mỗi service.

So sánh với phương án "dùng chung `@Entity`":

| | Interface + adapter (repo này) | Shared `@Entity` |
|---|---|---|
| Thêm cột vào bảng của order-service | matching-engine không biết, không quan tâm | **Phải migrate theo** |
| Đổi tên schema | Chuyện riêng | Ảnh hưởng cả hai |
| Đổi chiến lược khoá ID | Chuyện riêng | Ràng buộc chung |
| Code thừa | 24 dòng adapter mỗi service | 0 |

### 2.7. Tính đệ quy của Outbox

`matching-engine` dùng **đúng** pattern đó cho chiều về của saga:

```java
// matching-engine/src/main/java/com/trading/matching/entity/OutboxEventEntity.java:11-15
/**
 * Outbox của matching-engine: execution report cũng phải ra Kafka NGUYÊN TỬ
 * cùng với việc ghi execution. Không có outbox ở đây thì saga sẽ hỏng ở
 * chiều về: khớp lệnh xong, publish fail -> order-service treo ACCEPTED mãi mãi.
 */
```

```java
// matching-engine/src/main/java/com/trading/matching/service/MatchingService.java:97-106
private void emit(ExecutionReport report) {
    OutboxEventEntity event = new OutboxEventEntity();
    event.setAggregateId(report.clientOrderId());
    event.setEventType("EXECUTION_REPORT");
    event.setTopic("stock.executions");
    event.setMsgKey(report.symbol());   // cùng key với stock.orders -> giữ thứ tự theo mã
    event.setPayload(toJson(report));
    event.setCreatedAt(Instant.now());
    outboxRepo.save(event);
}
```

🔑 **Outbox không phải mẹo vặt cho một chỗ — nó là cơ chế nền tảng cho MỌI ranh giới giữa database và message broker.** Ở đâu có một `@Transactional` ghi DB và một mong muốn publish event, ở đó cần outbox.

---

## 3. Debezium CDC — phần thưởng thật sự của Outbox

### 3.1. Vì sao có thể đổi mà không sửa mã

```markdown
// cdc/README.md
## Điểm mấu chốt: KHÔNG phải đổi schema
`EventRouter` của Debezium đọc đúng bảng `outbox_event` hiện có (`id`, `msg_key`,
`payload`, `topic`). Message ra Kafka giống hệt message do relay tạo — consumer
không cần sửa một dòng nào. Đây là lý do outbox pattern đáng giá: nó tách
"ghi sự kiện" khỏi "cách chuyển sự kiện đi".
```

### 3.2. Cấu hình connector

```json
// cdc/order-outbox-connector.json
{
  "name": "order-outbox-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "trading",
    "database.password": "trading123",
    "database.dbname": "trading",
    "topic.prefix": "cdc-order",
    "slot.name": "order_outbox_slot",
    "plugin.name": "pgoutput",
    "table.include.list": "ordersvc.outbox_event",

    "//": "Outbox Event Router: biến 1 row outbox thành 1 message đúng topic/key,",
    "// ": "payload gửi đi là NGUYÊN VĂN cột payload - consumer không thấy khác gì so với relay",
    "transforms": "outbox",
    "transforms.outbox.type": "io.debezium.transforms.outbox.EventRouter",
    "transforms.outbox.table.field.event.id": "id",
    "transforms.outbox.table.field.event.key": "msg_key",
    "transforms.outbox.table.field.event.payload": "payload",
    "transforms.outbox.route.by.field": "topic",
    "transforms.outbox.route.topic.replacement": "${routedByValue}",
    "transforms.outbox.table.expand.json.payload": "false",

    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.storage.StringConverter"
  }
}
```

Giải thích các tham số then chốt:

| Tham số | Ý nghĩa |
|---|---|
| `plugin.name: pgoutput` | Plugin logical decoding **có sẵn trong Postgres 10+**, không cần cài `wal2json`/`decoderbufs` |
| `slot.name` | Tên replication slot. 🔑 Mỗi connector một slot riêng — hai connector dùng chung slot sẽ hỏng |
| `table.include.list` | Chỉ theo dõi bảng outbox, **không** theo dõi `orders`. Đây là điểm phân biệt "Outbox CDC" với "CDC toàn bảng" |
| `transforms.outbox.type: EventRouter` | SMT biến một row CDC thành một message Kafka đúng topic/key |
| `route.by.field: topic` | 🔑 Đọc cột `topic` của row để quyết định topic đích |
| `route.topic.replacement: ${routedByValue}` | Dùng **nguyên văn** giá trị cột đó, không thêm prefix |
| `table.expand.json.payload: false` | Giữ `payload` là chuỗi, **không** parse thành struct ⇒ consumer nhận đúng JSON như relay gửi |
| `StringConverter` | Khớp với `StringSerializer` của relay |

Và Postgres phải bật logical replication:

```yaml
# docker-compose.cdc.yml
postgres:
  command: >
    postgres -c wal_level=logical
             -c max_wal_senders=10
             -c max_replication_slots=10

order-service:
  environment:
    OUTBOX_RELAY_ENABLED: "false"     # 🔑 BẮT BUỘC

matching-engine:
  environment:
    OUTBOX_RELAY_ENABLED: "false"
```

⚠️ **`OUTBOX_RELAY_ENABLED=false` là bắt buộc, không phải tuỳ chọn.** Để cả hai cùng bật thì relay và CDC cùng publish ⇒ **mọi event nhân đôi**. Consumer dedupe được nên hệ vẫn *đúng*, nhưng tốn gấp đôi băng thông và làm nhiễu mọi metric. Comment trong `docker-compose.cdc.yml` nói rõ điều này.

💡 Lưu ý về cổng: Kafka Connect dùng cổng `8083` — **trùng với `order-service`**. Trong `docker-compose.cdc.yml` chúng ở hai container khác nhau nên không xung đột *trong mạng Docker*, nhưng cả hai đều publish `8083` ra host ⇒ xung đột khi chạy đồng thời. Đây là chi tiết cần lưu ý khi dựng môi trường CDC trên máy cá nhân.

### 3.3. So sánh Polling vs CDC

```markdown
// cdc/README.md
| | Polling relay | Debezium CDC |
|---|---|---|
| Độ trễ | ~500ms (chu kỳ poll) | ~ms (đọc WAL) |
| Tải lên DB | Query định kỳ + UPDATE mark published | Chỉ đọc WAL, không query, không UPDATE |
| Hạ tầng | Không thêm gì | Kafka Connect + connector phải vận hành |
| Vận hành | Đơn giản, debug bằng SQL | Phải canh **replication slot**: connector chết lâu → WAL không được dọn → **đầy đĩa Postgres** |
| Dọn dẹp outbox | Row còn lại, phải có job xoá | Có thể cấu hình xoá ngay sau khi ghi (row chỉ cần tồn tại trong WAL) |
| Thứ tự | Theo id, do relay gửi tuần tự | Theo thứ tự commit trong WAL |
```

🔑 **Rủi ro lớn nhất của CDC là replication slot — con dao hai lưỡi.**

Replication slot đảm bảo Postgres **không xoá WAL** cho tới khi consumer (connector) xác nhận đã đọc. Đó chính là thứ bảo đảm không mất event. Nhưng nếu connector down mà không ai để ý:

```
Postgres giữ lại TOÀN BỘ WAL từ điểm connector dừng
  → pg_wal/ phình dần
  → đĩa đầy
  → PostgreSQL DỪNG HOÀN TOÀN (không ghi được nữa)
  → toàn hệ thống sập
```

Và nó sập theo cách tệ nhất: **không phải vì lỗi của database, mà vì một connector bị quên.**

Alert bắt buộc nếu chạy CDC:

```sql
-- Độ trễ của từng slot tính bằng byte
SELECT slot_name,
       active,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS lag
FROM pg_replication_slots;
```

Alert khi `lag > 1GB` hoặc `active = false` quá N phút. Và cài `max_slot_wal_keep_size` (Postgres 13+) để Postgres tự vô hiệu hoá slot thay vì tự sát vì hết đĩa — đổi "mất event" lấy "database còn sống", một đánh đổi thường là đúng.

### 3.4. Khi nào nên chuyển sang CDC

```markdown
// cdc/README.md
Với hệ này, polling 500ms là đủ (lệnh không cần độ trễ micro-giây ở chặng
publish). CDC đáng chuyển khi outbox lớn tới mức query polling gây tải, hoặc
khi cần độ trễ dưới 100ms.
```

Nói cụ thể hơn, ba ngưỡng:

1. **Độ trễ.** Saga hiện tốn ~1 giây, trong đó **~1 giây là hai lần polling 500ms**. Nếu nghiệp vụ cần phản hồi dưới 200ms, CDC là cách duy nhất không phải đánh đổi tải DB.
2. **Tải DB.** Relay chạy mỗi 500ms trên mỗi pod. Với 3 pod order + 3 pod matching = 12 query `FOR UPDATE SKIP LOCKED` mỗi giây, cộng với `UPDATE` mark published. Ở quy mô hiện tại không đáng kể; ở quy mô hàng nghìn event/giây thì đáng.
3. **Dọn dẹp.** Với CDC, row outbox chỉ cần *tồn tại trong WAL* — có thể xoá ngay sau khi insert (Debezium vẫn bắt được vì nó đọc WAL, không đọc bảng). Với polling, phải có job xoá row đã publish, nếu không bảng phình vô hạn.

💡 **Một cách giảm độ trễ mà không cần CDC:** hạ `outbox.relay.interval-ms` xuống 100ms. Đổi lại là 10 query/giây/pod. Đó thường là bước đầu tiên nên thử trước khi mang Kafka Connect vào hệ thống.

---

## 4. Vấn đề được giải quyết — tổng hợp

| Vấn đề | Cơ chế |
|---|---|
| Lệnh có, event mất | 2 INSERT, 1 transaction |
| Event có, lệnh mất | Như trên |
| Kafka chết ⇒ không nhận lệnh | Event nằm trong DB, relay gửi sau |
| Nhiều pod relay publish trùng lô | `FOR UPDATE SKIP LOCKED` |
| Relay bị tuần tự hoá | `SKIP LOCKED` thay vì `FOR UPDATE` thuần |
| Thứ tự event đảo | `ORDER BY id` + gửi sync + rollback cả batch |
| Index phình theo bảng | Partial index `WHERE published_at IS NULL` |
| Khoá Kafka quyết định sai thời điểm | Cột `msg_key` ghi cùng transaction |
| Relay phải biết nghiệp vụ | Cột `topic` nằm trong dữ liệu |
| Đổi cơ chế vận chuyển phải sửa mã | `@ConditionalOnProperty` + cùng một bảng |
| Shared entity gây coupling | Interface `OutboxRecord` + adapter |
| Publish thất bại không ai biết | `Counter` + alert `OutboxPublishFailing` |
| Gửi trùng | Không giải quyết ở đây — **cố ý** để idempotent consumer lo |

---

## 5. Đánh đổi

### 5.1. Ưu

1. **Nguyên tử tuyệt đối** do chính database đảm bảo, không cần coordinator.
2. **Không XA, không 2PC** — không blocking, không giết throughput.
3. **Kafka chết không làm mất lệnh.**
4. **Relay vô tri** — đích đến và khoá nằm trong dữ liệu.
5. **Đổi được sang CDC** bằng một biến môi trường.
6. **Nhiều pod song song** nhờ `SKIP LOCKED`.
7. **Quan sát được** — truy vấn SQL thấy ngay event nào tắc.
8. **Module dùng chung coupling tối thiểu** — 2 interface, 5 method.

### 5.2. Nhược

1. **Độ trễ thêm ≤ `interval-ms`** (500ms). Với hai chặng outbox trong saga, đó là ~1 giây — thủ phạm chính của độ trễ end-to-end.
2. **At-least-once, không exactly-once.** Bắt buộc phải có idempotent consumer, nếu không thiết kế này *sai*.
3. **Bảng outbox phình vô hạn** nếu không có job dọn. Chưa có trong repo.
4. **Tải thêm lên DB:** 2 query/giây/pod (SELECT + UPDATE).
5. **Rollback cả batch** ⇒ tới 100 event gửi lại cho một lỗi.
6. **Payload trùng lặp dữ liệu** — `OrderResult` nằm cả trong `orders` lẫn trong `outbox_event.payload`.
7. ⚠️ **`acks=all` chỉ là danh nghĩa** với 1 broker (Gap #16).
8. **Phải nhớ `scanBasePackages`** — quên là relay không chạy và không có lỗi nào.

---

## 6. Phương án thay thế

| Phương án | Nguyên lý | Khi nào hợp | Vì sao repo không chọn |
|---|---|---|---|
| **XA / 2PC** | Coordinator điều phối prepare/commit | Hai database truyền thống | Kafka không hỗ trợ XA; blocking; throughput sụp |
| **Kafka Transactions (EOS)** | Nguyên tử consume-process-produce **trong Kafka** | Luồng Kafka → Kafka thuần | **Không bao gồm PostgreSQL** ⇒ vẫn là dual-write ở ranh giới DB |
| **Listen-to-yourself** | Publish Kafka trước, chính service đó consume rồi mới ghi DB | Khi Kafka là nguồn sự thật | Không có "đã nhận lệnh" đồng bộ để trả client; thứ tự ghi DB khó kiểm soát |
| **Debezium CDC toàn bảng** (không outbox) | Theo dõi `orders`, biến mỗi row change thành event | Replicate dữ liệu, data lake | Event mang hình dạng **bảng**, không mang hình dạng **nghiệp vụ**; đổi schema bảng là phá hợp đồng event |
| **Polling bảng nghiệp vụ** (không outbox) | Quét `orders WHERE published = false` | Hệ đơn giản | Trộn trạng thái vận chuyển vào bảng nghiệp vụ; không chở được nhiều loại event |
| **Saga với compensating transaction** | Ghi DB, publish; thất bại thì chạy bù | Khi bước sau có thể huỷ bước trước | Không giải quyết dual-write, chỉ dọn hậu quả. Và "huỷ một lệnh đã khớp" không phải là thứ bù được. |

🔑 **Phân biệt "Outbox CDC" với "CDC toàn bảng"** là câu hỏi phỏng vấn hay. CDC toàn bảng sinh event có hình dạng của *bảng* — đổi tên cột là phá mọi consumer, và một thao tác nghiệp vụ gồm 3 UPDATE sẽ sinh 3 event rời rạc. Outbox CDC sinh event có hình dạng *nghiệp vụ* do chính service quyết định, và một thao tác nghiệp vụ sinh đúng một event. Bảng `orders` tự do tiến hoá; hợp đồng event ổn định.

---

## 7. Núm vặn cấu hình

| Tham số | Nơi khai | Mặc định | Hệ quả khi đổi |
|---|---|---|---|
| `outbox.relay.enabled` | `application.yml` | `true` | 🔑 `false` khi dùng CDC. Để cả hai = event nhân đôi |
| `outbox.relay.interval-ms` | `application.yml` | 500 | Trực tiếp quyết định độ trễ saga. 100ms ⇒ nhanh gấp 5, tải DB gấp 5 |
| `outbox.relay.batch-size` | `application.yml` | 100 | Lớn ⇒ throughput cao, nhưng một lỗi làm gửi lại nhiều hơn |
| `.get(5, SECONDS)` | `OutboxRelay.java:59` | 5s (hardcode) | Nên là cấu hình; quá ngắn ⇒ rollback oan khi Kafka chậm |
| `wal_level` | `docker-compose.cdc.yml` | `logical` (chỉ CDC) | Bắt buộc cho logical replication |
| `max_replication_slots` | như trên | 10 | Phải ≥ số connector |
| `max_slot_wal_keep_size` | **chưa cấu hình** | — | ⚠️ Nên đặt để Postgres không tự sát vì slot bị bỏ quên |

---

## 8. Cách kiểm chứng

```bash
PSQL='docker compose exec postgres psql -U trading -d trading'

# 1. Outbox tồn đọng — phải luôn ~0
$PSQL -c "SELECT count(*) FILTER (WHERE published_at IS NULL) AS cho_gui,
                 count(*) FILTER (WHERE published_at IS NOT NULL) AS da_gui
          FROM ordersvc.outbox_event;"

# 2. Độ trễ relay thực tế
$PSQL -c "SELECT id, topic, msg_key,
                 round(extract(epoch from (published_at - created_at)) * 1000) AS tre_ms
          FROM ordersvc.outbox_event
          WHERE published_at IS NOT NULL
          ORDER BY id DESC LIMIT 10;"

# 3. Chứng minh partial index được dùng
$PSQL -c "EXPLAIN ANALYZE SELECT * FROM ordersvc.outbox_event
          WHERE published_at IS NULL ORDER BY id LIMIT 100 FOR UPDATE SKIP LOCKED;"
#  -> phải thấy 'Index Scan using idx_outbox_unpublished'

# 4. Metric relay
curl -s http://localhost:8083/actuator/prometheus | grep -E 'outbox_published|outbox_publish_failed'

# 5. Chứng minh nguyên tử: Kafka chết -> lệnh VẪN được nhận
docker compose stop kafka
curl -s -X POST http://localhost:8083/api/v1/orders \
  -H 'Content-Type: application/json' -H 'X-API-KEY: demo-key' \
  -d '{"clientOrderId":"OUTBOX-TEST","symbol":"HPG","side":"BUY","price":28500,"quantity":100,"accountId":"ACC001"}' \
  | jq -r .status
#  -> ACCEPTED (!!) — lệnh nằm an toàn trong DB
$PSQL -c "SELECT id, topic, published_at FROM ordersvc.outbox_event WHERE published_at IS NULL;"
#  -> event đang chờ

docker compose start kafka
sleep 15
$PSQL -c "SELECT count(*) FROM ordersvc.outbox_event WHERE published_at IS NULL;"
#  -> 0, relay đã gửi hết
curl -s http://localhost:8083/api/v1/orders/OUTBOX-TEST -H 'X-API-KEY: demo-key' | jq -r .status
#  -> FILLED, saga hoàn tất

# 6. Chứng minh SKIP LOCKED: mở 2 session, khoá song song
#    Session A:
$PSQL -c "BEGIN; SELECT id FROM ordersvc.outbox_event WHERE published_at IS NULL
          ORDER BY id LIMIT 5 FOR UPDATE SKIP LOCKED;"
#    Session B (chạy đồng thời) -> trả về CÁC ID KHÁC, không chờ

# 7. Tắt relay để quan sát tồn đọng
docker compose stop order-service
docker compose run -d -e OUTBOX_RELAY_ENABLED=false --name order-norelay order-service
#    đặt vài lệnh -> outbox tăng dần, lệnh mãi ACCEPTED

# === CDC ===
docker compose -f docker-compose.yml -f docker-compose.cdc.yml up -d --build
sleep 30
./cdc/register-connectors.sh
curl -s http://localhost:8083/connectors?expand=status | jq      # Kafka Connect REST

# Theo dõi replication slot — ALERT BẮT BUỘC khi chạy CDC
$PSQL -c "SELECT slot_name, active, restart_lsn,
                 pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS wal_lag
          FROM pg_replication_slots;"

# Kích thước WAL hiện tại
$PSQL -c "SELECT pg_size_pretty(sum(size)) FROM pg_ls_waldir();"
```

---

## 9. Hạn chế đã biết

| # | Loại | Vị trí | Vấn đề | Hướng sửa |
|:--:|---|---|---|---|
| 16 | **Gap** | Mọi `TopicBuilder` + 3 chỗ `acks=all` | `.replicas(1)` + 1 broker ⇒ `acks=all` ≡ `acks=1`. Độ bền của outbox producer chỉ là danh nghĩa | Multi-broker + `min.insync.replicas=2` |
| 17 | Doc | `OutboxRelay.java:62-67` | Rollback cả batch ⇒ tới 100 event gửi lại, không phải một. Mô tả trong `DESIGN-NOTES` nói nhẹ | Ghi rõ quy mô; hành vi là cố ý |
| — | **Ops** | `ordersvc.outbox_event`, `matchsvc.outbox_event` | **Không có job dọn row đã publish** ⇒ bảng phình vô hạn | Job xoá `WHERE published_at < now() - interval '7 days'`; hoặc partition theo ngày |
| — | Ops | `OutboxRelay.java:59` | Timeout 5s hardcode | Đưa thành `@Value` |
| — | Gap | CDC | Chưa cấu hình `max_slot_wal_keep_size` ⇒ slot bị bỏ quên có thể làm đầy đĩa Postgres | Đặt giới hạn + alert trên `pg_replication_slots` |
| — | Ops | `docker-compose.cdc.yml` | Kafka Connect dùng cổng 8083 trùng `order-service` trên host | Đổi port mapping khi chạy CDC local |

---

## 10. Câu hỏi phỏng vấn

**Q: Giải thích Transactional Outbox trong 60 giây.**

A: Bài toán là ghi database và publish message một cách nguyên tử, nhưng hai hệ thống không chia sẻ transaction — ghi xong mà publish fail thì hai bên lệch nhau vĩnh viễn. Outbox không cố giải bài toán phân tán; nó **biến bài toán phân tán thành bài toán cục bộ**: thay vì ghi hai hệ thống, ta ghi một database hai bảng — bảng nghiệp vụ và bảng `outbox_event` — trong cùng một local ACID transaction. Nguyên tử tuyệt đối do chính Postgres đảm bảo. Sau đó một relay đọc bảng outbox, publish lên Kafka, rồi đánh dấu `published_at`. Crash ở bất kỳ đâu chỉ gây gửi lại, tức at-least-once, và idempotent consumer biến nó thành effectively-once.

**Q: Nếu relay publish lên Kafka thành công rồi crash trước khi commit `published_at` thì sao?**

A: Event được gửi lại ở vòng relay sau. Đó là **at-least-once by design**, không phải bug — và thiết kế này chỉ *đúng* vì có idempotent consumer ở đầu kia: `INSERT INTO processed_event ... ON CONFLICT DO NOTHING` nằm trong cùng transaction với việc ghi execution, nên lần thứ hai trả về 0 row và message bị bỏ qua. Nếu bỏ idempotent consumer đi thì Outbox trở thành một cơ chế *sai*. Hai cơ chế này là một cặp, không tách rời được.

**Q: Vì sao không dùng Kafka transaction (exactly-once semantics) cho gọn?**

A: Vì Kafka EOS chỉ nguyên tử **trong phạm vi Kafka**: consume, process, produce, commit offset. Nó không bao gồm việc ghi PostgreSQL. Ngay khi một bên của ranh giới là database, ta lại quay về đúng bài toán dual-write. EOS giải một bài toán khác — và hai thứ dùng chung được: EOS cho luồng Kafka→Kafka thuần, Outbox cho ranh giới DB↔Kafka.

**Q: `FOR UPDATE SKIP LOCKED` khác gì `FOR UPDATE`?**

A: `FOR UPDATE` thuần làm transaction thứ hai **nằm chờ** cho tới khi transaction đầu nhả khoá. Với 3 pod relay, điều đó biến relay thành tuần tự: throughput bằng đúng một pod, hai pod kia ngồi chờ. `SKIP LOCKED` làm transaction thứ hai **bỏ qua** các row đang bị khoá và lấy row tiếp theo — nên pod 1 lấy row 1-100, pod 2 lấy 101-200, pod 3 lấy 201-300, chạy song song thật. Đây là cách biến một bảng SQL thành hàng đợi công việc đa tiêu thụ mà không cần message broker. Nó không loại trừ gửi trùng khi crash giữa chừng — và đó là điều chấp nhận có ý thức.

**Q: Làm sao chuyển từ polling relay sang CDC mà không downtime?**

A: Đây chính là điểm hay nhất của Outbox — nó tách "ghi sự kiện" khỏi "cách chuyển sự kiện đi", nên consumer không biết có gì thay đổi. Quy trình: (1) bật `wal_level=logical` trên Postgres — cần restart DB, nên làm ngoài phiên; (2) dựng Kafka Connect và đăng ký connector với `EventRouter` đọc đúng bảng `outbox_event` hiện có, cấu hình `route.by.field: topic` và `table.expand.json.payload: false` để message ra giống hệt relay; (3) xác nhận connector đang chạy và đọc được; (4) đặt `OUTBOX_RELAY_ENABLED=false` rồi rolling restart. Trong cửa sổ giữa (3) và (4), cả hai cùng publish ⇒ event nhân đôi — vô hại về tính đúng đắn nhờ dedupe, chỉ tốn băng thông. Và trước khi làm bất cứ điều gì, tôi sẽ dựng alert trên `pg_replication_slots`: rủi ro lớn nhất của CDC là một connector bị quên làm Postgres giữ WAL tới đầy đĩa.

**Q: Bảng outbox phình mãi. Xử lý thế nào, và có cạm bẫy gì?**

A: Job dọn định kỳ `DELETE FROM outbox_event WHERE published_at < now() - interval '7 days'`, hoặc partition theo ngày rồi `DROP PARTITION` (rẻ hơn nhiều so với `DELETE` vì không sinh dead tuple cần VACUUM). Cạm bẫy: không được dọn row **chưa** publish — đó là lệnh chưa ra Kafka. Và với `DELETE`, nhớ rằng Postgres chỉ đánh dấu row là chết; cần `VACUUM` để trả lại không gian, nên xoá hàng triệu row một lần có thể gây bloat và autovacuum chạy nặng. Nên xoá theo lô nhỏ. Với CDC thì khác hẳn: row chỉ cần tồn tại trong WAL nên có thể xoá **ngay sau khi insert** — Debezium vẫn bắt được vì nó đọc WAL chứ không đọc bảng.

**Q: Vì sao `OutboxRelay` gửi đồng bộ `.get(5s)` mà không gửi bất đồng bộ cho nhanh?**

A: Vì phải chắc chắn Kafka đã nhận **trước khi** đánh dấu `published_at`. Gửi bất đồng bộ rồi mark ngay tạo ra khả năng "đã mark published mà Kafka chưa nhận" — và khi đó event mất **vĩnh viễn**, không có cơ chế nào tìm lại được vì relay sẽ không bao giờ xét lại row đó. Nguyên tắc chi phối cả thiết kế này là: **mất lệnh tệ hơn gửi trùng rất nhiều** — trùng thì consumer dedupe lo, mất thì không gì cứu. Gửi đồng bộ cũng giữ đúng thứ tự `id` tăng dần, điều quan trọng vì khoá Kafka là `symbol`.

---

## 11. Bảng tổng kết

| Khía cạnh | Lựa chọn | Lý do | Cái giá |
|---|---|---|---|
| Nguyên tử DB ↔ Kafka | **Transactional Outbox** | Biến bài toán phân tán thành local ACID | Thêm bảng, thêm relay |
| Thay vì XA/2PC | Không dùng | Kafka không hỗ trợ XA; 2PC blocking | — |
| Vận chuyển | Polling relay 500ms | Đơn giản, debug bằng SQL | ≤500ms độ trễ mỗi chặng |
| Song song nhiều pod | `FOR UPDATE SKIP LOCKED` | Không chờ nhau, không trùng lô | Vẫn trùng khi crash — cố ý |
| Thứ tự | `ORDER BY id` + sync + rollback batch | Khoá Kafka là symbol ⇒ thứ tự quan trọng | 1 lỗi ⇒ tới 100 event gửi lại |
| Index | Partial `WHERE published_at IS NULL` | Luôn nhỏ bất kể bảng lớn | Chi phí cập nhật index khi mark |
| Đích đến | Cột `topic` trong dữ liệu | Relay vô tri; 1 bảng chở nhiều topic | — |
| Khoá Kafka | Cột `msg_key` ghi cùng transaction | Quyết định đúng thời điểm, cùng dữ liệu nghiệp vụ | — |
| Serializer | `StringSerializer` | Payload đã là JSON string | Phải nhớ, nếu không bị double-encode |
| Module dùng chung | `OutboxRecord` + `OutboxStore` interface | Chia sẻ hành vi, không chia sẻ cấu trúc | 24 dòng adapter mỗi service |
| Đổi cơ chế | `@ConditionalOnProperty` | Polling ↔ CDC bằng một biến môi trường | Phải nhớ tắt relay khi bật CDC |
| Ngữ nghĩa giao hàng | At-least-once | Mất lệnh tệ hơn trùng lệnh | **Bắt buộc** có idempotent consumer |
| Quan sát | 2 Counter + alert | Publish thất bại phải có người biết | — |
| Dọn dẹp | **Chưa có** | — | ⚠️ Bảng phình vô hạn |

> **Đọc tiếp:** [Chương 06 — Idempotency & Consumer Deduplication](./06-idempotency-va-consumer-deduplication.md) — nửa còn lại của cặp đôi, không phải phụ kiện.
