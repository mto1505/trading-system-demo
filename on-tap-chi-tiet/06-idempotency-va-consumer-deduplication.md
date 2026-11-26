# Chương 06 — Idempotency & Consumer Deduplication

> **Câu hỏi trung tâm:** at-least-once không phải lựa chọn — nó là **định mệnh** của mọi hệ thống phân tán. Vậy làm sao xử lý đúng một lần khi message có thể tới nhiều lần?
>
> **Tài liệu đối chiếu:** `DESIGN-NOTES.md` mục 10, `DEEP-DIVE.md` mục 7.

---

## 1. Nó là gì?

### 1.1. Ba ngữ nghĩa giao hàng và vì sao chỉ một cái khả thi

| Ngữ nghĩa | Nghĩa là | Khả thi? |
|---|---|---|
| **At-most-once** | Mỗi message tới ≤ 1 lần | Có, nhưng **mất dữ liệu**. Không dùng được cho lệnh. |
| **At-least-once** | Mỗi message tới ≥ 1 lần | ✅ **Khả thi** — và là thứ duy nhất đạt được ở tầng vận chuyển |
| **Exactly-once** | Đúng 1 lần | ❌ **Bất khả thi** ở tầng vận chuyển trên mạng không đáng tin |

🔑 **Vì sao exactly-once bất khả thi.** Đây là hệ quả của *Two Generals Problem*: người gửi không bao giờ biết chắc người nhận đã nhận. Kịch bản cụ thể và không thể tránh:

```
Producer → gửi message → Broker nhận và ghi thành công
                      ← ack bị mất trên đường về ✗
Producer: "chắc thất bại" → gửi lại → Broker có 2 bản ghi
```

Producer đứng trước hai lựa chọn, không có lựa chọn thứ ba:
- **Không gửi lại** ⇒ at-most-once ⇒ có thể mất.
- **Gửi lại** ⇒ at-least-once ⇒ có thể trùng.

Không có cách nào biết ack bị mất *trước khi* hay *sau khi* broker ghi.

🔑 **Lối thoát: "Effectively-once" = At-least-once delivery + Idempotent processing.**

Chấp nhận message tới nhiều lần, nhưng làm cho **xử lý lần thứ hai không gây tác dụng phụ**. Đây là cách duy nhất thế giới microservices đạt được thứ mà người ta gọi là "exactly-once".

```java
// matching-engine/src/main/java/com/trading/matching/MatchingEngineApplication.java:7-17
/**
 * MATCHING ENGINE (STUB) - minh hoạ IDEMPOTENT CONSUMER,
 * nửa còn lại của Transactional Outbox.
 *
 * Outbox đảm bảo AT-LEAST-ONCE (event có thể tới >= 1 lần).
 * Idempotent consumer biến at-least-once thành EFFECTIVELY-ONCE:
 * mỗi clientOrderId chỉ được xử lý đúng 1 lần dù message tới bao nhiêu lần.
 *
 * Cặp đôi Outbox + Idempotent Consumer chính là cách thế giới microservices
 * đạt "distributed transaction" mà không cần XA/2PC.
 */
```

### 1.2. Bốn nguồn sinh ra bản sao trong hệ thống này

| # | Nguồn | Chặn ở đâu |
|:--:|---|---|
| 1 | **Client retry** sau timeout (cùng `clientOrderId`) | Redis (lớp 1) + `UNIQUE(client_order_id)` (lớp 2) |
| 2 | **Producer retry** khi ack bị mất | `enable.idempotence=true` (trong phạm vi một phiên producer) |
| 3 | **OutboxRelay gửi lại** sau khi crash giữa publish và commit | `processed_event` ở matching-engine (lớp 3) |
| 4 | **Kafka redelivery** khi consumer crash sau xử lý, trước commit offset | Như trên |

🔑 Bốn nguồn này độc lập nhau và cần những cơ chế khác nhau. Không có một "viên đạn bạc" nào chặn được cả bốn.

---

## 2. Ba lớp dedupe — và vì sao phải đủ ba

### 2.1. Toàn cảnh

```
Client ──POST (clientOrderId)──►┌──────────────────────────────────────┐
                                │ order-service                         │
                                │                                       │
                                │ ┌───────────────────────────────────┐ │
                                │ │ LỚP 1: Redis GET idem:{id}        │ │
                                │ │ • ~0,5ms, tập trung mọi pod        │ │
                                │ │ • CÓ THỂ CHẾT — hệ vẫn đúng        │ │
                                │ └───────────────┬───────────────────┘ │
                                │                 │ miss                 │
                                │ ┌───────────────▼───────────────────┐ │
                                │ │ LỚP 2: UNIQUE(client_order_id)    │ │
                                │ │ • ~3ms, TUYỆT ĐỐI                  │ │
                                │ │ • 🔑 NỀN TẢNG — không được chết    │ │
                                │ └───────────────┬───────────────────┘ │
                                └─────────────────┼─────────────────────┘
                                                  │ outbox → Kafka
                                                  │ (at-least-once)
                                ┌─────────────────▼─────────────────────┐
                                │ matching-engine                        │
                                │ ┌────────────────────────────────────┐ │
                                │ │ LỚP 3: INSERT processed_event      │ │
                                │ │        ON CONFLICT DO NOTHING      │ │
                                │ │ • Trong CÙNG transaction với       │ │
                                │ │   việc ghi execution               │ │
                                │ │ • 🔑 Chống KHỚP LỆNH hai lần       │ │
                                │ └────────────────────────────────────┘ │
                                └────────────────────────────────────────┘

                        ┌──────────────────────────────────────┐
                        │ LỚP 4 (ngầm): OrderStateMachine      │
                        │ Trạng thái cuối không có đường ra     │
                        │ ⇒ execution report trùng tự vô hại    │
                        └──────────────────────────────────────┘
```

🔑 **Nguyên tắc nền:** mỗi lớp **phía trên có thể chết mà hệ vẫn đúng**. Cache chỉ tăng tốc; DB constraint là nguồn sự thật.

### 2.2. Lớp 1 — Redis fast path

```java
// order-service/src/main/java/com/trading/order/service/OrderService.java:76-87
// ===== Lớp 1: Redis fast-path (degrade gracefully nếu Redis chết) =====
String idemKey = "idem:" + req.clientOrderId();
try {
    String cached = redis.opsForValue().get(idemKey);
    if (cached != null) {
        duplicates.increment();
        return objectMapper.readValue(cached, OrderResult.class);
    }
} catch (Exception e) {
    log.warn("Redis khong kha dung, dung DB constraint lam dedupe: {}", e.getMessage());
}
```

**Vì sao cần lớp này khi đã có lớp 2?** Vì lớp 2 chỉ phát hiện trùng *sau khi* đã chạy pre-trade check và *sau khi* đã cố INSERT — tốn một round-trip DB và một exception. Với một client bug retry 100 lần/giây, Redis cắt toàn bộ tải đó khỏi database.

**Vì sao Redis không thể là lớp duy nhất:**
- Redis là in-memory; mất dữ liệu khi restart (trừ khi bật AOF `appendfsync always`, và khi đó nó chậm như database).
- Redis Cluster có thể mất ghi khi failover (replication bất đồng bộ).
- TTL 10 phút ⇒ sau đó Redis quên.

Nên Redis ở đây là **tăng tốc có điều kiện**, và ba dòng cấu hình làm cho điều đó an toàn:

```yaml
spring:
  data:
    redis:
      timeout: 200ms               # treo không được kéo treo lệnh
management:
  health:
    redis:
      enabled: false               # 🔑 chết không được kéo sập readiness
```

Cộng với `try/catch` bao quanh. Ba thứ này cùng nhau biến tuyên ngôn "Redis chỉ là cache" thành hiện thực kiểm chứng được — xem mục 6 (`docker compose stop redis` rồi thử lại).

### 2.3. Lớp 2 — UNIQUE constraint, chốt chặn thật

```sql
-- order-service/src/main/resources/db/migration/V1__orders_and_outbox.sql
CREATE TABLE orders (
    id              BIGSERIAL PRIMARY KEY,
    order_id        VARCHAR(36)  NOT NULL UNIQUE,
    client_order_id VARCHAR(64)  NOT NULL,
    ...
    -- DEDUPE TẬP TRUNG cấp DB: chốt chặn cuối cùng chống duplicate lệnh
    CONSTRAINT uq_client_order UNIQUE (client_order_id)
);
```

```java
// order-service/src/main/java/com/trading/order/entity/OrderEntity.java:11-16
/**
 * Bảng orders - hồ sơ mọi lệnh (kể cả REJECTED, phục vụ audit).
 * UNIQUE(client_order_id) = DEDUPE TẬP TRUNG cấp DB: dù Redis chết,
 * dù retry rơi vào pod khác, DB constraint vẫn chặn lệnh trùng tuyệt đối.
 */
```

```java
// order-service/src/main/java/com/trading/order/service/OrderService.java:89-100
try {
    persistence.saveWithOutbox(req, result);
} catch (DataIntegrityViolationException dup) {
    // Trùng client_order_id (retry rơi vào pod khác / Redis miss)
    duplicates.increment();
    OrderResult existing = persistence.findExisting(req.clientOrderId()).orElseThrow();
    cacheResult(idemKey, existing);
    return existing;
}
```

🔑 **Vì sao đây mới là chốt chặn thật.** Constraint được cưỡng chế bởi chính storage engine, trong cùng transaction, trên dữ liệu bền vững. Không phụ thuộc vào ứng dụng nhớ gì, không phụ thuộc vào pod nào xử lý, không phụ thuộc vào cache còn sống hay không. **10 pod chạy song song cùng INSERT một `client_order_id` thì đúng một pod thắng.**

💡 Chú ý nhánh `catch` không chỉ trả về kết quả cũ mà còn `cacheResult(idemKey, existing)` — **tự chữa lành cache**. Lần retry thứ ba sẽ hit Redis thay vì lại xuống DB.

⚠️ **Bug #4 — `orElseThrow()` che lỗi gốc.** Code giả định mọi `DataIntegrityViolationException` đều là vi phạm `uq_client_order`. Nhưng bảng còn có `order_id ... UNIQUE` và nhiều cột `NOT NULL`. Nếu vi phạm constraint khác, `findExisting` trả `Optional.empty()` và client nhận `NoSuchElementException` — **che hoàn toàn nguyên nhân thật**, biến một lỗi dễ sửa thành một cuộc điều tra. Sửa tối thiểu:

```java
return persistence.findExisting(req.clientOrderId())
        .map(existing -> { cacheResult(idemKey, existing); return existing; })
        .orElseThrow(() -> dup);        // giữ nguyên exception gốc
```

Sửa triệt để: đọc tên constraint từ `dup.getCause()` và chỉ xử lý nhánh duplicate khi đúng là `uq_client_order`.

### 2.4. 🔑 Lớp 3 — Idempotent consumer ở matching-engine

Đây là lớp **quan trọng nhất về mặt kỹ thuật**, vì nó là thứ biến Outbox từ một cơ chế *sai* thành một cơ chế *đúng*.

```java
// matching-engine/src/main/java/com/trading/matching/entity/ProcessedEventEntity.java:12-16
/**
 * Sổ ghi nhớ "đã xử lý": PRIMARY KEY client_order_id chính là cơ chế dedupe.
 * INSERT ... ON CONFLICT DO NOTHING trả về 0 row = đã xử lý rồi -> bỏ qua.
 */
@Entity
@Table(name = "processed_event", schema = "matchsvc")
public class ProcessedEventEntity {

    @Id
    @Column(name = "client_order_id", length = 64)
    private String clientOrderId;

    @Column(name = "processed_at", nullable = false)
    private Instant processedAt;
}
```

```sql
-- matching-engine/src/main/resources/db/migration/V1__matching_tables.sql
CREATE TABLE processed_event (
    client_order_id VARCHAR(64) PRIMARY KEY,   -- PK = cơ chế dedupe
    processed_at    TIMESTAMPTZ NOT NULL
);
```

```java
// matching-engine/src/main/java/com/trading/matching/repository/ProcessedEventRepository.java:10-23
/**
 * Atomic check-and-set bằng chính DB (không cần lock ứng dụng):
 * trả về 1 = lần đầu thấy id này -> xử lý; 0 = đã xử lý -> bỏ qua.
 * Chạy trong CÙNG transaction với việc ghi execution -> nguyên tử.
 */
@Modifying
@Query(value = """
        INSERT INTO matchsvc.processed_event (client_order_id, processed_at)
        VALUES (:id, now())
        ON CONFLICT (client_order_id) DO NOTHING
        """, nativeQuery = true)
int insertIfAbsent(@Param("id") String clientOrderId);
```

#### Vì sao `ON CONFLICT DO NOTHING` chứ không phải `if (exists) return;`

```java
// ❌ SAI — check-then-act, không nguyên tử
if (processedRepo.existsById(order.clientOrderId())) {
    return;
}
processedRepo.save(new ProcessedEventEntity(order.clientOrderId(), Instant.now()));
// ... khớp lệnh
```

Race condition với hai pod nhận bản sao của cùng một message:

```
thời điểm  Pod A                          Pod B
   t0      existsById("X") → false
   t1                                     existsById("X") → false
   t2      save("X")
   t3      KHỚP LỆNH                      save("X")  ← có thể thành công hoặc
   t4                                     KHỚP LỆNH     ném exception, nhưng
                                                        lệnh đã khớp hai lần
```

⇒ **Lệnh khớp hai lần.** Khách mua 200 cổ thay vì 100.

```java
// ✅ ĐÚNG — atomic test-and-set do chính database thực hiện
if (processedRepo.insertIfAbsent(order.clientOrderId()) == 0) {
    duplicated.increment();
    log.info("Bo qua message trung clientOrderId={}", order.clientOrderId());
    return;
}
```

PostgreSQL bảo đảm: trong hai `INSERT ... ON CONFLICT DO NOTHING` đồng thời cho cùng một khoá, **đúng một cái chèn được** (trả `1`), cái kia trả `0`. Không cần khoá ứng dụng, không cần `SELECT FOR UPDATE`, không có cửa sổ race.

#### Vì sao phải nằm trong CÙNG transaction

```java
// matching-engine/src/main/java/com/trading/matching/service/MatchingService.java:51-61
/**
 * MỘT TRANSACTION duy nhất gồm 3 việc:
 *   (1) ghi sổ processed_event  -> idempotency
 *   (2) ghi execution (nếu hợp lệ)
 *   (3) ghi outbox execution report -> saga chiều về
 *
 * Cả ba cùng commit hoặc cùng rollback. Đây là điểm mấu chốt: nếu (3)
 * nằm ngoài transaction, sẽ có trường hợp lệnh đã khớp mà order-service
 * không bao giờ biết -> lệnh treo ACCEPTED vĩnh viễn, tiền của khách bị giữ.
 */
@Transactional
public void process(OrderResult order) {
```

Nếu `processed_event` commit riêng, hai kịch bản hỏng:

| Thứ tự | Sự cố | Hậu quả |
|---|---|---|
| Ghi `processed_event` commit trước, rồi khớp lệnh thất bại | Sổ ghi "đã xử lý" nhưng chưa xử lý | 🔴 **Lệnh bị nuốt vĩnh viễn.** Redelivery thấy đã processed ⇒ bỏ qua ⇒ lệnh không bao giờ được khớp, cũng không bao giờ bị từ chối. Khách chờ mãi. |
| Khớp lệnh commit trước, rồi ghi `processed_event` thất bại | Đã khớp nhưng sổ trống | 🔴 **Redelivery khớp lần hai.** |

Nằm chung transaction ⇒ hai trạng thái đó không tồn tại được.

#### `@Modifying` và `int` trả về

`@Modifying` báo cho Spring Data biết đây là câu lệnh thay đổi dữ liệu (không phải `SELECT`), để nó dùng `executeUpdate()` thay vì `getResultList()`. Giá trị trả về là **số row bị ảnh hưởng** — chính là tín hiệu dedupe: `1` = chèn mới, `0` = đã có.

### 2.5. Lớp 4 (ngầm) — State machine ở order-service

```java
// order-service/src/main/java/com/trading/order/service/OrderStateMachine.java:3-14
/**
 * MÁY TRẠNG THÁI LỆNH - hai vai trò:
 *
 *  1. Chặn chuyển trạng thái vô lý (lệnh đã FILLED không thể quay lại ACCEPTED).
 *  2. LÀ MỘT LỚP IDEMPOTENCY MIỄN PHÍ: execution report tới lần thứ hai sẽ thấy
 *     lệnh đã ở trạng thái cuối -> transition không hợp lệ -> bỏ qua.
 *     Không cần thêm bảng processed_event ở phía order-service.
 *
 * Trạng thái cuối (terminal) không có đường ra - đơn giản hoá có chủ đích:
 * hệ chưa hỗ trợ partial fill (khớp một phần). Muốn hỗ trợ thì cần thêm
 * PARTIALLY_FILLED với self-loop và cộng dồn filledQty.
 */
private static final Map<String, Set<String>> ALLOWED = Map.of(
        ACCEPTED, Set.of(FILLED, REJECTED_BY_MATCHING),
        FILLED, Set.of(),
        REJECTED, Set.of(),
        REJECTED_BY_MATCHING, Set.of()
);
```

```java
// order-service/src/main/java/com/trading/order/service/OrderStatusService.java:55-62
if (!OrderStateMachine.canTransition(order.getStatus(), target)) {
    // Report trùng (redelivery) hoặc đến muộn -> bỏ qua, KHÔNG phải lỗi
    log.debug("Bo qua report: {} -> {} khong hop le (clientOrderId={})",
            order.getStatus(), target, report.clientOrderId());
    ignored.increment();
    return;
}
```

🔑 **Một cấu trúc dữ liệu 20 dòng thay thế được cả một bảng và một chỉ mục.** Vì mọi trạng thái cuối đều có `Set.of()` (không có đường ra), execution report tới lần thứ hai tất yếu bị từ chối transition.

⚠️ **Đánh đổi có hậu quả thật:** ngay khi thêm `PARTIALLY_FILLED` với self-loop (`PARTIALLY_FILLED → PARTIALLY_FILLED`) để hỗ trợ khớp một phần, lớp idempotency miễn phí **biến mất** — một report trùng sẽ cộng dồn `filledQty` lần thứ hai. Khi đó bắt buộc phải thêm bảng dedupe thật ở order-service, hoặc đưa `execId` duy nhất vào `ExecutionReport` và dedupe theo nó.

### 2.6. ⚠️ Gap #15 — lỗ hổng nghiêm trọng nhất chương này

```java
// matching-engine/src/main/java/com/trading/matching/entity/ExecutionEntity.java:24-28
@Column(name = "order_id", nullable = false, length = 36)
private String orderId;

@Column(name = "client_order_id", nullable = false, length = 64)   // ← KHÔNG CÓ unique!
private String clientOrderId;
```

Đối chiếu với order-service:

```java
// order-service/src/main/java/com/trading/order/entity/OrderEntity.java:27-28
@Column(name = "client_order_id", nullable = false, unique = true, length = 64)   // ← CÓ
private String clientOrderId;
```

🔴 **Hệ quả:** ở matching-engine, `processed_event` là lớp chặn khớp-trùng **duy nhất**, không có lưới an toàn phía sau. Nếu bảng đó bị truncate, bị dọn nhầm, hoặc migration lỗi, hệ thống sẽ khớp lệnh hai lần **và không gì chặn lại**.

So sánh hai service:

| | order-service | matching-engine |
|---|---|---|
| Lớp dedupe ứng dụng | Redis | `processed_event` |
| Lớp dedupe DB | **`UNIQUE(client_order_id)` trên `orders`** | ❌ **Không có** |
| Nếu lớp ứng dụng hỏng | Vẫn đúng | 🔴 **Khớp lệnh hai lần** |

**Hướng sửa — migration `V3`:**

```sql
-- matching-engine/src/main/resources/db/migration/V3__execution_unique.sql
-- Lưới an toàn thứ hai: processed_event không còn là lớp chặn duy nhất.
-- Sau migration này, job dọn processed_event chỉ còn là chuyện DUNG LƯỢNG,
-- không còn là chuyện ĐÚNG/SAI.
ALTER TABLE execution ADD CONSTRAINT uq_exec_client_order UNIQUE (client_order_id);
```

Lưu ý: migration này **chỉ chạy được nếu bảng chưa có row trùng**. Trước khi áp dụng lên môi trường đã chạy, phải kiểm tra:

```sql
SELECT client_order_id, count(*) FROM matchsvc.execution
GROUP BY client_order_id HAVING count(*) > 1;
```

💡 Và sau khi thêm, `ExecutionController` trở thành công cụ kiểm chứng chính thức:

```java
// matching-engine/src/main/java/com/trading/matching/controller/ExecutionController.java:10
/** Tra cứu kết quả khớp - dùng để verify idempotency (1 clientOrderId = đúng 1 execution). */
```

### 2.7. 🔑 Gap #12 — hợp đồng ngầm giữa retention và lịch dọn

Bảng `processed_event` **chỉ tăng**: mỗi lệnh một row, không bao giờ xoá. Sau một năm ở 10.000 lệnh/ngày là 3,65 triệu row. Rõ ràng cần job dọn.

Nhưng dọn như thế nào là một câu hỏi về **tính đúng đắn**, không phải về dung lượng:

```
# k8s/base/config/trading.env
# Retention của topic nghiệp vụ là MỘT HỢP ĐỒNG với bảng `processed_event` của
# matching-engine: job dọn processed_event phải giữ LÂU HƠN con số này. Dọn sớm
# hơn = replay từ offset cũ sẽ khớp lệnh HAI LẦN vì bản ghi chống trùng đã mất.
# 7 ngày
TOPIC_RETENTION_MS=604800000
```

**Kịch bản hỏng cụ thể:**

| Thời điểm | Sự kiện |
|---|---|
| Ngày 1 | Lệnh `X` được khớp. `processed_event` có row cho `X`. Message vẫn nằm trong `stock.orders` (retention 7 ngày). |
| Ngày 3 | Job dọn xoá mọi row `processed_at < now() - 2 days` ⇒ row của `X` **biến mất**. |
| Ngày 5 | Sự cố vận hành: ai đó reset offset của `matching-engine-group` về `earliest` để điều tra, hoặc consumer group bị xoá và tạo lại. |
| Ngày 5 | Message của `X` **vẫn còn** trong topic (chưa quá 7 ngày) ⇒ được giao lại ⇒ `insertIfAbsent` trả `1` (row đã bị xoá!) ⇒ 🔴 **khớp lệnh lần hai**. |

🔑 **Luật bất biến:**

$$\text{thời gian giữ } \texttt{processed\_event} \;>\; \text{retention của topic } \texttt{stock.orders}$$

Với `TOPIC_RETENTION_MS = 7 ngày`, job dọn phải giữ ít nhất **8-10 ngày** (có biên an toàn).

```sql
-- Job dọn ĐÚNG: giữ lâu hơn retention topic
DELETE FROM matchsvc.processed_event
WHERE processed_at < now() - interval '10 days';
```

⚠️ **Và đây là một "ràng buộc giữa hai con số ở hai file khác nhau"** — đúng loại ràng buộc mà `DEEP-DIVE.md` mục 11 kết luận là *"sớm muộn cũng lệch"*. `TOPIC_RETENTION_MS` nằm trong `trading.env`; lịch dọn (khi nào được viết) sẽ nằm trong một CronJob. Không có gì trong runtime giữ chúng khớp nhau. Repo đã bịt được một cặp tương tự (`maxReplicaCount` ↔ `partition/3`) bằng CI — cặp này thì chưa.

💡 Nếu thêm `UNIQUE(client_order_id)` trên `execution` (Gap #15), hợp đồng này **giảm nhẹ hẳn**: dọn sớm vẫn gây redelivery xử lý lại, nhưng `INSERT` execution sẽ đụng constraint và transaction rollback ⇒ không khớp hai lần. Khi đó lịch dọn chỉ còn là chuyện dung lượng. Đây là lý do nên sửa #15 trước khi viết job dọn.

---

## 3. Kiểm chứng bằng test tự động

```java
// matching-engine/src/test/java/com/trading/matching/IdempotentConsumerIT.java:27-32
/**
 * TEST QUAN TRỌNG NHẤT của hệ thống: chứng minh EFFECTIVELY-ONCE.
 * Gửi cùng 1 order 3 lần (giả lập outbox relay re-send / Kafka redelivery)
 * -> bảng execution phải có ĐÚNG 1 dòng.
 */
@Testcontainers
@SpringBootTest
class IdempotentConsumerIT {

    @Container
    static KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("trading").withUsername("trading").withPassword("trading123");
```

```java
// IdempotentConsumerIT.java:56-83
@Test
void duplicateMessagesProduceExactlyOneExecution() {
    String orderJson = """
            {"orderId":"o-1","clientOrderId":"dup-test-1","status":"ACCEPTED","reason":null,
             "symbol":"HPG","side":"BUY","price":28500.0,"quantity":100,
             "refPrice":28450.0,"riskLevel":"LOW","timestamp":%d}
            """.formatted(System.currentTimeMillis());
    ...
    try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
        for (int i = 0; i < 3; i++) {   // GỬI TRÙNG 3 LẦN
            producer.send(new ProducerRecord<>("stock.orders", "HPG", orderJson));
        }
        producer.flush();
    }

    // Chờ consumer xử lý cả 3, rồi khẳng định chỉ có 1 execution
    await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(500))
            .untilAsserted(() ->
                    assertThat(executionRepository.findByClientOrderId("dup-test-1")).hasSize(1));

    // Giữ nguyên khẳng định thêm 3s nữa (đề phòng message trễ tạo thêm dòng)
    await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(10))
            .untilAsserted(() ->
                    assertThat(executionRepository.findByClientOrderId("dup-test-1")).hasSize(1));
}
```

🔑 **Hai lần `await()` là một chi tiết rất đáng học.**

| | `await().atMost(30s).untilAsserted(…)` | `await().during(3s).atMost(10s).untilAsserted(…)` |
|---|---|---|
| Ngữ nghĩa | Chờ tới khi điều kiện **đúng lần đầu** | Khẳng định điều kiện **đúng liên tục suốt 3 giây** |
| Bắt được | "Đã có 1 execution" | **"Không có execution thứ 2 xuất hiện muộn"** |

Không có lần thứ hai, test sẽ **pass sai**: nó chỉ chứng minh rằng tại một thời điểm nào đó có đúng 1 row, không chứng minh rằng message thứ 2 và thứ 3 không tạo thêm row sau đó. Đây là mẫu đúng cho mọi assertion về *sự vắng mặt* trong hệ bất đồng bộ.

💡 Lưu ý test dùng Kafka và Postgres **thật** trong Docker, không mock. Nếu mock `ProcessedEventRepository`, ta chỉ test được rằng mình gọi đúng method — không test được rằng `ON CONFLICT DO NOTHING` thực sự nguyên tử. Tính chất đang được kiểm chứng ở đây là tính chất **của database**, nên database phải là thật.

---

## 4. Vấn đề được giải quyết

| Nếu không có | Hậu quả cụ thể |
|---|---|
| `clientOrderId` do client sinh | Client timeout retry ⇒ lệnh mới hoàn toàn ⇒ khách mua hai lần |
| Redis lớp 1 | Mọi retry xuống DB ⇒ tải cao, chậm |
| `UNIQUE(client_order_id)` | Redis chết ⇒ không gì chặn trùng |
| `processed_event` + `ON CONFLICT` | Relay gửi lại / Kafka redelivery ⇒ **khớp lệnh hai lần** |
| Cùng transaction cho (1)(2)(3) | Hoặc lệnh bị nuốt vĩnh viễn, hoặc khớp hai lần |
| State machine ở order-service | Execution report trùng ghi đè trạng thái cuối |
| `UNIQUE` trên `execution` (⚠️ **thiếu**) | `processed_event` là lớp duy nhất, không có lưới an toàn |
| Hợp đồng retention ↔ lịch dọn (⚠️ **chưa cưỡng chế**) | Replay từ offset cũ ⇒ khớp hai lần |

---

## 5. Đánh đổi

### 5.1. Ưu

1. **Effectively-once thật sự**, chứng minh được bằng test tự động.
2. **Nhiều lớp độc lập** — mỗi lớp phía trên chết mà hệ vẫn đúng.
3. **Nguyên tử do database đảm bảo**, không cần khoá ứng dụng.
4. **State machine = idempotency miễn phí** ở order-service.
5. **Degrade gracefully** — Redis chết chỉ làm chậm.

### 5.2. Nhược

1. **`processed_event` phình vô hạn** — và việc dọn nó là vấn đề *tính đúng đắn*, không chỉ dung lượng.
2. ⚠️ **`execution.client_order_id` thiếu `UNIQUE`** (Gap #15) — không có lưới an toàn.
3. ⚠️ **Hợp đồng retention ↔ lịch dọn không ai cưỡng chế** (Gap #12).
4. **Redis TTL 10 phút** tạo khoảng trống (tuy hành vi vẫn đúng, chỉ chậm hơn).
5. ⚠️ **`orElseThrow()` che lỗi gốc** (Bug #4).
6. **Mỗi message tốn một INSERT thêm** vào `processed_event`.
7. **Idempotency chỉ theo `clientOrderId`** — nếu nghiệp vụ sau này cần nhiều execution cho một lệnh (partial fill), khoá này không còn đủ.

---

## 6. Phương án thay thế

| Phương án | Nguyên lý | Khi nào hợp | Vì sao repo không chọn |
|---|---|---|---|
| **Kafka EOS** (`transactional.id`) | Nguyên tử consume-process-produce trong Kafka | Luồng Kafka → Kafka thuần | Không bao gồm PostgreSQL ⇒ vẫn dual-write ở ranh giới DB |
| **Dedupe bằng Redis `SETNX`** | `SET key NX EX ttl` làm khoá phân tán | Khi không có DB trong luồng | Redis mất dữ liệu khi failover ⇒ không đủ bền cho tiền |
| **Bloom filter** | Cấu trúc xác suất, bộ nhớ cực nhỏ | Hàng tỉ khoá, chấp nhận false positive | **False positive = bỏ qua một lệnh thật.** Tuyệt đối không dùng cho tiền. |
| **Dedupe theo offset Kafka** | Lưu offset đã xử lý | Khi message có thứ tự tuyệt đối | Rebalance làm offset đổi partition; relay gửi lại tạo offset mới cho cùng event |
| **Natural idempotency** (operation tự nó idempotent) | Thiết kế thao tác sao cho lặp lại vô hại, ví dụ `SET balance = 100` thay vì `balance += 10` | Khi nghiệp vụ cho phép | "Khớp lệnh" vốn không idempotent — nó tạo một `execution` mới mỗi lần |
| **`UNIQUE` trên bảng nghiệp vụ** (không có `processed_event`) | Để constraint tự chặn | Khi mỗi message sinh đúng một row | 🔑 **Nên làm THÊM** (Gap #15), nhưng không thay thế được: message bị từ chối bởi `TradingRules` **không** sinh row `execution` nào, nên không có gì để constraint chặn ⇒ report REJECTED sẽ được phát nhiều lần |

💡 Dòng cuối cùng là lý do `processed_event` **không thể** bị thay bằng `UNIQUE` trên `execution`. Hai thứ bổ sung nhau: `processed_event` phủ **mọi** message (kể cả message bị từ chối), `UNIQUE` phủ **mọi** execution (kể cả khi `processed_event` hỏng).

---

## 7. Núm vặn cấu hình

| Tham số | Nơi khai | Mặc định | Ghi chú |
|---|---|---|---|
| `IDEM_TTL` | `OrderService.java:39` | 10 phút (hardcode) | Dài ⇒ Redis phình; ngắn ⇒ nhiều retry xuống DB |
| `spring.data.redis.timeout` | `application.yml` | 200ms | Trần cho phần tăng tốc |
| `management.health.redis.enabled` | `application.yml` | `false` | 🔑 `true` ⇒ Redis chết kéo sập readiness |
| `TOPIC_RETENTION_MS` | `trading.env` | 7 ngày | 🔑 **Hợp đồng** với lịch dọn `processed_event` |
| Lịch dọn `processed_event` | **chưa có** | — | ⚠️ Phải giữ **lâu hơn** retention topic |
| `auto.offset.reset` | `matching/KafkaConfig.java:41` | `earliest` | Lệnh không được bỏ sót |

---

## 8. Cách kiểm chứng

```bash
API='-H Content-Type:application/json -H X-API-KEY:demo-key'
PSQL='docker compose exec postgres psql -U trading -d trading'

# 1. Gửi trùng 5 lần -> cùng một orderId
for i in $(seq 1 5); do
  curl -s -X POST http://localhost:8083/api/v1/orders $API \
    -d '{"clientOrderId":"IDEM-01","symbol":"HPG","side":"BUY","price":28500,"quantity":100,"accountId":"ACC001"}' \
    | jq -r .orderId
done
#  -> 5 dòng GIỐNG HỆT NHAU

# 2. Đúng 1 row trong orders
$PSQL -c "SELECT count(*) FROM ordersvc.orders WHERE client_order_id = 'IDEM-01';"   # -> 1

# 3. Đúng 1 execution
sleep 3
curl -s http://localhost:8084/api/v1/executions/IDEM-01 | jq 'length'                 # -> 1

# 4. Sổ processed_event
$PSQL -c "SELECT * FROM matchsvc.processed_event WHERE client_order_id = 'IDEM-01';"

# 5. Metric dedupe
curl -s http://localhost:8083/actuator/prometheus | grep orders_duplicate_total
curl -s http://localhost:8084/actuator/prometheus | grep matching_duplicate_skipped_total

# 6. Chứng minh Redis chỉ là cache — tắt Redis, dedupe VẪN hoạt động
docker compose stop redis
for i in 1 2 3; do
  curl -s -X POST http://localhost:8083/api/v1/orders $API \
    -d '{"clientOrderId":"IDEM-NOREDIS","symbol":"HPG","side":"BUY","price":28500,"quantity":100,"accountId":"ACC001"}' \
    | jq -r .orderId
done
#  -> vẫn 3 dòng giống nhau (nhờ UNIQUE constraint)
$PSQL -c "SELECT count(*) FROM ordersvc.orders WHERE client_order_id = 'IDEM-NOREDIS';"  # -> 1
docker compose logs --tail=5 order-service | grep "Redis khong kha dung"
docker compose start redis

# 7. Bơm TRỰC TIẾP vào Kafka 5 bản sao (bỏ qua order-service hoàn toàn)
#    -> chứng minh lớp 3 độc lập với lớp 1 và 2
MSG='{"orderId":"direct-1","clientOrderId":"KAFKA-DUP-01","status":"ACCEPTED","reason":null,
      "symbol":"HPG","side":"BUY","price":28500.0,"quantity":100,
      "refPrice":28450.0,"riskLevel":"LOW","timestamp":1764057600000}'
for i in $(seq 1 5); do echo "$MSG"; done | \
  docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:19092 --topic stock.orders \
    --property "parse.key=true" --property "key.separator=|" 2>/dev/null || \
  for i in $(seq 1 5); do
    echo "$MSG" | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
      --bootstrap-server localhost:19092 --topic stock.orders
  done
sleep 5
curl -s http://localhost:8084/api/v1/executions/KAFKA-DUP-01 | jq 'length'    # -> 1
$PSQL -c "SELECT count(*) FROM matchsvc.execution WHERE client_order_id = 'KAFKA-DUP-01';"

# 8. ⚠️ Chứng minh Gap #15: execution KHÔNG có UNIQUE
$PSQL -c "\d matchsvc.execution"
#  -> chỉ có PK trên id, KHÔNG có uq trên client_order_id
$PSQL -c "SELECT conname, pg_get_constraintdef(oid) FROM pg_constraint
          WHERE conrelid = 'matchsvc.execution'::regclass;"

# 9. ⚠️ Mô phỏng kịch bản Gap #12: xoá processed_event rồi replay
#    (CHỈ LÀM Ở MÔI TRƯỜNG THỬ NGHIỆM)
$PSQL -c "DELETE FROM matchsvc.processed_event WHERE client_order_id = 'KAFKA-DUP-01';"
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:19092 --group matching-engine-group \
  --reset-offsets --to-earliest --topic stock.orders --execute
#    (phải dừng matching-engine trước khi reset)
#  -> sau khi khởi động lại, execution của KAFKA-DUP-01 sẽ có 2 row

# 10. Chạy test tự động
mvn -pl matching-engine test -Dtest=IdempotentConsumerIT
```

---

## 9. Hạn chế đã biết

| # | Loại | Vị trí | Vấn đề | Hướng sửa |
|:--:|---|---|---|---|
| 15 | **Gap** | `matching/entity/ExecutionEntity.java:24-25` | `client_order_id` **không** có `unique` ⇒ `processed_event` là lớp chặn khớp-trùng duy nhất, không có lưới an toàn (khác hẳn `OrderEntity.java:27-28`) | Thêm `unique = true` + migration `V3`. Khi đó Gap #12 chỉ còn là chuyện dung lượng |
| 12 | **Gap** | `matchsvc.processed_event` | Bảng phình vô hạn — nhưng dọn **sớm hơn retention của `stock.orders`** sẽ mở cửa cho khớp lệnh hai lần khi replay | Job dọn theo `processed_at`, giữ **lâu hơn** `TOPIC_RETENTION_MS`; thêm comment chéo ở cả hai nơi |
| 4 | **Bug** | `order/service/OrderService.java:97` | `orElseThrow()` biến mọi `DataIntegrityViolationException` khác thành `NoSuchElementException`, che lỗi gốc | `orElseThrow(() -> dup)` hoặc kiểm tra tên constraint |
| — | Ops | `OrderService.java:39` | `IDEM_TTL` hardcode | `@Value` |
| — | Thiết kế | `OrderStateMachine` | Lớp idempotency miễn phí **mất** khi thêm `PARTIALLY_FILLED` self-loop | Thêm `execId` vào `ExecutionReport` và dedupe theo nó |

---

## 10. Câu hỏi phỏng vấn

**Q: Exactly-once có tồn tại không?**

A: Không, ở tầng vận chuyển trên mạng không đáng tin thì không — đó là hệ quả của Two Generals Problem. Người gửi không bao giờ biết chắc ack bị mất trước hay sau khi người nhận ghi thành công, nên chỉ có hai lựa chọn: không gửi lại (at-most-once, có thể mất) hoặc gửi lại (at-least-once, có thể trùng). Thứ thực tế đạt được gọi là **effectively-once**: at-least-once delivery cộng với idempotent processing. Nghĩa là ta chấp nhận message tới nhiều lần nhưng làm cho lần thứ hai không gây tác dụng phụ. Lưu ý Kafka có "exactly-once semantics" — nhưng phạm vi của nó chỉ trong Kafka (consume-process-produce-commit), không bao gồm một database bên ngoài.

**Q: Vì sao `INSERT ... ON CONFLICT DO NOTHING` chứ không phải kiểm tra tồn tại rồi insert?**

A: Vì `if (exists) return; else insert;` là check-then-act — không nguyên tử. Hai pod nhận bản sao của cùng một message: cả hai `exists` trả false vì cả hai chạy trước khi bên kia insert, cả hai đi tiếp, và lệnh được khớp hai lần. `INSERT ... ON CONFLICT DO NOTHING` là **atomic test-and-set do chính database thực hiện** — PostgreSQL bảo đảm trong hai insert đồng thời cho cùng khoá, đúng một cái chèn được và trả `1`, cái kia trả `0`. Không cần khoá ứng dụng, không có cửa sổ race. Và quan trọng không kém: nó nằm trong **cùng transaction** với việc ghi execution, nên không tồn tại trạng thái "đã ghi sổ mà chưa xử lý" hay ngược lại.

**Q: Hệ thống có ba lớp dedupe. Nếu phải bỏ một lớp, bạn bỏ cái nào?**

A: Redis — lớp 1. Nó là lớp duy nhất có thể chết mà hệ vẫn **đúng**; bỏ nó chỉ làm chậm hơn và tăng tải DB. Hai lớp kia thì không bỏ được: `UNIQUE(client_order_id)` là nguồn sự thật chống lệnh trùng, và `processed_event` là thứ biến Outbox từ một cơ chế sai thành một cơ chế đúng. Thực ra câu hỏi đáng quan tâm hơn là ở chiều ngược lại: hệ **đang thiếu** một lớp — `execution.client_order_id` không có `UNIQUE`, nên ở matching-engine `processed_event` là lớp chặn duy nhất, không có lưới an toàn. Đó là thứ tôi sẽ thêm trước khi bỏ bất cứ gì.

**Q: Bảng `processed_event` phình mãi. Bạn dọn thế nào?**

A: Và đây chính là chỗ bẫy — dọn bảng này là vấn đề **tính đúng đắn**, không phải dung lượng. Có một hợp đồng ngầm: thời gian giữ `processed_event` phải **dài hơn** retention của topic `stock.orders` (7 ngày). Nếu dọn sau 2 ngày mà message vẫn còn trong topic tới ngày 7, thì một lần reset offset về `earliest` — chuyện hoàn toàn bình thường khi điều tra sự cố — sẽ làm hệ khớp lệnh hai lần, vì bản ghi chống trùng đã bị xoá. Nên job dọn phải giữ ít nhất 8-10 ngày. Và trước khi viết job đó, tôi sẽ thêm `UNIQUE` trên `execution.client_order_id`: khi có lưới an toàn thứ hai, dọn sớm chỉ gây xử lý lại chứ không gây khớp hai lần, và lịch dọn trở lại thành chuyện dung lượng thuần tuý.

**Q: Trong `IdempotentConsumerIT` có hai lần `await()`. Lần thứ hai để làm gì?**

A: Lần đầu `await().atMost(30s)` chờ tới khi có đúng 1 execution — nó chứng minh "đã xử lý xong". Lần hai `await().during(3s)` khẳng định điều kiện **đúng liên tục suốt 3 giây** — nó chứng minh "không có execution thứ hai xuất hiện muộn". Thiếu lần hai thì test pass sai: nó chỉ nói rằng tại một thời điểm nào đó có 1 row, không nói rằng message thứ 2 và 3 không tạo thêm row sau đó. Đây là mẫu chung cho mọi assertion về *sự vắng mặt* trong hệ bất đồng bộ — bạn không thể chứng minh một thứ không xảy ra bằng cách kiểm tra một lần.

**Q: Dùng Bloom filter cho dedupe thì sao? Nó nhẹ hơn nhiều.**

A: Tuyệt đối không, với tiền. Bloom filter có false positive — nó có thể nói "đã thấy khoá này" với một khoá chưa từng thấy. Trong ngữ cảnh này, false positive nghĩa là **bỏ qua một lệnh thật của khách hàng**, và lệnh đó biến mất không dấu vết: không khớp, không từ chối, không log gì bất thường. Bloom filter hợp cho những bài toán mà false positive chỉ gây thêm một lần kiểm tra chậm (ví dụ: lọc trước khi truy vấn cache), không hợp cho bài toán mà false positive gây mất dữ liệu.

---

## 11. Bảng tổng kết

| Lớp | Cơ chế | Nguồn bản sao chặn được | Có thể chết? | Chi phí |
|:--:|---|---|:--:|---|
| 0 | `clientOrderId` do client sinh | — (điều kiện tiên quyết của mọi lớp) | ❌ | — |
| 0 | `enable.idempotence=true` | Producer retry khi mất ack | ❌ | ~0 |
| 1 | Redis `GET idem:{id}`, TTL 10′ | Client retry | ✅ **Có** | ~0,5ms |
| 2 | `UNIQUE(client_order_id)` trên `orders` | Client retry, Redis miss, đa pod | ❌ **Không** | ~3ms |
| 3 | `INSERT processed_event ON CONFLICT DO NOTHING` | Relay gửi lại, Kafka redelivery | ❌ **Không** | ~1ms |
| 3b | ⚠️ `UNIQUE` trên `execution` — **THIẾU** | Lưới an toàn khi lớp 3 hỏng | — | ~0 |
| 4 | `OrderStateMachine` trạng thái cuối | Execution report trùng | ❌ | ~0 (miễn phí) |

| Nguyên tắc | Phát biểu |
|---|---|
| 🔑 1 | Exactly-once không tồn tại. Effectively-once = at-least-once + idempotent processing. |
| 🔑 2 | Cache chỉ tăng tốc; **DB constraint là nguồn sự thật của dedupe**. |
| 🔑 3 | Test-and-set phải **nguyên tử** và phải nằm **trong cùng transaction** với tác dụng phụ. |
| 🔑 4 | Mỗi lớp phía trên phải chết được mà hệ vẫn đúng. |
| 🔑 5 | Thời gian giữ sổ dedupe phải **dài hơn** retention của topic. |
| 🔑 6 | Không bao giờ dùng cấu trúc xác suất (Bloom filter) cho dedupe tiền tệ. |

> **Đọc tiếp:** [Chương 07 — Matching Engine, Trading Rules & Saga Choreography](./07-matching-engine-va-saga-choreography.md) — nơi ba lớp này được ghép vào một vòng đời lệnh khép kín.
