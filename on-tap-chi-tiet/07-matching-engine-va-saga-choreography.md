# Chương 07 — Matching Engine, Trading Rules & Saga Choreography

> **Câu hỏi trung tâm:** làm sao khép kín vòng đời một lệnh qua hai service không chia sẻ transaction, và làm sao biết khi vòng đó bị đứt?
>
> **Tài liệu đối chiếu:** `DESIGN-NOTES.md` mục 11-12, `DEEP-DIVE.md` mục 9.

---

## 1. Nó là gì?

### 1.1. Saga Choreography

**Saga** là chuỗi các transaction cục bộ, mỗi transaction thuộc về một service, nối với nhau bằng event. Không có transaction toàn cục; tính đúng đắn đạt được bằng cách mỗi bước phản ứng với event của bước trước.

Hai biến thể:

| | **Choreography** (repo này) | Orchestration |
|---|---|---|
| Điều phối | Không có ai điều phối. Mỗi service tự phản ứng với event. | Một orchestrator ra lệnh cho từng bước |
| Logic saga nằm ở | Phân tán trong các service | Tập trung một chỗ |
| Nhìn thấy toàn cảnh | Khó — phải ghép từ log + trace | Dễ |
| Thêm bước mới | Sửa nhiều service | Sửa orchestrator |
| Điểm hỏng tập trung | Không có | Có |
| Phù hợp | **2-4 bước** | > 5 bước, có bồi hoàn phức tạp |

### 1.2. Vòng saga của hệ thống này

```
┌──────────────────────────────────────────────────────────────────────┐
│                                                                        │
│  order-service                                        matching-engine  │
│  ─────────────                                        ───────────────  │
│                                                                        │
│  T1: @Transactional                                                    │
│      INSERT orders (ACCEPTED)                                          │
│      INSERT outbox_event ──────┐                                       │
│                                 │                                      │
│                       stock.orders                                     │
│                                 │                                      │
│                                 └──────────► T2: @Transactional        │
│                                                  INSERT processed_event │
│                                                  INSERT execution      │
│                                                  INSERT outbox_event ──┐│
│                                                                        ││
│                                              stock.executions          ││
│                                                                        ││
│  T3: @Transactional ◄──────────────────────────────────────────────────┘│
│      UPDATE orders                                                      │
│        SET status = FILLED | REJ_MATCH                                   │
│        WHERE id = ? AND version = ?                                      │
│                                                                          │
└──────────────────────────────────────────────────────────────────────┘

3 transaction cục bộ · 2 topic Kafka · 2 bảng outbox · 0 transaction toàn cục
```

🔑 **Điểm then chốt: mỗi transaction cục bộ đều bao gồm cả việc ghi outbox event của bước kế tiếp.** Nhờ vậy không có khe hở nào giữa "làm xong việc" và "báo cho bước sau biết".

### 1.3. Matching engine — phạm vi mô phỏng

```java
// matching-engine/src/main/java/com/trading/matching/entity/ExecutionEntity.java:11
/** Kết quả khớp lệnh (giả lập fill toàn bộ tại giá tham chiếu). */
```

**Những gì nó LÀ:** idempotent consumer, nơi thi hành luật sàn, nguồn phát execution report, một transaction nguyên tử ba việc.

**Những gì nó KHÔNG LÀ:** một matching engine thật. Không có:
- **Limit order book** (sổ lệnh chờ theo mức giá) — lệnh khớp ngay tại `refPrice`, không chờ đối ứng.
- **Khớp một phần** (partial fill) — luôn khớp toàn bộ hoặc từ chối toàn bộ.
- **Thuật toán ưu tiên** (price-time priority) — không có hàng đợi để xếp thứ tự.
- **Các loại lệnh** (ATO/ATC/MP/MTL) — chỉ có lệnh giới hạn khớp ngay.
- **Đối ứng mua/bán** — một lệnh BUY không cần lệnh SELL nào tồn tại.

Đó là lựa chọn có chủ đích: mục tiêu của repo là minh hoạ **pattern phân tán**, không phải nghiệp vụ khớp lệnh. Một matching engine thật sẽ là một process đơn luồng trên bare metal với LMAX Disruptor (xem [chương 01](./01-kien-truc-tong-the-va-microservices-boundary.md) mục 6.2).

---

## 2. Cơ chế hoạt động

### 2.1. Consumer — thứ tự commit quyết định ngữ nghĩa

```java
// matching-engine/src/main/java/com/trading/matching/consumer/OrderConsumer.java:17-24
@KafkaListener(topics = "stock.orders",
               groupId = "matching-engine-group",
               containerFactory = "orderListenerFactory")
public void onOrder(OrderResult order) {
    matchingService.process(order);
    // Offset commit SAU khi transaction DB commit (listener trả về) ->
    // crash giữa chừng chỉ gây redelivery, dedupe lo phần còn lại.
}
```

🔑 **Thứ tự này là tất cả.** Spring Kafka commit offset *sau khi* listener trả về bình thường. Vì `process()` là `@Transactional`, transaction DB commit trước, rồi offset mới được commit.

| Thứ tự | Crash giữa chừng gây ra |
|---|---|
| **DB commit → offset commit** (hiện tại) | Redelivery ⇒ dedupe lo ⇒ **an toàn** |
| Offset commit → DB commit | 🔴 **Mất lệnh vĩnh viễn** — offset đã tiến, message không bao giờ được giao lại |

Và cấu hình consumer:

```java
// matching-engine/src/main/java/com/trading/matching/config/KafkaConfig.java:40-44
// KHÁC với cache enriched (latest): LỆNH không được bỏ sót -> earliest
props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
        CooperativeStickyAssignor.class.getName());
```

```java
// matching-engine/src/main/java/com/trading/matching/config/KafkaConfig.java:36-39
props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, OrderResult.class);
props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.trading.*");
props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
```

💡 Ba dòng cuối đáng giải thích:
- **`VALUE_DEFAULT_TYPE`** — nói cho deserializer biết deserialize thành class nào khi không có type header.
- **`USE_TYPE_INFO_HEADERS = false`** — 🔑 **bắt buộc** ở đây, vì message do `OutboxRelay` gửi bằng `StringSerializer`, **không có** header `__TypeId__`. Nếu để `true` (mặc định), deserializer tìm header không thấy và ném exception.
- **`TRUSTED_PACKAGES`** — phòng thủ chống deserialization gadget: chỉ cho phép deserialize class trong package được khai báo. Nếu để `*`, một message độc hại mang `__TypeId__` trỏ tới một class nguy hiểm trong classpath có thể gây thực thi mã. Đây là một lỗ hổng thật của Jackson polymorphic deserialization.

### 2.2. 🔑 Một transaction, ba việc

```java
// matching-engine/src/main/java/com/trading/matching/service/MatchingService.java:51-95
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
    if (processedRepo.insertIfAbsent(order.clientOrderId()) == 0) {
        duplicated.increment();
        log.info("Bo qua message trung clientOrderId={}", order.clientOrderId());
        return;
    }

    String violation = rules.validate(order);
    if (violation != null) {
        businessRejected.increment();
        log.info("Matching TU CHOI {}: {}", order.clientOrderId(), violation);
        emit(new ExecutionReport(order.orderId(), order.clientOrderId(), order.symbol(),
                order.side(), "REJECTED", violation, null, 0L, System.currentTimeMillis()));
        return;
    }

    BigDecimal fillPrice = order.refPrice() != null ? order.refPrice() : order.price();
    ExecutionEntity exec = new ExecutionEntity();
    exec.setOrderId(order.orderId());
    exec.setClientOrderId(order.clientOrderId());
    exec.setSymbol(order.symbol());
    exec.setSide(order.side());
    exec.setFillPrice(fillPrice);
    exec.setQuantity(order.quantity());
    exec.setExecTime(Instant.now());
    executionRepo.save(exec);

    matched.increment();
    log.info("KHOP LENH {} {} {} x{} @ {}", order.clientOrderId(),
            order.side(), order.symbol(), order.quantity(), fillPrice);

    emit(new ExecutionReport(order.orderId(), order.clientOrderId(), order.symbol(),
            order.side(), "FILLED", null, fillPrice, order.quantity(),
            System.currentTimeMillis()));
}
```

**Bảng hậu quả nếu tách transaction:**

| Phần bị tách ra ngoài | Kịch bản hỏng | Hậu quả |
|---|---|---|
| (1) `processed_event` | Commit riêng trước, rồi khớp thất bại | 🔴 Lệnh bị **nuốt vĩnh viễn** — redelivery thấy "đã xử lý" nên bỏ qua |
| (1) `processed_event` | Commit riêng sau, khớp xong rồi fail | 🔴 Redelivery **khớp lần hai** |
| (3) outbox report | Khớp xong, publish fail | 🔴 Lệnh **treo `ACCEPTED` vĩnh viễn**, tiền khách bị giữ cho giao dịch đã hoàn tất |
| (2) execution | Ghi sổ + báo FILLED nhưng không có execution | 🔴 Sổ sách lệch, không đối soát được |

🔑 **Nhánh REJECTED cũng phát report** (`:70-72`) — đây là điểm nhiều người bỏ sót khi tự cài saga. Nếu chỉ phát report khi FILLED, thì mọi lệnh bị từ chối bởi luật sàn sẽ treo `ACCEPTED` mãi mãi. **Saga phải đóng được cả ở nhánh thất bại.**

💡 `fillPrice = order.refPrice() != null ? order.refPrice() : order.price()` — ưu tiên giá tham chiếu đã đóng băng lúc nhận lệnh. Fallback về `price()` chỉ xảy ra nếu lệnh đến từ nguồn không qua `order-service` (ví dụ bơm thẳng vào Kafka khi test).

### 2.3. Trading Rules — ranh giới tin cậy

```java
// matching-engine/src/main/java/com/trading/matching/service/TradingRules.java:12-22
/**
 * Luật giao dịch tại sàn - KHÁC với pre-trade check ở order-service:
 *
 *   order-service check   = bảo vệ KHÁCH HÀNG (fat-finger, data cũ, rủi ro cao)
 *   matching-engine check = luật của SÀN (biên độ, lô, phiên) - là nguồn sự thật
 *
 * Hai tầng cố tình trùng lặp một phần: order-service từ chối sớm cho nhanh,
 * nhưng KHÔNG được tin tưởng - client có thể gọi thẳng, hoặc tham số hai bên
 * lệch nhau. Sàn luôn kiểm tra lại. Đây là lý do saga cần chiều về:
 * lệnh qua được cửa 1 vẫn có thể chết ở cửa 2.
 */
```

```java
// matching-engine/src/main/java/com/trading/matching/service/TradingRules.java:25-62
@Value("${matching.price-band-pct:7}")
private BigDecimal priceBandPct;      // HOSE 7%, HNX 10%, phái sinh theo VN30

@Value("${matching.lot-size:100}")
private long lotSize;                 // cổ phiếu lô chẵn 100; phái sinh = 1

@Value("${matching.session-check-enabled:false}")
private boolean sessionCheckEnabled;  // tắt mặc định để demo chạy 24/7

@Value("${matching.session-zone:Asia/Ho_Chi_Minh}")
private String sessionZone;

/** @return null nếu hợp lệ, ngược lại trả lý do từ chối. */
public String validate(OrderResult order) {
    if (order.quantity() <= 0 || order.quantity() % lotSize != 0) {
        return "INVALID_LOT_SIZE must be multiple of " + lotSize;
    }

    BigDecimal ref = order.refPrice() != null ? order.refPrice() : order.price();
    if (ref == null || ref.signum() <= 0) {
        return "NO_REFERENCE_PRICE";
    }
    BigDecimal deviation = order.price().subtract(ref).abs()
            .divide(ref, 6, RoundingMode.HALF_UP)
            .multiply(BigDecimal.valueOf(100));
    if (deviation.compareTo(priceBandPct) > 0) {
        return "PRICE_OUT_OF_BAND " + deviation.setScale(2, RoundingMode.HALF_UP)
                + "% > " + priceBandPct + "%";
    }

    if (sessionCheckEnabled && !inSession()) {
        return "MARKET_CLOSED";
    }
    return null;
}
```

```java
// matching-engine/src/main/java/com/trading/matching/service/TradingRules.java:64-69
private boolean inSession() {
    LocalTime now = ZonedDateTime.now(ZoneId.of(sessionZone)).toLocalTime();
    boolean morning = !now.isBefore(LocalTime.of(9, 0)) && !now.isAfter(LocalTime.of(11, 30));
    boolean afternoon = !now.isBefore(LocalTime.of(13, 0)) && !now.isAfter(LocalTime.of(14, 45));
    return morning || afternoon;
}
```

#### Ba luật và cơ sở nghiệp vụ

| Luật | Mặc định | Cơ sở thực tế |
|---|---|---|
| **Lô chẵn** | bội số 100 | HOSE giao dịch lô chẵn 100 cổ phiếu (lô lẻ 1-99 có bảng riêng). Phái sinh (VN30F) đặt `lot-size: 1`. |
| **Biên độ giá** | ±7% | HOSE ±7%, HNX ±10%, UPCoM ±15%, ngày đầu niêm yết ±20%. Phái sinh theo quy định VN30. |
| **Phiên giao dịch** | tắt | HOSE: sáng 9:00-11:30, chiều 13:00-14:45 (ATC 14:30-14:45). `Asia/Ho_Chi_Minh`. |

💡 **`sessionCheckEnabled: false` mặc định** để demo chạy 24/7. Bật lên trong môi trường mô phỏng thật.

⚠️ `inSession()` **không kiểm tra ngày nghỉ** — thứ Bảy, Chủ nhật, lễ Tết. Một luật sàn đầy đủ cần lịch giao dịch (trading calendar), không chỉ khung giờ.

⚠️ Và nó tính giờ **tại thời điểm matching xử lý**, không phải tại thời điểm khách đặt lệnh. Một lệnh đặt lúc 14:44:59 nhưng nằm trong hàng đợi 2 giây sẽ bị từ chối `MARKET_CLOSED` — về nghiệp vụ là sai. Nếu bật kiểm tra phiên ở production, phải dùng `order.timestamp()` thay vì `now()`.

#### 🔑 Ranh giới tin cậy — vì sao trùng lặp là đúng

| | `order-service` pre-trade | `matching-engine` TradingRules |
|---|---|---|
| Bảo vệ | **Khách hàng** | **Sàn** |
| Luật | staleness > 10s, risk HIGH, lệch > 5% | lô chẵn 100, biên độ ±7%, phiên |
| Nguồn sự thật? | Không | **Có** |
| Được tin? | **Không** | Có |
| Đổi khi nào | Khẩu vị rủi ro của công ty chứng khoán đổi | Cơ quan quản lý đổi quy định |

Ba lý do matching **phải** kiểm lại dù order-service đã kiểm:

1. **Client có thể bỏ qua order-service.** Trong namespace Kubernetes, bất kỳ pod nào có quyền tới Kafka đều publish được vào `stock.orders`. NetworkPolicy chặn pod ngoài namespace, nhưng không chặn một service nội bộ bị chiếm quyền.
2. **Tham số hai bên cố ý lệch nhau.** `order.max-price-deviation-pct: 5` vs `matching.price-band-pct: 7`. Lệnh lệch 6% qua được cửa 1 và **hợp lệ** ở cửa 2 — đó là thiết kế, không phải lỗi: cửa 1 chặt hơn để bảo vệ khách, cửa 2 nới hơn vì đó mới là luật thật.
3. **order-service không biết luật sàn**, và không nên biết. Mỗi lần HOSE đổi biên độ mà phải deploy service nhận lệnh là một kiến trúc sai.

🔑 **Và đây chính là lý do saga cần chiều về:** lệnh qua cửa 1 vẫn có thể chết ở cửa 2. Không có đường phản hồi thì `order-service` không bao giờ biết.

⚠️ Một hệ quả phải chấp nhận: nếu `order.max-price-deviation-pct` bị đặt **lớn hơn** `matching.price-band-pct` (ví dụ 10 vs 7), thì hàng loạt lệnh sẽ qua cửa 1 rồi chết ở cửa 2 — client nhận `201 ACCEPTED` rồi 2 giây sau thành `REJ_MATCH`. Về kỹ thuật không sai, nhưng về trải nghiệm là tệ. **Bất biến cần giữ:** `order.max-price-deviation-pct ≤ matching.price-band-pct`. Đây lại là một ràng buộc giữa hai con số ở hai file khác nhau, hiện không ai cưỡng chế.

### 2.4. Phát execution report

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

Hợp đồng dữ liệu:

```java
// common-contracts/src/main/java/com/trading/common/model/ExecutionReport.java:5-23
/**
 * Kết quả xử lý lệnh do matching-engine phát ngược về (topic stock.executions).
 * Đây là "reply leg" của SAGA CHOREOGRAPHY:
 *   order-service --stock.orders--> matching-engine --stock.executions--> order-service
 *
 * status: FILLED | REJECTED  (REJECTED = matching từ chối vì luật giao dịch,
 * khác với REJECTED lúc pre-trade check ở order-service).
 */
public record ExecutionReport(
        String orderId,
        String clientOrderId,
        String symbol,
        String side,
        String status,
        String reason,
        BigDecimal fillPrice,
        Long filledQty,
        long execTime
) {}
```

💡 `msgKey = symbol` **cùng khoá với `stock.orders`** — có chủ đích. Nếu một mã có nhiều lệnh, chúng được khớp theo thứ tự và report cũng về theo thứ tự. Với mô hình hiện tại (một lệnh một report) điều này chưa tạo khác biệt, nhưng khi có partial fill thì thứ tự report cho cùng một lệnh trở thành bắt buộc.

⚠️ `ExecutionReport` **không có `execId`** — không có định danh duy nhất cho chính bản report. Với mô hình một-report-một-lệnh thì `clientOrderId` đủ làm khoá dedupe. Ngay khi hỗ trợ partial fill (nhiều report cho một lệnh), cần thêm `execId` để dedupe, vì `OrderStateMachine` lúc đó không còn cung cấp idempotency miễn phí nữa.

### 2.5. order-service đóng vòng

#### Retry ngoài transaction

```java
// order-service/src/main/java/com/trading/order/consumer/ExecutionReportConsumer.java:22-41
@KafkaListener(topics = "stock.executions",
               groupId = "order-service-exec-group",
               containerFactory = "executionListenerFactory")
public void onExecutionReport(ExecutionReport report) {
    // Retry vòng ngoài (không trong @Transactional) vì transaction đã rollback
    // rồi mới ném exception - phải mở transaction MỚI để thử lại.
    for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
        try {
            orderStatusService.apply(report);
            return;
        } catch (OptimisticLockingFailureException ex) {
            log.warn("Xung dot optimistic lock lan {} cho clientOrderId={}",
                    attempt, report.clientOrderId());
            if (attempt == MAX_ATTEMPTS) {
                // Ném ra -> error handler retry rồi đẩy DLT (đã cấu hình ở container factory)
                throw ex;
            }
        }
    }
}
```

🔑 **Vì sao retry phải ở ngoài `@Transactional`.** Khi Hibernate ném `OptimisticLockingFailureException`, transaction hiện tại đã bị đánh dấu **rollback-only**. Retry *bên trong* cùng transaction đó sẽ nhận `UnexpectedRollbackException: Transaction silently rolled back because it has been marked as rollback-only` — một lỗi tốn nhiều giờ để hiểu nếu chưa gặp. Phải thoát ra, để transaction cũ kết thúc, rồi mở transaction **mới** đọc lại dữ liệu đã cập nhật.

⚠️ Comment nói "ném ra → error handler retry rồi đẩy DLT (đã cấu hình ở container factory)". Nhưng `executionListenerFactory` (`order-service/config/KafkaConfig.java:97-104`) **không** đặt `setCommonErrorHandler` với `DeadLetterPublishingRecoverer` — chỉ `pricing-risk-service` có. Thực tế: exception được `DefaultErrorHandler` mặc định xử lý (retry 10 lần không backoff, rồi log và bỏ qua). Topic `stock.executions.DLT` được Job tạo ra nhưng **không ai ghi vào**. Đây là comment mô tả một hành vi chưa được cài đặt.

#### Áp trạng thái

```java
// order-service/src/main/java/com/trading/order/service/OrderStatusService.java:32-75
/**
 * Áp execution report lên lệnh. @Version lo chuyện hai report tới song song:
 * pod thua cuộc ném OptimisticLockingFailureException -> consumer retry.
 */
@Transactional
public void apply(ExecutionReport report) {
    Optional<OrderEntity> found = repository.findByClientOrderId(report.clientOrderId());
    if (found.isEmpty()) {
        // Về lý thuyết không xảy ra: lệnh được COMMIT trước khi outbox event ra Kafka.
        // Log để phát hiện nếu ai đó publish thẳng vào stock.orders (bỏ qua outbox).
        log.warn("Nhan execution report cho lenh khong ton tai clientOrderId={}",
                report.clientOrderId());
        ignored.increment();
        return;
    }

    OrderEntity order = found.get();
    String target = "FILLED".equals(report.status())
            ? OrderStateMachine.FILLED
            : OrderStateMachine.REJECTED_BY_MATCHING;

    if (!OrderStateMachine.canTransition(order.getStatus(), target)) {
        // Report trùng (redelivery) hoặc đến muộn -> bỏ qua, KHÔNG phải lỗi
        log.debug("Bo qua report: {} -> {} khong hop le (clientOrderId={})",
                order.getStatus(), target, report.clientOrderId());
        ignored.increment();
        return;
    }

    order.setStatus(target);
    order.setReason(report.reason());
    order.setFilledQty(report.filledQty());
    order.setAvgFillPrice(report.fillPrice());
    order.setUpdatedAt(Instant.now());

    if (OrderStateMachine.FILLED.equals(target)) {
        filled.increment();
        log.info("Lenh {} da KHOP {} @ {}", report.clientOrderId(),
                report.filledQty(), report.fillPrice());
    } else {
        rejectedByMatching.increment();
        log.info("Lenh {} bi matching TU CHOI: {}", report.clientOrderId(), report.reason());
    }
}
```

💡 **Nhánh `found.isEmpty()` là một cảm biến, không phải xử lý lỗi.** Về lý thuyết nó không bao giờ chạy: lệnh được COMMIT (cùng transaction với outbox event) trước khi event ra Kafka. Nếu counter `orders_report_ignored_total` tăng ở nhánh này, nghĩa là **có ai đó publish thẳng vào `stock.orders` bỏ qua order-service** — đúng thứ mà `TradingRules` tồn tại để phòng. Đây là một mẫu hay: *viết log/metric cho nhánh "không thể xảy ra" để biết khi nó xảy ra*.

⚠️ Lưu ý: cùng một counter `ignored` được tăng cho **hai** tình huống rất khác nhau — "lệnh không tồn tại" (bất thường, cần điều tra) và "report trùng" (hoàn toàn bình thường). Nên tách thành hai counter, nếu không alert không phân biệt được.

#### 🔑 State machine — một cơ chế, hai công dụng

```java
// order-service/src/main/java/com/trading/order/service/OrderStateMachine.java:3-37
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
public final class OrderStateMachine {

    public static final String ACCEPTED = "ACCEPTED";
    public static final String FILLED = "FILLED";
    public static final String REJECTED = "REJECTED";
    public static final String REJECTED_BY_MATCHING = "REJ_MATCH";

    private static final Map<String, Set<String>> ALLOWED = Map.of(
            ACCEPTED, Set.of(FILLED, REJECTED_BY_MATCHING),
            FILLED, Set.of(),
            REJECTED, Set.of(),
            REJECTED_BY_MATCHING, Set.of()
    );

    private OrderStateMachine() {}

    public static boolean canTransition(String from, String to) {
        return ALLOWED.getOrDefault(from, Set.of()).contains(to);
    }
}
```

Đồ thị trạng thái:

```
                  ┌─────────────┐
                  │  (tạo mới)   │
                  └──┬───────┬──┘
     pre-trade fail  │       │  pre-trade pass
                     ▼       ▼
             ┌──────────┐  ┌──────────┐
             │ REJECTED │  │ ACCEPTED │
             │ (cửa 1)  │  └─┬──────┬─┘
             └──────────┘    │      │
            trạng thái cuối   │      │
                    ┌─────────┘      └─────────┐
          FILLED    ▼                          ▼  REJECTED (luật sàn)
             ┌──────────┐              ┌───────────────┐
             │  FILLED  │              │   REJ_MATCH   │
             └──────────┘              └───────────────┘
            trạng thái cuối             trạng thái cuối
```

Mọi trạng thái cuối có `Set.of()` — **không có đường ra**. Nhờ vậy:

| Công dụng | Cơ chế |
|---|---|
| Chặn transition vô lý | `FILLED → ACCEPTED` bị chặn; `REJECTED → FILLED` bị chặn |
| **Idempotency miễn phí** | Report lần hai thấy trạng thái cuối ⇒ `canTransition` false ⇒ bỏ qua |

🔑 **Một `Map` 6 dòng thay thế được cả một bảng `processed_event` và một chỉ mục** ở phía order-service.

⚠️ **Nhưng đó là đánh đổi có hậu quả thật.** Để hỗ trợ khớp một phần, cần:

```java
ACCEPTED,          Set.of(PARTIALLY_FILLED, FILLED, REJECTED_BY_MATCHING),
PARTIALLY_FILLED,  Set.of(PARTIALLY_FILLED, FILLED, CANCELLED),   // ← self-loop
```

Ngay khi có self-loop, `canTransition(PARTIALLY_FILLED, PARTIALLY_FILLED)` trả `true` ⇒ **lớp idempotency biến mất** ⇒ report trùng sẽ cộng dồn `filledQty` lần thứ hai ⇒ khách thấy mình khớp 200 trong khi chỉ khớp 100. Khi đó bắt buộc phải thêm `execId` vào `ExecutionReport` và một bảng dedupe thật ở order-service.

💡 Đây là ví dụ rất tốt cho câu "một tính năng tưởng nhỏ kéo theo thay đổi kiến trúc": thêm partial fill không chỉ là thêm một trạng thái.

#### Optimistic lock

```java
// order-service/src/main/java/com/trading/order/entity/OrderEntity.java:70-80
/**
 * OPTIMISTIC LOCKING: hai execution report tới song song trên 2 pod
 * cùng UPDATE một lệnh -> pod thua cuộc nhận OptimisticLockingFailureException
 * và retry trên dữ liệu mới, thay vì âm thầm ghi đè (lost update).
 * Chọn optimistic thay vì pessimistic vì xung đột là HIẾM - khoá bi quan
 * sẽ trả giá lock trên mọi update chỉ để phòng trường hợp hiếm.
 */
@Version
@Column(nullable = false)
private Long version;
```

```sql
-- order-service/src/main/resources/db/migration/V2__order_state_machine.sql
ALTER TABLE orders ADD COLUMN version        BIGINT        NOT NULL DEFAULT 0;
ALTER TABLE orders ADD COLUMN updated_at     TIMESTAMPTZ;
ALTER TABLE orders ADD COLUMN filled_qty     BIGINT;
ALTER TABLE orders ADD COLUMN avg_fill_price NUMERIC(18,4);

-- Truy vấn vận hành hay dùng: "lệnh nào đang treo chưa có phản hồi khớp?"
CREATE INDEX idx_orders_pending ON orders (created_at) WHERE status = 'ACCEPTED';
```

SQL mà Hibernate sinh ra:

```sql
UPDATE ordersvc.orders
SET status = ?, reason = ?, filled_qty = ?, avg_fill_price = ?, updated_at = ?,
    version = 6
WHERE id = ? AND version = 5;
```

Nếu `rowsAffected = 0` ⇒ ai đó đã sửa row (version không còn là 5) ⇒ ném `OptimisticLockingFailureException`.

| | Optimistic (`@Version`) | Pessimistic (`SELECT FOR UPDATE`) |
|---|---|---|
| Chi phí khi không xung đột | **0** | Một khoá row trên **mọi** update |
| Chi phí khi xung đột | Retry (đọc lại + ghi lại) | Chờ |
| Nguy cơ deadlock | Không | Có |
| Hợp khi | **Xung đột hiếm** | Xung đột thường xuyên |

Ở đây xung đột đòi hỏi **hai execution report cho cùng một lệnh tới gần như đồng thời trên hai pod** — rất hiếm. Optimistic là lựa chọn đúng.

🔑 **Partial index `idx_orders_pending`** là một chi tiết đẹp: nó chỉ chứa các lệnh đang `ACCEPTED` (tức đang trong saga, chưa đóng). Số đó luôn nhỏ. Query vận hành quan trọng nhất — *"lệnh nào đang treo?"* — chạy tức thì bất kể bảng `orders` có bao nhiêu triệu row.

---

## 3. 🔑 Phát hiện saga đứt — alert quan trọng nhất của hệ

```yaml
# monitoring/alert-rules.yml:25-36
# Saga đứt chiều về: lệnh ACCEPTED mãi không nhận được execution report.
# Đây là cảnh báo nghiêm trọng nhất trong hệ - nghĩa là khách bị giữ tiền
# cho một lệnh không bao giờ kết thúc.
- alert: SagaStuckOrders
  expr: (orders_accepted_total - (orders_filled_total + orders_rejected_by_matching_total)) > 10
  for: 2m
  labels: { severity: critical }
  annotations:
    summary: "Co lenh ACCEPTED khong nhan duoc execution report"
    description: "Kiem tra: outbox_publish_failed_total, matching-engine con song khong, consumer lag topic stock.orders/stock.executions"
```

**Cách công thức hoạt động:** ba counter đến từ ba chỗ trong mã:

| Counter | Tăng ở | Nghĩa |
|---|---|---|
| `orders_accepted_total` | `OrderService.java:102` | Lệnh vào saga |
| `orders_filled_total` | `OrderStatusService.java:68` | Saga đóng với FILLED |
| `orders_rejected_by_matching_total` | `OrderStatusService.java:72` | Saga đóng với REJ_MATCH |

Hiệu số = **số lệnh đang trong saga**. Ở trạng thái khoẻ mạnh, hiệu số dao động quanh 0 (lệnh vào rồi ra trong ~1 giây). Hiệu số > 10 kéo dài 2 phút = saga đang đứt.

Bốn nguyên nhân có thể, theo thứ tự nên kiểm tra:

```bash
# 1. Relay có publish được không?
curl -s http://localhost:8083/actuator/prometheus | grep outbox_publish_failed_total

# 2. matching-engine còn sống?
curl -s http://localhost:8084/actuator/health | jq -r .status

# 3. Lag của consumer group
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:19092 --describe \
  --group matching-engine-group --group order-service-exec-group

# 4. Outbox tồn đọng ở cả hai service
docker compose exec postgres psql -U trading -d trading -c "
  SELECT 'order' AS svc, count(*) FROM ordersvc.outbox_event WHERE published_at IS NULL
  UNION ALL
  SELECT 'match', count(*) FROM matchsvc.outbox_event WHERE published_at IS NULL;"
```

Và truy vấn trực tiếp danh sách lệnh treo (dùng partial index):

```sql
SELECT client_order_id, symbol, side, price, quantity, created_at,
       round(extract(epoch from (now() - created_at))) AS treo_giay
FROM ordersvc.orders
WHERE status = 'ACCEPTED' AND created_at < now() - interval '1 minute'
ORDER BY created_at;
```

⚠️ **Hạn chế của công thức alert này:** counter trong Micrometer **reset về 0 khi pod restart**. Hàm `rate()` của Prometheus xử lý được counter reset, nhưng phép **trừ hai counter thô** thì không. Sau một rolling update, ba counter reset ở những thời điểm hơi khác nhau và hiệu số có thể âm hoặc dương giả. Công thức bền hơn:

```promql
sum(increase(orders_accepted_total[10m]))
  - sum(increase(orders_filled_total[10m]))
  - sum(increase(orders_rejected_by_matching_total[10m])) > 10
```

Hoặc tốt nhất: đo trực tiếp thứ cần đo bằng một **gauge** đếm lệnh `ACCEPTED` quá hạn, truy vấn từ chính `idx_orders_pending`.

---

## 4. Kiểm chứng saga chiều về bằng test

```java
// matching-engine/src/test/java/com/trading/matching/SagaRejectionIT.java:28-34
/**
 * SAGA - chiều về: lệnh qua được pre-trade check của order-service nhưng
 * vi phạm luật sàn (giá lệch 40% so với tham chiếu, quá biên độ 7%).
 * Kỳ vọng: KHÔNG có execution, và có một execution report REJECTED nằm
 * trong outbox để trả ngược về order-service.
 */
```

```java
// matching-engine/src/test/java/com/trading/matching/SagaRejectionIT.java:57-85
@Test
void orderOutsidePriceBandIsRejectedAndReported() {
    String clientOrderId = "saga-reject-1";
    // refPrice 28450 nhưng đặt 40000 -> lệch ~40% > biên độ 7%
    String orderJson = """
            {"orderId":"o-9","clientOrderId":"%s","status":"ACCEPTED","reason":null,
             "symbol":"HPG","side":"BUY","price":40000.0,"quantity":100,
             "refPrice":28450.0,"riskLevel":"LOW","timestamp":%d}
            """.formatted(clientOrderId, System.currentTimeMillis());
    ...
    await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(500))
            .untilAsserted(() -> {
                assertThat(outboxRepository.findAll())
                        .anyMatch(e -> e.getAggregateId().equals(clientOrderId)
                                && e.getTopic().equals("stock.executions")
                                && e.getPayload().contains("REJECTED")
                                && e.getPayload().contains("PRICE_OUT_OF_BAND"));
                // và tuyệt đối không được có execution nào
                assertThat(executionRepository.findByClientOrderId(clientOrderId)).isEmpty();
            });
}
```

💡 Test này khẳng định **hai** điều trong cùng một `untilAsserted`: report REJECTED **có** trong outbox, **và** không có execution nào. Điều thứ hai quan trọng không kém — nó chứng minh rằng nhánh từ chối thực sự `return` trước khi ghi execution, chứ không phải ghi rồi mới từ chối.

Cũng lưu ý test assert trên **bảng outbox**, không phải trên Kafka. Đó là lựa chọn đúng: outbox là nơi `MatchingService` thực sự ghi; việc nó có lên Kafka hay không là trách nhiệm của `OutboxRelay` và được kiểm riêng.

---

## 5. Vấn đề được giải quyết

| Nếu không có | Hậu quả |
|---|---|
| Saga chiều về | Lệnh bị luật sàn từ chối treo `ACCEPTED` vĩnh viễn |
| Report ở nhánh REJECTED | Như trên — saga chỉ đóng được ở nhánh thành công |
| Outbox ở matching-engine | Khớp xong, publish fail ⇒ lệnh treo `ACCEPTED` |
| Một transaction ba việc | Lệnh bị nuốt, hoặc khớp hai lần, hoặc sổ sách lệch |
| `processed_event` trong cùng transaction | Như trên |
| State machine | Report trùng ghi đè trạng thái cuối |
| `@Version` | Lost update khi hai report tới song song |
| TradingRules ở matching | Client bypass order-service đặt lệnh sai luật sàn |
| `refPrice` đóng băng | Lệnh bị từ chối vì thị trường chạy trong lúc chờ |
| Alert `SagaStuckOrders` | Saga đứt mà không ai biết — khách bị giữ tiền âm thầm |
| Partial index `idx_orders_pending` | Query "lệnh nào treo?" quét cả bảng |
| Log ở nhánh "không thể xảy ra" | Không biết khi ai đó bơm thẳng vào Kafka |

---

## 6. Đánh đổi

### 6.1. Ưu

1. **Vòng đời lệnh khép kín** — mọi lệnh kết thúc ở một trạng thái cuối.
2. **Không transaction toàn cục** — ba transaction cục bộ, nối bằng event.
3. **Hai tầng kiểm tra độc lập** với ranh giới tin cậy rõ ràng.
4. **State machine = idempotency miễn phí.**
5. **Optimistic lock không tốn gì** ở đường chạy bình thường.
6. **Phát hiện được khi saga đứt** bằng alert nghiệp vụ.
7. **Luật sàn là cấu hình**, đổi không cần deploy mã.

### 6.2. Nhược

1. **Độ trễ ~1 giây** — chủ yếu do hai lần polling relay 500ms.
2. **Eventual consistency nhìn thấy được từ client**: `201 ACCEPTED` rồi 2s sau thành `REJ_MATCH`.
3. **Không nhìn thấy toàn cảnh saga** — phải ghép từ log, metric, trace, bảng.
4. **Không có bồi hoàn thật.** Nếu matching khớp xong mà order-service không bao giờ nhận được report (ví dụ mất hẳn DB của order), không có cơ chế tự động nào huỷ execution. Phải can thiệp thủ công.
5. **Không có timeout cho saga.** Lệnh `ACCEPTED` nằm mãi; chỉ có alert, không có cơ chế tự chuyển sang `EXPIRED`.
6. ⚠️ **Thiếu `UNIQUE` trên `execution.client_order_id`** (Gap #15).
7. ⚠️ **`stock.executions` không có DLT thật** dù comment nói có.
8. ⚠️ **Công thức alert dùng phép trừ counter thô** — không bền khi pod restart.
9. **Không hỗ trợ partial fill** — và thêm nó sẽ phá lớp idempotency miễn phí.
10. ⚠️ **Bất biến `order.max-price-deviation-pct ≤ matching.price-band-pct`** không ai cưỡng chế.

---

## 7. Phương án thay thế

| Phương án | Khi nào hợp | Vì sao repo không chọn |
|---|---|---|
| **Orchestration Saga** (Temporal, Camunda) | > 5 bước, bồi hoàn phức tạp, cần nhìn toàn cảnh | 2 bước thì orchestrator là phức tạp thừa; thêm một hệ thống phải vận hành |
| **Process Manager tự viết** | Cần timeout và bồi hoàn | Có thể là bước nâng cấp đúng khi thêm `EXPIRED` và bồi hoàn |
| **Request/Reply đồng bộ** (REST) | Khi cần kết quả ngay | matching chậm ⇒ order chậm; matching chết ⇒ order chết; mỗi lệnh chiếm một thread suốt vòng |
| **Event Sourcing** cho vòng đời lệnh | Cần audit tuyệt đối, tái dựng trạng thái tại thời điểm T | Phức tạp lớn; repo đã có `outbox_event` bất biến + `orders` giữ cả REJECTED, đủ cho audit |
| **Spring Statemachine** | State machine phức tạp, có guard/action/hierarchical state | 4 trạng thái thì một `Map` 6 dòng rõ ràng hơn và không thêm phụ thuộc |
| **Pessimistic lock** thay `@Version` | Xung đột thường xuyên | Xung đột ở đây hiếm; khoá bi quan trả giá trên mọi update |

---

## 8. Núm vặn cấu hình

| Tham số | Nơi khai | Mặc định | Ghi chú |
|---|---|---|---|
| `matching.price-band-pct` | `matching-engine/application.yml` | 7 | HOSE 7, HNX 10, UPCoM 15. 🔑 Phải **≥** `order.max-price-deviation-pct` |
| `matching.lot-size` | như trên | 100 | Cổ phiếu 100, phái sinh 1 |
| `matching.session-check-enabled` | như trên | `false` | Bật để từ chối lệnh ngoài giờ |
| `matching.session-zone` | như trên | `Asia/Ho_Chi_Minh` | — |
| Khung giờ phiên | `TradingRules.java:66-67` | hardcode | ⚠️ Nên là cấu hình; thiếu lịch nghỉ lễ |
| `MAX_ATTEMPTS` | `ExecutionReportConsumer.java:14` | 3 | Số lần retry optimistic lock |
| `outbox.relay.interval-ms` | cả hai service | 500 | 🔑 Nhân đôi trong saga ⇒ thủ phạm chính của độ trễ |
| `SagaStuckOrders` threshold | `alert-rules.yml:27` | > 10 trong 2 phút | Tuỳ lưu lượng; hệ tải cao cần ngưỡng cao hơn |

---

## 9. Cách kiểm chứng

```bash
API='-H Content-Type:application/json -H X-API-KEY:demo-key'
URL='http://localhost:8083/api/v1/orders'
PSQL='docker compose exec postgres psql -U trading -d trading'

# === Nhánh 1: saga thành công ===
curl -s -X POST $URL $API \
  -d '{"clientOrderId":"SAGA-OK","symbol":"HPG","side":"BUY","price":28500,"quantity":100,"accountId":"ACC001"}' \
  | jq '{status, refPrice}'
sleep 2
curl -s $URL/SAGA-OK $API | jq '{status, filledQty, avgFillPrice, createdAt, updatedAt}'
#  -> FILLED, filledQty=100

# === Nhánh 2: qua cửa 1, chết ở cửa 2 (LÔ LẺ) ===
curl -s -X POST $URL $API \
  -d '{"clientOrderId":"SAGA-LOT","symbol":"HPG","side":"BUY","price":28500,"quantity":150,"accountId":"ACC001"}' \
  | jq -r .status
#  -> ACCEPTED (order-service KHÔNG kiểm lô!)
sleep 2
curl -s $URL/SAGA-LOT $API | jq '{status, reason}'
#  -> {"status":"REJ_MATCH","reason":"INVALID_LOT_SIZE must be multiple of 100"}

# === Nhánh 3: biên độ — cần đặt order.max-price-deviation-pct > 7 để qua cửa 1 ===
#     Hoặc bơm thẳng vào Kafka để bỏ qua cửa 1 hoàn toàn:
echo '{"orderId":"direct-band","clientOrderId":"SAGA-BAND","status":"ACCEPTED","reason":null,
       "symbol":"HPG","side":"BUY","price":40000.0,"quantity":100,
       "refPrice":28450.0,"riskLevel":"LOW","timestamp":1764057600000}' | \
docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:19092 --topic stock.orders
sleep 3
docker compose logs --tail=20 matching-engine | grep "TU CHOI"
#  -> PRICE_OUT_OF_BAND 40.60% > 7%
$PSQL -c "SELECT payload FROM matchsvc.outbox_event WHERE aggregate_id = 'SAGA-BAND';"
#  -> report REJECTED trong outbox
docker compose logs --tail=20 order-service | grep "khong ton tai"
#  -> cảm biến bắt được: lệnh này không qua order-service

# === Bật kiểm tra phiên giao dịch ===
docker compose stop matching-engine
docker compose run -d -e MATCHING_SESSION_CHECK_ENABLED=true --name me-session matching-engine
#    đặt lệnh ngoài 9:00-11:30 / 13:00-14:45 -> REJ_MATCH MARKET_CLOSED

# === Sức khoẻ saga ===
# Phân bố trạng thái
$PSQL -c "SELECT status, count(*) FROM ordersvc.orders GROUP BY status ORDER BY 2 DESC;"

# Lệnh đang treo (dùng partial index idx_orders_pending)
$PSQL -c "SELECT client_order_id, symbol, created_at,
                 round(extract(epoch from (now() - created_at))) AS treo_giay
          FROM ordersvc.orders
          WHERE status = 'ACCEPTED' AND created_at < now() - interval '1 minute'
          ORDER BY created_at;"

# Xác nhận partial index được dùng
$PSQL -c "EXPLAIN SELECT * FROM ordersvc.orders WHERE status = 'ACCEPTED' ORDER BY created_at;"

# Ba counter của alert SagaStuckOrders
curl -s http://localhost:8083/actuator/prometheus | \
  grep -E '^orders_(accepted|filled|rejected_by_matching)_total'

# Thời gian hoàn thành saga (p50/p99 thực tế)
$PSQL -c "SELECT
            percentile_cont(0.5)  WITHIN GROUP (ORDER BY updated_at - created_at) AS p50,
            percentile_cont(0.99) WITHIN GROUP (ORDER BY updated_at - created_at) AS p99
          FROM ordersvc.orders WHERE updated_at IS NOT NULL;"

# === Mô phỏng saga đứt ===
docker compose stop matching-engine
for i in $(seq 1 15); do
  curl -s -o /dev/null -X POST $URL $API \
    -d "{\"clientOrderId\":\"STUCK-$i\",\"symbol\":\"HPG\",\"side\":\"BUY\",\"price\":28500,\"quantity\":100,\"accountId\":\"ACC001\"}"
done
sleep 5
$PSQL -c "SELECT count(*) FROM ordersvc.orders WHERE status = 'ACCEPTED';"   # -> 15
#    Prometheus: SagaStuckOrders sẽ FIRING sau 2 phút
docker compose start matching-engine
sleep 20
$PSQL -c "SELECT count(*) FROM ordersvc.orders WHERE status = 'ACCEPTED';"   # -> 0, tự phục hồi

# === Test tự động ===
mvn -pl matching-engine test -Dtest=SagaRejectionIT
mvn -pl matching-engine test -Dtest=IdempotentConsumerIT
```

---

## 10. Hạn chế đã biết

| # | Loại | Vị trí | Vấn đề | Hướng sửa |
|:--:|---|---|---|---|
| 15 | **Gap** | `matching/entity/ExecutionEntity.java:24-25` | `client_order_id` không có `unique` ⇒ `processed_event` là lớp chặn duy nhất | Thêm `unique = true` + migration `V3` |
| — | Gap | `order/config/KafkaConfig.java:97-104` | Comment ở `ExecutionReportConsumer` nói có DLT nhưng factory không đặt recoverer; `stock.executions.DLT` không ai ghi vào | Thêm `DeadLetterPublishingRecoverer`, hoặc sửa comment |
| — | Gap | `monitoring/alert-rules.yml:27` | Phép trừ counter thô không bền khi pod restart | Dùng `increase(...[10m])`, hoặc gauge đếm lệnh treo |
| — | Ops | `OrderStatusService.java:44,61` | Cùng counter `ignored` cho hai tình huống rất khác nhau (lệnh không tồn tại vs report trùng) | Tách thành hai counter |
| — | Ops | `TradingRules.java:64-69` | `inSession()` thiếu lịch nghỉ lễ; dùng `now()` thay vì `order.timestamp()` | Trading calendar; dùng timestamp của lệnh |
| — | Ops | `TradingRules.java:66-67` | Khung giờ phiên hardcode | `@Value` |
| — | Gap | `ExecutionReport` | Không có `execId` ⇒ không dedupe được khi có nhiều report cho một lệnh | Thêm `execId` trước khi làm partial fill |
| — | Gap | cấu hình | Bất biến `order.max-price-deviation-pct ≤ matching.price-band-pct` không ai cưỡng chế | Kiểm ở CI hoặc ở startup |
| — | Thiết kế | Toàn saga | Không có timeout / trạng thái `EXPIRED`; không có bồi hoàn | Process manager với timeout |

---

## 11. Câu hỏi phỏng vấn

**Q: Giải thích saga trong hệ này. Vì sao chọn choreography?**

A: Vòng đời lệnh gồm ba transaction cục bộ nối bằng hai topic Kafka: order-service ghi lệnh + outbox event trong một transaction; matching-engine ghi sổ dedupe + execution + outbox report trong một transaction; order-service cập nhật trạng thái trong một transaction. Không có transaction toàn cục, không có coordinator. Chọn choreography vì chỉ có **hai bước** — một orchestrator sẽ là phức tạp thừa và thêm một điểm hỏng tập trung. Nếu mở rộng thành `order → risk-limit → funding → matching → settlement → clearing` thì tôi sẽ chuyển sang orchestration, vì với 6 bước và bồi hoàn chéo, logic phân tán trở nên không ai đọc nổi.

**Q: Saga đứt giữa chừng thì sao?**

A: Phải phân biệt *đứt tạm thời* và *đứt thật*. Đứt tạm thời — matching-engine chết, Kafka chết, relay tắc — thì hệ **tự phục hồi**: event nằm trong outbox hoặc trong topic, được xử lý khi thành phần trở lại. Tôi có thể chứng minh bằng cách `docker compose stop matching-engine`, đặt 15 lệnh, rồi `start` lại — cả 15 lệnh tự đóng. Đứt thật — ví dụ mất hẳn database của order-service — thì **không có bồi hoàn tự động**; phải can thiệp thủ công. Và điều quan trọng nhất: hệ **biết** khi nó đứt, nhờ alert `SagaStuckOrders` so số lệnh vào saga với số lệnh ra. Đó là cảnh báo nghiêm trọng nhất trong hệ, vì nó nghĩa là khách đang bị giữ tiền cho một lệnh không kết thúc.

**Q: Vì sao matching-engine kiểm lại luật mà order-service đã kiểm? Không phải thừa sao?**

A: Hai bên bảo vệ hai đối tượng khác nhau và có ranh giới tin cậy khác nhau. Pre-trade check bảo vệ **khách hàng** — fat-finger, dữ liệu cũ, rủi ro cao. TradingRules thi hành luật của **sàn** — lô chẵn, biên độ, phiên. Quan trọng hơn: order-service **không được tin**, vì trong cùng namespace bất kỳ pod nào có quyền tới Kafka đều publish được vào `stock.orders`. Và tham số hai bên **cố ý lệch nhau**: 5% ở order, 7% ở matching — cửa 1 chặt hơn để bảo vệ khách, cửa 2 là luật thật. Cuối cùng, luật sàn đổi theo cơ quan quản lý; nếu đặt nó ở order-service thì mỗi lần HOSE đổi biên độ lại phải deploy service nhận lệnh.

**Q: `OrderStateMachine` chỉ là một `Map` 6 dòng. Sao không dùng Spring Statemachine?**

A: Vì bài toán có 4 trạng thái và 2 transition hợp lệ. Spring Statemachine mang lại guard, action, hierarchical state, persistence của machine — không thứ nào cần ở đây, và nó thêm một phụ thuộc cùng một mô hình lập trình mới. Cái `Map` này còn có một thuộc tính mà framework không tự cho: vì mọi trạng thái cuối ánh xạ tới `Set.of()`, nó **đồng thời là một lớp idempotency** — execution report trùng tự động bị bỏ qua, nên order-service không cần bảng `processed_event`. Điểm cần cảnh báo: thuộc tính đó biến mất ngay khi thêm `PARTIALLY_FILLED` với self-loop, nên thêm partial fill không chỉ là thêm một trạng thái — nó kéo theo cả một cơ chế dedupe mới.

**Q: Client nhận `201 ACCEPTED` rồi 2 giây sau lệnh thành `REJ_MATCH`. Đó chẳng phải API tệ sao?**

A: Không — đó là mô hình đúng cho xử lý bất đồng bộ, và sàn thật cũng vậy: FIX protocol có `ExecutionReport` với `OrdStatus = NEW` trước, rồi mới tới `FILLED` hoặc `REJECTED`. `201` nghĩa là "yêu cầu đã được ghi nhận bền vững", không phải "giao dịch đã hoàn tất". API tệ sẽ là *chặn* HTTP request một giây chờ saga xong — khi đó mỗi lệnh chiếm một thread suốt vòng saga và hệ sập ở 200 req/s. Cái cần làm đúng là **tài liệu API phải nói rõ** `ACCEPTED` chưa phải trạng thái cuối, và cung cấp đường tra cứu bằng `clientOrderId` — cả hai đều có.

**Q: Vì sao retry optimistic lock nằm ngoài `@Transactional`?**

A: Vì khi Hibernate ném `OptimisticLockingFailureException`, transaction hiện tại đã bị đánh dấu rollback-only. Retry bên trong cùng transaction đó sẽ nhận `UnexpectedRollbackException` — transaction *đã* hỏng, không có gì cứu được nữa. Phải thoát ra ngoài cho transaction cũ rollback xong, rồi mở transaction **mới** để đọc lại row với version mới nhất. Đây là chi tiết dễ sai và thường mất hàng giờ để chẩn đoán nếu chưa gặp bao giờ.

---

## 12. Bảng tổng kết

| Khía cạnh | Lựa chọn | Lý do | Cái giá |
|---|---|---|---|
| Kiểu saga | **Choreography** | Chỉ 2 bước; không cần orchestrator | Khó nhìn toàn cảnh |
| Nguyên tử mỗi bước | 1 transaction gồm dedupe + nghiệp vụ + outbox | Không có khe hở giữa "làm xong" và "báo tin" | — |
| Chiều về | `stock.executions`, có cả nhánh REJECTED | Saga phải đóng được ở **cả** nhánh thất bại | Thêm một topic, một outbox |
| Thứ tự commit | DB commit → offset commit | Crash chỉ gây redelivery, không mất lệnh | Phụ thuộc dedupe |
| Dedupe | `processed_event` trong cùng transaction | Atomic test-and-set | ⚠️ Thiếu `UNIQUE` trên `execution` |
| Luật sàn | `TradingRules`, cấu hình bằng `@Value` | Nguồn sự thật; đổi không cần deploy | ⚠️ Thiếu lịch nghỉ lễ |
| Ranh giới tin cậy | Hai tầng, cố tình trùng một phần | order-service không được tin | Logic trùng lặp ở hai nơi |
| Giá khớp | `refPrice` đóng băng lúc nhận lệnh | Không từ chối vì thị trường chạy trong lúc chờ | Không phản ánh giá thị trường lúc khớp |
| Đóng vòng đời | State machine, trạng thái cuối không lối ra | Chặn transition vô lý + idempotency miễn phí | Không hỗ trợ partial fill |
| Ghi song song | `@Version` optimistic + retry 3 lần ngoài tx | Xung đột hiếm, không trả giá ở đường bình thường | Retry khi va chạm |
| Truy vấn vận hành | Partial index `WHERE status='ACCEPTED'` | "Lệnh nào treo?" chạy tức thì bất kể bảng lớn | — |
| Phát hiện đứt | `SagaStuckOrders`: accepted − (filled + rejected) | Đo đúng thứ quan trọng nhất về nghiệp vụ | ⚠️ Phép trừ counter thô không bền khi restart |
| Cảm biến bất thường | Log + counter ở nhánh "không thể xảy ra" | Biết khi ai đó bơm thẳng vào Kafka | ⚠️ Dùng chung counter với trường hợp bình thường |

> **Đọc tiếp:** [Chương 08 — Observability: Metrics, Tracing & Alerting](./08-observability-metrics-tracing-va-alerting.md) — làm sao *biết* hệ thống đang hỏng.
