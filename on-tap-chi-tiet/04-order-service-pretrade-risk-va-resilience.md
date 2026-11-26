# Chương 04 — Order Service: Pre-trade Risk & Resilience

> **Câu hỏi trung tâm:** làm sao một cổng REST nhận lệnh tiền thật vừa nhanh, vừa không mất lệnh, vừa không tạo lệnh trùng, vừa không sập khi hạ tầng phía dưới ốm?
>
> **Tài liệu đối chiếu:** `DESIGN-NOTES.md` mục 7-8, `DEEP-DIVE.md` mục 4 & 7 & 8.7.

---

## 1. Nó là gì?

`order-service` là **service duy nhất hướng ra client** (service duy nhất có route trong Ingress nhận POST). Nó đóng ba vai cùng lúc:

| Vai | Thành phần | Luồng |
|---|---|---|
| **Cổng nhận lệnh** | `OrderController` → `OrderService` → `OrderPersistence` | REST vào → pre-trade check → ghi DB + outbox |
| **Read-model consumer** | `MarketDataCache` | `stock.enriched` → `ConcurrentHashMap` |
| **Saga reply handler** | `ExecutionReportConsumer` → `OrderStatusService` | `stock.executions` → cập nhật trạng thái lệnh |

```java
// order-service/src/main/java/com/trading/order/OrderServiceApplication.java:7-26
/**
 * ORDER SERVICE - Service đặt lệnh.
 *
 * Điểm thiết kế quan trọng: order-service KHÔNG gọi REST sang pricing-risk-service
 * mỗi lần đặt lệnh (chậm + coupling). Thay vào đó nó SUBSCRIBE topic "stock.enriched"
 * và giữ LOCAL CACHE giá/rủi ro mới nhất.
 *
 * -> Pre-trade risk check chỉ là 1 lần đọc ConcurrentHashMap: micro-giây.
 * -> pricing-risk-service chết, order-service vẫn hoạt động (dữ liệu hơi cũ - trade-off).
 * Đây là pattern "local materialized view" / CQRS-read-model qua event.
 */
// scanBasePackages: OutboxRelay nam o package com.trading.outbox (module outbox-support),
// NGOAI com.trading.order nen component scan mac dinh khong thay -> relay khong chay,
// outbox_event khong bao gio duoc publish. Phai khai bao them package nay.
@SpringBootApplication(scanBasePackages = {"com.trading.order", "com.trading.outbox"})
@EnableScheduling
public class OrderServiceApplication { ... }
```

💡 **`scanBasePackages` là một cạm bẫy thật, không phải chi tiết vụn.** `@SpringBootApplication` mặc định quét package của chính class đó trở xuống. `OutboxRelay` nằm ở `com.trading.outbox`, **ngoài** `com.trading.order` ⇒ không được đăng ký bean ⇒ `@Scheduled` không chạy ⇒ `outbox_event` tích tụ vĩnh viễn và **không có lỗi nào được ném ra**. Triệu chứng: lệnh trả `201 ACCEPTED` nhưng mãi không thành `FILLED`, và `SELECT count(*) FROM outbox_event WHERE published_at IS NULL` tăng đều. Đây là lý do comment được viết ngay trên annotation.

---

## 2. Cơ chế hoạt động — bốn lớp phòng thủ

```java
// order-service/src/main/java/com/trading/order/service/OrderService.java:21-36
/**
 * Luồng đặt lệnh v3 - PHÒNG THỦ NHIỀU LỚP chống duplicate + dual-write:
 *
 *  1. Redis GET idem:{clientOrderId}   -> hit: trả kết quả cũ (nhanh, TẬP TRUNG mọi pod)
 *  2. Pre-trade check in-memory        -> staleness / risk / fat-finger
 *  3. TRANSACTIONAL OUTBOX             -> INSERT orders + outbox_event trong 1 local tx
 *     UNIQUE(client_order_id)          -> tầng dedupe CUỐI CÙNG: Redis chết vẫn chặn trùng
 *  4. Redis SET kết quả (TTL 10')      -> tăng tốc lần retry sau
 *  (5. OutboxRelay publish Kafka; matching-engine idempotent consumer)
 *
 * Redis ở đây là CACHE TĂNG TỐC, không phải source of truth của dedupe -
 * DB constraint mới là. Vì vậy Redis chết -> hệ vẫn ĐÚNG, chỉ chậm hơn 1 chút.
 */
```

### 2.1. Biên vào: filter, validation, rate limit

**API key filter** — `config/ApiKeyFilter.java:32-41`:

```java
String path = request.getRequestURI();
// Chỉ bảo vệ API nghiệp vụ; actuator để K8s probe + Prometheus scrape
if (path.startsWith("/api/") && !apiKey.equals(request.getHeader("X-API-KEY"))) {
    response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
    response.getWriter().write("{\"error\":\"invalid or missing X-API-KEY\"}");
    return;
}
chain.doFilter(request, response);
```

Comment trong file nói thẳng đây là **sàn, không phải trần** — chi tiết ở [chương 12](./12-bao-mat-he-thong.md).

**Bean validation** — `model/OrderRequest.java`:

```java
public record OrderRequest(
        // clientOrderId do CLIENT sinh (UUID) - chìa khoá idempotency:
        // client timeout rồi retry cùng clientOrderId -> nhận lại KẾT QUẢ CŨ,
        // không tạo lệnh trùng. Bắt buộc với API tài chính.
        @NotBlank String clientOrderId,
        @NotBlank String symbol,
        @NotNull @Pattern(regexp = "BUY|SELL") String side,
        @NotNull @Positive BigDecimal price,
        @Min(1) long quantity,
        @NotBlank String accountId
) {}
```

🔑 **`clientOrderId` do CLIENT sinh, không phải server.** Đây là nguyên tắc nền của idempotency trong API tài chính. Nếu server sinh id, client timeout sẽ không biết lệnh có tạo thành công hay không, và retry sẽ tạo lệnh mới. Với client-generated key, retry mang theo cùng một khoá ⇒ server nhận ra và trả lại kết quả cũ.

Đây cũng là lý do endpoint tra cứu dùng `clientOrderId` chứ không phải `orderId`:

```java
// order-service/src/main/java/com/trading/order/controller/OrderController.java:90-98
/**
 * Tra một lệnh theo clientOrderId - khoá mà CLIENT tự sinh, nên tra được
 * ngay cả khi client không giữ orderId do server cấp.
 *
 * Dùng clientOrderId chứ không dùng orderId vì đây chính là chìa khoá
 * idempotency: client timeout rồi muốn biết lệnh cũ ra sao thì nó chỉ có
 * trong tay clientOrderId.
 */
@GetMapping("/{clientOrderId}")
@RateLimiter(name = "orders")
public ResponseEntity<OrderView> getByClientOrderId(@PathVariable String clientOrderId) { ... }
```

**Rate limiter** — `OrderController.java:45-52`:

```java
/**
 * RateLimiter 200 req/s (cấu hình trong yml): chặn máy đặt lệnh loạn /
 * client bug retry vô hạn trước khi nó đè chết DB và Kafka.
 * Vượt ngưỡng -> RequestNotPermitted -> HTTP 429 (handler bên dưới).
 */
@PostMapping
@RateLimiter(name = "orders")
public ResponseEntity<OrderResult> place(@Valid @RequestBody OrderRequest request) {
    OrderResult result = orderService.placeOrder(request);
    HttpStatus status = "ACCEPTED".equals(result.status())
            ? HttpStatus.CREATED : HttpStatus.UNPROCESSABLE_ENTITY;
    return ResponseEntity.status(status).body(result);
}
```

```yaml
# order-service/src/main/resources/application.yml
resilience4j:
  ratelimiter:
    instances:
      orders:
        limit-for-period: 200      # 200 lệnh/giây/pod
        limit-refresh-period: 1s
        timeout-duration: 0s       # không xếp hàng chờ - từ chối ngay (fail-fast)
```

🔑 **`timeout-duration: 0s` là điểm tinh tế nhất ở đây.** Nếu đặt `100ms`, request vượt ngưỡng sẽ **nằm chờ** 100ms — nghĩa là nó vẫn chiếm một thread Tomcat suốt thời gian đó. Dưới cơn bão retry, thread pool cạn và pod chết *dù rate limiter đang "bảo vệ" nó*. `0s` = từ chối tức thì, trả thread về pool ngay.

⚠️ **Hai hạn chế của rate limiter này:**
- **Per-pod, không per-cluster.** 3 pod ⇒ trần thực tế là 600 req/s. Nếu cần trần cụm, phải dùng Redis-based rate limiter hoặc Ingress-level (`nginx.ingress.kubernetes.io/limit-rps: "20"` trong `values.yaml` là lớp thứ hai).
- **Per-service, không per-account.** Một `accountId` đặt lệnh loạn sẽ đẩy mọi khách hàng khác vào `429`. Rate limit theo account là bước nâng cấp cần thiết cho production — comment trong `ApiKeyFilter` cũng liệt kê nó.

**Mã HTTP được chọn cẩn thận:**

| Mã | Khi nào | Vì sao không dùng mã khác |
|---|---|---|
| `201 Created` | `ACCEPTED` | Bản ghi đã được tạo bền vững |
| `422 Unprocessable Entity` | `REJECTED` (pre-trade) | **Không** dùng `400`: request đúng cú pháp; client sửa lại cũng vẫn bị từ chối nếu thị trường vẫn rủi ro cao |
| `429 Too Many Requests` | Vượt rate limit | Có `Retry-After` thì tốt hơn nữa (hiện chưa có) |
| `401 Unauthorized` | Thiếu/sai API key | — |
| `404 Not Found` | Tra `clientOrderId` không tồn tại | — |

### 2.2. Lớp 1 — Redis fast-path

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

Ba dòng cấu hình biến "Redis chỉ là cache" từ tuyên ngôn thành hiện thực:

```yaml
# order-service/src/main/resources/application.yml
spring:
  data:
    redis:
      host: ${REDIS_HOST:localhost}
      port: 6379
      timeout: 200ms               # Redis chậm KHÔNG được kéo chậm lệnh: timeout ngắn,
                                   # lỗi thì fallback xuống DB dedupe (degrade gracefully)
management:
  health:
    redis:
      enabled: false   # Redis là cache phụ - nó chết KHÔNG được làm readiness fail
                       # (nếu không K8s sẽ rút pod khỏi Service dù pod vẫn phục vụ tốt)
```

🔑 **`management.health.redis.enabled: false` là dòng quan trọng nhất trong cả file yml.** Mặc định Spring Boot đưa Redis health indicator vào `readinessState`. Redis chết ⇒ readiness probe fail ⇒ Kubernetes **rút toàn bộ pod order-service khỏi Service** ⇒ không ai đặt được lệnh — dù mọi pod vẫn hoàn toàn có khả năng phục vụ qua DB constraint. Đây là trường hợp kinh điển của *health check quá nghiêm làm sập hệ thống khoẻ mạnh*.

Nguyên tắc rút ra: **health check phải phản ánh khả năng phục vụ, không phản ánh sự hiện diện của mọi dependency.** Dependency nào mà thiếu nó vẫn phục vụ được thì không được vào readiness.

`timeout: 200ms` cũng cùng tinh thần: Redis treo không được làm treo lệnh. 200ms là trần cho phần "tăng tốc" — vượt quá đó thì đi đường DB còn nhanh hơn.

**Ghi kết quả vào cache** — `OrderService.java:137-144`:

```java
private void cacheResult(String key, OrderResult result) {
    try {
        redis.opsForValue().set(key, objectMapper.writeValueAsString(result), IDEM_TTL);
    } catch (Exception e) {
        log.warn("Khong cache duoc ket qua vao Redis: {}", e.getMessage());
    }
}
```

`IDEM_TTL = Duration.ofMinutes(10)` (`:39`).

⚠️ **TTL 10 phút tạo một khoảng trống logic.** Sau 10 phút, Redis quên; nếu client retry ở phút thứ 11, request rơi xuống DB, đụng `uq_client_order`, và được xử lý đúng ở nhánh `DataIntegrityViolationException`. Nghĩa là **hành vi vẫn đúng**, chỉ chậm hơn. TTL chọn 10 phút vì đó là khoảng thời gian hợp lý cho một client retry; giữ vĩnh viễn sẽ làm Redis phình theo số lệnh.

### 2.3. Lớp 2 — Ba luật pre-trade

```java
// order-service/src/main/java/com/trading/order/service/OrderService.java:109-128
private OrderResult check(OrderRequest req) {
    EnrichedMarketData md = cache.get(req.symbol()).orElse(null);

    if (md == null || System.currentTimeMillis() - md.calcTime() > maxDataAgeMs) {
        return reject(req, "NO_FRESH_MARKET_DATA");
    }
    if ("HIGH".equals(md.riskLevel())) {
        return reject(req, "RISK_LEVEL_HIGH volatility=" + md.volatility());
    }
    BigDecimal deviation = req.price().subtract(md.lastPrice()).abs()
            .divide(md.lastPrice(), 6, RoundingMode.HALF_UP)
            .multiply(BigDecimal.valueOf(100));
    if (deviation.compareTo(maxDeviationPct) > 0) {
        return reject(req, "PRICE_DEVIATION " + deviation.setScale(2, RoundingMode.HALF_UP)
                + "% > " + maxDeviationPct + "%");
    }
    return new OrderResult(UUID.randomUUID().toString(), req.clientOrderId(), "ACCEPTED", null,
            req.symbol(), req.side(), req.price(), req.quantity(),
            md.lastPrice(), md.riskLevel(), System.currentTimeMillis());
}
```

```yaml
order:
  max-price-deviation-pct: 5     # chặn lệnh lệch >5% giá tham chiếu
  max-data-age-ms: 10000         # dữ liệu >10s -> coi như stale, không cho đặt
```

#### Luật 1 — Staleness check

**Vấn đề nó giải quyết, nói cho cụ thể:** cache trong RAM của `order-service` **không có TTL**. `MarketDataCache.cache.put()` ghi đè mà không bao giờ xoá. Nếu `pricing-risk-service` sập, hoặc Kafka tắc, hoặc consumer group rebalance kẹt, thì cache *vẫn còn giá trị cũ* và `cache.get("HPG")` vẫn trả về một `EnrichedMarketData` trông hoàn toàn hợp lệ. Không có staleness check, khách sẽ đặt lệnh theo giá của 10 phút trước.

**`calcTime` là gì:** `RiskCalculator.java:84` đặt `System.currentTimeMillis()` tại thời điểm **tính xong**, không phải thời điểm tick phát sinh (`sourceEventTime` mới là cái đó). Nên hiệu số `now − calcTime` đo đúng thứ cần đo: *dữ liệu này được tính cách đây bao lâu*.

**Vì sao từ chối chứ không đặt bừa theo giá cũ:** triết lý **fail-safe** — với tiền của người khác, không chắc chắn thì không nhận lệnh. Từ chối oan gây khó chịu; cho qua nhầm gây mất tiền, khiếu nại, và trách nhiệm pháp lý.

⚠️ **Gap #6 — check bị vô hiệu hoá âm thầm khi lệch đồng hồ.** Nếu đồng hồ pod `pricing-risk-service` chạy **nhanh hơn** pod `order-service`, thì `calcTime > now` và hiệu số ra **số âm**. `số âm > 10000` luôn false ⇒ staleness check không bao giờ kích hoạt ⇒ dữ liệu cũ bao nhiêu cũng qua. Không log, không metric, không ai biết. Phòng vệ đúng:

```java
long age = System.currentTimeMillis() - md.calcTime();
if (age < -1000) {              // lệch đồng hồ bất thường
    clockSkewCounter.increment();
    log.warn("calcTime o tuong lai {}ms - kiem tra NTP", -age);
    return reject(req, "CLOCK_SKEW_DETECTED");   // fail-safe
}
if (md == null || age > maxDataAgeMs) { ... }
```

⚠️ **Ops #5 — ngưỡng toàn cục không hợp mọi mã.** `max-data-age-ms` là một `@Value` duy nhất cho mọi symbol. Nhưng:

| Nhóm mã | Tần suất tick thực tế | 10s có hợp lý? |
|---|---|---|
| VN30F2607 (phái sinh) | nhiều lần/giây | Quá rộng — dữ liệu 10s đã rất cũ |
| HPG, VCB (bluechip) | vài lần/giây | Hợp lý |
| Mã thanh khoản thấp | có khi 30-60 giây/tick | **Quá hẹp — bị từ chối oan liên tục** |

Hướng sửa: ngưỡng theo nhóm mã (`Map<String, Long>` hoặc cấu hình theo tier), hoặc ngưỡng động dựa trên khoảng cách tick trung bình quan sát được của chính mã đó.

#### Luật 2 — Risk level

`"HIGH".equals(md.riskLevel())` — chặn thẳng khi volatility năm hoá ≥ 35% (`RiskCalculator.java:75`).

⚠️ Điểm yếu: ngưỡng 15/35 **hardcode trong `RiskCalculator`**, nhưng hệ quả của nó (từ chối lệnh) xảy ra ở `OrderService`. Hai service, hai nhịp deploy, một hằng số không ai cưỡng chế. Nếu đội quant đổi ngưỡng để "phân loại chính xác hơn", họ có thể vô tình làm toàn bộ thị trường bị chặn đặt lệnh.

#### Luật 3 — Fat-finger

$$\text{deviation} = \frac{|P_{\text{lệnh}} - P_{\text{tham chiếu}}|}{P_{\text{tham chiếu}}} \times 100$$

Chặn trường hợp kinh điển: gõ `285000` thay vì `28500` (thừa một số 0).

💡 Lưu ý `divide(md.lastPrice(), 6, RoundingMode.HALF_UP)` — `BigDecimal.divide` **bắt buộc** phải chỉ định scale và rounding mode khi phép chia không kết thúc, nếu không ném `ArithmeticException: Non-terminating decimal expansion`. Đây là lỗi hay gặp khi mới dùng `BigDecimal`.

**`refPrice = md.lastPrice()` được đóng băng vào lệnh** (`:126`) và đi theo nó xuống `matching-engine`. 🔑 Nếu không đóng băng, một lệnh nằm trong hàng đợi 3 giây có thể bị `TradingRules` từ chối vì thị trường đã chạy đi — một kết cục không giải thích được cho khách hàng.

### 2.4. Lớp 3 — Persist + Outbox, DB constraint là chốt chặn

```java
// order-service/src/main/java/com/trading/order/service/OrderService.java:89-102
// ===== Lớp 3: persist + outbox, DB constraint chặn trùng tuyệt đối =====
try {
    persistence.saveWithOutbox(req, result);
} catch (DataIntegrityViolationException dup) {
    // Trùng client_order_id (retry rơi vào pod khác / Redis miss)
    duplicates.increment();
    OrderResult existing = persistence.findExisting(req.clientOrderId()).orElseThrow();
    cacheResult(idemKey, existing);
    return existing;
}

if ("ACCEPTED".equals(result.status())) accepted.increment(); else rejected.increment();
```

Chi tiết về `saveWithOutbox` nằm ở [chương 05](./05-distributed-transactions-outbox-va-cdc.md). Ở đây chỉ nhấn mạnh ràng buộc làm nền:

```sql
-- order-service/src/main/resources/db/migration/V1__orders_and_outbox.sql
-- DEDUPE TẬP TRUNG cấp DB: chốt chặn cuối cùng chống duplicate lệnh
CONSTRAINT uq_client_order UNIQUE (client_order_id)
```

🔑 **Thứ tự ưu tiên của ba lớp dedupe:**

| Lớp | Tốc độ | Phạm vi | Có thể chết không? |
|---|---|---|---|
| Redis | ~0,5ms | Mọi pod (tập trung) | **Có** — hệ vẫn đúng |
| DB UNIQUE | ~3ms | Mọi pod (tuyệt đối) | **Không** — đây là nền |
| `processed_event` ở matching | ~3ms | Chống khớp trùng | **Không** |

⚠️ **Bug #4 — `orElseThrow()` che lỗi gốc.** Dòng `:97` giả định mọi `DataIntegrityViolationException` đều là vi phạm `uq_client_order`. Nhưng bảng `orders` còn có `order_id VARCHAR(36) NOT NULL UNIQUE`. Nếu vì lý do nào đó `UUID.randomUUID()` trùng (gần như không thể, nhưng cũng có thể do lỗi logic khác), hoặc một constraint `NOT NULL` bị vi phạm, thì `findExisting` trả `Optional.empty()` và client nhận `NoSuchElementException` — **che hoàn toàn lỗi gốc**, khiến việc chẩn đoán gần như bất khả thi.

Sửa tối thiểu:

```java
} catch (DataIntegrityViolationException dup) {
    duplicates.increment();
    return persistence.findExisting(req.clientOrderId())
            .map(existing -> { cacheResult(idemKey, existing); return existing; })
            .orElseThrow(() -> dup);    // giữ nguyên exception gốc
}
```

Sửa triệt để: kiểm tra tên constraint trong `dup.getCause()` (với Postgres là `ConstraintViolationException.getConstraintName()`) và chỉ xử lý nhánh duplicate khi đúng là `uq_client_order`.

### 2.5. Circuit Breaker & fail-safe fallback

```java
// order-service/src/main/java/com/trading/order/service/OrderService.java:69-75
/**
 * CircuitBreaker "orders-db": DB chết -> sau ngưỡng lỗi, breaker MỞ,
 * request fail-fast ngay (không xếp hàng chờ timeout DB, không cạn thread pool).
 * Fallback trả REJECTED SYSTEM_UNAVAILABLE - fail-safe: không chắc chắn thì không nhận lệnh.
 */
@CircuitBreaker(name = "orders-db", fallbackMethod = "systemUnavailable")
public OrderResult placeOrder(OrderRequest req) { ... }
```

```java
// order-service/src/main/java/com/trading/order/service/OrderService.java:145-152
/** Fallback khi breaker mở hoặc lỗi hệ thống: fail-fast, KHÔNG nhận lệnh mù. */
private OrderResult systemUnavailable(OrderRequest req, Throwable t) {
    rejected.increment();
    log.error("He thong khong san sang, tu choi lenh clientOrderId={}", req.clientOrderId(), t);
    return new OrderResult(UUID.randomUUID().toString(), req.clientOrderId(), "REJECTED",
            "SYSTEM_UNAVAILABLE", req.symbol(), req.side(), req.price(), req.quantity(),
            null, null, System.currentTimeMillis());
}
```

```yaml
resilience4j:
  circuitbreaker:
    instances:
      orders-db:
        sliding-window-size: 20
        minimum-number-of-calls: 10
        failure-rate-threshold: 50 # >=50% call lỗi -> MỞ breaker
        wait-duration-in-open-state: 10s
        permitted-number-of-calls-in-half-open-state: 3
```

**Ba trạng thái của breaker:**

```
         ≥50% lỗi trong 20 call gần nhất
CLOSED ──────────────────────────────────► OPEN
  ▲                                          │
  │                                          │ sau 10 giây
  │  3/3 call thử thành công                 ▼
  └──────────────────────────  HALF_OPEN ◄───┘
                                    │
                                    │ 1 call thất bại
                                    └──────────► OPEN
```

| Tham số | Giá trị | Ý nghĩa |
|---|---|---|
| `sliding-window-size: 20` | | Tính tỉ lệ lỗi trên 20 call gần nhất |
| `minimum-number-of-calls: 10` | | 🔑 Chưa đủ 10 call thì **không đánh giá** — tránh mở breaker vì 1/1 lỗi lúc khởi động |
| `failure-rate-threshold: 50` | | ≥50% lỗi ⇒ mở |
| `wait-duration-in-open-state: 10s` | | Sau 10s chuyển sang HALF_OPEN để thăm dò |
| `permitted-number-of-calls-in-half-open-state: 3` | | Cho 3 call thử; thất bại một cái là quay lại OPEN |

🔑 **Vấn đề breaker giải quyết chính xác là gì.** Không phải "làm DB khoẻ lại" — nó không làm được điều đó. Nó ngăn **thread pool exhaustion**: nếu DB treo và mỗi request chờ timeout 30 giây, thì ở 200 req/s ta có 6000 request đang chờ, Tomcat hết 200 thread, và pod không trả lời được cả health check ⇒ Kubernetes restart pod ⇒ mất luôn các request đang xử lý. Breaker mở biến 30 giây chờ thành vài micro-giây từ chối, giữ pod sống và trả lời được.

🔑 **Fallback trả `REJECTED SYSTEM_UNAVAILABLE`, không trả `ACCEPTED`.** Đây lại là fail-safe: không ghi được bản ghi lệnh thì không được nói với khách là đã nhận lệnh. Nếu trả `ACCEPTED` khi DB chết, khách tưởng đã đặt lệnh nhưng không có gì trong hệ thống — tình huống tệ nhất có thể.

⚠️ Một điểm cần biết: `@CircuitBreaker` bọc **toàn bộ** `placeOrder`, bao gồm cả Redis và pre-trade check. Nhưng Redis đã có try/catch riêng và pre-trade check không bao giờ ném exception, nên thực tế breaker chỉ đếm lỗi từ `persistence.saveWithOutbox()` — đúng như tên `orders-db`.

⚠️ `DataIntegrityViolationException` (lệnh trùng) **được bắt trong method**, nên nó không tính là lỗi của breaker. Đó là điều đúng: lệnh trùng không phải dấu hiệu DB ốm.

### 2.6. Saga reply leg — đóng vòng đời lệnh

```java
// order-service/src/main/java/com/trading/order/consumer/ExecutionReportConsumer.java:23-41
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

🔑 **Vòng retry nằm NGOÀI `@Transactional`** — đây là một trong những chi tiết dễ sai nhất với Spring. Khi Hibernate ném `OptimisticLockingFailureException`, transaction hiện tại **đã bị đánh dấu rollback-only**. Nếu retry bên trong cùng transaction đó, ta sẽ nhận `UnexpectedRollbackException: Transaction silently rolled back because it has been marked as rollback-only` — một lỗi mà nhiều người mất hàng giờ để hiểu. Phải thoát ra ngoài, để transaction cũ kết thúc, rồi mở transaction **mới** đọc lại dữ liệu đã cập nhật.

⚠️ Comment nói "ném ra → error handler retry rồi đẩy DLT (đã cấu hình ở container factory)". Nhưng `executionListenerFactory` (`order-service/config/KafkaConfig.java:97-104`) **không** đặt `setCommonErrorHandler` với `DeadLetterPublishingRecoverer` — chỉ `pricing-risk-service` có. Nên thực tế: exception được `DefaultErrorHandler` mặc định xử lý (retry 10 lần không backoff, rồi log và bỏ qua). Topic `stock.executions.DLT` được Job tạo ra nhưng **không ai ghi vào**. Đây là một **Doc/Gap** chưa nằm trong danh sách 22 điểm — comment mô tả một hành vi chưa được cài đặt.

Chi tiết `OrderStatusService` và `OrderStateMachine` ở [chương 07](./07-matching-engine-va-saga-choreography.md).

### 2.7. Endpoint liệt kê — phân trang là bắt buộc

```java
// order-service/src/main/java/com/trading/order/controller/OrderController.java:55-88
/**
 * Liệt kê lệnh, mới nhất trước.
 *
 * LUÔN phân trang: bảng orders giữ cả lệnh REJECTED để audit nên chỉ có
 * tăng. `size` bị chặn cứng ở 200 - không để client tự đặt 1_000_000 rồi
 * kéo sập pod.
 */
@GetMapping
@RateLimiter(name = "orders")
public Page<OrderView> list(
        @RequestParam(required = false) String status,
        @RequestParam(required = false) String accountId,
        @RequestParam(defaultValue = "0") int page,
        @RequestParam(defaultValue = "20") int size) {

    int safeSize = Math.min(Math.max(size, 1), MAX_PAGE_SIZE);
    Pageable p = PageRequest.of(Math.max(page, 0), safeSize,
            Sort.by(Sort.Direction.DESC, "createdAt"));

    String st = (status == null || status.isBlank()) ? null : status.trim().toUpperCase();
    String acc = (accountId == null || accountId.isBlank()) ? null : accountId.trim();

    Page<OrderEntity> found;
    if (acc != null && st != null)      found = orderRepository.findAllByAccountIdAndStatus(acc, st, p);
    else if (acc != null)               found = orderRepository.findAllByAccountId(acc, p);
    else if (st != null)                found = orderRepository.findAllByStatus(st, p);
    else                                found = orderRepository.findAll(p);

    return found.map(OrderView::from);
}
```

```java
// order-service/src/main/java/com/trading/order/repository/OrderRepository.java:13-24
/*
 * Phân trang là BẮT BUỘC cho endpoint liệt kê, không phải tuỳ chọn.
 * Bảng orders giữ MỌI lệnh kể cả REJECTED để phục vụ audit, nên nó chỉ có
 * tăng. Một `findAll()` không giới hạn sẽ kéo cả bảng vào heap 336Mi của pod
 * và giết nó bằng OOM - và sự cố đó chỉ xảy ra ở môi trường có dữ liệu thật,
 * không bao giờ lộ ra lúc dev.
 */
```

💡 Ba chi tiết phòng thủ đáng học ở method này:
- `Math.min(Math.max(size, 1), MAX_PAGE_SIZE)` — kẹp cả hai đầu. `size=0` gây `IllegalArgumentException` từ `PageRequest`; `size=-1` cũng vậy; `size=1000000` kéo sập pod.
- `Math.max(page, 0)` — `page=-1` cũng ném exception.
- `status.trim().toUpperCase()` — chuẩn hoá đầu vào, để `?status=filled` cũng hoạt động.

Và `OrderView` thay vì `OrderEntity` — ba lý do ghi rõ trong `model/OrderView.java`:

```java
/**
 *  1. Entity còn gắn với persistence context. Trả thẳng nó thì Jackson serialize
 *     lúc đã ra ngoài transaction, và mọi quan hệ LAZY sẽ nổ
 *     LazyInitializationException - lỗi chỉ xuất hiện khi có dữ liệu thật, không
 *     bao giờ thấy lúc unit test.
 *
 *  2. Thêm một cột vào bảng là tự động lọt ra API, kể cả cột nội bộ. Ở đây đang
 *     có `version` (optimistic locking) và `id` (khoá tự tăng) - hai thứ client
 *     không cần biết và không nên phụ thuộc vào.
 *
 *  3. Đổi tên cột trong DB sẽ âm thầm phá hợp đồng API. Có DTO thì compiler bắt
 *     được ngay tại chỗ map.
 */
```

⚠️ Lưu ý: endpoint `list` không lọc theo quyền — bất kỳ ai có API key đều xem được lệnh của **mọi** `accountId`. Với API key tĩnh dùng chung, đây là lỗ hổng phân quyền thật. Production cần JWT mang `accountId` và lọc theo đó ở tầng service.

### 2.8. Graceful shutdown

```yaml
# order-service/src/main/resources/application.yml
server:
  port: 8083
  shutdown: graceful   # ngung nhan request moi, xu ly not request dang chay

spring:
  lifecycle:
    timeout-per-shutdown-phase: 30s   # du cho Kafka listener commit offset cuoi
```

Ghép với manifest K8s (`k8s/base/apps/order-service.yaml`):

```yaml
terminationGracePeriodSeconds: 60
lifecycle:
  preStop:
    sleep: { seconds: 5 }
```

🔑 **Ba con số phải cộng lại cho khớp:**

```
preStop (5s) + timeout-per-shutdown-phase (30s) = 35s  ≤  terminationGracePeriodSeconds (60s)  ✅
```

Trình tự khi pod bị xoá:
1. Kubernetes gỡ pod khỏi endpoint của Service **và** gửi `SIGTERM` — **song song**, không theo thứ tự.
2. `preStop: sleep 5` giữ container sống thêm 5 giây để kube-proxy/ingress kịp cập nhật bảng định tuyến. Không có bước này, request vẫn được gửi tới pod đã bắt đầu tắt.
3. `SIGTERM` → Spring `server.shutdown: graceful` ngừng nhận request mới, xử lý nốt request đang chạy.
4. Kafka listener container (`SmartLifecycle`) dừng poll, xử lý nốt batch hiện tại, commit offset cuối.
5. Nếu quá 30s chưa xong ⇒ Spring cưỡng chế dừng.
6. Nếu quá 60s container chưa thoát ⇒ Kubernetes gửi `SIGKILL`.

⚠️ **Gap #20:** `market-gateway` và `pricing-risk-service` **thiếu** cả `server.shutdown: graceful` lẫn `timeout-per-shutdown-phase`. Kafka listener của chúng vẫn dừng êm nhờ `SmartLifecycle`, nhưng **Tomcat đóng ngay lập tức** khi nhận `SIGTERM` ⇒ mọi request REST đang bay bị cắt giữa chừng — đúng thứ mà `preStop: sleep 5` vừa cất công bảo vệ.

---

## 3. Vấn đề được giải quyết

| Nếu không có | Hậu quả |
|---|---|
| `clientOrderId` do client sinh | Client timeout retry ⇒ lệnh trùng, khách mua hai lần |
| Redis fast-path | Mỗi retry đều xuống DB ⇒ chậm hơn, tải DB cao hơn |
| `UNIQUE(client_order_id)` | Redis chết ⇒ không gì chặn trùng |
| Staleness check | Khách đặt lệnh theo giá đã chết (cache không có TTL) |
| Risk check | Khách vào thị trường đang sụp đổ |
| Fat-finger check | Gõ nhầm số 0 ⇒ lệnh lệch 900% |
| `refPrice` đóng băng | Lệnh bị matching từ chối vì giá đã chạy trong lúc chờ |
| Rate limiter | Client bug retry vô hạn đè chết DB và Kafka |
| Circuit breaker | DB treo ⇒ thread pool cạn ⇒ pod chết ⇒ mất request đang xử lý |
| `health.redis.enabled: false` | Redis chết ⇒ K8s rút **toàn bộ** pod khỏi Service |
| Phân trang | `findAll()` trên bảng chỉ-tăng ⇒ OOM ở môi trường có dữ liệu thật |
| `OrderView` DTO | `LazyInitializationException`; cột nội bộ lọt ra API |
| Graceful shutdown | Request đang bay bị cắt mỗi lần rolling update |

---

## 4. Đánh đổi

### 4.1. Ưu

1. **Pre-trade check ~0ms** — đọc RAM, không gọi mạng.
2. **Degrade gracefully** — Redis chết, hệ vẫn đúng; DB chết, hệ từ chối rõ ràng thay vì treo.
3. **Idempotency 3 lớp** — mỗi lớp độc lập đủ để giữ tính đúng đắn.
4. **Fail-safe xuyên suốt** — mọi nhánh bất định đều dẫn tới từ chối.
5. **Không mất lệnh** nhờ Outbox.
6. **Rolling update không cắt request** (ở service này).

### 4.2. Nhược

1. **Dữ liệu giá là eventual consistent** — staleness check là thuế bắt buộc phải trả.
2. **Rate limit per-pod, per-service** — không phải trần cụm, không phải per-account.
3. **Pre-trade check không được tin** — matching-engine phải kiểm lại, nghĩa là logic trùng lặp một phần ở hai nơi.
4. ⚠️ **Lệch đồng hồ vô hiệu hoá staleness check âm thầm** (Gap #6).
5. ⚠️ **Ngưỡng staleness toàn cục** từ chối oan mã thanh khoản thấp (Ops #5).
6. ⚠️ **`orElseThrow()` che lỗi gốc** (Bug #4).
7. ⚠️ **`stock.executions` không có DLT thật** dù comment nói có.
8. ⚠️ **Không phân quyền theo account** ở endpoint liệt kê.
9. **Ngưỡng risk 15/35 hardcode ở service khác** — ràng buộc liên-service không ai cưỡng chế.

---

## 5. Phương án thay thế

| Phương án | Khi nào hợp | Vì sao repo không chọn |
|---|---|---|
| **REST đồng bộ sang pricing** | Cần dữ liệu tuyệt đối mới nhất | Temporal coupling; pricing chết ⇒ order chết |
| **Redis làm market data store chung** | Nhiều pod cần cùng một view | Thêm network hop vào mỗi lệnh; mỗi pod chỉ cần mã nó phụ trách |
| **Dedupe chỉ bằng Redis** | Muốn đơn giản | Redis không bền; mất dữ liệu ⇒ lệnh trùng |
| **Dedupe chỉ bằng DB** | Muốn ít thành phần | Mọi retry đều xuống DB; chậm hơn và tải cao hơn |
| **Rate limit ở Ingress** | Muốn trần toàn cụm | Repo **có dùng** như lớp thứ hai (`limit-rps: 20` trong Helm values); lớp trong service vẫn cần để bảo vệ pod cụ thể |
| **Bulkhead thay vì breaker** | Muốn cô lập tài nguyên theo nhóm | Breaker đủ cho một dependency duy nhất; bulkhead hợp khi có nhiều downstream |
| **Retry + backoff thay vì breaker** | Lỗi thoáng qua | Với DB treo, retry làm tình hình tệ hơn (thêm tải lên hệ đang ốm) |
| **Đặt pre-trade check ở matching-engine** | Muốn một nơi duy nhất | Mất phản hồi tức thì cho khách; mọi lệnh sai đều phải đi hết vòng saga mới biết |

---

## 6. Núm vặn cấu hình

| Tham số | Mặc định | Hệ quả khi đổi |
|---|---|---|
| `order.max-price-deviation-pct` | 5 | 🔑 Phải **nhỏ hơn** `matching.price-band-pct` (7), nếu không lệnh qua cửa 1 sẽ chết ở cửa 2 hàng loạt |
| `order.max-data-age-ms` | 10000 | Nhỏ ⇒ từ chối nhiều; lớn ⇒ cho đặt theo giá cũ. ⚠️ Nên theo nhóm mã |
| `resilience4j.ratelimiter.orders.limit-for-period` | 200 | Per-pod. Trần cụm = giá trị này × số pod |
| `...timeout-duration` | `0s` | 🔑 Khác 0 ⇒ request chờ, chiếm thread, thread pool cạn |
| `...failure-rate-threshold` | 50 | Thấp ⇒ breaker nhạy, mở oan; cao ⇒ mở muộn |
| `...minimum-number-of-calls` | 10 | 🔑 Thấp ⇒ mở breaker vì vài lỗi lúc khởi động |
| `...wait-duration-in-open-state` | 10s | Thời gian hệ bị "cắt" trước khi thử lại |
| `spring.data.redis.timeout` | 200ms | Trần cho phần tăng tốc |
| `management.health.redis.enabled` | `false` | 🔑 `true` ⇒ Redis chết kéo sập readiness toàn bộ pod |
| `IDEM_TTL` | 10 phút (hardcode) | Dài ⇒ Redis phình; ngắn ⇒ nhiều retry xuống DB |
| `MAX_PAGE_SIZE` | 200 (hardcode) | Trần chống OOM |
| `hikari.maximum-pool-size` | 20 | Mỗi lệnh một transaction ngắn |
| `security.api-key` | `demo-key` | ⚠️ Prod phải từ Secret/Vault |
| `spring.lifecycle.timeout-per-shutdown-phase` | 30s | Phải ≤ `terminationGracePeriodSeconds` − `preStop` |

---

## 7. Cách kiểm chứng

```bash
API='-H Content-Type:application/json -H X-API-KEY:demo-key'
URL='http://localhost:8083/api/v1/orders'

# 1. Lệnh hợp lệ -> 201
curl -s -w '\n%{http_code}\n' -X POST $URL $API \
  -d '{"clientOrderId":"T-001","symbol":"HPG","side":"BUY","price":28500,"quantity":100,"accountId":"ACC001"}'

# 2. Fat-finger -> 422 PRICE_DEVIATION
curl -s -X POST $URL $API \
  -d '{"clientOrderId":"T-FAT","symbol":"HPG","side":"BUY","price":285000,"quantity":100,"accountId":"ACC001"}' | jq .reason

# 3. Mã không tồn tại -> 422 NO_FRESH_MARKET_DATA
curl -s -X POST $URL $API \
  -d '{"clientOrderId":"T-NX","symbol":"KHONGCO","side":"BUY","price":100,"quantity":100,"accountId":"ACC001"}' | jq .reason

# 4. Thiếu API key -> 401
curl -s -o /dev/null -w '%{http_code}\n' -X POST $URL -H 'Content-Type: application/json' \
  -d '{"clientOrderId":"T-NOKEY","symbol":"HPG","side":"BUY","price":28500,"quantity":100,"accountId":"ACC001"}'

# 5. Validation sai -> 400
curl -s -o /dev/null -w '%{http_code}\n' -X POST $URL $API \
  -d '{"clientOrderId":"T-BAD","symbol":"HPG","side":"HOLD","price":28500,"quantity":100,"accountId":"ACC001"}'

# 6. Idempotency: gửi 3 lần, so sánh orderId
for i in 1 2 3; do
  curl -s -X POST $URL $API \
    -d '{"clientOrderId":"T-DUP","symbol":"HPG","side":"BUY","price":28500,"quantity":100,"accountId":"ACC001"}' | jq -r .orderId
done
#  -> 3 dòng GIỐNG HỆT NHAU

# 7. Rate limiter -> xuất hiện 429
seq 1 400 | xargs -P 50 -I{} curl -s -o /dev/null -w '%{http_code}\n' -X POST $URL $API \
  -d '{"clientOrderId":"RL-{}","symbol":"HPG","side":"BUY","price":28500,"quantity":100,"accountId":"ACC001"}' \
  | sort | uniq -c

# 8. Redis chết -> hệ VẪN ĐÚNG (chứng minh Redis chỉ là cache)
docker compose stop redis
curl -s http://localhost:8083/actuator/health | jq -r .status      # vẫn UP
curl -s -X POST $URL $API \
  -d '{"clientOrderId":"T-NOREDIS","symbol":"HPG","side":"BUY","price":28500,"quantity":100,"accountId":"ACC001"}' | jq -r .status
curl -s -X POST $URL $API \
  -d '{"clientOrderId":"T-NOREDIS","symbol":"HPG","side":"BUY","price":28500,"quantity":100,"accountId":"ACC001"}' | jq -r .orderId
#  -> lần 2 trả CÙNG orderId (nhờ DB constraint, không nhờ Redis)
docker compose logs --tail=3 order-service | grep "Redis khong kha dung"
docker compose start redis

# 9. Circuit breaker: DB chết -> SYSTEM_UNAVAILABLE, không treo
docker compose stop postgres
for i in $(seq 1 15); do
  curl -s -X POST $URL $API \
    -d "{\"clientOrderId\":\"CB-$i\",\"symbol\":\"HPG\",\"side\":\"BUY\",\"price\":28500,\"quantity\":100,\"accountId\":\"ACC001\"}" \
    | jq -r .reason
done
#  -> vài lỗi đầu chậm, sau đó SYSTEM_UNAVAILABLE trả về TỨC THÌ (breaker đã mở)
curl -s http://localhost:8083/actuator/metrics/resilience4j.circuitbreaker.state | jq
docker compose start postgres

# 10. Phân trang
curl -s "$URL?page=0&size=5&status=FILLED" $API | jq '{total:.totalElements, pages:.totalPages, size:.size}'
curl -s "$URL?size=999999" $API | jq .size      # -> 200 (bị kẹp)

# 11. Metrics nghiệp vụ
curl -s http://localhost:8083/actuator/prometheus | grep -E 'orders_(accepted|rejected|duplicate|filled)_total'
```

---

## 8. Hạn chế đã biết

| # | Loại | Vị trí | Vấn đề | Hướng sửa |
|:--:|---|---|---|---|
| 4 | **Bug** | `service/OrderService.java:97` | `orElseThrow()` biến mọi `DataIntegrityViolationException` khác thành `NoSuchElementException`, che lỗi gốc | `orElseThrow(() -> dup)`, hoặc kiểm tra tên constraint |
| 5 | **Ops** | `service/OrderService.java:112` | Staleness 10s là `@Value` toàn cục ⇒ mã thanh khoản thấp bị từ chối oan | Ngưỡng theo nhóm mã hoặc ngưỡng động |
| 6 | **Gap** | `service/OrderService.java:112` | Đồng hồ pricing chạy nhanh ⇒ hiệu số âm ⇒ check bị vô hiệu hoá **âm thầm** | Counter + từ chối khi lệch bất thường |
| — | Gap | `config/KafkaConfig.java:97-104` | Comment ở `ExecutionReportConsumer` nói có DLT, nhưng `executionListenerFactory` không đặt `DeadLetterPublishingRecoverer` | Thêm recoverer, hoặc sửa comment |
| — | Gap | `controller/OrderController.java:55-88` | `list` không lọc theo quyền — API key dùng chung xem được lệnh mọi account | JWT mang `accountId`, lọc ở service |
| — | Ops | `service/OrderService.java:115` | Ngưỡng `riskLevel` 15/35 hardcode ở `RiskCalculator` (service khác) nhưng hệ quả xảy ra ở đây | Đưa vào cấu hình chung, hoặc truyền ngưỡng theo event |
| — | Ops | `config/ApiKeyFilter.java` | API key tĩnh, không phân quyền | OAuth2/JWT, mTLS, rate limit per-account |

---

## 9. Câu hỏi phỏng vấn

**Q: Vì sao `clientOrderId` do client sinh mà không phải server?**

A: Vì idempotency chỉ có ý nghĩa khi client *có thể lặp lại cùng một yêu cầu*. Nếu server sinh id, client gặp timeout sẽ không biết lệnh đã tạo hay chưa, và retry tạo ra một lệnh mới hoàn toàn — khách mua hai lần. Với khoá do client sinh, retry mang theo cùng khoá, server nhận ra và trả lại kết quả cũ. Đây cũng là lý do endpoint tra cứu dùng `clientOrderId` chứ không phải `orderId`: sau một timeout, thứ duy nhất client còn trong tay là khoá nó tự sinh.

**Q: Redis chết lúc 2 giờ sáng. Hệ thống phản ứng thế nào?**

A: Vẫn đúng, chỉ chậm hơn một chút. Ba dòng cấu hình làm điều đó thành hiện thực: `timeout: 200ms` để Redis treo không kéo treo lệnh; `try/catch` quanh lời gọi để lỗi chỉ thành một dòng warn rồi đi tiếp; và quan trọng nhất `management.health.redis.enabled: false` để readiness probe không fail — nếu thiếu dòng này, Kubernetes sẽ rút **toàn bộ** pod order-service khỏi Service và không ai đặt được lệnh, dù mọi pod vẫn phục vụ tốt qua DB constraint. Nguyên tắc chung: health check phải phản ánh *khả năng phục vụ*, không phản ánh *sự hiện diện của mọi dependency*.

**Q: Vì sao fallback của circuit breaker trả `REJECTED` thay vì `ACCEPTED` hay HTTP 503?**

A: Fail-safe với tiền của người khác. Nếu không ghi được bản ghi lệnh thì tuyệt đối không được nói với khách là đã nhận — khách sẽ tưởng mình đang có vị thế trong khi hệ thống trống rỗng. Về việc dùng `REJECTED` thay vì `503`: lựa chọn hiện tại giữ **cùng một hình dạng response** cho mọi kết quả, nên client chỉ cần một đường xử lý. Nhưng tôi thấy `503 Service Unavailable` kèm `Retry-After` sẽ đúng ngữ nghĩa HTTP hơn, vì `SYSTEM_UNAVAILABLE` là lỗi *tạm thời của hệ thống*, còn `422` ngụ ý *yêu cầu có vấn đề*. Client cần phân biệt hai thứ: cái đầu nên retry, cái sau thì không.

**Q: Pre-trade check của order-service và TradingRules của matching-engine trùng nhau một phần. Đó chẳng phải vi phạm DRY sao?**

A: Trùng lặp ở đây là có chủ đích, và hai bên bảo vệ hai đối tượng khác nhau: pre-trade check bảo vệ **khách hàng** (fat-finger, dữ liệu cũ, rủi ro cao), TradingRules thi hành luật của **sàn** (lô chẵn, biên độ, phiên). Quan trọng hơn: order-service **không được tin** — client có thể publish thẳng vào `stock.orders` bỏ qua nó, và hai bên có tham số khác nhau (5% vs 7%) một cách cố ý. Nếu dồn về một nơi: đặt ở order-service thì sàn mất khả năng tự bảo vệ; đặt ở matching-engine thì khách phải chờ hết vòng saga mới biết mình gõ nhầm. Ranh giới tin cậy quan trọng hơn DRY ở đây.

**Q: Rate limiter 200 req/s nhưng có 3 pod. Trần thật là bao nhiêu?**

A: 600 req/s, và đó là một hạn chế thật. Resilience4j RateLimiter là in-memory, per-instance. Nếu cần trần toàn cụm, có ba cách: (1) rate limit ở Ingress — repo đã làm, `nginx.ingress.kubernetes.io/limit-rps: "20"` trong `values.yaml`; (2) Redis-based distributed rate limiter — nhưng khi đó Redis lại trở thành thành phần thiết yếu, mâu thuẫn với nguyên tắc "Redis chỉ là cache" ở đây; (3) API Gateway chuyên dụng. Hạn chế thứ hai còn đáng ngại hơn: rate limit hiện là per-service chứ không per-account, nên một khách hàng đặt lệnh loạn sẽ đẩy mọi khách khác vào `429`.

**Q: Staleness check dùng `System.currentTimeMillis() - md.calcTime()`. Điểm yếu là gì?**

A: Nó giả định đồng hồ hai pod đồng bộ. Nếu đồng hồ pricing chạy **nhanh** hơn order, hiệu số ra số âm, điều kiện `> 10000` luôn false, và staleness check bị **vô hiệu hoá hoàn toàn mà không có bất kỳ dấu hiệu nào** — không log, không metric, không alert. Đây đúng là mẫu "lỗi im lặng nguy hiểm hơn lỗi ồn ào". Đáng chú ý là cùng mẫu này xuất hiện ở `TickConsumer` với `Math.max(0, now - eventTime)` — kẹp giá trị làm metric trông đẹp trong khi đang che giấu lệch đồng hồ. Cách sửa: đếm riêng số lần hiệu số âm bất thường bằng một counter, và từ chối lệnh (fail-safe) khi phát hiện, thay vì âm thầm cho qua.

---

## 10. Bảng tổng kết

| Khía cạnh | Lựa chọn | Lý do | Cái giá |
|---|---|---|---|
| Đọc giá | CQRS local view (`ConcurrentHashMap`) | ~0ms; pricing chết order vẫn chạy | Eventual consistency ⇒ bắt buộc staleness check |
| Khoá idempotency | `clientOrderId` do client sinh | Retry sau timeout nhận lại kết quả cũ | Client phải sinh UUID |
| Dedupe lớp 1 | Redis, TTL 10′, timeout 200ms | Nhanh, tập trung mọi pod | Có thể chết — chấp nhận |
| Dedupe lớp 2 | `UNIQUE(client_order_id)` | 🔑 Chốt chặn tuyệt đối | ~3ms |
| Health check | Loại Redis khỏi readiness | Cache phụ không được kéo sập pod khoẻ | Mất tín hiệu Redis trong `/health` |
| Pre-trade | 3 luật, fail-safe | Bảo vệ khách khỏi dữ liệu cũ / rủi ro / gõ nhầm | Từ chối oan; ⚠️ lệch đồng hồ vô hiệu hoá |
| `refPrice` | Đóng băng tại thời điểm nhận | Matching từ chối theo giá lúc đặt, không theo giá hiện tại | — |
| Rate limit | 200/s/pod, `timeout 0s` | Fail-fast, không chiếm thread | Per-pod, per-service |
| Circuit breaker | 50% / 20 call / 10s | Chống thread pool exhaustion | Mở oan nếu `minimum-number-of-calls` quá thấp |
| Fallback | `REJECTED SYSTEM_UNAVAILABLE` | Fail-safe: không ghi được thì không nhận | Nên là `503` + `Retry-After` |
| Ghi lệnh | 2 INSERT, 1 transaction | Không dual-write | Độ trễ relay ≤ 500ms |
| Đóng vòng đời | State machine + optimistic lock + retry ngoài tx | Chặn transition vô lý, idempotency miễn phí | Chưa hỗ trợ partial fill |
| Liệt kê | Phân trang bắt buộc, trần 200, DTO | Chống OOM, chống rò rỉ cột nội bộ | ⚠️ Chưa phân quyền theo account |
| Shutdown | graceful 30s + preStop 5s ≤ grace 60s | Không cắt request khi rolling update | ⚠️ 2 service khác còn thiếu |

> **Đọc tiếp:** [Chương 05 — Distributed Transactions, Outbox Pattern & CDC](./05-distributed-transactions-outbox-va-cdc.md) — mổ xẻ lớp 3, trái tim tính đúng đắn của hệ thống.
