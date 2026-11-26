# Chương 00 — Đường Đi Của Một Lệnh: Xuyên Suốt End-to-End

> **Mục đích chương này:** thay vì mô tả từng pattern tách rời, ta bám theo **đúng một lệnh** đi qua toàn hệ thống, dừng lại ở từng dòng mã nó chạm vào. Hết chương, bạn sẽ biết *chính xác* chuyện gì xảy ra giữa lúc bấm `curl` và lúc lệnh hiện `FILLED`.
>
> **Tài liệu đối chiếu:** `GUIDE.md` mục 3, `DESIGN-NOTES.md`, `DEEP-DIVE.md` mục 7-9.

---

## 1. Lệnh chúng ta sẽ theo dõi

```bash
curl -i -X POST http://localhost:8083/api/v1/orders \
  -H 'Content-Type: application/json' \
  -H 'X-API-KEY: demo-key' \
  -d '{
        "clientOrderId": "DEMO-0001",
        "symbol":        "HPG",
        "side":          "BUY",
        "price":         28500,
        "quantity":      100,
        "accountId":     "ACC001"
      }'
```

Kết quả mong đợi sau ~1-2 giây:

```bash
curl -s http://localhost:8083/api/v1/orders/DEMO-0001 -H 'X-API-KEY: demo-key' | jq .status
# "FILLED"
```

Giữa hai lệnh `curl` đó, lệnh đi qua **2 service, 2 topic Kafka, 2 transaction database, 2 bảng outbox** và quay về. Dưới đây là toàn bộ hành trình.

---

## 2. Điều kiện tiên quyết: dòng giá phải đang chảy

Trước khi lệnh được chấp nhận, hệ thống **phải** đã có dữ liệu giá tươi cho mã `HPG`. Đây không phải chi tiết phụ — nó là lớp phòng thủ đầu tiên. Nên ta bắt đầu từ dòng giá.

### 2.1. Tick được sinh ra — `SimulatedFeed`

`market-gateway/src/main/java/com/trading/gateway/feed/SimulatedFeed.java:38-51`

```java
@Scheduled(fixedRate = 200) // 4 mã x 5 lần/giây = 20 tick/s (demo)
public void generate() {
    if (!enabled) return;
    long now = System.currentTimeMillis();
    lastPrices.replaceAll((symbol, price) -> {
        // Random walk +-0.2%
        double drift = 1 + (ThreadLocalRandom.current().nextDouble() - 0.5) * 0.004;
        BigDecimal next = price.multiply(BigDecimal.valueOf(drift))
                .setScale(2, RoundingMode.HALF_UP);
        long vol = ThreadLocalRandom.current().nextLong(1, 500);
        producer.send(new TickEvent(symbol, next, vol, now));
        return next;
    });
}
```

Ba chi tiết đáng chú ý:

1. **`replaceAll` trên `ConcurrentHashMap`** giữ giá mới làm input cho lần sinh kế tiếp — đó là định nghĩa của random walk: giá kế tiếp là hàm của giá hiện tại, không phải số ngẫu nhiên độc lập. Nhờ vậy chuỗi giá có *tính liên tục* và volatility tính ra mới có nghĩa.
2. **`eventTime = now` lấy một lần cho cả 4 mã.** Đây là mốc thời gian **tại nguồn**, dùng ở hạ nguồn để đo latency end-to-end.
3. **`feed.simulated.enabled`** tắt được bằng config. Prod đặt `SIMULATED_FEED=false` (`k8s/overlays/prod/kustomization.yaml`) và cắm feed thật qua REST.

### 2.2. Map domain model sang wire format

`market-gateway/src/main/java/com/trading/gateway/kafka/TickProducer.java:28-47`

```java
public void send(TickEvent tick) {
    // Map domain model (record) -> wire format (Avro). Tách 2 tầng có chủ đích:
    // REST API / code nội bộ dùng record gọn gàng, wire format do schema quản.
    // Decimal logical type yêu cầu ĐÚNG scale=4 như khai báo trong .avsc.
    TickEventAvro avro = TickEventAvro.newBuilder()
            .setSymbol(tick.symbol())
            .setPrice(tick.price().setScale(4, RoundingMode.HALF_UP))
            .setVolume(tick.volume())
            .setEventTime(tick.eventTime())
            .build();

    kafkaTemplate.send("stock.ticks", tick.symbol(), avro)
            .whenComplete((result, ex) -> {
                if (ex != null) {
                    errorCounter.increment();
                    log.error("Gui tick that bai: {}", tick, ex);
                } else {
                    sentCounter.increment();
                }
            });
}
```

🔑 **`setScale(4, HALF_UP)` không phải làm đẹp.** Avro `decimal` logical type mã hoá `BigDecimal` thành `bytes` theo **unscaled value** cộng với `scale` cố định trong schema. Nếu `BigDecimal` truyền vào có scale khác 4, Avro ném exception lúc serialize. `SimulatedFeed` tạo giá `setScale(2)`, nên dòng này là chỗ bắt buộc phải chuẩn hoá.

🔑 **Partition key là `tick.symbol()`.** Đây là quyết định kiến trúc quan trọng nhất của toàn hệ, không phải tối ưu hoá — xem mục 3.2 và [chương 02](./02-kafka-messaging-va-schema-registry.md).

**Gửi bất đồng bộ (`whenComplete`)**: gateway không chờ Kafka ack. Với tick giá, mất một tick lẻ không nghiêm trọng (tick kế tiếp 200ms sau sẽ sửa lại bức tranh), nhưng chặn luồng feed thì nghiêm trọng. Đối lập hoàn toàn với luồng lệnh — xem mục 5.3.

### 2.3. Tick nằm trong Kafka

```
Topic: stock.ticks    12 partition    retention 7 ngày
key   = "HPG"
value = Avro binary, ~30 byte (JSON tương đương ~95 byte)
       [magic byte 0x00][4 byte schema ID][payload Avro]
```

Consumer đọc 4 byte schema ID, hỏi Schema Registry để lấy writer schema (có cache local), rồi deserialize. Đó là lý do message Avro nhỏ hơn JSON 3-5 lần: tên field không lặp lại trong mỗi message.

---

## 3. pricing-risk-service làm giàu dữ liệu

### 3.1. Consumer nhận cả một lô

`pricing-risk-service/src/main/java/com/trading/pricing/consumer/TickConsumer.java:51-68`

```java
@KafkaListener(topics = "stock.ticks", containerFactory = "tickListenerFactory")
public void onTicks(List<TickEventAvro> avroTicks) {
    long start = System.nanoTime();
    long now = System.currentTimeMillis();

    for (TickEventAvro avro : avroTicks) {
        // Map wire format (Avro) -> domain model (record) ngay tại biên;
        // toàn bộ logic phía trong không biết gì về Avro
        TickEvent tick = new TickEvent(avro.getSymbol(), avro.getPrice(),
                avro.getVolume(), avro.getEventTime());
        EnrichedMarketData enriched = riskCalculator.onTick(tick);   // O(1)
        snapshotStore.update(enriched);                              // O(1), in-memory
        enrichedTemplate.send("stock.enriched", enriched.symbol(), enriched); // async
        e2eLatencyTimer.record(Math.max(0, now - tick.eventTime()), TimeUnit.MILLISECONDS);
    }
    processingTimer.record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
}
```

**Toàn bộ hot path không có một lệnh I/O chặn nào.** Ba việc trong vòng lặp: tính toán thuần CPU, `ConcurrentHashMap.put`, và một `send` bất đồng bộ. Ghi database nằm ở chỗ khác hẳn (mục 3.4).

`containerFactory = "tickListenerFactory"` trỏ tới `KafkaConfig.java:61-84`, nơi đặt `setBatchListener(true)` và `MAX_POLL_RECORDS_CONFIG = 1000`. Một lần `poll()` lấy tối đa 1000 tick, gọi listener **một lần** với cả `List`. Chi phí cố định mỗi lần gọi (span tracing, commit offset) được chia cho 1000 bản ghi.

### 3.2. 🔑 Vì sao thứ tự theo mã là bắt buộc

`pricing-risk-service/src/main/java/com/trading/pricing/service/RiskCalculator.java:42-89`

```java
private static class SymbolState {
    double lastPrice = Double.NaN;
    double ema = Double.NaN;
    final Deque<Double> returns = new ArrayDeque<>(WINDOW);
    double sum = 0, sumSq = 0; // incremental mean/variance

    EnrichedMarketData update(TickEvent tick) {
        double px = tick.price().doubleValue();

        // 1) EMA giá
        ema = Double.isNaN(ema) ? px : EMA_ALPHA * px + (1 - EMA_ALPHA) * ema;

        // 2) Log-return + sliding window variance (O(1))
        if (!Double.isNaN(lastPrice) && lastPrice > 0) {
            double r = Math.log(px / lastPrice);
            if (returns.size() == WINDOW) {
                double old = returns.pollFirst();
                sum -= old; sumSq -= old * old;
            }
            returns.addLast(r);
            sum += r; sumSq += r * r;
        }
        lastPrice = px;
        ...
    }
}
```

Hai công thức này **phụ thuộc thứ tự**:

| Công thức | Phụ thuộc | Hậu quả nếu sai thứ tự |
|---|---|---|
| `ema = α·px + (1-α)·ema` | giá trị `ema` của tick **trước đó** | EMA hội tụ về một đường khác hẳn; càng lâu càng lệch |
| `r = ln(px / lastPrice)` | `lastPrice` là giá **liền trước** | log-return sai dấu và sai độ lớn ⇒ variance sai ⇒ volatility sai ⇒ `riskLevel` sai |

Và `riskLevel` chính là thứ `order-service` dùng để **từ chối lệnh của khách**. Sai thứ tự tick ⇒ từ chối oan hoặc cho qua lệnh đáng ra phải chặn.

**Kafka chỉ đảm bảo thứ tự trong phạm vi một partition.** Vì `key = symbol`, mọi tick của `HPG` băm về cùng một partition, nên:
- Chúng được ghi theo thứ tự.
- Chúng được đọc theo thứ tự.
- Chúng được xử lý bởi **đúng một consumer thread** tại mọi thời điểm.

Lợi ích kèm theo — `RiskCalculator.java:35-40`:

```java
public EnrichedMarketData onTick(TickEvent tick) {
    SymbolState st = states.computeIfAbsent(tick.symbol(), s -> new SymbolState());
    synchronized (st) {
        return st.update(tick);
    }
}
```

`synchronized` ở đây **gần như không bao giờ bị tranh chấp**: chỉ một thread xử lý `HPG`. JVM nhận ra lock uncontended và dùng biased/thin lock — chi phí vài nanosecond. Khối `synchronized` tồn tại như một mạng an toàn cho trường hợp rebalance làm partition đổi chủ, chứ không phải cho luồng chạy bình thường.

### 3.3. Kết quả: `EnrichedMarketData`

`common-contracts/src/main/java/com/trading/common/model/EnrichedMarketData.java`

```java
public record EnrichedMarketData(
        String symbol,
        BigDecimal lastPrice,
        BigDecimal emaPrice,
        BigDecimal volatility,
        BigDecimal var95,
        String riskLevel,        // LOW / MEDIUM / HIGH
        long calcTime,
        long sourceEventTime
) {}
```

Phân loại rủi ro — `RiskCalculator.java:75`:

```java
String riskLevel = vol < 15 ? "LOW" : vol < 35 ? "MEDIUM" : "HIGH";
```

`vol` là volatility **năm hoá tính theo %**: `stdPerTick * sqrt(252 * 6.5 * 3600) * 100`. Hằng số `ANNUALIZE` giả định mỗi tick cách nhau ~1 giây (252 ngày giao dịch × 6,5 giờ × 3600 giây).

⚠️ Giả định đó **sai với feed của chính repo này**: `SimulatedFeed` phát 5 tick/giây cho mỗi mã, tức khoảng cách tick là 200ms chứ không phải 1s. Volatility năm hoá vì thế bị thổi lên khoảng $\sqrt{5} \approx 2,24$ lần. Con số tuyệt đối không dùng để ra quyết định tài chính thật được; nhưng *phân lớp tương đối* giữa các mã vẫn có nghĩa, và ngưỡng 15/35 được chọn để khớp với dải giá trị thực tế của feed này.

`calcTime = System.currentTimeMillis()` — mốc thời gian **tại thời điểm tính xong**. Đây là trường mà `order-service` dùng cho staleness check (mục 4.3).

### 3.4. Cold path: write-behind xuống PostgreSQL

`pricing-risk-service/src/main/java/com/trading/pricing/service/SnapshotStore.java:36-68`

```java
public void update(EnrichedMarketData data) {
    latest.put(data.symbol(), data);   // hot path - O(1), lock-free read
}

@Scheduled(fixedDelay = 5000)
public void flushToDatabase() {
    if (latest.isEmpty()) return;
    Instant now = Instant.now();
    List<MarketSnapshotEntity> batch = latest.values().stream().map(d -> {
        MarketSnapshotEntity e = new MarketSnapshotEntity();
        e.setSymbol(d.symbol());
        ...
        e.setSnapTime(now);
        return e;
    }).toList();
    try {
        repository.saveAll(batch);   // Hibernate batch insert
    } catch (Exception ex) {
        // Không ném exception: DB lỗi không được làm chết scheduler / hot path
        log.error("Flush DB that bai, se thu lai lan sau", ex);
    }
}
```

**Hai đường tách biệt hoàn toàn:**

| | Hot path | Cold path |
|---|---|---|
| Tần suất | mỗi tick (20/s demo, hàng chục nghìn/s thật) | mỗi 5 giây |
| Thao tác | `ConcurrentHashMap.put` | `saveAll` batch insert |
| Thời gian | nanosecond | vài chục đến vài trăm ms |
| DB chết thì | **không ảnh hưởng** | log lỗi, thử lại lần sau |
| Mất dữ liệu khi pod chết | tối đa 5s snapshot | — |

Mất 5 giây snapshot chấp nhận được vì **Kafka vẫn giữ raw tick 7 ngày** (`TOPIC_RETENTION_MS=604800000`): có thể replay để dựng lại. Database ở đây là *bản sao để truy vấn lịch sử*, không phải nguồn sự thật của luồng realtime.

⚠️ **Ops #3 (chương 14):** `latest` không bao giờ bị `remove`. Nên mọi lần flush đều ghi lại **toàn bộ** symbol, kể cả khi thị trường đã đóng cửa và không có tick mới. 50 mã × 17.280 lần flush/ngày = **864.000 row/ngày**, gần như toàn bộ là bản sao chỉ khác cột `snap_time`. Hướng sửa (flush theo diff) ở [chương 03](./03-pricing-realtime-risk-va-write-behind-cache.md) mục 9.

### 3.5. Publish `stock.enriched`

`pricing-risk-service/config/KafkaConfig.java:100-117` — producer này dùng **JSON**, không phải Avro:

```java
props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
props.put(ProducerConfig.ACKS_CONFIG, "1");
props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
```

Vì sao `stock.ticks` dùng Avro còn `stock.enriched` dùng JSON? Đây là **di cư từng topic một** — `DEEP-DIVE.md` mục 8.6. Chuyển một topic sang Avro cần: viết `.avsc`, sinh class, sửa producer, sửa mọi consumer, test tương thích. Làm cả 4 topic cùng lúc là một lần deploy khổng lồ không rollback được từng phần. Làm từng topic thì mỗi bước đều nhỏ và đảo ngược được. `stock.ticks` được chọn trước vì nó có lưu lượng cao nhất — nơi tiết kiệm băng thông đáng tiền nhất.

`acks=1` cho dữ liệu giá: mất một bản ghi enriched không chết ai, bản kế tiếp 200ms sau sẽ ghi đè. Đối lập với `acks=all` ở luồng lệnh (mục 5.3).

---

## 4. order-service nhận lệnh

### 4.1. Qua cổng bảo vệ

`order-service/src/main/java/com/trading/order/config/ApiKeyFilter.java:32-41`

```java
@Override
protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
                                FilterChain chain) throws ServletException, IOException {
    String path = request.getRequestURI();
    // Chỉ bảo vệ API nghiệp vụ; actuator để K8s probe + Prometheus scrape
    if (path.startsWith("/api/") && !apiKey.equals(request.getHeader("X-API-KEY"))) {
        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        response.getWriter().write("{\"error\":\"invalid or missing X-API-KEY\"}");
        return;
    }
    chain.doFilter(request, response);
}
```

Chỉ `/api/**` bị chặn; `/actuator/**` để mở có chủ đích vì kubelet probe và Prometheus scrape không mang được API key. Đây là "sàn, không phải trần" — xem [chương 12](./12-bao-mat-he-thong.md).

### 4.2. Validation và rate limit

`order-service/src/main/java/com/trading/order/model/OrderRequest.java`

```java
public record OrderRequest(
        @NotBlank String clientOrderId,
        @NotBlank String symbol,
        @NotNull @Pattern(regexp = "BUY|SELL") String side,
        @NotNull @Positive BigDecimal price,
        @Min(1) long quantity,
        @NotBlank String accountId
) {}
```

`order-service/src/main/java/com/trading/order/controller/OrderController.java:45-52`

```java
@PostMapping
@RateLimiter(name = "orders")
public ResponseEntity<OrderResult> place(@Valid @RequestBody OrderRequest request) {
    OrderResult result = orderService.placeOrder(request);
    HttpStatus status = "ACCEPTED".equals(result.status())
            ? HttpStatus.CREATED : HttpStatus.UNPROCESSABLE_ENTITY;
    return ResponseEntity.status(status).body(result);
}
```

Mã HTTP được chọn cẩn thận:
- `201 CREATED` — lệnh được nhận, đã có bản ghi.
- `422 UNPROCESSABLE_ENTITY` — **request hợp lệ về cú pháp nhưng bị từ chối về nghiệp vụ**. Không phải `400` (400 nghĩa là client gửi sai định dạng, sửa lại thì sẽ được — ở đây sửa lại cũng vẫn bị từ chối nếu thị trường vẫn rủi ro cao).
- `429 TOO_MANY_REQUESTS` — vượt RateLimiter, qua `@ExceptionHandler(RequestNotPermitted.class)` ở `OrderController.java:104-108`.

RateLimiter cấu hình ở `order-service/src/main/resources/application.yml`:

```yaml
resilience4j:
  ratelimiter:
    instances:
      orders:
        limit-for-period: 200      # 200 lệnh/giây/pod
        limit-refresh-period: 1s
        timeout-duration: 0s       # không xếp hàng chờ - từ chối ngay (fail-fast)
```

🔑 `timeout-duration: 0s` là điểm tinh tế. Nếu đặt `100ms`, request vượt ngưỡng sẽ **nằm chờ** 100ms rồi mới bị từ chối — nghĩa là mỗi request thừa vẫn chiếm một thread Tomcat. Dưới cơn bão retry, thread pool cạn và pod chết dù rate limiter đang "bảo vệ" nó. `0s` = từ chối tức thì, trả thread về pool ngay.

### 4.3. Bốn lớp phòng thủ chống trùng

`order-service/src/main/java/com/trading/order/service/OrderService.java:74-107`

```java
@CircuitBreaker(name = "orders-db", fallbackMethod = "systemUnavailable")
public OrderResult placeOrder(OrderRequest req) {
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

    // ===== Lớp 2: pre-trade check =====
    OrderResult result = check(req);

    // ===== Lớp 3: persist + outbox, DB constraint chặn trùng tuyệt đối =====
    try {
        persistence.saveWithOutbox(req, result);
    } catch (DataIntegrityViolationException dup) {
        duplicates.increment();
        OrderResult existing = persistence.findExisting(req.clientOrderId()).orElseThrow();
        cacheResult(idemKey, existing);
        return existing;
    }

    if ("ACCEPTED".equals(result.status())) accepted.increment(); else rejected.increment();

    // ===== Lớp 4: cache kết quả =====
    cacheResult(idemKey, result);
    return result;
}
```

Lệnh `DEMO-0001` của ta đi lần đầu nên Redis miss, rơi xuống lớp 2.

🔑 **Nguyên tắc nền:** Redis là **cache tăng tốc**, DB UNIQUE constraint là **source of truth của dedupe**. Khối `try/catch` quanh Redis không nuốt lỗi âm thầm — nó log cảnh báo rồi đi tiếp, vì mọi lớp phía trên có thể chết mà hệ vẫn đúng.

⚠️ **Bug #4 (chương 14):** `orElseThrow()` ở dòng 97 giả định mọi `DataIntegrityViolationException` đều là vi phạm `uq_client_order`. Nếu đó là một constraint khác (ví dụ `order_id` trùng), `findExisting` trả `Optional.empty()` và ta nhận `NoSuchElementException` — che mất lỗi gốc. Sửa: kiểm tra tên constraint, hoặc tối thiểu `orElseThrow(() -> dup)` để giữ nguyên exception ban đầu.

### 4.4. Ba luật pre-trade

`order-service/src/main/java/com/trading/order/service/OrderService.java:109-128`

```java
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

| Luật | Ngưỡng | Bảo vệ ai khỏi cái gì |
|---|---|---|
| **Staleness** | `calcTime` cũ hơn 10s | Khách khỏi việc đặt lệnh dựa trên giá đã chết. Nếu `pricing-risk-service` sập hoặc Kafka tắc, cache trong RAM *vẫn còn giá trị cũ* — không check thì khách mua theo giá 10 phút trước. |
| **Risk level** | `riskLevel == HIGH` | Khách khỏi vào thị trường đang biến động cực mạnh. |
| **Fat-finger** | lệch > 5% giá tham chiếu | Khách khỏi gõ nhầm `285000` thay vì `28500`. |

🔑 **Triết lý fail-safe: không chắc chắn thì không nhận lệnh.** Ba luật đều *từ chối* khi gặp bất định, không bao giờ *cho qua*. Với tiền của người khác, từ chối oan gây khó chịu; cho qua nhầm gây mất tiền và kiện tụng.

🔑 **Đây là đọc RAM, không phải gọi mạng.** `cache.get(...)` chạm vào `ConcurrentHashMap` trong chính JVM này:

`order-service/src/main/java/com/trading/order/consumer/MarketDataCache.java:16-29`

```java
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

Đây là **CQRS local materialized view**. Nếu thay bằng `restTemplate.getForObject("http://pricing-risk-service/...")` thì:
- Thêm 1-5ms vào mỗi lệnh.
- `pricing-risk-service` chậm 100ms ⇒ `order-service` chậm 100ms (temporal coupling).
- `pricing-risk-service` chết ⇒ `order-service` chết theo.

Cái giá phải trả: dữ liệu trễ 1-5ms so với thực tế, và có thể trễ **vô hạn** nếu luồng Kafka đứt. Đó chính là lý do staleness check tồn tại — nó là *điều kiện đi kèm bắt buộc* của lựa chọn cache này, không phải tính năng cộng thêm.

⚠️ **Gap #6 (chương 14):** `System.currentTimeMillis() - md.calcTime()` giả định đồng hồ hai pod đồng bộ. Nếu đồng hồ của pricing chạy **nhanh hơn**, hiệu số ra số âm, `> maxDataAgeMs` luôn false, và staleness check bị vô hiệu hoá **âm thầm** — không log, không metric. Nên có phòng vệ cho nhánh âm bất thường.

⚠️ **Ops #5:** `max-data-age-ms` là một `@Value` toàn cục. Mã thanh khoản cao (VN30F) có tick vài lần/giây; mã thanh khoản thấp có thể 30 giây mới có một tick. Ngưỡng 10s chung làm nhóm thứ hai bị từ chối oan liên tục.

### 4.5. 🔑 Trái tim: hai INSERT trong một transaction

Lệnh `DEMO-0001` qua được cả ba luật, `check()` trả về `OrderResult` với `status = ACCEPTED` và `refPrice = md.lastPrice()`. Giờ tới bước quan trọng nhất của cả hệ thống:

`order-service/src/main/java/com/trading/order/service/OrderPersistence.java:37-66`

```java
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
    ...
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

**Vì sao đây là điểm mấu chốt.** Cách viết "tự nhiên" sẽ là:

```java
orderRepository.save(order);                      // ghi DB
kafkaTemplate.send("stock.orders", result);       // publish Kafka
```

Đó là **dual-write** — ghi vào hai hệ thống không chia sẻ transaction. Bốn kịch bản hỏng:

| Thời điểm crash | Hậu quả |
|---|---|
| Sau `save`, trước `send` | Lệnh có trong DB, matching-engine **không bao giờ biết**. Lệnh treo `ACCEPTED` vĩnh viễn, tiền khách bị giữ. |
| `send` thành công, `save` rollback | Matching khớp một lệnh **không tồn tại**. Có execution không có order. |
| `send` timeout nhưng Kafka đã nhận | Retry gây lệnh trùng. |
| Kafka tạm thời chết | Lệnh bị từ chối dù DB hoàn toàn khoẻ. |

XA/2PC về lý thuyết giải được, nhưng: Kafka **không hỗ trợ XA**; và 2PC blocking (transaction manager chết giữa prepare và commit ⇒ khoá treo) giết throughput.

**Mẹo của Outbox: biến bài toán phân tán thành bài toán cục bộ.** Không ghi hai hệ thống nữa — ghi *một* hệ thống (Postgres) hai bảng, trong một local ACID transaction. Nguyên tử tuyệt đối, do chính database đảm bảo.

Bảng — `order-service/src/main/resources/db/migration/V1__orders_and_outbox.sql`:

```sql
CREATE TABLE outbox_event (
    id             BIGSERIAL PRIMARY KEY,
    aggregate_type VARCHAR(30) NOT NULL,
    aggregate_id   VARCHAR(64) NOT NULL,
    event_type     VARCHAR(30) NOT NULL,
    topic          VARCHAR(60) NOT NULL,   -- relay đọc để biết gửi đi đâu
    msg_key        VARCHAR(64) NOT NULL,   -- relay đọc để giữ thứ tự
    payload        TEXT        NOT NULL,   -- JSON của OrderResult
    created_at     TIMESTAMPTZ NOT NULL,
    published_at   TIMESTAMPTZ             -- NULL = chưa publish
);

-- Partial index: relay chỉ quét event chưa publish -> index nhỏ, quét nhanh
CREATE INDEX idx_outbox_unpublished ON outbox_event (id) WHERE published_at IS NULL;
```

🔑 **Partial index** là chi tiết đáng học. Bảng `outbox_event` chỉ tăng; sau một tháng có thể hàng triệu row, nhưng số row `published_at IS NULL` tại mọi thời điểm chỉ vài chục. Index thường sẽ bao cả triệu row đã publish — vô dụng và tốn RAM. `WHERE published_at IS NULL` làm index chỉ chứa đúng phần relay cần quét, nên nó luôn nhỏ bất kể bảng lớn tới đâu.

Và `orders` — chốt chặn cuối của dedupe:

```sql
CONSTRAINT uq_client_order UNIQUE (client_order_id)
```

Dù Redis chết, dù retry rơi vào pod khác, dù có 10 pod chạy song song — database vẫn chặn lệnh trùng tuyệt đối.

### 4.6. Client nhận phản hồi

```http
HTTP/1.1 201 Created
Content-Type: application/json

{
  "orderId":       "9f1c…-uuid",
  "clientOrderId": "DEMO-0001",
  "status":        "ACCEPTED",
  "reason":        null,
  "symbol":        "HPG",
  "side":          "BUY",
  "price":         28500,
  "quantity":      100,
  "refPrice":      28512.00,
  "riskLevel":     "LOW",
  "timestamp":     1764057600123
}
```

🔑 **`ACCEPTED` không phải `FILLED`.** Lệnh đã được ghi nhận và đang trên đường tới sàn. Nó vẫn có thể bị từ chối ở cửa thứ hai (mục 5.5). Đây chính là lý do saga phải có chiều về.

Lưu ý `refPrice: 28512.00` — giá tham chiếu được **đóng băng tại thời điểm nhận lệnh** và đi theo lệnh. `matching-engine` sẽ dùng chính con số này để kiểm biên độ, chứ không đọc giá mới nhất. Nếu không, một lệnh nằm trong hàng đợi 3 giây có thể bị từ chối vì thị trường đã chạy đi chỗ khác — một kết cục không thể giải thích cho khách hàng.

---

## 5. Outbox Relay đưa lệnh lên Kafka

### 5.1. Vòng lặp relay

`outbox-support/src/main/java/com/trading/outbox/OutboxRelay.java:50-71`

```java
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

Ba quyết định đáng mổ xẻ:

**(a) `@Transactional` bao cả vòng lặp.** `event.setPublishedAt(...)` không gọi `save()` — nó dựa vào **dirty checking** của Hibernate: entity đang nằm trong persistence context, thay đổi được flush khi transaction commit. Nghĩa là `published_at` chỉ được ghi khi *cả batch* thành công.

**(b) Gửi đồng bộ `.get(5s)`.** Ngược hoàn toàn với `TickProducer` (bất đồng bộ). Lý do: ta phải *chắc chắn* Kafka đã nhận trước khi đánh dấu published. Gửi bất đồng bộ rồi mark ngay sẽ tạo ra trường hợp "đã mark published mà Kafka chưa nhận" = **mất lệnh vĩnh viễn**. Mất lệnh tệ hơn gửi trùng rất nhiều, vì trùng thì consumer dedupe lo được, còn mất thì không gì cứu.

**(c) Rollback cả batch khi một event lỗi.** Giữ đúng thứ tự `id` tăng dần. Nếu bỏ qua event lỗi và đi tiếp, thứ tự event của cùng một mã có thể đảo.

⚠️ **Doc #17 (chương 14):** hậu quả thực tế của (c) ít được nói rõ — nếu event thứ 50 trong batch 100 lỗi, thì **49 event đã lên Kafka rồi sẽ được gửi lại** ở lần relay sau, không phải một event. Hành vi này là cố ý (ưu tiên thứ tự hơn số lần gửi trùng), nhưng cần hiểu đúng quy mô khi đọc metric `outbox_published_total`.

### 5.2. `FOR UPDATE SKIP LOCKED` — nhiều pod relay song song

`order-service/src/main/java/com/trading/order/repository/OutboxEventRepository.java:16-23`

```java
@Query(value = """
        SELECT * FROM ordersvc.outbox_event
        WHERE published_at IS NULL
        ORDER BY id
        LIMIT :n
        FOR UPDATE SKIP LOCKED
        """, nativeQuery = true)
List<OutboxEventEntity> lockUnpublishedBatch(@Param("n") int n);
```

| Cách viết | Hành vi khi 3 pod cùng chạy |
|---|---|
| `SELECT ... LIMIT 100` (không khoá) | Cả 3 pod lấy **cùng 100 row** ⇒ mỗi event publish 3 lần |
| `SELECT ... FOR UPDATE` | Pod 1 khoá, pod 2 và 3 **nằm chờ** ⇒ relay tuần tự hoá, throughput bằng 1 pod |
| `SELECT ... FOR UPDATE SKIP LOCKED` | Pod 1 lấy row 1-100, pod 2 **bỏ qua** các row bị khoá và lấy 101-200, pod 3 lấy 201-300 ⇒ song song thật |

🔑 `SKIP LOCKED` là cách biến một bảng thành hàng đợi công việc đa tiêu thụ mà không cần message broker. Đây là một trong những tính năng Postgres hữu dụng nhất và ít người biết nhất.

Vẫn **không** loại trừ gửi trùng: pod crash sau khi Kafka nhận nhưng trước khi commit `published_at` ⇒ event được gửi lại. Đó là **at-least-once by design**, và đó là lý do bắt buộc phải có idempotent consumer ở đầu kia.

### 5.3. Producer của relay: cấu hình tương phản

`order-service/src/main/java/com/trading/order/config/KafkaConfig.java:61-74`

```java
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

Đặt cạnh producer tick (`market-gateway/config/KafkaProducerConfig.java:52-58`):

| Thuộc tính | Tick (`stock.ticks`) | Lệnh (`stock.orders`) | Vì sao khác |
|---|---|---|---|
| `acks` | `1` | `all` | Mất tick: bản kế tiếp sửa lại. Mất lệnh: mất tiền. |
| `enable.idempotence` | không đặt | `true` | Chặn producer-side duplicate khi retry nội bộ |
| `retries` | mặc định | `MAX_VALUE` | Lệnh phải tới bằng mọi giá |
| `linger.ms` | `5` | mặc định (`0`) | Tick gom lô cho throughput; lệnh ưu tiên độ trễ |
| `compression` | `lz4` | không | Tick nhiều và lặp (nén tốt); lệnh ít |
| `batch.size` | `65536` | mặc định | như trên |
| Cách gửi | async `whenComplete` | sync `.get(5s)` | Mục 5.1 (b) |

🔑 **Hai profile producer cho hai loại dữ liệu khác bản chất** — đây là một trong những điểm thiết kế đáng nói nhất của repo. Dùng chung một cấu hình cho cả hai nghĩa là hoặc luồng tick bị làm chậm vô ích, hoặc luồng lệnh không đủ bền.

⚠️ **Gap #16 (chương 14):** mọi `TopicBuilder` trong repo khai `.replicas(1)` và cluster demo chỉ có 1 broker. Với 1 bản sao, `acks=all` ≡ `acks=1` — **không có replica nào để chờ**. Độ bền của outbox producer hiện chỉ là *danh nghĩa*. Để `acks=all` có ý nghĩa thật cần multi-broker + `min.insync.replicas=2`.

---

## 6. matching-engine xử lý

### 6.1. Consumer

`matching-engine/src/main/java/com/trading/matching/consumer/OrderConsumer.java:17-24`

```java
@KafkaListener(topics = "stock.orders",
               groupId = "matching-engine-group",
               containerFactory = "orderListenerFactory")
public void onOrder(OrderResult order) {
    matchingService.process(order);
    // Offset commit SAU khi transaction DB commit (listener trả về) ->
    // crash giữa chừng chỉ gây redelivery, dedupe lo phần còn lại.
}
```

🔑 **Thứ tự commit quyết định ngữ nghĩa giao hàng.** Spring Kafka commit offset *sau khi* listener trả về bình thường. Vì `process()` là `@Transactional`, transaction DB commit trước, rồi offset mới được commit. Crash giữa hai mốc đó ⇒ message được giao lại ⇒ dedupe xử lý. Nếu commit offset *trước* khi xử lý, crash sẽ làm **mất lệnh** — không bao giờ được phép.

`AUTO_OFFSET_RESET_CONFIG = "earliest"` (`matching/config/KafkaConfig.java:41`) — khác hẳn `"latest"` ở consumer tick. Lý do ghi ngay trong comment: "LỆNH không được bỏ sót". Consumer group mới (hoặc offset hết hạn) phải đọc lại từ đầu, không được nhảy tới cuối.

### 6.2. 🔑 Một transaction, ba việc

`matching-engine/src/main/java/com/trading/matching/service/MatchingService.java:60-95`

```java
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
    ...
    executionRepo.save(exec);

    matched.increment();
    log.info("KHOP LENH {} {} {} x{} @ {}", order.clientOrderId(),
            order.side(), order.symbol(), order.quantity(), fillPrice);

    emit(new ExecutionReport(order.orderId(), order.clientOrderId(), order.symbol(),
            order.side(), "FILLED", null, fillPrice, order.quantity(),
            System.currentTimeMillis()));
}
```

Ba việc **cùng commit hoặc cùng rollback**:

1. Ghi sổ `processed_event` → idempotency
2. Ghi `execution` (nếu hợp lệ) → kết quả khớp
3. Ghi `outbox_event` → saga chiều về

🔑 **Nếu (3) nằm ngoài transaction:** sẽ có trường hợp lệnh đã khớp (execution đã ghi) mà `order-service` không bao giờ biết ⇒ lệnh treo `ACCEPTED` vĩnh viễn, tiền của khách bị giữ cho một giao dịch đã hoàn tất. Đây là loại lỗi tệ nhất trong hệ thống tài chính: dữ liệu ở hai nơi mâu thuẫn và không có cơ chế tự chữa.

### 6.3. Dedupe bằng chính database

`matching-engine/src/main/java/com/trading/matching/repository/ProcessedEventRepository.java:14-23`

```java
@Modifying
@Query(value = """
        INSERT INTO matchsvc.processed_event (client_order_id, processed_at)
        VALUES (:id, now())
        ON CONFLICT (client_order_id) DO NOTHING
        """, nativeQuery = true)
int insertIfAbsent(@Param("id") String clientOrderId);
```

Trả về `1` = lần đầu thấy id này ⇒ xử lý tiếp. Trả về `0` = đã xử lý ⇒ bỏ qua.

🔑 **Vì sao không `if (exists) return; else insert;`?** Vì đó là **check-then-act**, không nguyên tử. Hai pod cùng nhận bản sao của một message: cả hai `exists` trả false, cả hai insert, cả hai khớp lệnh. Lệnh khớp hai lần. `INSERT ... ON CONFLICT DO NOTHING` là **atomic test-and-set do chính database thực hiện** — Postgres bảo đảm chỉ một trong hai insert thành công, kẻ thua nhận `0` và dừng.

Và vì nó nằm trong **cùng transaction** với `executionRepo.save()`, không thể có trạng thái "đã ghi sổ mà chưa khớp" hay ngược lại.

⚠️ **Gap #15 (chương 14) — nghiêm trọng:** `ExecutionEntity.clientOrderId` **không** có `unique = true` (`matching/entity/ExecutionEntity.java:24-25`), khác hẳn `OrderEntity.clientOrderId` ở order-service có `unique = true`. Nghĩa là `processed_event` là lớp chặn khớp-trùng **duy nhất**, không có lưới an toàn phía sau. Nếu bảng đó bị dọn nhầm, hoặc bị truncate, hệ thống sẽ khớp lệnh hai lần mà không gì chặn lại. Sửa: thêm `unique = true` + migration `V3`.

### 6.4. Luật của sàn — ranh giới tin cậy

`matching-engine/src/main/java/com/trading/matching/service/TradingRules.java:37-62`

```java
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

🔑 **Hai tầng kiểm tra cố tình trùng lặp một phần** — đây là điểm hay nhất về mặt thiết kế an toàn:

| | `order-service` pre-trade | `matching-engine` TradingRules |
|---|---|---|
| Bảo vệ | **Khách hàng** | **Sàn** |
| Luật | staleness, risk HIGH, lệch > 5% | lô chẵn 100, biên độ ±7%, phiên giao dịch |
| Vai trò | Từ chối **sớm** cho nhanh | **Nguồn sự thật** |
| Được tin không? | **Không** | Có |

Vì sao `matching-engine` phải kiểm lại dù `order-service` đã kiểm?
- Client có thể publish thẳng vào `stock.orders` (bỏ qua order-service).
- Tham số hai bên có thể lệch: `order.max-price-deviation-pct: 5` vs `matching.price-band-pct: 7`. Một lệnh lệch 6% **qua được cửa 1 nhưng vẫn hợp lệ ở cửa 2**; một lệnh lệch 8% qua cửa 1 nhờ cấu hình sai sẽ bị cửa 2 chặn.
- Luật sàn thay đổi theo sàn (HOSE 7%, HNX 10%, phái sinh theo quy định VN30) và theo ngày (ngày giao dịch không hưởng quyền). Đặt luật đó ở `order-service` nghĩa là mỗi lần sàn đổi quy định phải deploy service nhận lệnh.

🔑 **Và đây chính là lý do saga cần chiều về:** lệnh qua được cửa 1 vẫn có thể chết ở cửa 2. Không có đường phản hồi thì `order-service` không bao giờ biết.

### 6.5. Phát execution report qua outbox

`matching-engine/src/main/java/com/trading/matching/service/MatchingService.java:97-106`

```java
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

🔑 **Tính đệ quy của Outbox.** Cùng một pattern, cùng một `OutboxRelay` (module `outbox-support`), cùng một `@ConditionalOnProperty`, chỉ khác bảng (`matchsvc.outbox_event` thay vì `ordersvc.outbox_event`) và topic đích. Đây là bằng chứng rằng Outbox không phải mẹo vặt cho một chỗ, mà là **cơ chế nền tảng** cho mọi ranh giới giữa database và message broker.

Và lưu ý cách module dùng chung được thiết kế:

`outbox-support/src/main/java/com/trading/outbox/OutboxRecord.java`

```java
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

**Chia sẻ hành vi, không chia sẻ cấu trúc dữ liệu** — đây là ranh giới đúng cho thư viện dùng chung trong microservices. Nếu `outbox-support` export một `@Entity`, thì đổi một cột trong bảng outbox của `order-service` sẽ buộc `matching-engine` phải migrate theo. Với `interface` + adapter (`JpaOutboxStore` ở mỗi service), hai bảng tiến hoá độc lập.

---

## 7. Vòng saga khép lại

### 7.1. order-service nhận execution report

`order-service/src/main/java/com/trading/order/consumer/ExecutionReportConsumer.java:23-41`

```java
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
                throw ex;
            }
        }
    }
}
```

🔑 **Vòng retry nằm NGOÀI `@Transactional` có chủ đích.** Khi `OptimisticLockingFailureException` được ném, transaction đã bị đánh dấu rollback. Retry *bên trong* cùng transaction đó sẽ gặp `UnexpectedRollbackException` — một lỗi khó hiểu mà nhiều người mất hàng giờ để gỡ. Phải thoát ra, để transaction cũ rollback xong, rồi mở transaction **mới** đọc lại dữ liệu mới nhất.

### 7.2. Áp trạng thái, có bảo vệ

`order-service/src/main/java/com/trading/order/service/OrderStatusService.java:36-75`

```java
@Transactional
public void apply(ExecutionReport report) {
    Optional<OrderEntity> found = repository.findByClientOrderId(report.clientOrderId());
    if (found.isEmpty()) {
        // Về lý thuyết không xảy ra: lệnh được COMMIT trước khi outbox event ra Kafka.
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
    ...
}
```

### 7.3. 🔑 State machine — một cơ chế, hai công dụng

`order-service/src/main/java/com/trading/order/service/OrderStateMachine.java:16-37`

```java
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

    public static boolean canTransition(String from, String to) {
        return ALLOWED.getOrDefault(from, Set.of()).contains(to);
    }
}
```

| Công dụng | Cơ chế |
|---|---|
| **1. Chặn chuyển trạng thái vô lý** | Lệnh đã `FILLED` không thể quay về `ACCEPTED`; lệnh `REJECTED` ở pre-trade không thể bị matching đổi trạng thái |
| **2. Một lớp idempotency miễn phí** | Report tới lần thứ hai thấy lệnh đã ở trạng thái cuối ⇒ `canTransition` false ⇒ bỏ qua |

Nhờ (2), `order-service` **không cần bảng `processed_event`** riêng. Trạng thái cuối không có đường ra, nên redelivery tự động vô hại. Một cấu trúc dữ liệu 20 dòng thay thế được cả một bảng và một chỉ mục.

Đánh đổi: trạng thái cuối không có đường ra nghĩa là hệ **chưa hỗ trợ khớp một phần** (partial fill). Muốn hỗ trợ cần thêm `PARTIALLY_FILLED` với self-loop (`PARTIALLY_FILLED → PARTIALLY_FILLED`) và cộng dồn `filledQty` — và ngay khi có self-loop, lớp idempotency miễn phí **biến mất**, buộc phải thêm bảng dedupe thật.

### 7.4. Optimistic lock

`order-service/src/main/java/com/trading/order/entity/OrderEntity.java:70-80`

```java
/**
 * OPTIMISTIC LOCKING: hai execution report tới song song trên 2 pod
 * cùng UPDATE một lệnh -> pod thua cuộc nhận OptimisticLockingFailureException
 * và retry trên dữ liệu mới, thay vì âm thầm ghi đè (lost update).
 */
@Version
@Column(nullable = false)
private Long version;
```

Hibernate sinh SQL:

```sql
UPDATE ordersvc.orders
SET status = ?, reason = ?, filled_qty = ?, avg_fill_price = ?, updated_at = ?, version = 6
WHERE id = ? AND version = 5;
```

Nếu `rowsAffected = 0`, nghĩa là ai đó đã sửa row này (version đã khác 5) ⇒ ném `OptimisticLockingFailureException`.

**Vì sao optimistic chứ không pessimistic?** Xung đột ở đây là **hiếm** — hai report cho cùng một lệnh đến đúng cùng lúc. Khoá bi quan (`SELECT ... FOR UPDATE`) phải trả giá khoá trên **mọi** update chỉ để phòng một trường hợp hiếm. Optimistic không tốn gì ở đường chạy bình thường, chỉ trả giá retry khi thật sự va chạm.

### 7.5. Kết quả cuối

```bash
curl -s http://localhost:8083/api/v1/orders/DEMO-0001 -H 'X-API-KEY: demo-key' | jq
```

```json
{
  "orderId":       "9f1c…-uuid",
  "clientOrderId": "DEMO-0001",
  "status":        "FILLED",
  "reason":        null,
  "symbol":        "HPG",
  "side":          "BUY",
  "price":         28500.0000,
  "quantity":      100,
  "refPrice":      28512.0000,
  "riskLevel":     "LOW",
  "accountId":     "ACC001",
  "filledQty":     100,
  "avgFillPrice":  28512.0000,
  "createdAt":     "2026-11-25T09:00:00.123Z",
  "updatedAt":     "2026-11-25T09:00:01.456Z"
}
```

Lưu ý `avgFillPrice = 28512` chứ không phải `28500`: `MatchingService.java:79` khớp tại `refPrice` nếu có. Đây là đơn giản hoá của mô phỏng — sàn thật khớp theo sổ lệnh đối ứng.

Và lưu ý đây là `OrderView`, **không phải `OrderEntity`** — `order-service/model/OrderView.java` giải thích ba lý do:

```java
/**
 *  1. Entity còn gắn với persistence context. Trả thẳng nó thì Jackson serialize
 *     lúc đã ra ngoài transaction, và mọi quan hệ LAZY sẽ nổ
 *     LazyInitializationException - lỗi chỉ xuất hiện khi có dữ liệu thật.
 *
 *  2. Thêm một cột vào bảng là tự động lọt ra API, kể cả cột nội bộ. Ở đây đang
 *     có `version` (optimistic locking) và `id` (khoá tự tăng) - hai thứ client
 *     không cần biết và không nên phụ thuộc vào.
 *
 *  3. Đổi tên cột trong DB sẽ âm thầm phá hợp đồng API. Có DTO thì compiler bắt
 *     được ngay tại chỗ map.
 */
```

---

## 8. Toàn cảnh một lượt — bảng thời gian

| # | Mốc | Thành phần | Dòng mã | Độ trễ tích luỹ (ước lượng local) |
|:--:|---|---|---|---|
| 1 | Tick sinh ra | `SimulatedFeed` | `:38-51` | t₀ |
| 2 | Produce `stock.ticks` | `TickProducer` | `:36` | +1-6ms (linger 5ms) |
| 3 | Consume batch | `TickConsumer` | `:52` | +1-50ms (fetch.max.wait 50ms) |
| 4 | Tính risk O(1) | `RiskCalculator` | `:46-89` | +µs |
| 5 | Cập nhật snapshot | `SnapshotStore` | `:36` | +µs |
| 6 | Produce `stock.enriched` | `TickConsumer` | `:63` | +1-6ms |
| 7 | Cache ở order-service | `MarketDataCache` | `:25` | +1-5ms |
| — | *(client gửi lệnh bất kỳ lúc nào sau đây)* | | | |
| 8 | Qua API key filter | `ApiKeyFilter` | `:36` | — |
| 9 | Rate limit + validate | `OrderController` | `:46-47` | <1ms |
| 10 | Redis idempotency GET | `OrderService` | `:78` | <1ms (timeout 200ms) |
| 11 | Pre-trade check | `OrderService` | `:109-128` | <1ms (đọc RAM) |
| 12 | **2 INSERT 1 transaction** | `OrderPersistence` | `:37-66` | +3-10ms |
| 13 | Redis SET + trả 201 | `OrderService` | `:104-106` | <1ms |
| 14 | Relay khoá batch | `OutboxEventRepository` | `:16-23` | +0-500ms (fixedDelay) |
| 15 | Publish `stock.orders` sync | `OutboxRelay` | `:58-59` | +2-10ms (acks=all) |
| 16 | Mark `published_at`, commit | `OutboxRelay` | `:60` | +1-3ms |
| 17 | Consume lệnh | `OrderConsumer` | `:21` | +1-10ms |
| 18 | Dedupe + luật + 2 INSERT | `MatchingService` | `:60-95` | +5-15ms |
| 19 | Relay publish `stock.executions` | `OutboxRelay` | `:58` | +0-500ms + 2-10ms |
| 20 | Consume report | `ExecutionReportConsumer` | `:26` | +1-10ms |
| 21 | State machine + optimistic update | `OrderStatusService` | `:36-75` | +3-10ms |
| | **Tổng saga (bước 12 → 21)** | | | **~0,5-1,1 giây** |

🔑 **Thủ phạm chính của độ trễ saga là hai lần `@Scheduled(fixedDelay = 500ms)` của relay**, không phải xử lý nghiệp vụ. Hạ `outbox.relay.interval-ms` xuống 100ms sẽ cắt phần lớn độ trễ, đổi lại là 10 lần query `FOR UPDATE SKIP LOCKED` mỗi giây trên mỗi pod. Debezium CDC (chương 05) loại bỏ hẳn polling: nó đọc WAL và đẩy event trong vài mili-giây.

---

## 9. Ba nhánh rẽ khác của cùng hành trình

### 9.1. Lệnh bị từ chối ở cửa 1 (pre-trade)

```bash
curl -i -X POST http://localhost:8083/api/v1/orders \
  -H 'X-API-KEY: demo-key' -H 'Content-Type: application/json' \
  -d '{"clientOrderId":"DEMO-FAT","symbol":"HPG","side":"BUY",
       "price":285000,"quantity":100,"accountId":"ACC001"}'
```

```http
HTTP/1.1 422 Unprocessable Entity
{"status":"REJECTED","reason":"PRICE_DEVIATION 900.00% > 5%", ...}
```

Hành trình dừng tại `OrderService.java:122`. Lệnh **vẫn được ghi vào bảng `orders`** (`OrderPersistence.java:38-54`) để phục vụ audit, nhưng **không sinh outbox event** (`:56` — `if ("ACCEPTED".equals(result.status()))`). Matching-engine không bao giờ thấy nó.

💡 Giữ cả lệnh bị từ chối là yêu cầu kiểm toán tài chính: phải trả lời được "tại sao 14h32 ngày X khách không đặt được lệnh". Hệ quả là bảng `orders` **chỉ tăng**, và đó là lý do `OrderRepository` bắt buộc phân trang (`OrderRepository.java:13-24`).

### 9.2. Lệnh bị từ chối ở cửa 2 (luật sàn)

```bash
# quantity 150 không chia hết cho lô 100
curl -s -X POST http://localhost:8083/api/v1/orders \
  -H 'X-API-KEY: demo-key' -H 'Content-Type: application/json' \
  -d '{"clientOrderId":"DEMO-LOT","symbol":"HPG","side":"BUY",
       "price":28500,"quantity":150,"accountId":"ACC001"}'
# -> 201 ACCEPTED  (order-service không kiểm lô!)

sleep 2
curl -s http://localhost:8083/api/v1/orders/DEMO-LOT -H 'X-API-KEY: demo-key' | jq '{status,reason}'
# -> {"status":"REJ_MATCH","reason":"INVALID_LOT_SIZE must be multiple of 100"}
```

Đây là minh hoạ sống động nhất cho việc **vì sao saga cần chiều về**: client nhận `201 ACCEPTED`, rồi 2 giây sau lệnh thành `REJ_MATCH`. Đúng đó là hệ thống đang hoạt động chính xác — `order-service` không biết luật lô chẵn, chỉ sàn mới biết.

Test tự động cho nhánh này: `matching-engine/src/test/java/com/trading/matching/SagaRejectionIT.java:57-85`.

### 9.3. Lệnh gửi trùng

```bash
for i in 1 2 3; do
  curl -s -o /dev/null -w "%{http_code}\n" -X POST http://localhost:8083/api/v1/orders \
    -H 'X-API-KEY: demo-key' -H 'Content-Type: application/json' \
    -d '{"clientOrderId":"DEMO-DUP","symbol":"HPG","side":"BUY",
         "price":28500,"quantity":100,"accountId":"ACC001"}'
done
# 201 / 201 / 201   <- cùng một orderId, cùng một kết quả

sleep 2
curl -s http://localhost:8084/api/v1/executions/DEMO-DUP | jq 'length'
# 1   <- ĐÚNG MỘT execution
```

Ba lần gọi, ba lần `201`, nhưng:
- Lần 1: đi hết luồng, ghi DB, cache Redis.
- Lần 2, 3: Redis hit ở `OrderService.java:78-83` ⇒ trả **y nguyên kết quả cũ**, không chạm DB.
- Nếu Redis chết: cả ba đều xuống DB, lần 2-3 đụng `uq_client_order` ⇒ `DataIntegrityViolationException` ⇒ nhánh `:94-100` trả lại bản ghi hiện có.
- Và kể cả nếu cả hai lớp trên hỏng: `matching-engine` vẫn chỉ khớp một lần nhờ `processed_event`.

Ba lớp, mỗi lớp độc lập đủ để giữ tính đúng đắn. Test: `IdempotentConsumerIT.java:56-83`.

---

## 10. Quan sát hành trình bằng công cụ

```bash
# 1. Dòng giá có đang chảy không?
curl -s http://localhost:8082/api/v1/marketdata/HPG | jq
curl -s http://localhost:8081/actuator/prometheus | grep gateway_ticks_sent

# 2. Cache ở order-service đã có dữ liệu chưa? (gián tiếp: thử đặt lệnh)
#    NO_FRESH_MARKET_DATA = cache rỗng hoặc dữ liệu quá 10s

# 3. Outbox còn tồn đọng không? (phải luôn ~0)
docker compose exec postgres psql -U trading -d trading -c \
  "SELECT count(*) AS pending FROM ordersvc.outbox_event WHERE published_at IS NULL;"

# 4. Relay có đang publish không?
curl -s http://localhost:8083/actuator/prometheus | grep -E 'outbox_published|outbox_publish_failed'

# 5. Matching đã nhận lệnh chưa?
docker compose logs -f matching-engine | grep "KHOP LENH"

# 6. Có bao nhiêu lệnh đang treo? (chỉ số sức khoẻ saga quan trọng nhất)
docker compose exec postgres psql -U trading -d trading -c \
  "SELECT status, count(*) FROM ordersvc.orders GROUP BY status;"

# 7. Xem toàn bộ trace của một lệnh
open http://localhost:9411        # Zipkin: tìm theo traceId trong log
```

💡 **TraceId trong log.** Mọi service đặt `logging.pattern.level: "%5p [${spring.application.name:},%X{traceId:-},%X{spanId:-}]"`. Nghĩa là mỗi dòng log mang sẵn traceId — copy nó vào Zipkin là thấy toàn bộ hành trình xuyên 4 service. Không có dòng này thì distributed tracing chỉ dùng được một nửa: thấy được biểu đồ nhưng không nối được từ một dòng log cụ thể tới biểu đồ đó.

---

## 11. Câu hỏi phỏng vấn điển hình cho chương này

**Q: Giữa `order-service` và `matching-engine` không có transaction chung. Làm sao đảm bảo không có lệnh nào bị mất hoặc khớp hai lần?**

A: Hai cơ chế bù nhau.
*Không mất*: Outbox — lệnh và event cùng commit trong một local transaction Postgres, nên nếu lệnh tồn tại thì event chắc chắn tồn tại; relay gửi lại cho tới khi Kafka nhận (`acks=all`, `retries=MAX_VALUE`), và `published_at` chỉ được ghi **sau** khi Kafka ack.
*Không khớp hai lần*: idempotent consumer — `INSERT ... ON CONFLICT DO NOTHING` trên `processed_event` nằm **trong cùng transaction** với việc ghi execution.
Kết quả: at-least-once delivery + idempotent processing = **effectively-once**. Có test chứng minh: `IdempotentConsumerIT`.

**Q: Vì sao không dùng Kafka transaction (exactly-once semantics) cho gọn?**

A: Kafka EOS chỉ nguyên tử **trong phạm vi Kafka** — consume, process, produce, commit offset. Nó không bao gồm việc ghi PostgreSQL. Ngay khi một bên của ranh giới là database, ta lại quay về bài toán dual-write. Outbox giải đúng ranh giới đó. (Có thể dùng cả hai: Kafka EOS cho luồng Kafka→Kafka thuần, Outbox cho ranh giới DB↔Kafka.)

**Q: Client nhận `201 ACCEPTED` rồi lệnh thành `REJ_MATCH`. Đó không phải là API thiết kế tệ sao?**

A: Không — đó là mô hình đúng cho xử lý bất đồng bộ. `201` nghĩa là "yêu cầu đã được ghi nhận bền vững", không phải "giao dịch đã hoàn tất". Sàn thật cũng vậy: FIX protocol có `ExecutionReport` với `OrdStatus = NEW` rồi mới tới `FILLED`/`REJECTED`. API thiết kế tệ sẽ là *chặn* HTTP request 1 giây chờ saga xong — khi đó mỗi lệnh chiếm một thread suốt vòng saga và hệ sập ở 200 req/s.

**Q: Độ trễ saga ~1 giây có chấp nhận được không?**

A: Tuỳ phân khúc. Với sàn môi giới bán lẻ (retail brokerage) thì có — khách bấm đặt lệnh và thấy kết quả sau 1 giây là bình thường. Với market making / HFT thì tuyệt đối không: ở đó người ta không dùng kiến trúc này mà gom tất cả vào một process trên bare-metal, dùng LMAX Disruptor ring buffer, giao tiếp qua shared memory, triệt tiêu GC. Và trong 1 giây đó, ~1 giây là do hai lần polling relay — đổi sang CDC sẽ xuống còn vài chục mili-giây mà không đổi một dòng mã nghiệp vụ.

---

## 12. Bảng tổng kết

| Giai đoạn | Thành phần | Pattern cốt lõi | Nếu bỏ pattern này thì sao |
|---|---|---|---|
| Sinh & nạp tick | `market-gateway` | Avro + Schema Registry, producer tốc độ | Message lớn 3-5x; thay đổi schema phá consumer âm thầm |
| Làm giàu dữ liệu | `pricing-risk-service` | Batch listener + tính toán $O(1)$ + `key=symbol` | EMA/volatility sai số ⇒ từ chối lệnh sai |
| Lưu snapshot | `SnapshotStore` | Write-behind cache | I/O database chặn hot path; DB chết kéo sập luồng realtime |
| Chống poison pill | `DeadLetterPublishingRecoverer` | Dead Letter Topic | Một message hỏng làm consumer kẹt vĩnh viễn, hoặc mất message âm thầm |
| Đọc giá khi đặt lệnh | `MarketDataCache` | CQRS local materialized view | Temporal coupling: pricing chậm ⇒ order chậm; pricing chết ⇒ order chết |
| Bảo vệ khách | `OrderService.check()` | Fail-safe pre-trade | Lệnh đặt theo giá đã chết; fat-finger không ai chặn |
| Chống trùng | Redis + `UNIQUE` + `processed_event` | Idempotency 3 lớp | Client retry ⇒ mua hai lần |
| Ghi lệnh | `OrderPersistence` | **Transactional Outbox** | Dual-write ⇒ lệnh treo vĩnh viễn hoặc execution không có order |
| Đưa event ra Kafka | `OutboxRelay` | Polling publisher + `SKIP LOCKED` | Nhiều pod publish trùng, hoặc relay tuần tự hoá mất throughput |
| Luật sàn | `TradingRules` | Hai tầng kiểm tra, ranh giới tin cậy | Client bypass order-service đặt được lệnh sai luật |
| Phản hồi | `emit()` → outbox | **Saga choreography** | Lệnh treo `ACCEPTED` mãi mãi, tiền khách bị giữ |
| Đóng vòng đời | `OrderStateMachine` | State machine = idempotency miễn phí | Report trùng ghi đè trạng thái cuối |
| Ghi song song | `@Version` | Optimistic locking | Lost update: hai report ghi đè nhau |

> **Đọc tiếp:** [Chương 01 — Kiến trúc tổng thể & Microservices Boundary](./01-kien-truc-tong-the-va-microservices-boundary.md) để hiểu *vì sao* ranh giới service lại được vạch ở đúng những chỗ đó.
