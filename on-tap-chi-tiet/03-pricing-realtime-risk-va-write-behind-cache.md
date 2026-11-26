# Chương 03 — Pricing, Real-time Risk & Write-Behind Cache

> **Câu hỏi trung tâm:** làm sao tính thống kê rủi ro trên hàng chục nghìn tick mỗi giây mà bộ nhớ không phình và độ trễ không trôi?
>
> **Tài liệu đối chiếu:** `DESIGN-NOTES.md` mục 5-6, `DEEP-DIVE.md` mục 2-3 & 5.

---

## 1. Nó là gì?

`pricing-risk-service` là **trái tim tính toán** của hệ thống. Nó là service duy nhất:
- Có một **hot path thực sự** (xử lý mọi tick, đo bằng micro-giây).
- Giữ **trạng thái trong RAM** mà tính đúng đắn phụ thuộc vào (`SymbolState`).
- Có **hai đường ghi dữ liệu tách biệt hoàn toàn** (hot/cold path).

```java
// pricing-risk-service/src/main/java/com/trading/pricing/PricingRiskApplication.java:7-19
/**
 * PRICING-RISK SERVICE - Trái tim của hệ thống.
 *
 * Luồng xử lý:
 *   1. Consume batch tick từ Kafka topic "stock.ticks"
 *   2. Tính toán: giá EMA, volatility (độ biến động), VaR đơn giản, risk level
 *   3. Cập nhật snapshot IN-MEMORY (đọc nhanh <1ms) - hot path
 *   4. Publish kết quả enriched vào topic "stock.enriched" cho order-service
 *   5. Ghi PostgreSQL theo lô định kỳ (write-behind) - cold path
 *
 * Nguyên tắc tốc độ: TÁCH HOT PATH (tính toán, in-memory) khỏi COLD PATH (ghi DB).
 * Không bao giờ để I/O database chặn luồng xử lý tick.
 */
```

### 1.1. Bốn thành phần

| Lớp | File | Vai trò |
|---|---|---|
| `TickConsumer` | `consumer/TickConsumer.java` | Biên vào: Avro → domain, điều phối, đo metric |
| `RiskCalculator` | `service/RiskCalculator.java` | Thuật toán thuần tuý, không phụ thuộc Spring/Kafka |
| `SnapshotStore` | `service/SnapshotStore.java` | Cache in-memory + write-behind xuống DB |
| `MarketDataController` | `controller/MarketDataController.java` | REST đọc: realtime từ RAM, lịch sử từ DB |

🔑 Lưu ý ranh giới: `RiskCalculator` **không biết gì về Avro, Kafka hay Spring Data**. Nó nhận `TickEvent` (record thuần) và trả `EnrichedMarketData` (record thuần). Nhờ vậy nó unit-test được không cần container, và đổi wire format không chạm tới nó.

---

## 2. Vai trò trong hệ thống

| Nó cung cấp cho ai | Qua đường nào | Dùng để làm gì |
|---|---|---|
| `order-service` | `stock.enriched` → `MarketDataCache` | 🔑 Pre-trade risk check — quyết định nhận hay từ chối lệnh khách |
| UI / dashboard | `GET /api/v1/marketdata` | Hiển thị giá realtime |
| Phân tích / báo cáo | `GET /api/v1/marketdata/{symbol}/history` | Truy vấn lịch sử snapshot từ Postgres |
| Prometheus | `/actuator/prometheus` | `pricing_e2e_latency`, `pricing_batch_process` |

Nghĩa là: **đầu ra của service này trực tiếp quyết định khách có đặt được lệnh hay không.** Sai số ở đây không phải sai số thống kê học thuật — nó là từ chối oan hoặc cho qua nhầm.

---

## 3. Cơ chế hoạt động

### 3.1. Hot path — `TickConsumer`

```java
// pricing-risk-service/src/main/java/com/trading/pricing/consumer/TickConsumer.java:45-68
/**
 * BATCH LISTENER: xử lý cả lô tick trong 1 lần gọi.
 * Toàn bộ hot path chỉ gồm: tính toán in-memory + put map + send async.
 * KHÔNG có blocking I/O nào ở đây.
 */
@KafkaListener(topics = "stock.ticks", containerFactory = "tickListenerFactory")
public void onTicks(List<TickEventAvro> avroTicks) {
    long start = System.nanoTime();
    long now = System.currentTimeMillis();

    for (TickEventAvro avro : avroTicks) {
        // Map wire format (Avro) -> domain model (record) ngay tại biên;
        // toàn bộ logic phía trong không biết gì về Avro (dễ test, dễ đổi wire format)
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

Phân tích từng chi tiết:

**`long now` lấy MỘT LẦN ngoài vòng lặp.** Nếu gọi `System.currentTimeMillis()` cho từng bản ghi trong batch 1000, ta trả thêm ~1000 lần gọi hệ thống. Lấy một lần là đủ chính xác vì cả batch được xử lý trong vài trăm micro-giây.

**`Math.max(0, now - tick.eventTime())`** — chặn giá trị âm. Nếu đồng hồ của gateway chạy nhanh hơn của pricing, hiệu số ra số âm và Micrometer sẽ ghi một mẫu vô nghĩa (hoặc ném exception với một số backend). `max(0, …)` làm cho metric luôn hợp lệ.

⚠️ Nhưng đây cũng là chỗ **che giấu vấn đề**: nếu lệch đồng hồ lớn, latency luôn báo 0 và trông như hệ thống hoàn hảo. Cùng một mẫu lỗi xuất hiện ở staleness check của `order-service` (Gap #6, [chương 04](./04-order-service-pretrade-risk-va-resilience.md)). Xử lý đúng là **đếm riêng** số lần hiệu số âm bằng một counter — khi counter đó khác 0, có nghĩa cần kiểm tra NTP.

**`send(...)` không chờ.** `KafkaTemplate.send()` trả `CompletableFuture`; ở đây ta không `.get()` và không gắn callback. Một tick enriched mất đi không sao — tick kế tiếp ghi đè. Đối lập hoàn toàn với `OutboxRelay` (gửi sync `.get(5s)`).

**Hai Timer khác nhau đo hai thứ khác nhau:**

```java
// TickConsumer.java:34-43
this.processingTimer = Timer.builder("pricing.batch.process")
        .description("Thoi gian xu ly 1 batch tick")
        .publishPercentiles(0.5, 0.95, 0.99)
        .register(registry);
this.e2eLatencyTimer = Timer.builder("pricing.e2e.latency")
        .description("Latency tu nguon toi khi tinh xong")
        .publishPercentiles(0.5, 0.95, 0.99)
        .register(registry);
```

| Metric | Đo gì | Tăng nghĩa là |
|---|---|---|
| `pricing_batch_process` | Thời gian CPU xử lý một lô | Thuật toán chậm đi, GC, CPU throttle |
| `pricing_e2e_latency` | `now − eventTime` | **Lag Kafka**, không phải thuật toán chậm |

🔑 Phân biệt hai metric này là chìa khoá chẩn đoán: `e2e_latency` vọt mà `batch_process` bình thường ⇒ nghẽn ở Kafka hoặc consumer không theo kịp ⇒ scale. Cả hai cùng vọt ⇒ nghẽn ở CPU/GC ⇒ xem heap, xem có ai thêm I/O vào hot path không.

### 3.2. Thuật toán $O(1)$ — `RiskCalculator`

```java
// pricing-risk-service/src/main/java/com/trading/pricing/service/RiskCalculator.java:14-33
/**
 * Bộ tính toán rủi ro streaming - O(1) mỗi tick.
 *
 * Kỹ thuật: dùng Welford-style incremental stats trên cửa sổ trượt log-return,
 * KHÔNG lưu toàn bộ lịch sử -> memory cố định, tốc độ ổn định.
 *
 * State per-symbol được bảo vệ bằng synchronized trên chính object state.
 * Vì Kafka key = symbol nên mỗi symbol chỉ được 1 consumer thread xử lý
 * -> contention gần như bằng 0 (lock luôn uncontended).
 */
@Component
public class RiskCalculator {

    private static final int WINDOW = 120;           // 120 tick gần nhất
    private static final double ANNUALIZE = Math.sqrt(252 * 6.5 * 3600); // giả định tick ~1s
    private static final double Z_95 = 1.645;        // z-score cho VaR 95%
    private static final double EMA_ALPHA = 0.1;

    private final Map<String, SymbolState> states = new ConcurrentHashMap<>();
```

#### (a) EMA — Exponential Moving Average

```java
// RiskCalculator.java:49-50
// 1) EMA giá
ema = Double.isNaN(ema) ? px : EMA_ALPHA * px + (1 - EMA_ALPHA) * ema;
```

$$\text{EMA}_t = \alpha \cdot P_t + (1-\alpha)\cdot \text{EMA}_{t-1}, \quad \alpha = 0{,}1$$

So với trung bình trượt đơn giản (SMA):

| | SMA(n) | **EMA** |
|---|---|---|
| Bộ nhớ | O(n) — phải giữ n giá | **O(1)** — một biến `double` |
| Chi phí mỗi tick | O(1) với deque, nhưng vẫn phải giữ mảng | **O(1)** |
| Phản ứng với giá mới | Mọi giá trong cửa sổ có trọng số như nhau | Giá mới có trọng số cao hơn, cũ mờ dần theo hàm mũ |
| Nhược | Giá rơi khỏi cửa sổ gây "nhảy bậc" | Không bao giờ quên hoàn toàn quá khứ |

💡 **Ý nghĩa của `α = 0,1`:** "nửa đời" của một quan sát là $\ln(0{,}5)/\ln(0{,}9) \approx 6{,}6$ tick. Sau ~22 tick, một giá chỉ còn đóng góp ~10% trọng số. Tăng `α` ⇒ EMA bám giá sát hơn nhưng nhiễu hơn; giảm `α` ⇒ mượt hơn nhưng phản ứng chậm.

`Double.isNaN(ema) ? px : …` khởi tạo EMA bằng chính giá đầu tiên — tránh để EMA xuất phát từ 0 rồi phải mất hàng chục tick mới leo lên vùng giá thật.

#### (b) Sliding-window variance — 🔑 phần hay nhất

```java
// RiskCalculator.java:52-63
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
```

**Vì sao dùng log-return chứ không phần trăm thay đổi?**

$$r_t = \ln\frac{P_t}{P_{t-1}}$$

| | Simple return $(P_t - P_{t-1})/P_{t-1}$ | **Log return** |
|---|---|---|
| Cộng dồn qua nhiều kỳ | Phải nhân: $(1+r_1)(1+r_2)-1$ | **Cộng được**: $r_{1,2} = r_1 + r_2$ |
| Đối xứng | +50% rồi −50% ≠ 0 (còn −25%) | **Đối xứng**: $\ln(1{,}5) + \ln(0{,}667) = 0$ |
| Phân phối | Lệch phải | Xấp xỉ chuẩn (giả định chuẩn của tài chính) |

Tính cộng được là lý do toàn bộ toán học tài chính (Black-Scholes, VaR, GARCH) dựng trên log-return.

**Bí quyết $O(1)$:** thay vì giữ 120 giá trị rồi mỗi tick duyệt lại để tính variance (= $O(n)$ mỗi tick), ta giữ hai *tổng tích luỹ*:

$$\text{sum} = \sum r_i, \qquad \text{sumSq} = \sum r_i^2$$

Khi cửa sổ đầy, giá trị cũ nhất bị **trừ khỏi cả hai tổng** rồi giá trị mới được **cộng vào**. Variance tính từ hai tổng đó:

```java
// RiskCalculator.java:65-73
int n = returns.size();
double vol = 0, var95 = 0;
if (n >= 2) {
    double mean = sum / n;
    double variance = Math.max(0, (sumSq / n) - mean * mean);
    double stdPerTick = Math.sqrt(variance);
    vol = stdPerTick * ANNUALIZE * 100;      // % năm hoá
    var95 = Z_95 * stdPerTick * 100;         // % VaR 1-tick horizon
}
```

$$\text{Var}(X) = E[X^2] - (E[X])^2 = \frac{\text{sumSq}}{n} - \left(\frac{\text{sum}}{n}\right)^2$$

| Cách tính | Chi phí mỗi tick | Bộ nhớ |
|---|---|---|
| Lưu toàn bộ lịch sử, tính lại | $O(n)$ với $n$ tăng vô hạn | **Vô hạn** |
| Giữ cửa sổ 120, duyệt lại mỗi tick | $O(120)$ | $O(120)$ |
| **Cửa sổ + tổng tích luỹ (repo này)** | **$O(1)$** | $O(120)$ |

🔑 **Vì sao `Math.max(0, …)` ở dòng tính variance.** Công thức $E[X^2] - (E[X])^2$ nổi tiếng là **không ổn định số học**: khi các giá trị rất nhỏ (log-return cỡ `1e-4`) và gần nhau, hai số hạng gần bằng nhau và phép trừ gây *catastrophic cancellation* — kết quả có thể ra số âm cực nhỏ (`-1e-19`). `Math.sqrt(số âm)` trả `NaN`, và `NaN` lan ra toàn bộ chuỗi tính toán phía sau. `max(0, …)` là cái kẹp chặn đúng chỗ.

💡 Thuật toán Welford thật sự (cập nhật mean và M2 trực tiếp) ổn định số học hơn và không cần kẹp. Comment trong mã nói "Welford-style" — chính xác hơn thì đây là *shifted-sum approach*, họ hàng gần chứ không phải Welford. Với log-return ở quy mô này, cặp `sum/sumSq` + kẹp là đủ, và nó có ưu điểm mà Welford không có: **trừ được giá trị rời khỏi cửa sổ** (Welford không có phép trừ cho sliding window).

#### (c) Năm hoá volatility

```java
private static final double ANNUALIZE = Math.sqrt(252 * 6.5 * 3600); // giả định tick ~1s
```

$$\sigma_{\text{năm}} = \sigma_{\text{tick}} \times \sqrt{\text{số tick mỗi năm}} = \sigma_{\text{tick}} \times \sqrt{252 \times 6{,}5 \times 3600}$$

Quy tắc **căn bậc hai của thời gian** (square-root-of-time rule): phương sai của tổng các biến độc lập cùng phân phối là tổng phương sai, nên độ lệch chuẩn nhân theo $\sqrt{T}$. 252 = số ngày giao dịch/năm, 6,5 = số giờ/phiên (chuẩn Mỹ), 3600 = giây/giờ. Kết quả: $\sqrt{5.896.800} \approx 2428$.

⚠️ **Giả định "tick ~1s" sai với chính feed của repo này.** `SimulatedFeed` phát 5 tick/giây cho mỗi mã (`@Scheduled(fixedRate = 200)`), nên khoảng cách tick là 200ms. Với tần suất thật cao gấp 5, hệ số năm hoá đúng phải là $\sqrt{252 \times 6{,}5 \times 3600 \times 5}$, tức lớn hơn $\sqrt 5 \approx 2{,}24$ lần. Nghĩa là **volatility đang bị thổi lên ~2,24 lần** so với con số năm hoá đúng.

Hệ quả thực tế:
- Con số tuyệt đối **không** dùng để ra quyết định tài chính thật được.
- *Phân lớp tương đối* giữa các mã vẫn có nghĩa (mã nào biến động hơn mã nào).
- Ngưỡng 15/35 trong mã được chọn để khớp dải giá trị **của feed này**, nên hệ thống vẫn phân loại hợp lý — nhưng đó là sự trùng khớp do hiệu chỉnh, không phải do công thức đúng.

💡 Cách sửa đúng: đo khoảng cách tick trung bình thực tế trong cửa sổ và năm hoá theo nó, thay vì hằng số. Hoặc đơn giản hơn: đưa `ANNUALIZE` thành `@Value` cấu hình theo môi trường.

#### (d) VaR 95%

```java
private static final double Z_95 = 1.645;        // z-score cho VaR 95%
...
var95 = Z_95 * stdPerTick * 100;                 // % VaR 1-tick horizon
```

$$\text{VaR}_{95\%} = z_{0{,}95} \times \sigma = 1{,}645 \times \sigma$$

Đọc là: *"với xác suất 95%, mức lỗ trong một bước tick không vượt quá VaR%"*. Đây là **parametric VaR** (variance-covariance), giả định log-return phân phối chuẩn.

⚠️ Hạn chế cố hữu của parametric VaR — đáng nói khi phỏng vấn:
- **Giả định phân phối chuẩn sai với thị trường thật.** Lợi suất tài chính có *đuôi dày* (fat tails): sự kiện 5-sigma xảy ra thường xuyên hơn nhiều lần so với dự đoán của phân phối chuẩn.
- **VaR không nói gì về mức lỗ khi vượt ngưỡng.** "95% không lỗ quá 2%" không cho biết 5% còn lại lỗ 3% hay 80%. Đó là lý do ngành chuyển dần sang **Expected Shortfall / CVaR** (kỳ vọng lỗ *có điều kiện* khi đã vượt VaR) — Basel III yêu cầu ES thay VaR.
- **Chân trời 1 tick** là cực ngắn. VaR trong quản trị rủi ro thật thường tính cho 1 ngày hoặc 10 ngày.
- **Không phải measure nhất quán:** VaR không thoả tính subadditive — VaR của danh mục có thể lớn hơn tổng VaR các thành phần, mâu thuẫn với trực giác về đa dạng hoá.

Các phương án thay thế: historical simulation (dùng phân phối thực nghiệm, không giả định chuẩn), Monte Carlo (mô phỏng nhiều kịch bản), EWMA/GARCH cho volatility có nhớ.

#### (e) Phân loại rủi ro và chuyển đổi về BigDecimal

```java
// RiskCalculator.java:75-89
String riskLevel = vol < 15 ? "LOW" : vol < 35 ? "MEDIUM" : "HIGH";

return new EnrichedMarketData(
        tick.symbol(),
        tick.price(),
        bd(ema, 4),
        bd(vol, 6),
        bd(var95, 6),
        riskLevel,
        System.currentTimeMillis(),
        tick.eventTime()
);
```
```java
// RiskCalculator.java:90-93
private static BigDecimal bd(double v, int scale) {
    if (Double.isNaN(v) || Double.isInfinite(v)) return BigDecimal.ZERO;
    return new BigDecimal(v, MathContext.DECIMAL64).setScale(scale, RoundingMode.HALF_UP);
}
```

🔑 **Hàm `bd()` làm ba việc đúng:**
1. **Chặn `NaN`/`Infinity`** trước khi chúng ra khỏi service. Nếu một `NaN` lọt vào `EnrichedMarketData`, Jackson serialize nó thành `NaN` (JSON không hợp lệ) hoặc `null`, rồi `order-service` ném `NullPointerException` ở `md.volatility()`.
2. **`MathContext.DECIMAL64`** giới hạn 16 chữ số có nghĩa — tránh `new BigDecimal(double)` sinh ra chuỗi dài vô nghĩa như `28500.00000000000363797880709171295166015625`.
3. **`setScale(…, HALF_UP)`** khớp với precision của cột DB (`NUMERIC(18,4)` cho giá, `NUMERIC(10,6)` cho volatility).

💡 **Ranh giới double/BigDecimal trong repo này đặt đúng chỗ:** `double` ở trong thuật toán (nhanh, không cấp phát, sai số `1e-16` không ai quan tâm với thống kê), `BigDecimal` ở biên vào/ra (tiền tệ, khớp DB, hợp đồng API).

#### (f) Vòng đời trạng thái

```java
// RiskCalculator.java:33-40
private final Map<String, SymbolState> states = new ConcurrentHashMap<>();

public EnrichedMarketData onTick(TickEvent tick) {
    SymbolState st = states.computeIfAbsent(tick.symbol(), s -> new SymbolState());
    synchronized (st) {
        return st.update(tick);
    }
}
```

Bộ nhớ mỗi mã: `ArrayDeque` 120 `Double` (~120 × 16 byte boxed + overhead ≈ 2,5KB) + 4 `double` ≈ **~3KB/mã**. 1000 mã ≈ 3MB. Không đáng kể so với heap `1536Mi`.

⚠️ **`states` cũng không bao giờ bị xoá** — cùng mẫu vấn đề với `SnapshotStore.latest`. Với số mã cố định (sàn có vài nghìn mã) thì chấp nhận được. Nhưng nếu ai đó bơm nhầm symbol sinh ngẫu nhiên vào `stock.ticks`, map này phình tới OOM. Phòng vệ đúng là Caffeine cache với `expireAfterAccess`.

⚠️ **Trạng thái mất khi pod restart.** EMA và cửa sổ 120 tick phải hâm nóng lại. Với feed 5 tick/s thì mất ~24 giây để cửa sổ đầy — trong khoảng đó volatility tính trên ít mẫu và không ổn định, có thể gây phân loại `riskLevel` sai. Cần nhớ điều này khi rolling update giữa phiên giao dịch: `CooperativeStickyAssignor` giúp giảm số partition phải đổi chủ, nhưng pod mới vẫn khởi đầu với state rỗng.

### 3.3. Write-behind cache — `SnapshotStore`

```java
// pricing-risk-service/src/main/java/com/trading/pricing/service/SnapshotStore.java:15-25
/**
 * WRITE-BEHIND CACHE:
 * - Hot path: chỉ update ConcurrentHashMap (nanoseconds)
 * - Cold path: @Scheduled flush snapshot mới nhất xuống Postgres mỗi 5s (batch insert)
 *
 * Ưu điểm: DB chậm/chết KHÔNG ảnh hưởng luồng tính toán realtime.
 * Nhược điểm: mất tối đa 5s dữ liệu snapshot nếu pod chết (chấp nhận được
 * vì Kafka vẫn giữ raw tick, có thể replay để rebuild).
 */
```

```java
// SnapshotStore.java:36-68
public void update(EnrichedMarketData data) {
    latest.put(data.symbol(), data);   // hot path - O(1), lock-free read
}

public Optional<EnrichedMarketData> get(String symbol) {
    return Optional.ofNullable(latest.get(symbol));
}

public Map<String, EnrichedMarketData> all() {
    return Map.copyOf(latest);
}

@Scheduled(fixedDelay = 5000)
public void flushToDatabase() {
    if (latest.isEmpty()) return;
    Instant now = Instant.now();
    List<MarketSnapshotEntity> batch = latest.values().stream().map(d -> {
        MarketSnapshotEntity e = new MarketSnapshotEntity();
        e.setSymbol(d.symbol());
        e.setLastPrice(d.lastPrice());
        e.setEmaPrice(d.emaPrice());
        e.setVolatility(d.volatility());
        e.setVar95(d.var95());
        e.setRiskLevel(d.riskLevel());
        e.setSnapTime(now);
        return e;
    }).toList();
    try {
        repository.saveAll(batch);   // Hibernate batch insert (xem application.yml)
    } catch (Exception ex) {
        // Không ném exception: DB lỗi không được làm chết scheduler / hot path
        log.error("Flush DB that bai, se thu lai lan sau", ex);
    }
}
```

**Ba đặc điểm đáng mổ xẻ:**

**(a) `Map.copyOf(latest)` trong `all()`** — trả về bản sao bất biến. Nếu trả thẳng `latest`, caller có thể `put`/`clear` vào trạng thái nội bộ của service, hoặc gặp `ConcurrentModificationException` khi iterate trong lúc hot path đang ghi. Cái giá: mỗi lần gọi `GET /api/v1/marketdata` tạo một bản sao — chấp nhận được vì số mã nhỏ và endpoint này không nằm trên hot path.

**(b) `catch (Exception)` nuốt lỗi có chủ đích.** Nếu ném ra, Spring `@Scheduled` với `fixedDelay` vẫn chạy lần sau (không như `ScheduledExecutorService` thuần, nơi một exception dừng hẳn task) — nhưng log sẽ đầy stacktrace và không ai phân biệt được lỗi thật. Quan trọng hơn: không ném là tuyên ngôn rõ ràng rằng **DB không phải thành phần thiết yếu của luồng realtime**.

⚠️ Nhưng đây là một ví dụ của "lỗi im lặng nguy hiểm hơn lỗi ồn ào": nếu DB chết 3 ngày, service vẫn báo `UP`, API realtime vẫn trả dữ liệu, và không ai biết lịch sử snapshot đã mất 3 ngày. Phòng vệ đúng: tăng một `Counter` mỗi lần flush thất bại và đặt alert lên nó — đúng mẫu mà `OutboxRelay` đã làm với `outbox.publish.failed`.

**(c) `batch_size: 100` ở Hibernate:**

```yaml
# pricing-risk-service/src/main/resources/application.yml
spring:
  jpa:
    properties:
      hibernate:
        jdbc:
          batch_size: 100        # gom 100 INSERT/round-trip khi saveAll
        order_inserts: true
```

Không có `batch_size`, `saveAll(100 entities)` sinh **100 round-trip** tới Postgres. Với 100, nó sinh 1 round-trip chứa 100 câu INSERT. `order_inserts: true` sắp xếp các INSERT theo bảng để batch không bị vỡ khi có nhiều loại entity.

⚠️ Lưu ý: batching JDBC **không hoạt động** với `GenerationType.IDENTITY` trong các phiên bản Hibernate cũ, vì Hibernate phải lấy generated key ngay sau mỗi insert. Hibernate 6 (Spring Boot 3.x) đã hỗ trợ batching với IDENTITY qua `getGeneratedKeys`, nên cấu hình này có hiệu lực — nhưng nếu nâng/hạ version cần kiểm chứng lại bằng `spring.jpa.properties.hibernate.generate_statistics=true`.

### 3.4. ⚠️ Ops #3 — bom hẹn giờ của write-behind

Đây là hạn chế nghiêm trọng nhất của service này, và nó rất đáng phân tích vì là mẫu lỗi hay gặp.

**Chuỗi nguyên nhân:**

1. `flushToDatabase()` tạo `new MarketSnapshotEntity()` với `id = null`.
2. Entity khai `@GeneratedValue(strategy = GenerationType.IDENTITY)` (`MarketSnapshotEntity.java:20-21`).
3. Spring Data thấy `id == null` ⇒ coi là entity mới ⇒ luôn phát `INSERT`, **không bao giờ `UPDATE`**.
4. Nguồn của batch là `latest.values()` — **toàn bộ** map.
5. `latest` chỉ có `put` (`:37`), **không có chỗ nào `remove`/`clear`**.

⇒ Một khi `HPG` đã tick lúc 9h00, nó nằm trong map vĩnh viễn, và **mọi** lần flush sau đó đều ghi lại nó, dù dữ liệu y hệt.

Guard duy nhất `if (latest.isEmpty()) return;` chỉ chặn đúng lúc service vừa khởi động.

**Quy mô — 50 mã, pod chạy 24h, thị trường đã đóng cửa:**

| | |
|---|---|
| Flush mỗi | 5 giây |
| Số lần flush/ngày | 86.400 / 5 = **17.280** |
| Row/mã/ngày | 17.280 |
| 50 mã | **864.000 row/ngày** — gần như toàn bộ là bản sao |

```
id    symbol  last_price  ema_price  volatility  snap_time
8821  HPG     27.5000     27.4812    18.420000   15:00:05
8871  HPG     27.5000     27.4812    18.420000   15:00:10   <- y hệt
8921  HPG     27.5000     27.4812    18.420000   15:00:15   <- y hệt
```

**Cách sửa — flush theo diff:**

```java
private final Map<String, EnrichedMarketData> lastFlushed = new ConcurrentHashMap<>();

@Scheduled(fixedDelay = 5000)
public void flushToDatabase() {
    Instant now = Instant.now();
    List<MarketSnapshotEntity> batch = latest.entrySet().stream()
            .filter(e -> e.getValue() != lastFlushed.get(e.getKey()))  // so sánh reference là đủ
            .map(e -> toEntity(e.getValue(), now))
            .toList();
    if (batch.isEmpty()) return;
    try {
        repository.saveAll(batch);
        latest.forEach(lastFlushed::put);   // CHỈ cập nhật mốc khi ghi THÀNH CÔNG
    } catch (Exception ex) {
        flushFailed.increment();            // thêm counter cho alert
        log.error("Flush DB that bai, se thu lai lan sau", ex);
    }
}
```

💡 **Vì sao so sánh bằng `!=` (reference) là đủ và rẻ:** `RiskCalculator.onTick()` tạo một `EnrichedMarketData` **mới** mỗi tick. Nên identity khác nhau ⟺ có tick mới. Không cần `equals()` (vốn phải so 8 field, trong đó có `BigDecimal.equals` nhạy cảm với scale).

💡 **`lastFlushed` chỉ cập nhật SAU khi `saveAll` thành công** — giữ nguyên tinh thần "lỗi DB không làm mất dữ liệu": flush thất bại thì lần sau vẫn thử lại cùng dữ liệu đó.

> **Vì sao không dùng upsert theo `symbol`?** Nghe gọn hơn (mỗi mã một row, ghi đè), nhưng sẽ **giết** `MarketSnapshotRepository.findRecent(symbol, limit)` — query tồn tại đúng để đọc lịch sử nhiều mốc thời gian. Upsert chỉ hợp nếu bỏ luôn `findRecent` và coi bảng thuần là nơi khôi phục state sau restart.

Dù có diff hay không, bảng time-series kiểu này về lâu dài vẫn cần **retention** (xoá/archive row cũ hơn N ngày) hoặc **partition theo ngày** (`PARTITION BY RANGE (snap_time)`).

### 3.5. REST API — hai nguồn dữ liệu

```java
// pricing-risk-service/src/main/java/com/trading/pricing/controller/MarketDataController.java:13-47
/**
 * REST API cung cấp data cho order-service (hoặc UI):
 * - Realtime: đọc từ in-memory snapshot (<1ms)
 * - Lịch sử: đọc từ Postgres
 */
@GetMapping("/{symbol}")
public ResponseEntity<EnrichedMarketData> latest(@PathVariable String symbol) {
    return store.get(symbol)
            .map(ResponseEntity::ok)
            .orElse(ResponseEntity.notFound().build());
}

@GetMapping
public Map<String, EnrichedMarketData> all() {
    return store.all();
}

@GetMapping("/{symbol}/history")
public List<MarketSnapshotEntity> history(@PathVariable String symbol,
                                          @RequestParam(defaultValue = "50") int limit) {
    return repository.findRecent(symbol, limit);
}
```

```java
// pricing-risk-service/src/main/java/com/trading/pricing/repository/MarketSnapshotRepository.java:10-17
@Query(value = """
        SELECT * FROM market_snapshot
        WHERE symbol = :symbol
        ORDER BY snap_time DESC LIMIT :limit
        """, nativeQuery = true)
List<MarketSnapshotEntity> findRecent(@Param("symbol") String symbol, @Param("limit") int limit);
```

Index hỗ trợ query này:

```sql
-- pricing-risk-service/src/main/resources/db/migration/V1__init_market_snapshot.sql
CREATE INDEX idx_symbol_time ON market_snapshot (symbol, snap_time);
```

Thứ tự cột `(symbol, snap_time)` là đúng: Postgres dùng cột đầu để lọc `WHERE symbol = ?`, cột thứ hai để phục vụ `ORDER BY snap_time DESC` mà không cần sort thêm. Đảo lại thành `(snap_time, symbol)` thì query này phải quét index theo thời gian rồi lọc — chậm hơn nhiều.

⚠️ Ba điểm cần lưu ý ở controller này:
- **`limit` không bị chặn trần.** `?limit=10000000` sẽ kéo hàng triệu row vào heap. Khác hẳn `OrderController` có `MAX_PAGE_SIZE = 200`. Nên thêm `Math.min(limit, 500)`.
- **`history` trả thẳng `MarketSnapshotEntity`**, không qua DTO — ngược với chuẩn mà `OrderView` đã đặt ra ở order-service. Thêm một cột vào bảng là tự động lọt ra API.
- **Không có API key filter** ở service này (chỉ order-service có `ApiKeyFilter`). Ở local thì không sao vì service không expose ra ngoài; trên cluster thì Ingress chỉ route `/api/v1/marketdata` tới đây và NetworkPolicy chặn đường khác — nhưng trong namespace thì bất kỳ pod nào cũng gọi được.

---

## 4. Vấn đề được giải quyết

| Nếu không có | Hậu quả cụ thể |
|---|---|
| Thuật toán $O(1)$ | Lưu toàn bộ lịch sử ⇒ bộ nhớ tăng vô hạn; tính lại mỗi tick ⇒ latency trôi theo thời gian chạy |
| Write-behind | Mỗi tick một `INSERT` ⇒ ở 100.000 tick/s là 100.000 INSERT/s (bất khả thi); DB chậm ⇒ hot path chậm theo; DB chết ⇒ hệ dừng |
| Batch listener | Chi phí cố định mỗi lần gọi (span, commit) nhân với số tick thay vì chia cho 1000 |
| `key = symbol` | EMA/volatility sai ⇒ `riskLevel` sai ⇒ từ chối lệnh sai |
| DLT | Một tick hỏng chặn vĩnh viễn cả partition |
| Tách domain/wire | `RiskCalculator` phụ thuộc Avro ⇒ không unit-test được, đổi wire format phải sửa thuật toán |

---

## 5. Đánh đổi

### 5.1. Ưu

1. **Hot path không có I/O chặn** — latency ổn định, không phụ thuộc DB.
2. **Bộ nhớ cố định** O(số mã), không phụ thuộc số tick.
3. **DB chết không ảnh hưởng realtime.**
4. **Đọc realtime < 1ms** từ `ConcurrentHashMap`.
5. **Thuật toán thuần tuý, dễ test** — `RiskCalculator` không phụ thuộc framework.

### 5.2. Nhược

1. **Mất tối đa 5 giây snapshot khi pod chết.** Giảm nhẹ: Kafka giữ raw tick 7 ngày để replay.
2. **Trạng thái mất khi restart** — phải hâm nóng lại 120 tick (~24s ở feed hiện tại), trong đó `riskLevel` không đáng tin.
3. **Volatility năm hoá sai hệ số ~2,24×** do giả định tần suất tick.
4. **Parametric VaR giả định phân phối chuẩn** — không phản ánh đuôi dày của thị trường thật.
5. ⚠️ **Flush ghi lại toàn bộ mã kể cả khi không đổi** (Ops #3).
6. ⚠️ **Lỗi flush im lặng** — không counter, không alert.
7. ⚠️ **Lỗi deserialize không vào DLT** (Gap #1); **một tick lỗi đẩy cả lô 1000** (Gap #2).
8. ⚠️ **Thiếu `server.shutdown: graceful`** (Gap #20) — request REST đang bay bị cắt khi pod tắt.

---

## 6. Phương án thay thế

| Phương án | Khi nào hợp | Vì sao repo không chọn |
|---|---|---|
| **Kafka Streams / ksqlDB** | Cần join nhiều stream, windowed aggregation phức tạp, state store fault-tolerant | Phép tính ở đây là incremental thuần trên một stream; thêm RocksDB + changelog topic để đổi lấy thứ 60 dòng Java đã làm. Điểm đáng cân nhắc lại: Streams khôi phục state sau restart. |
| **Apache Flink** | Event-time processing, watermark, exactly-once state, cửa sổ phức tạp | Thừa cho bài toán này; thêm một cluster phải vận hành |
| **Redis làm snapshot store** | Cần nhiều pod chia sẻ chung một view | Thêm một network hop vào hot path (~0,5ms) — mất đúng thứ đang tối ưu. Và mỗi pod chỉ cần biết các mã nó được gán partition. |
| **Write-through cache** | Cần không mất dữ liệu snapshot | DB nằm trên hot path ⇒ mất toàn bộ lợi thế |
| **Timescale/InfluxDB** cho snapshot | Lưu time-series dài hạn, nén tốt, downsample tự động | Postgres đủ cho quy mô demo; nhưng là nâng cấp đúng khi bật retention/partition |
| **Welford thật sự** | Cần ổn định số học tối đa | Không hỗ trợ phép trừ cho sliding window; cặp sum/sumSq + kẹp `max(0,…)` đủ ở quy mô này |
| **EWMA volatility** (thay cửa sổ cố định) | Muốn volatility có "nhớ" mượt như EMA | Cửa sổ 120 dễ giải thích hơn; EWMA sẽ là nâng cấp tự nhiên nếu cần |

---

## 7. Núm vặn cấu hình

| Tham số | Nơi khai | Mặc định | Hệ quả khi đổi |
|---|---|---|---|
| `WINDOW` | `RiskCalculator.java:28` | 120 | Lớn ⇒ volatility mượt, phản ứng chậm; nhỏ ⇒ nhạy, nhiễu. Cũng là thời gian hâm nóng sau restart. |
| `EMA_ALPHA` | `:31` | 0,1 | Lớn ⇒ EMA bám giá sát, nhiễu; nhỏ ⇒ mượt, trễ |
| `ANNUALIZE` | `:29` | √(252·6,5·3600) | ⚠️ Hằng số, giả định tick 1s — nên thành `@Value` |
| `Z_95` | `:30` | 1,645 | Đổi mức tin cậy: 1,96 cho 97,5%, 2,326 cho 99% |
| Ngưỡng 15/35 | `:75` | hardcode | 🔑 Trực tiếp quyết định lệnh bị từ chối hay không — **nên là cấu hình, không nên hardcode** |
| `@Scheduled(fixedDelay)` | `SnapshotStore.java:48` | 5000ms | Nhỏ ⇒ mất ít dữ liệu hơn, nhiều INSERT hơn |
| `hibernate.jdbc.batch_size` | `application.yml` | 100 | Nên khớp cỡ batch flush điển hình |
| `maximum-pool-size` | `application.yml` | 10 | Chỉ scheduler + history API dùng DB ⇒ pool nhỏ đủ |
| `max.poll.records` | `KafkaConfig.java:56` | 1000 | ⚠️ Nguy hiểm nếu ai đó thêm I/O vào hot path |
| `TRACING_PROBABILITY` | env | 0,1 | Trace 100% hot path tốn CPU + băng thông đáng kể |

---

## 8. Cách kiểm chứng

```bash
# 1. Dòng giá có chảy không?
curl -s http://localhost:8082/api/v1/marketdata | jq
curl -s http://localhost:8082/api/v1/marketdata/HPG | jq

# 2. Lịch sử snapshot (chứng minh write-behind đang chạy)
curl -s 'http://localhost:8082/api/v1/marketdata/HPG/history?limit=5' | jq

# 3. Chứng minh Ops #3: đếm row trùng lặp
docker compose exec postgres psql -U trading -d trading -c "
  SELECT symbol, last_price, ema_price, count(*) AS so_ban_sao
  FROM market_snapshot GROUP BY symbol, last_price, ema_price
  HAVING count(*) > 1 ORDER BY so_ban_sao DESC LIMIT 5;"

# 4. Tốc độ tăng trưởng bảng
docker compose exec postgres psql -U trading -d trading -c "
  SELECT count(*) AS tong_row,
         pg_size_pretty(pg_total_relation_size('market_snapshot')) AS kich_thuoc,
         min(snap_time), max(snap_time)
  FROM market_snapshot;"

# 5. Hai metric latency — phân biệt nghẽn thuật toán vs nghẽn Kafka
curl -s http://localhost:8082/actuator/prometheus | grep -E 'pricing_batch_process|pricing_e2e_latency'

# 6. Lag của consumer group
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:19092 --describe --group pricing-risk-group

# 7. Chứng minh DB chết KHÔNG ảnh hưởng realtime
docker compose stop postgres
sleep 12
curl -s http://localhost:8082/api/v1/marketdata/HPG | jq    # VẪN TRẢ DỮ LIỆU
docker compose logs --tail=5 pricing-risk-service | grep "Flush DB that bai"
docker compose start postgres

# 8. Bơm tick thủ công để kiểm chứng thuật toán
for p in 100 101 102 103 104; do
  curl -s -X POST http://localhost:8081/api/v1/ticks \
    -H 'Content-Type: application/json' \
    -d "{\"symbol\":\"TEST\",\"price\":$p,\"volume\":10,\"eventTime\":$(date +%s%3N)}"
done
sleep 2
curl -s http://localhost:8082/api/v1/marketdata/TEST | jq
#  -> emaPrice nằm giữa 100 và 104, gần 104 hơn; volatility > 0
```

---

## 9. Hạn chế đã biết

| # | Loại | Vị trí | Vấn đề | Hướng sửa |
|:--:|---|---|---|---|
| 1 | **Gap** | `config/KafkaConfig.java:44` | Thiếu `ErrorHandlingDeserializer` ⇒ lỗi deserialize trong `poll()`, DLT không bắt được, consumer lặp lỗi vô hạn | Bọc `KafkaAvroDeserializer`; viết IT bơm byte rác |
| 2 | **Gap** | `consumer/TickConsumer.java:52` | Batch listener không ném `BatchListenerFailedException` ⇒ 1 tick lỗi đẩy cả lô 1000 vào DLT | Bọc vòng lặp, ném `BatchListenerFailedException(msg, e, i)` |
| 3 | **Ops** | `service/SnapshotStore.java:48-68` | `latest` không bao giờ xoá ⇒ flush ghi lại mọi mã kể cả khi không đổi (~17.280 row/mã/ngày) | Flush theo diff với `lastFlushed`; thêm retention/partition |
| 20 | Gap | `application.yml` | Thiếu `server.shutdown: graceful` + `timeout-per-shutdown-phase` | Thêm hai dòng; giữ `preStop + timeout ≤ terminationGracePeriodSeconds` |
| — | Ops | `SnapshotStore.java:65` | `catch` nuốt lỗi flush không counter, không alert ⇒ DB chết 3 ngày không ai biết | Thêm `Counter` + alert, như `outbox.publish.failed` |
| — | Doc | `RiskCalculator.java:29` | `ANNUALIZE` giả định tick 1s, thực tế 200ms ⇒ volatility thổi lên ~2,24× | Đo khoảng cách tick thực tế, hoặc đưa thành `@Value` |
| — | Ops | `RiskCalculator.java:33` | `states` không bao giờ xoá ⇒ symbol rác làm phình map tới OOM | Caffeine `expireAfterAccess` |
| — | Gap | `MarketDataController.java:44` | `limit` không chặn trần ⇒ `?limit=10000000` kéo sập pod | `Math.min(limit, 500)` |
| — | Gap | `MarketDataController.java:45` | Trả thẳng `MarketSnapshotEntity`, không qua DTO | Thêm `SnapshotView` như `OrderView` |
| — | Ops | `RiskCalculator.java:75` | Ngưỡng 15/35 hardcode nhưng trực tiếp quyết định từ chối lệnh | `@Value` + cấu hình theo nhóm mã |

---

## 10. Câu hỏi phỏng vấn

**Q: Giải thích vì sao thuật toán này là $O(1)$ mỗi tick.**

A: Vì ta không lưu dữ liệu để tính lại, mà duy trì *trạng thái tích luỹ*. EMA chỉ cần một biến `double` — $\text{EMA}_t$ là hàm của $P_t$ và $\text{EMA}_{t-1}$. Variance dùng hai tổng `sum` và `sumSq`: khi cửa sổ đầy, giá trị rời đi được trừ khỏi cả hai, giá trị mới được cộng vào, rồi variance = `sumSq/n − (sum/n)²`. Deque 120 phần tử chỉ tồn tại để biết *giá trị nào* phải trừ, không phải để duyệt lại. Nên mỗi tick là một số hằng định các phép toán, bất kể service đã chạy một phút hay một tháng.

**Q: Điểm yếu số học của công thức `sumSq/n − mean²` là gì? Repo xử lý thế nào?**

A: Catastrophic cancellation. Log-return ở đây cỡ `1e-4`, nên `sumSq/n` và `mean²` là hai số rất gần nhau; phép trừ hai số gần bằng nhau trong floating-point làm mất phần lớn chữ số có nghĩa và có thể ra số âm cực nhỏ như `-1e-19`. `Math.sqrt(số âm)` trả `NaN`, và `NaN` lan ra toàn bộ chuỗi tính toán — `volatility` thành `NaN`, `riskLevel` so sánh với `NaN` luôn false nên rơi vào nhánh `HIGH`, và mọi lệnh bị từ chối. Repo chặn bằng `Math.max(0, …)` ngay tại chỗ tính variance, cộng thêm lớp phòng vệ thứ hai trong hàm `bd()` chuyển `NaN`/`Infinity` thành `BigDecimal.ZERO` trước khi ra khỏi service. Thuật toán Welford thật sự ổn định hơn, nhưng nó không có phép trừ nên không dùng được cho sliding window.

**Q: Write-behind làm mất tối đa 5 giây dữ liệu. Sao chấp nhận được?**

A: Vì phải hỏi: *mất cái gì*. Không phải mất lệnh hay giao dịch — đó là bảng `market_snapshot`, một **bản sao để truy vấn lịch sử**. Nguồn sự thật của luồng realtime là Kafka, nơi raw tick được giữ 7 ngày và replay được để dựng lại snapshot. Nếu đây là bảng `orders` thì câu trả lời ngược lại hoàn toàn — và quả thật `order-service` dùng transaction đồng bộ chứ không write-behind. Lựa chọn cơ chế lưu trữ ở đây theo đúng giá trị của dữ liệu.

**Q: Tôi thấy `catch (Exception ex) { log.error(...); }` trong `flushToDatabase`. Đó không phải nuốt lỗi sao?**

A: Có, và nó có chủ đích — ném ra sẽ biến lỗi DB thành vấn đề của scheduler, trong khi tuyên ngôn thiết kế là DB không phải thành phần thiết yếu của luồng realtime. Nhưng tôi đồng ý đây là một thiếu sót thật: lỗi im lặng nguy hiểm hơn lỗi ồn ào. DB chết ba ngày thì service vẫn báo `UP`, API realtime vẫn trả dữ liệu, và không ai biết lịch sử đã mất. Đúng ra mỗi cơ chế "degrade gracefully" phải đi kèm một cơ chế phát hiện. Ở đây là thêm một `Counter` và một alert — chính xác mẫu mà `OutboxRelay` đã làm với `outbox.publish.failed`.

**Q: Pod restart giữa phiên giao dịch. Điều gì xảy ra với độ chính xác?**

A: `SymbolState` nằm trong RAM nên mất hết — EMA về `NaN` và khởi tạo lại bằng giá đầu tiên nhận được, cửa sổ 120 log-return rỗng. Trong ~24 giây (120 tick ở 5 tick/s), volatility được tính trên ít mẫu nên không ổn định, và `riskLevel` có thể phân loại sai. Hệ quả dây chuyền: `order-service` nhận `riskLevel` không đáng tin trong khoảng đó, có thể từ chối oan với `RISK_LEVEL_HIGH`. Giảm nhẹ: `CooperativeStickyAssignor` giữ partition ở lại consumer cũ khi có thể, nên rolling update chỉ làm mất state của phần partition thật sự đổi chủ. Giải pháp triệt để nếu điều này quan trọng: persist state định kỳ (như chính `market_snapshot` đã làm — chỉ cần đọc lại lúc khởi động để hâm nóng EMA), hoặc chuyển sang Kafka Streams với changelog topic.

**Q: Volatility năm hoá ở đây có dùng được cho quyết định tài chính thật không?**

A: Không, vì hai lý do. Thứ nhất, hệ số năm hoá giả định tick cách nhau 1 giây nhưng feed phát 5 tick/giây, nên con số bị thổi lên khoảng 2,24 lần. Thứ hai, và quan trọng hơn: parametric VaR giả định log-return phân phối chuẩn, trong khi thị trường thật có đuôi dày — sự kiện 5-sigma xảy ra thường xuyên hơn nhiều so với dự đoán của phân phối chuẩn, và VaR không nói gì về mức lỗ *khi đã* vượt ngưỡng. Đó là lý do Basel III chuyển sang Expected Shortfall. Cái dùng được ở đây là **phân lớp tương đối**: mã nào biến động hơn mã nào, và ngưỡng 15/35 được hiệu chỉnh cho dải giá trị của feed này nên phân loại vẫn hợp lý trong nội bộ hệ thống.

---

## 11. Bảng tổng kết

| Khía cạnh | Lựa chọn | Lý do | Cái giá |
|---|---|---|---|
| Vị trí tính toán | In-memory, $O(1)$ | Không I/O trên hot path | State mất khi restart |
| EMA | $\alpha = 0{,}1$ | O(1) bộ nhớ, phản ứng mượt | Không bao giờ quên hẳn quá khứ |
| Variance | Cửa sổ 120 + sum/sumSq | $O(1)$ mỗi tick | Catastrophic cancellation ⇒ phải kẹp `max(0,…)` |
| Return | Log-return | Cộng được, đối xứng, chuẩn hoá | — |
| VaR | Parametric 95% ($z = 1{,}645$) | Rẻ, dễ giải thích | Giả định chuẩn, không phản ánh đuôi dày |
| Năm hoá | √(252·6,5·3600) | Quy tắc căn bậc hai của thời gian | ⚠️ Sai hệ số ~2,24× với feed thực tế |
| Lưu DB | Write-behind 5s, batch 100 | DB không chặn hot path | Mất ≤ 5s; ⚠️ ghi lại cả mã không đổi |
| Đọc realtime | `ConcurrentHashMap` | < 1ms, lock-free | Mỗi pod một view riêng |
| Đọc lịch sử | Native query + index `(symbol, snap_time)` | Index phục vụ cả `WHERE` lẫn `ORDER BY` | ⚠️ `limit` không chặn trần |
| Xử lý lỗi Kafka | Retry 3× + DLT | Không mất dữ liệu âm thầm | ⚠️ Chưa bắt lỗi deserialize; đẩy cả lô |
| Xử lý lỗi DB | Nuốt + log | DB không thiết yếu cho realtime | ⚠️ Không counter, không alert |
| Ranh giới double/BigDecimal | double trong thuật toán, BigDecimal ở biên | Nhanh khi tính thống kê, chính xác khi là tiền | Phải nhớ `bd()` chặn `NaN` |

> **Đọc tiếp:** [Chương 04 — Order Service, Pre-trade Risk & Resilience](./04-order-service-pretrade-risk-va-resilience.md) — nơi `riskLevel` tính ở đây được dùng để quyết định số phận một lệnh.
