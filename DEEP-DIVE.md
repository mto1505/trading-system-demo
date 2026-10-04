# Deep Dive — Giải thích chi tiết các quyết định thiết kế

> Tài liệu bổ trợ cho `DESIGN-NOTES.md`. `DESIGN-NOTES.md` trả lời **"chọn gì, đánh đổi gì"**;
> tài liệu này trả lời **"cơ chế chạy ra sao, nằm ở dòng nào, và chỗ nào code chưa khớp tài liệu"**.
>
> Mọi tham chiếu `file:dòng` đã được đối chiếu với source tại thời điểm viết.

## Mục lục

1. [Partition key = symbol — nằm ở đâu trong code](#1-partition-key--symbol--nằm-ở-đâu-trong-code)
2. [Vì sao thứ tự theo mã là bắt buộc (EMA & volatility)](#2-vì-sao-thứ-tự-theo-mã-là-bắt-buộc-ema--volatility)
3. [Write-behind cache — hot path vs cold path](#3-write-behind-cache--hot-path-vs-cold-path)
4. [Staleness check — `max-data-age-ms`](#4-staleness-check--max-data-age-ms)
5. [Dead Letter Queue — không mất message âm thầm](#5-dead-letter-queue--không-mất-message-âm-thầm)
6. [12 partition + KEDA — scale theo đúng tín hiệu](#6-12-partition--keda--scale-theo-đúng-tín-hiệu)
7. [Idempotency với `clientOrderId`](#7-idempotency-với-clientorderid)
8. [V3 — Dual-write và cách bịt bằng Outbox](#8-v3--dual-write-và-cách-bịt-bằng-outbox)
9. [V4 — Vòng đời lệnh trọn vẹn](#9-v4--vòng-đời-lệnh-trọn-vẹn)
10. [Một manifest, ba môi trường — chỗ kustomize không lo hộ](#10-một-manifest-ba-môi-trường--chỗ-kustomize-không-lo-hộ)
11. [Tổng hợp các điểm nên xem lại](#11-tổng-hợp-các-điểm-nên-xem-lại)

---

## 1. Partition key = symbol — nằm ở đâu trong code

"Partition key = symbol" **không phải một config riêng**. Nó là **tham số thứ hai** của mọi lệnh
`send(topic, key, value)`. Kafka băm key đó (`murmur2(key) % numPartitions`) để chọn partition,
nên mọi message cùng symbol luôn rơi vào cùng một partition.

Có 4 chỗ trong code production:

| # | Topic | Vị trí | Cách đặt key |
|---|---|---|---|
| 1 | `stock.ticks` | `market-gateway/.../kafka/TickProducer.java:38` | Truyền trực tiếp |
| 2 | `stock.enriched` | `pricing-risk-service/.../consumer/TickConsumer.java:63` | Truyền trực tiếp |
| 3 | `stock.orders` | `order-service/.../service/OrderPersistence.java:61` | Qua outbox |
| 4 | `stock.executions` | `matching-engine/.../service/MatchingService.java:102` | Qua outbox |

### Truyền trực tiếp

```java
// TickProducer.java:38
kafkaTemplate.send("stock.ticks", tick.symbol(), avro)

// TickConsumer.java:63
enrichedTemplate.send("stock.enriched", enriched.symbol(), enriched);
```

### Qua outbox pattern

Hai topic còn lại không gửi thẳng. Key được lưu vào cột `msg_key` của bảng outbox trong cùng
transaction với dữ liệu nghiệp vụ:

```java
// OrderPersistence.java:61
event.setMsgKey(result.symbol());   // key = symbol giữ thứ tự theo mã

// MatchingService.java:102
event.setMsgKey(report.symbol());   // cùng key với stock.orders -> giữ thứ tự theo mã
```

Điểm gửi chung của cả hai nằm ở relay:

```java
// outbox-support/src/main/java/com/trading/outbox/OutboxRelay.java:58
kafkaTemplate.send(event.getTopic(), event.getMsgKey(), event.getPayload())
```

Cột `msgKey` được khai báo ở `OutboxEventEntity` của mỗi service và trong interface
`OutboxRecord.getMsgKey()`.

Lý do thiết kế: `DESIGN-NOTES.md:36-38` (mục 2.2).

---

## 2. Vì sao thứ tự theo mã là bắt buộc (EMA & volatility)

Cả hai phép tính trong `RiskCalculator` đều là **đệ quy theo tick trước đó**, nên đổi thứ tự
input là đổi kết quả.

### Hai công thức phụ thuộc thứ tự

**EMA** — `pricing-risk-service/.../service/RiskCalculator.java:52`

```java
ema = Double.isNaN(ema) ? px : EMA_ALPHA * px + (1 - EMA_ALPHA) * ema;
```

Giá trị mới tính từ `ema` cũ. Tick đến sau luôn có trọng số lớn hơn tick đến trước, nên hoán vị
thứ tự cho ra con số khác hẳn.

**Log-return / volatility** — `RiskCalculator.java:56`

```java
double r = Math.log(px / lastPrice);
```

`lastPrice` **phải** là tick liền trước theo thời gian của chính mã đó. `r` sau đó đẩy vào cửa sổ
trượt 120 phần tử (`returns`, `sum`, `sumSq`) để ra variance → volatility → VaR 95%.

### Hậu quả khi sai thứ tự

Kafka chỉ bảo đảm thứ tự **trong một partition**. Nếu key là null (round-robin) hoặc là trường
khác, 3 tick HPG `100 → 101 → 102` có thể rơi vào 3 partition, 3 thread xử lý song song, đến
theo thứ tự `102 → 100 → 101`:

| | Đúng thứ tự | Sai thứ tự |
|---|---|---|
| r₁ | `ln(101/100)` = **+0.00995** | `ln(100/102)` = **−0.0198** |
| r₂ | `ln(102/101)` = +0.00985 | `ln(101/100)` = +0.00995 |

Dấu return **đảo ngược** — hệ thống báo mã đang rớt giá trong khi thực tế nó tăng. Variance phình
lên vì biên độ giả, `riskLevel` (`RiskCalculator.java:76`) nhảy sang `HIGH` vô cớ, và lệnh bị chặn
oan ở pre-trade check.

Tệ nhất: sai số **tích luỹ vĩnh viễn**. `ema`, `sum`, `sumSq` là state sống trong RAM — một lần
lệch là lệch mãi cho đến khi restart pod.

### Lợi ích kèm theo: lock luôn uncontended

```java
// RiskCalculator.java:36-37
SymbolState st = states.computeIfAbsent(tick.symbol(), s -> new SymbolState());
synchronized (st) {
```

`synchronized` ở đây cần cho memory-visibility, nhưng vì mỗi symbol chỉ do **một** thread chạm
vào, lock luôn uncontended → JVM tối ưu thành biased/thin lock, chi phí gần bằng 0.

Lưu ý phân biệt: `synchronized` bảo vệ được **data race**, nhưng **không** bảo vệ được **sai thứ
tự**. Nếu nhiều thread cùng ghi một symbol thì vừa trả giá lock thật, vừa vẫn ra kết quả sai.

---

## 3. Write-behind cache — hot path vs cold path

"Write-behind" là đối lập của "write-through":

- **Write-through** — tính xong, ghi DB **ngay**, chờ DB trả lời rồi mới return.
- **Write-behind** — tính xong, ghi RAM rồi return **luôn**; chuyện xuống DB để thread khác làm sau.

"Hot" / "cold" ở đây là **tần suất chạy**, không phải nhiệt độ dữ liệu.

### Hot path — mỗi tick, phải nhanh

`pricing-risk-service/.../consumer/TickConsumer.java:52-70`, vòng lặp chỉ có 3 việc:

```java
EnrichedMarketData enriched = riskCalculator.onTick(tick);            // :61  CPU thuần, O(1)
snapshotStore.update(enriched);                                       // :62  map.put()
enrichedTemplate.send("stock.enriched", enriched.symbol(), enriched); // :63  async, không chờ ack
```

Và `update()` đúng một dòng — `SnapshotStore.java:37`:

```java
latest.put(data.symbol(), data);   // hot path - O(1), lock-free read
```

**Không một lệnh blocking I/O nào.** Không `repository.save()`, không chờ network.

### Cold path — mỗi 5 giây, được phép chậm

`SnapshotStore.java:48` — chạy trên thread scheduler riêng:

```java
@Scheduled(fixedDelay = 5000)
...
repository.saveAll(batch);   // :64  Hibernate batch insert
```

Với `hibernate.jdbc.batch_size: 100` (`application.yml:26`), 100 row đi trong một round-trip JDBC.

### Ba lý do tách ra

**1. Tải DB không còn tỉ lệ với tick rate.** Đây là điểm cốt lõi. `latest` là
`Map<String, EnrichedMarketData>` key theo symbol, nên trong 5 giây dù HPG có 500 tick, map chỉ
giữ **một** bản mới nhất (conflation). Số row mỗi lần flush = **số symbol**, không phải số tick.
Tick rate tăng 10 lần → tải DB **không đổi**.

**2. DB chết không làm chết việc tính toán.** `SnapshotStore.java:65-68`:

```java
} catch (Exception ex) {
    // Không ném exception: DB lỗi không được làm chết scheduler / hot path
    log.error("Flush DB that bai, se thu lai lan sau", ex);
}
```

Postgres sập → log một dòng, 5 giây sau thử lại. Nếu là write-through, DB chậm 50ms/insert sẽ dội
thẳng vào consumer → không kịp poll → lag dồn → quá `max.poll.interval` → rebalance → càng tệ.

**3. REST đọc từ RAM.** `MarketDataController` inject thẳng `SnapshotStore`; `get()` / `all()` đọc
`ConcurrentHashMap` → sub-millisecond, không tranh connection pool với luồng ghi.

### Cái giá

- **Mất tối đa 5 giây** snapshot nếu pod chết. Chấp nhận được vì Kafka giữ raw tick → replay rebuild
  được. Kafka là source-of-truth, Postgres chỉ là materialized view (`DESIGN-NOTES.md:53`).
- **DB không có full tick history**, chỉ snapshot 5s/lần. Dữ liệu trung gian bị nuốt — cố ý. Nếu
  nghiệp vụ cần đủ mọi tick → thêm consumer riêng ghi TimescaleDB/ClickHouse.

### ⚠️ Vấn đề: `latest` không bao giờ bị xoá → sinh row trùng

`flushToDatabase()` tạo `new MarketSnapshotEntity()` (`:53`) với `id = null`, mà entity khai
`@GeneratedValue(strategy = IDENTITY)` (`MarketSnapshotEntity.java:20-21`) → Spring Data luôn phát
`INSERT`, không bao giờ `UPDATE`.

Nguồn của batch là `latest.values()` — **toàn bộ** map. Mà `latest` chỉ có `put` (`:37`), không có
chỗ nào `remove` / `clear`. Nên một khi HPG đã tick lúc 9h00, nó nằm trong map vĩnh viễn và **mọi**
lần flush sau đó đều ghi lại nó, dù dữ liệu y hệt.

Guard duy nhất `if (latest.isEmpty()) return;` (`:50`) chỉ chặn lúc service vừa khởi động.

**Quy mô:** 50 symbol, pod chạy 24h, thị trường đã đóng cửa:

| | |
|---|---|
| Flush mỗi | 5 giây |
| Số lần flush / ngày | 86.400 / 5 = **17.280** |
| Row / symbol / ngày | 17.280 |
| 50 symbol | **864.000 row/ngày** — gần như toàn bộ là bản sao |

Chỉ khác nhau cột `snap_time`:

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
        log.error("Flush DB that bai, se thu lai lan sau", ex);
    }
}
```

So sánh bằng `!=` (reference) là đủ và rẻ: `RiskCalculator.onTick()` tạo object mới mỗi tick, nên
identity khác nhau ⟺ có tick mới. `lastFlushed` chỉ cập nhật **sau** khi `saveAll` thành công, giữ
nguyên tinh thần "lỗi DB không làm mất dữ liệu".

> **Lưu ý về giải pháp upsert:** upsert theo `symbol` (mỗi mã một row, ghi đè) nghe gọn hơn nhưng
> sẽ **giết** `MarketSnapshotRepository.findRecent(symbol, limit)` — query này tồn tại đúng để đọc
> lịch sử nhiều mốc thời gian. Upsert chỉ hợp nếu bạn bỏ luôn `findRecent` và coi bảng thuần là nơi
> khôi phục state sau restart.

Dù có diff hay không, bảng time-series kiểu này về lâu dài vẫn cần **retention** (xoá/archive row
cũ hơn N ngày) hoặc partition theo ngày.

---

## 4. Staleness check — `max-data-age-ms`

Lớp pre-trade check **thứ nhất** — `order-service/.../service/OrderService.java:112`:

```java
EnrichedMarketData md = cache.get(req.symbol()).orElse(null);
if (md == null || System.currentTimeMillis() - md.calcTime() > maxDataAgeMs) {
    return reject(req, "NO_FRESH_MARKET_DATA");
}
```

Ngưỡng đến từ `application.yml:62` qua `@Value` (`OrderService.java:52`):

```yaml
order:
  max-data-age-ms: 10000         # dữ liệu >10s -> coi như stale, không cho đặt
```

### Vấn đề nó giải quyết

order-service **không gọi REST** sang pricing-risk-service. Nó giữ read-model cục bộ
`MarketDataCache`, nghe topic `stock.enriched` và ghi đè liên tục (`MarketDataCache.java:24`):

```java
public void onEnriched(EnrichedMarketData data) {
    cache.put(data.symbol(), data);
}
```

Đây là `ConcurrentHashMap` trần — **không TTL, không expiry, không eviction**. Đó chính là cái bẫy:
nếu dòng dữ liệu đứt, cache **không rỗng, không báo lỗi** — nó chỉ im lặng giữ mãi giá trị cuối
cùng. `cache.get("HPG")` vẫn trả về một object trông hoàn toàn hợp lệ.

Các sự cố làm dòng dữ liệu đứt mà cache không hề biết:

| Sự cố | Cache nhìn thấy |
|---|---|
| pricing-risk-service chết | Giá cuối cùng trước khi chết, đứng yên |
| market-gateway chết (hết tick nguồn) | Y hệt |
| Consumer của order-service lag / kẹt rebalance | Y hệt |
| Mã đó đơn giản là không có giao dịch | Y hệt |

Không có staleness check thì 3 tiếng sau pricing chết, order-service vẫn vui vẻ nhận lệnh, và lớp
fat-finger (`OrderService.java:121`) càng vô dụng — nó so `req.price()` với `md.lastPrice()` đã mốc
meo, nên "lệch 5%" chẳng nói lên điều gì về giá hiện tại.

### `calcTime` là gì

Dấu thời gian do **pricing-risk-service** đóng vào lúc tính xong — `RiskCalculator.java:85`,
`System.currentTimeMillis()` ngay trong `SymbolState.update()`. Nên `now - md.calcTime()` đo đúng
thứ cần đo: **dữ liệu này được tính cách đây bao lâu**, gồm cả thời gian nằm trong Kafka và chờ
consumer poll.

Record còn có `eventTime` riêng (thời điểm tick sinh ra ở gateway) — check này **cố tình** dùng
`calcTime` chứ không phải `eventTime`.

### Vì sao từ chối chứ không đặt bừa

`DESIGN-NOTES.md:65` gọi đây là nguyên tắc **fail-safe**: khi không chắc chắn, chọn phương án an
toàn (không khớp) thay vì phương án tiện (khớp với data cũ). Lệnh bị từ chối → khách bấm lại,
phiền một chút. Lệnh khớp theo giá sai → tiền thật mất, không rollback được.

Cùng logic với `systemUnavailable()` (`OrderService.java:146`) khi circuit breaker mở.

Đây là **cái giá của việc chọn cache cục bộ thay vì gọi REST**: gọi REST thì pricing chết là lỗi
hiện ra ngay; cache thì lỗi **im lặng**, nên phải tự dựng cơ chế phát hiện.

### ⚠️ Hai điểm cần lưu ý khi tune

**Mã thanh khoản thấp bị từ chối oan.** Check này không phân biệt "hệ thống hỏng" với "mã này đơn
giản không ai giao dịch". Một mã penny 15 phút mới có một lệnh khớp sẽ **luôn** stale → mọi lệnh
đều `NO_FRESH_MARKET_DATA`. Con số 10s hợp với mã sôi động như VN30F; nếu danh mục có mã trầm, cần
ngưỡng theo nhóm mã thay vì một `@Value` toàn cục duy nhất.

**Lệch đồng hồ giữa các pod.** `System.currentTimeMillis()` chạy ở pod order-service, còn
`calcTime` đóng ở pod pricing. Với NTP chuẩn thì lệch cỡ mili giây, vô hại trước ngưỡng 10.000ms.
Nhưng nếu đồng hồ pricing chạy **nhanh hơn** vài chục giây thì `now - calcTime` ra **số âm** → mọi
dữ liệu đều có vẻ "tươi" vĩnh viễn → **check bị vô hiệu hoá mà không ai hay**. Nếu muốn chặt, thêm
phòng vệ cho nhánh âm bất thường.

---

## 5. Dead Letter Queue — không mất message âm thầm

Ý chính nằm ở chữ **"âm thầm"** (silently). Không phải "không mất message" — mà là "không mất mà
không ai biết".

### Hành vi cũ: retry rồi skip

`DefaultErrorHandler` mặc định khi **không** có recoverer: retry theo backoff, hết lượt thì **log
một dòng ERROR rồi commit offset và đi tiếp**. Message hỏng biến mất:

- Dòng log đó trôi trong hàng triệu dòng khác, không ai thấy.
- Không có metric nào nhảy, không alert nào kêu.
- Dữ liệu **không khôi phục được** — offset đã commit, consumer không quay lại.
- Ba tháng sau phát hiện báo cáo lệch, không cách nào biết đã mất bao nhiêu, mã nào, lúc nào.

### Vì sao không chọn "retry mãi cho đến khi được"

Phương án đối lập này **tệ hơn**. Một message hỏng vĩnh viễn (bytes lỗi, schema sai) sẽ không bao
giờ retry thành công. Consumer kẹt ở đúng offset đó, không commit được, không đi tiếp được →
**toàn bộ partition đứng**. Mọi tick phía sau của tất cả symbol thuộc partition đó dồn lag vô hạn.
Một record rác giết cả một phần hệ thống. Đó là **poison message**.

DLQ là đường thứ ba: **lấy message hỏng ra khỏi luồng chính, nhưng cất vào một chỗ bền vững** thay
vì vứt đi.

### Code

`pricing-risk-service/.../config/KafkaConfig.java:77-78`:

```java
var recoverer = new DeadLetterPublishingRecoverer(dltTemplate);
factory.setCommonErrorHandler(new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 3)));
```

`FixedBackOff(1000L, 3)` = cách nhau 1 giây, retry 3 lần (tổng 4 lần thử, ~3 giây). Hết lượt →
publish sang `<topic gốc>.DLT` kèm header chẩn đoán:

| Header | Nội dung |
|---|---|
| `kafka_dlt-exception-fqcn` | Class của exception |
| `kafka_dlt-exception-message` | Message lỗi |
| `kafka_dlt-exception-stacktrace` | Stacktrace đầy đủ |
| `kafka_dlt-original-topic` | `stock.ticks` |
| `kafka_dlt-original-partition` | Partition gốc |
| `kafka_dlt-original-offset` | Offset gốc |
| `kafka_dlt-original-timestamp` | Thời điểm gốc |

Mở một message trong DLT ra là biết **nó là gì, hỏng vì sao, đến từ đâu** — đủ để sửa và replay.

Topic đích (`KafkaConfig.java:100-102`):

```java
return TopicBuilder.name("stock.ticks.DLT").partitions(1).replicas(1).build();
```

1 partition vì DLT đáng ra phải rất ít message (nhiều thì chính nó là sự cố), và retention nên dài
hơn topic chính để có thời gian điều tra.

Producer riêng cho DLT (`:95`) dùng `acks=all`, chặt hơn `acks=1` của luồng chính (`:112`) — hợp lý
vì đây là dữ liệu chẩn đoán cuối cùng còn sót lại.

### Cái giá: DLT không ai đọc thì vô nghĩa

`DESIGN-NOTES.md:121` nói thẳng điều này. DLT chỉ **chuyển** vấn đề từ "mất âm thầm" sang "nằm chờ
ai đó xử lý". Phải có:

- **Alert trên message count của `stock.ticks.DLT`** — bất kỳ message nào vào đây đều bất thường,
  ngưỡng nên là `> 0`, không phải một con số lớn.
- **Quy trình replay**: ai xem, sửa thế nào, publish ngược lại ra sao.

Nếu không, kết quả thực tế **giống hệt skip**, chỉ tốn thêm một topic.

Message replay từ DLT **mất thứ tự** so với luồng chính. Với tick data chấp nhận được; nhưng nếu áp
cùng cách này cho `stock.orders` thì phải cân nhắc, vì lệnh thì thứ tự có ý nghĩa.

### ⚠️ Hai khoảng trống trong cài đặt hiện tại

**1. Batch listener → cả lô vào DLT, không phải một record.**

`factory.setBatchListener(true)` (`:69`) và `MAX_POLL_RECORDS = 1000` (`:59`). `TickConsumer.onTicks()`
nhận `List<TickEventAvro>` và không bắt exception nào. Với `DefaultErrorHandler` ở chế độ batch,
nếu listener ném exception mà **không phải** `BatchListenerFailedException`, Spring không biết record
nào hỏng → retry cả lô, rồi đẩy **toàn bộ** lô vào DLT. **Một tick lỗi kéo 999 tick lành đi cùng.**

Cách sửa — chỉ ra index hỏng:

```java
for (int i = 0; i < avroTicks.size(); i++) {
    try {
        process(avroTicks.get(i));
    } catch (Exception e) {
        throw new BatchListenerFailedException("Tick loi", e, i);
    }
}
```

Spring sẽ coi các record trước index `i` là đã xử lý, chỉ gửi record `i` sang DLT, rồi chạy tiếp
từ `i+1`.

**2. Không có `ErrorHandlingDeserializer` → poison pill phổ biến nhất không tới được DLT.**

`KafkaConfig.java:44` đặt `KafkaAvroDeserializer` **trực tiếp** làm value deserializer. Lỗi
deserialize (bytes rác, schema không khớp, Schema Registry trả 404) xảy ra **bên trong
`consumer.poll()`**, trước khi listener được gọi. Lúc đó chưa có `ConsumerRecord` nào để đưa cho
recoverer, nên `DefaultErrorHandler` không có gì để publish sang DLT.

Cách chuẩn của Spring Kafka:

```java
props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, KafkaAvroDeserializer.class);
```

> Chưa chạy thử để xác nhận Spring Kafka 3.1 (đi kèm Boot 3.2.5, `pom.xml:15`) xử lý nhánh này
> chính xác ra sao. Nhưng `ErrorHandlingDeserializer` là điều kiện được tài liệu Spring Kafka nêu rõ
> để DLT bắt được lỗi deserialize, và hiện **không có ở đâu trong repo**.
>
> **Nên kiểm chứng bằng integration test:** bơm vài byte rác vào `stock.ticks` qua Testcontainers
> rồi xem nó có hiện trong `stock.ticks.DLT` không.

---

## 6. 12 partition + KEDA — scale theo đúng tín hiệu

Hai thay đổi **phải đi cùng nhau**: nới trần scale (phần 1) và sửa tín hiệu kích hoạt scale (phần 2).
Làm một cái mà thiếu cái kia thì vô dụng.

### Phần 1 — Vì sao số partition là trần cứng

Luật Kafka: trong một consumer group, **một partition chỉ được gán cho đúng một consumer**.
Consumer dư ra không nhận gì, ngồi không.

Mỗi pod ở đây không phải một consumer mà là **ba**, vì `factory.setConcurrency(3)`
(`KafkaConfig.java:71`) tạo 3 consumer thread độc lập:

```
12 partition ÷ 3 consumer/pod = 4 pod
```

Pod thứ 5 join group, nhận về **không partition nào** — 3 thread nằm im, chỉ còn phục vụ REST. Tốn
tiền, không tăng throughput. Đó là lý do `maxReplicaCount: 4` (`k8s/legacy/08-keda-scaledobject.yaml:21`).

**Vì sao phải provision dư ngay từ đầu** (`DESIGN-NOTES.md:143`):

1. **Không giảm được.** Kafka cho tăng partition, không cho giảm. Đặt hụt thì sửa được, đặt dư thì
   không gỡ — nhưng cái giá của dư chỉ là thêm vài file segment trên broker (`:183`).

2. **Tăng về sau làm vỡ mapping key→partition.** Partitioner mặc định là
   `murmur2(key) % numPartitions`. Đổi 6 → 12 thì HPG đang đi partition 2 có thể nhảy sang
   partition 8. Hậu quả đúng bằng [mục 2](#2-vì-sao-thứ-tự-theo-mã-là-bắt-buộc-ema--volatility):
   tick cũ ở partition 2, tick mới ở partition 8, hai consumer khác nhau xử lý → `lastPrice` và
   `ema` loạn. Tệ hơn, state in-memory của symbol đó đang nằm ở pod cũ; consumer mới bắt đầu lại từ
   `NaN`, EMA phải hội tụ lại từ đầu.

Nên "12 từ đầu" không phải tiết kiệm công — nó là **tránh một thao tác không thể làm an toàn khi hệ
đang chạy**.

### Phần 2 — HPA CPU là tín hiệu sai

HPA cũ (`k8s/legacy/04-pricing-risk.yaml:66+`, đã đánh dấu LEGACY) scale theo CPU. Với Kafka consumer, CPU
và "hệ có đang tụt hậu không" gần như không liên quan — **sai cả hai chiều**:

| Tình huống | CPU | Lag | HPA CPU làm gì | Đúng ra phải làm gì |
|---|---|---|---|---|
| Nghẽn I/O (chờ DB, chờ downstream ack) | Thấp | Hàng triệu | **Không scale** | Scale lên gấp |
| GC nặng / REST traffic cao | Cao | 0 | **Scale lên** | Không cần |

Chiều thứ nhất là chiều nguy hiểm: hệ thống đang tụt hậu nghiêm trọng mà autoscaler ngồi yên vì CPU
nhàn.

KEDA bỏ qua CPU, query thẳng Kafka: với mỗi partition, so **committed offset của consumer group**
với **end offset của partition** → ra lag. Đó đúng là đại lượng cần phản ứng.

```yaml
# k8s/legacy/08-keda-scaledobject.yaml (ban cu)
triggers:
  - type: kafka
    metadata:
      consumerGroup: pricing-risk-group
      topic: stock.ticks
      lagThreshold: "1000"
```

`matching-engine` dùng `lagThreshold: "500"` (`:51`) — thấp hơn vì lệnh nhạy cảm hơn tick: chậm một
giây là khách nhìn thấy, còn tick chậm một giây chỉ ảnh hưởng độ tươi của chỉ số.

**Lợi ích phụ:** scale-to-min khi thị trường đóng cửa. Không có tick → lag = 0 → co về
`minReplicaCount: 2`. HPA CPU không làm được sạch sẽ vì JVM idle vẫn tiêu CPU nền.

### Cái giá

- Thêm một operator phải cài và vận hành (KEDA, qua Helm — lệnh ở comment đầu file).
- **Vẫn bị trần partition.** KEDA không phá được luật Kafka; nó chỉ giúp quyết định đúng *khi nào*
  nên tiến tới trần đó.
- Ngưỡng lag phải tune theo throughput thật. 1000 là phỏng đoán ban đầu.
- **Hai autoscaler cùng nhắm một Deployment sẽ đánh nhau** — comment ở `:10` nhắc phải
  `kubectl delete hpa pricing-risk-hpa -n trading` trước khi apply KEDA. Lỗi vận hành rất dễ mắc.

### ⚠️ Năm chỗ trong YAML nên xem lại

**1. `lagThreshold` là lag trên mỗi *replica*, không phải mỗi *partition*.**
Comment ở `:29` ghi "lag trung bình/partition". KEDA Kafka scaler báo **tổng lag** của topic lên HPA
với target kiểu `AverageValue`, nên số pod mong muốn ≈ `ceil(tổng lag / lagThreshold)`. Tổng lag
4000 → 4 pod. Hiểu nhầm thành per-partition sẽ tune lệch 12 lần.

**2. `activationLagThreshold: "100"` hiện không có tác dụng.**
Tham số này chỉ chi phối bước chuyển **0 → 1 replica**. Ở đây `minReplicaCount: 2`, không bao giờ
về 0, nên nó không bao giờ được dùng đến.

**3. `cooldownPeriod` không chống flapping như comment nói.**
Trong KEDA, `cooldownPeriod` là thời gian chờ trước khi co **về 0 / idleReplicaCount**. Việc co từ
4 → 2 do HPA bên dưới quyết định, theo `stabilizationWindowSeconds` (mặc định 300s). Nên
`cooldownPeriod: 120` của matching-engine (`:47`, ghi chú "chống giật") thực tế không làm gì. Muốn
đúng ý:

```yaml
advanced:
  horizontalPodAutoscalerConfig:
    behavior:
      scaleDown:
        stabilizationWindowSeconds: 120
```

**4. Comment ở `k8s/legacy/04-pricing-risk.yaml:7` còn sót lại từ thời 6 partition.**

```yaml
replicas: 2        # 2 pod x concurrency 3 = 6 consumer = 6 partition (khớp 100%)
```

Giờ là 12 partition → 2 pod = 6 consumer, mỗi consumer ôm 2 partition, không còn "khớp 100%".
Tương tự khối comment `:62-64` vẫn nói "6 partition / concurrency 3 = tối đa 2 pod". Ngoài ra để
`replicas: 2` cố định trong Deployment khi đã có autoscaler sẽ gây giằng co nếu apply lại bằng
GitOps (mỗi lần apply reset về 2 rồi KEDA kéo lên lại).

**5. `maxReplicaCount` và `setConcurrency` là cặp số gắn nhau nhưng ở hai repo khác nhau.**
KEDA Kafka scaler **tự** chặn không scale quá số partition (mặc định `allowIdleConsumers: false`) —
nhưng nó chỉ biết 12 partition, **không biết** mỗi pod có 3 thread. Nếu chỉ dựa vào cơ chế đó, KEDA
sẽ cho lên tới 12 pod. Con số `maxReplicaCount: 4` là rào chắn **do người đặt**. Ai đổi
`setConcurrency(3)` thì **bắt buộc** phải sửa `maxReplicaCount` tương ứng — không có gì cưỡng chế.

### Trạng thái: cả năm điểm trên đã được xử lý ở bộ manifest mới

Phần phân tích phía trên giữ nguyên vì nó giải thích **vì sao** — còn YAML đã chuyển sang
`k8s/base` + `k8s/overlays` ([mục 10](#10-một-manifest-ba-môi-trường--chỗ-kustomize-không-lo-hộ)).
Bản cũ nằm ở `k8s/legacy/` để đối chiếu, không dùng nữa.

| # | Đã làm gì | Ở đâu |
|---|---|---|
| 1 | Comment ghi rõ `lagThreshold` là lag **trên mỗi replica**, kèm ví dụ "tổng lag 4000 → 4 pod" | `k8s/components/keda/scaledobject-pricing-risk.yaml:38-41` |
| 2 | `activationLagThreshold` được ghi rõ là **chỉ dùng cho ngưỡng 0 → 1**, giữ lại để sẵn cho scale-to-zero | `:42-45` |
| 3 | Thay bằng `behavior.scaleDown.stabilizationWindowSeconds`; `cooldownPeriod` bị bỏ hẳn | `:26-29` |
| 4 | Số partition thành **tham số theo môi trường** (3/6/12) thay vì một con số cứng trong comment; `replicas` đặt bằng `minReplicaCount` để không giằng co với autoscaler | `k8s/overlays/*/kustomization.yaml` |
| 5 | **Đã có thứ cưỡng chế**: CI build cả ba overlay rồi chặn merge nếu `maxReplicaCount > KAFKA_PARTITIONS / 3` | `.github/workflows/k8s-validate.yml` |

Điểm 5 đáng nói riêng: đó là lần đầu một ràng buộc trong tài liệu này được chuyển từ "phải nhớ" sang
"máy kiểm". Hai cặp ràng buộc cùng loại vẫn chưa có ai canh — `timeout-per-shutdown-phase` ↔
`terminationGracePeriodSeconds` ([mục 10.4](#104-ngân-sách-tắt-pod--ba-con-số-phải-cộng-lại-cho-khớp)),
và retention của `stock.orders` ↔ lịch dọn `processed_event` ([#12](#11-tổng-hợp-các-điểm-nên-xem-lại)).

---

## 7. Idempotency với `clientOrderId`

Vấn đề gốc là **timeout mơ hồ**: client gọi `POST /api/v1/orders`, mạng đứt giữa chừng, client
không có cách nào biết lệnh đã vào hay chưa. Hai lựa chọn đều sai — không retry thì có thể mất
lệnh, retry thì có thể đặt hai lần. Với hệ tài chính, đặt hai lần là tiền thật.

Lối thoát duy nhất: client tự sinh một **`clientOrderId`** (UUID) và gửi kèm, server cam kết
*"một id → nhiều nhất một lệnh"*. Khi đó retry trở nên an toàn tuyệt đối.

> **Lưu ý:** mục `V2.7` trong `DESIGN-NOTES.md` mô tả bản **v2** (map per-pod trong RAM); phần
> "Nhược" ở `:165` nói cache per-pod nên 2 retry vào 2 pod vẫn lọt. **Code đã đi xa hơn** — javadoc
> ở `OrderService.java:22` ghi rõ "Luồng đặt lệnh **v3**". Phần tài liệu đó đã lạc hậu so với code.

### Lớp 1 — Redis fast path (`OrderService.java:77-88`)

```java
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

Khác hẳn map per-pod: Redis **tập trung**, mọi pod nhìn chung một chỗ, nên load balancer ném retry
vào pod nào cũng bắt được.

Chú ý khối `catch` — Redis chết thì chỉ log cảnh báo rồi **đi tiếp**. Lựa chọn có chủ đích, giải
thích ở `OrderService.java:32-33`:

> Redis ở đây là CACHE TĂNG TỐC, không phải source of truth của dedupe — DB constraint mới là.
> Vì vậy Redis chết → hệ vẫn ĐÚNG, chỉ chậm hơn 1 chút.

### Lớp 2 — UNIQUE constraint ở DB (chốt chặn thật)

`order-service/.../entity/OrderEntity.java:28`:

```java
@Column(name = "client_order_id", nullable = false, unique = true, length = 64)
private String clientOrderId;
```

Xử lý ở `OrderService.java:92-100`:

```java
try {
    persistence.saveWithOutbox(req, result);
} catch (DataIntegrityViolationException dup) {
    duplicates.increment();
    OrderResult existing = persistence.findExisting(req.clientOrderId()).orElseThrow();
    cacheResult(idemKey, existing);
    return existing;
}
```

Đây mới là lớp **chống được race thật sự**. Kịch bản: hai retry đến gần như đồng thời, rơi vào hai
pod, cả hai cùng miss Redis (chưa ai kịp ghi), cả hai cùng qua pre-trade check, cả hai cùng
`INSERT`. Unique index ở Postgres sẽ chặn: một transaction thắng, transaction kia bị block cho đến
khi bên thắng commit rồi nhận `DataIntegrityViolationException`. Pod thua đọc lại bản ghi đã có,
trả đúng kết quả đó, và tiện thể nạp vào Redis cho lần sau.

**Không lock ứng dụng, không distributed lock, không `SETNX`** — chỉ dùng tính nguyên tử sẵn có của
unique index. Rẻ và chắc nhất.

Chi tiết hay: Redis TTL chỉ 10 phút (`IDEM_TTL`, `:39`), còn constraint ở DB là **vĩnh viễn**. Retry
sau 11 phút vẫn không tạo được lệnh trùng — chỉ đi đường chậm (miss Redis → đụng constraint → đọc
DB) thay vì đường nhanh.

### Lớp 3 — Dedupe lại ở matching-engine

Vì sao cần thêm lớp nữa khi order-service đã bảo đảm đúng một dòng trong DB? Vì **Kafka là
at-least-once**. Dù chỉ có một lệnh, event `ORDER_ACCEPTED` vẫn có thể đến matching-engine nhiều
lần: `OutboxRelay` crash sau khi `send` nhưng trước khi đánh dấu đã xử lý, hoặc consumer rebalance
làm replay các offset chưa commit.

`matching-engine/.../service/MatchingService.java:62-66`:

```java
if (processedRepo.insertIfAbsent(order.clientOrderId()) == 0) {
    duplicated.increment();
    log.info("Bo qua message trung clientOrderId={}", order.clientOrderId());
    return;
}
```

`insertIfAbsent` là SQL thuần (`ProcessedEventRepository.java:16-21`):

```sql
INSERT INTO matchsvc.processed_event (client_order_id, processed_at)
VALUES (:id, now())
ON CONFLICT (client_order_id) DO NOTHING
```

Trả `1` = lần đầu, `0` = đã xử lý. Lại là check-and-set nguyên tử bằng chính DB.

**Điểm mấu chốt** nằm ở `@Transactional` trên `process()` (`:60`): ghi `processed_event`, ghi
`execution`, và ghi outbox execution report **nằm chung một transaction**. Nếu tách ra, sẽ có trạng
thái "đã đánh dấu processed nhưng chưa ghi execution" → message replay bị bỏ qua → **lệnh biến
mất**. Cùng commit hoặc cùng rollback mới cho ra ngữ nghĩa *effectively-once*.

Và `clientOrderId` là **cùng một khoá xuyên suốt** cả ba lớp — client sinh ra nó, order-service
dedupe theo nó, matching-engine dedupe theo nó. Không có bước dịch khoá nào để sai.

### ⚠️ Bốn điểm cần lưu ý

**1. Kết quả REJECTED cũng bị "đóng băng" theo id.**
Lệnh đầu bị từ chối vì `NO_FRESH_MARKET_DATA`, dòng DB đã ghi với status REJECTED. Client retry
**cùng** `clientOrderId` 30 giây sau, khi dữ liệu đã tươi trở lại → vẫn nhận lại REJECTED cũ.

Đây là ngữ nghĩa idempotency **đúng** (một id, một kết quả), nhưng client phải hiểu:
- *Retry vì nghi ngờ mạng* → dùng lại id cũ.
- *Đặt lại sau khi bị từ chối* → **phải sinh id mới**.

Nên ghi rõ trong tài liệu API.

**2. `orElseThrow()` ở `:97` có thể che lỗi thật.**
`DataIntegrityViolationException` không chỉ đến từ `client_order_id` — nó có thể đến từ constraint
khác (ví dụ `order_id` trùng, hay lỗi ở bảng outbox). Khi đó `findExisting` trả rỗng →
`NoSuchElementException` → client nhận 500 với thông điệp vô nghĩa, nguyên nhân thật bị nuốt.

Nên kiểm tra đúng tên constraint, hoặc ít nhất `orElseThrow(() -> dup)` để giữ lại exception gốc.

**3. Fallback của circuit breaker không được ghi nhận idempotent.**
`systemUnavailable()` (`:146`) trả REJECTED mà không chạm DB, nên không có dòng nào mang
`clientOrderId` đó → retry cùng id sẽ được xử lý lại từ đầu. Đây thực ra là hành vi **mong muốn**
(lỗi hệ thống thì phải cho thử lại), nhưng là một **ngoại lệ có chủ đích** trong hợp đồng
idempotency, đáng ghi chú.

**4. `processed_event` phình vô hạn.**
Mỗi lệnh một dòng, không bao giờ xoá. Cần job dọn theo `processed_at` (giữ ví dụ 30 ngày) — nhưng
**khoảng giữ phải dài hơn retention của topic `stock.orders`**, nếu không replay một message cũ hơn
mốc dọn sẽ bị khớp lần hai.

---

## 8. V3 — Dual-write và cách bịt bằng Outbox

V3 trả lời **một câu hỏi duy nhất**: ghi hai hệ thống sao cho nguyên tử? Mọi thứ còn lại trong
V3 (Avro, Resilience4j, Testcontainers, CI) là bịt các lỗ hổng chất lượng xung quanh.

### 8.1. Bài toán dual-write

Khi nhận một lệnh, `order-service` phải làm **hai** việc ở **hai hệ thống**:

1. Ghi Postgres (audit, dedupe, trạng thái)
2. Publish Kafka (để matching-engine xử lý)

Không có cách nào làm nguyên tử, và **cả hai thứ tự đều hỏng**:

| Thứ tự | Crash ở giữa | Hậu quả |
|---|---|---|
| DB trước, Kafka sau | DB đã ghi, Kafka chưa | Lệnh tồn tại nhưng **không ai xử lý** — treo vĩnh viễn |
| Kafka trước, DB sau | Kafka đã có, DB chưa | Lệnh được khớp nhưng **không có bản ghi** — không audit được |

**Vì sao không XA/2PC** (`DESIGN-NOTES.md:202`): Kafka không hỗ trợ XA — hết chuyện. Kể cả có,
2PC là giao thức **blocking**: coordinator chết ở pha 2 thì participant giữ lock chờ vô hạn. Với
đường nóng của hệ trading, một coordinator treo là cả sàn treo.

### 8.2. Mẹo của Outbox: biến bài toán phân tán thành bài toán cục bộ

Thay vì ghi DB + Kafka, ghi **hai bảng trong cùng một database**:

```java
// OrderPersistence.java:37  — @Transactional
orderRepository.save(order);        // bảng orders
...
outboxRepository.save(event);       // bảng outbox_event
```

Một local ACID transaction của Postgres. Nguyên tử tuyệt đối, không cần giao thức gì cả. Việc đưa
event lên Kafka tách thành tiến trình riêng — `OutboxRelay`, `@Scheduled` 500ms.

Phân tích crash tại từng điểm:

| Crash tại | Trạng thái | Vì sao vẫn đúng |
|---|---|---|
| Trước COMMIT | Không lệnh, không event | Client retry cùng `clientOrderId` |
| Sau COMMIT, trước publish | Event nằm chờ trong `outbox_event` | Relay chu kỳ sau gửi — **không mất** |
| Sau publish, trước mark | Event gửi lại | At-least-once → consumer dedupe lo |

### 8.3. Hai chi tiết kỹ thuật đáng học

**`FOR UPDATE SKIP LOCKED`** (`order-service/.../OutboxEventRepository.java:22`) — cho phép **nhiều
pod relay chạy song song**. Pod A khoá 100 dòng đầu; pod B gặp dòng bị khoá thì *bỏ qua* thay vì
chờ, lấy 100 dòng tiếp theo. Không có `SKIP LOCKED` thì relay chỉ scale được 1 pod.

**Partial index** (`order-service/.../db/migration/V1__orders_and_outbox.sql:34`):

```sql
CREATE INDEX idx_outbox_unpublished ON outbox_event (id) WHERE published_at IS NULL;
```

Bảng outbox có thể có hàng triệu dòng đã publish, nhưng index chỉ chứa dòng **chưa** publish —
thường vài chục. Query relay luôn nhanh bất kể bảng lớn cỡ nào.

### 8.4. Idempotent Consumer — nửa còn lại, không phải phụ kiện

Outbox chỉ cho **at-least-once**. Điểm hay bị bỏ sót: *Outbox và Idempotent Consumer là một cơ chế
gồm hai nửa — thiếu nửa nào cũng vô nghĩa.* Chi tiết ở [mục 7](#7-idempotency-với-clientorderid).

**Vì sao không dùng Kafka transactions** (`DESIGN-NOTES.md:223`): Kafka EOS chỉ exactly-once trong
phạm vi **Kafka → Kafka**, nó không biết gì về Postgres. Cặp Outbox + Idempotent Consumer phủ được
cả hai, và không trả giá throughput cho transaction coordinator.

### 8.5. Redis — một nguyên tắc biến thành ba dòng config

Mục này là ví dụ tốt nhất trong cả repo về việc **nguyên tắc thiết kế phải được thực thi ở tầng
vận hành**. Nguyên tắc: *Redis là cache, DB constraint mới là source of truth*. Ba hệ quả, cả ba
đều có thật trong code:

| Config | Vị trí | Vì sao |
|---|---|---|
| `health.redis.enabled: false` | `order-service/application.yml:90` | Redis chết **không được** kéo readiness fail — nếu không K8s rút pod khỏi Service dù pod vẫn phục vụ đúng |
| `timeout: 200ms` | `order-service/application.yml:30` | Cache chậm không được kéo chậm lệnh |
| `try/catch` quanh Redis GET | `OrderService.java:85` | Lỗi Redis không được ném lên |

Thiếu dòng thứ nhất sẽ tạo nghịch lý: hệ thống thiết kế để sống sót khi Redis chết, nhưng K8s lại
tự tay giết hết pod vì health check báo đỏ.

### 8.6. Avro — di cư từng topic một

Phạm vi **có chủ đích**: chỉ `stock.ticks` chuyển Avro (`pricing/KafkaConfig.java:44`);
`stock.enriched` vẫn JSON (`:111`). Migrate từng topic là cách làm thực tế, không "big bang".

Nó buộc phải xây pattern **tách wire format khỏi domain model** (`TickEventAvro` ↔ `TickEvent`),
map tại biên — `TickProducer.java:31-36` và `TickConsumer.java:58-59`. Toàn bộ logic bên trong
không biết gì về Avro: dễ test, dễ đổi wire format.

Lý do hoãn `stock.orders` / `stock.executions` (`DESIGN-NOTES.md:388`) rất thuyết phục: payload
outbox nằm trong cột `TEXT` của Postgres. Nhét Avro binary vào đó cần base64 (phình ~33%, mất khả
năng debug bằng SQL) hoặc để relay tự serialize — mà relay là module **tổng quát**, không được
biết kiểu dữ liệu nghiệp vụ.

### 8.7. Resilience4j — hai tầng, và một chi tiết tinh tế

```yaml
# order-service/application.yml:41-47
ratelimiter:
  instances:
    orders:
      limit-for-period: 200      # 200 lệnh/giây/pod
      timeout-duration: 0s       # không xếp hàng chờ - từ chối ngay
```

`timeout-duration: 0` nghĩa là **không xếp hàng**. Với lệnh trading, chờ trong queue rồi khớp trễ
còn tệ hơn bị từ chối ngay — khách bị từ chối thì biết mà xử lý, khách bị khớp muộn 3 giây thì đã lỗ.

Chi tiết tinh tế nhất: `DataIntegrityViolationException` được catch **bên trong** `placeOrder`
(`OrderService.java:94`) nên không bao giờ thoát ra ngoài. Kết quả: **duplicate lệnh không làm
circuit breaker mở nhầm** — breaker chỉ thấy lỗi hạ tầng thật. Nếu catch ở ngoài, một client retry
mạnh tay có thể tự làm breaker mở và khiến cả pod ngừng nhận lệnh.

---

## 9. V4 — Vòng đời lệnh trọn vẹn

V3 làm chắc chuyện *đưa lệnh đi*. V4 trả lời câu khó hơn: **lệnh sau khi rời order-service thì kết
thúc ở đâu?**

### 9.1. Lỗ hổng của V3

`order-service` trả `ACCEPTED` rồi **hết**. Bảng `orders` mãi ở trạng thái đó. Matching-engine
khớp xong, không ai biết. Đây không phải thiếu sót thẩm mỹ: **khách bị giữ ký quỹ cho một lệnh mà
hệ thống không biết đã khớp hay chưa.**

### 9.2. Saga choreography — và tính đệ quy của Outbox

```
order-service --stock.orders--> matching-engine --stock.executions--> order-service
     (outbox)                        (outbox)                     (state machine)
```

Điểm sâu sắc nhất của V4: **matching-engine cũng phải có outbox của riêng nó**, vì bài toán
dual-write lặp lại y hệt ở chiều về. Nếu execution report publish ngoài transaction, sẽ có trường
hợp đã ghi `execution` mà report mất → order-service treo ACCEPTED vĩnh viễn, **đúng cái vừa đi sửa**.

Nên `MatchingService.process()` (`:60`) gói **ba** việc trong một transaction: `processed_event` +
`execution` + `outbox_event`.

Choreography thắng ở luồng 2 chặng, nhưng `DESIGN-NOTES.md:308` nêu đúng tiêu chí chuyển đổi:

> khi thêm chặng thứ ba và thứ tư (khớp → thanh toán → cập nhật vị thế → thông báo), choreography
> biến thành "event spaghetti" — không ai đọc code mà biết được toàn bộ luồng.

### 9.3. State machine — một cơ chế, hai công dụng

```java
// OrderStateMachine.java
ACCEPTED, Set.of(FILLED, REJECTED_BY_MATCHING),
FILLED,   Set.of(),       // terminal - không có đường ra
```

Vai trò hiển nhiên: chặn chuyển trạng thái vô lý. Vai trò thứ hai: **idempotency miễn phí** —
report đến lần hai thấy lệnh đã `FILLED`, `canTransition(FILLED, FILLED)` trả `false` → bỏ qua
(`OrderStatusService.java:57`). Không cần bảng `processed_event` ở phía order-service.

`@Version` (`OrderEntity.java:77`) chống lost update khi hai report tới song song.

Hai ghi chú thực chiến:

- **Vì sao optimistic chứ không pessimistic:** xung đột ở đây *hiếm* (cùng `clientOrderId` → cùng
  key symbol → thường cùng partition, cùng consumer). Khoá bi quan bắt **mọi** update trả giá lock
  chỉ để phòng trường hợp hiếm.
- **Retry phải nằm NGOÀI `@Transactional`.** Code tránh đúng lỗi kinh điển này —
  `ExecutionReportConsumer.java:27-39` retry ở vòng ngoài vì *"transaction đã rollback rồi mới ném
  exception - phải mở transaction MỚI để thử lại"*. Gắn `@Retryable` cùng chỗ với `@Transactional`
  sẽ retry trong một transaction đã chết.

### 9.4. Kiểm tra hai lần — ranh giới tin cậy

| Tầng | Bảo vệ ai | Ví dụ |
|---|---|---|
| order-service pre-trade | **Khách hàng** | fat-finger 5%, dữ liệu cũ, mã rủi ro cao |
| matching-engine `TradingRules` | **Sàn** | biên độ 7%, lô chẵn 100, phiên giao dịch |

Trùng lặp có chủ đích. Lý do không phải "cho chắc" mà là **ranh giới tin cậy**: order-service kiểm
tra để từ chối sớm cho nhanh, nhưng kết quả đó **không được tin** — ai đó có thể publish thẳng vào
topic, hoặc tham số hai bên lệch sau một lần deploy. Sàn luôn kiểm tra lại.

Và chính vì lệnh qua cửa một vẫn chết ở cửa hai, **saga mới cần chiều về**. Ba mục 9.2, 9.4 gắn
với nhau chứ không rời rạc.

### 9.5. `outbox-support` — chia sẻ hành vi, không chia sẻ cấu trúc

Hai service cùng có outbox. Cách dễ nhất là một `@Entity` dùng chung — **đã cố tình không làm**.
Shared JPA entity kéo theo schema chung, cấu hình Hibernate chung, vòng đời migration chung: đổi
một cột là hai service phải deploy cùng lúc. Đúng loại coupling mà microservices sinh ra để tránh.

Module chung chỉ chứa **port** (`OutboxRecord`, `OutboxStore`) và **hành vi** (`OutboxRelay`). Mỗi
service tự sở hữu bảng, entity, native query. Module cố tình giữ **rất nhỏ — ba file, không logic
nghiệp vụ** — vì một module chung vẫn là một điểm phối hợp.

### 9.6. Debezium CDC — phần thưởng thật sự của Outbox

`outbox.relay.enabled=false` tắt polling; Debezium `EventRouter` đọc WAL và phát ra **message giống
hệt**, consumer không sửa một dòng. Hai file connector có thật trong `cdc/`.

Đây mới là giá trị sâu của outbox pattern: nó tách *ghi sự kiện* khỏi *cách chuyển sự kiện đi*.

| | Polling relay | Debezium CDC |
|---|---|---|
| Độ trễ | ~500ms | ~mili-giây |
| Tải DB | Query định kỳ + UPDATE mark | Chỉ đọc WAL |
| Hạ tầng | Không thêm gì | Kafka Connect + connector |
| Rủi ro vận hành | Thấp, debug bằng SQL | **Replication slot** |

**Rủi ro cần nhớ riêng:** nếu connector chết mà không ai để ý, Postgres giữ lại toàn bộ WAL từ điểm
đó và **đĩa đầy dần cho tới khi DB dừng**. Bắt buộc alert trên `pg_replication_slots`.

Và: bật CDC mà quên tắt relay → cả hai cùng publish → event nhân đôi. Consumer dedupe được nên
không sai kết quả, nhưng tốn băng thông và làm nhiễu mọi metric.

### 9.7. Bảo mật Kafka — ba lớp, ba câu hỏi khác nhau

| Lớp | Trả lời câu hỏi | Thiếu nó |
|---|---|---|
| TLS | "Ai đọc được đường truyền?" | Lệnh đi dạng rõ trong mạng |
| SASL/SCRAM | "Client này là ai?" | Ai vào được mạng là dùng được |
| ACL | "Người này được làm gì?" | Một service bị chiếm = mất tất cả |

`security/create-acls.sh` áp đặc quyền tối thiểu: `market-gateway` **chỉ** có quyền Write
`stock.ticks` — gateway bị chiếm cũng không đọc được lệnh của ai.

**Cái giá nói thẳng:** TLS làm broker **mất zero-copy** (dữ liệu phải qua tầng mã hoá thay vì đi
thẳng từ page cache ra socket) → throughput topic tick giảm đáng kể. Phải đo lại trước khi cam kết
SLA. Thêm nữa: cert hết hạn = cả hệ dừng, một dạng sự cố hẹn giờ.

### 9.8. k6 — hiệu năng thành hợp đồng

Suốt V1–V3 mọi con số latency đều là *lý luận thiết kế*. `loadtest/order-load.js` đặt threshold
`p95<200ms, p99<500ms` — vi phạm là **fail build**.

Chi tiết tinh ý: `clientOrderId` **duy nhất mỗi request**. Nếu dùng lại id, Redis trả cache ngay và
bạn đang đo tốc độ Redis chứ không đo đường đặt lệnh thật — cái bẫy khiến kết quả đẹp giả tạo.

Giới hạn tự thừa nhận: chạy trên máy dev, Kafka + Postgres + 4 service + k6 tranh nhau CPU. Số này
dùng để **so sánh trước/sau một thay đổi**, không dùng cam kết SLA.

### 9.9. Mạch xuyên suốt V3 → V4

Bốn nguyên tắc lặp lại, mỗi version áp vào một bài toán mới:

1. **Biến bài toán phân tán thành bài toán cục bộ.** Outbox: thay vì ghi 2 hệ thống, ghi 2 bảng.
   Idempotency: thay vì distributed lock, dùng unique constraint.
2. **Mỗi cơ chế phòng thủ cần lớp sau đỡ.** Redis → DB constraint → `processed_event`.
   Pre-trade → `TradingRules`.
3. **Tách *cái gì* với *cách làm*.** Outbox tách "ghi event" khỏi "chuyển event" → đổi polling sang
   CDC bằng một property.
4. **Một cơ chế gánh hai việc nếu có thể.** State machine vừa chặn transition sai, vừa là
   idempotency.

---

## 10. Một manifest, ba môi trường — chỗ kustomize không lo hộ

Kustomize giải quyết gọn một bài toán: *cùng cấu trúc, khác tham số*. `base/` mô tả hệ thống, mỗi
overlay vặn núm. Nhưng có một lớp vấn đề nó **không chạm tới**: các núm đó ràng buộc lẫn nhau, và
ràng buộc nằm ngoài YAML — trong luật của Kafka, trong vòng đời của `initdb`, trong thứ tự tín hiệu
của kubelet. Mục này đi qua năm ràng buộc như vậy.

### 10.1. Số partition là tham số môi trường — và nó phá vỡ tính "giống nhau"

Trần scale ở [mục 6](#6-12-partition--keda--scale-theo-đúng-tín-hiệu) là `partition / concurrency`.
Vì partition khác nhau theo môi trường, **hình dạng consumer group cũng khác nhau**:

| Môi trường | partition | pod (min → max) | consumer thread | partition / consumer |
|---|---|---|---|---|
| dev | 3 | 1 | 3 | **1** |
| staging | 6 | 1 → 2 | 3 → 6 | 2 → 1 |
| prod | 12 | 2 → 4 | 6 → 12 | 2 → 1 |

Đọc cột cuối: **dev là môi trường duy nhất mà một consumer thread không bao giờ ôm quá một
partition.** Hệ quả cụ thể, không phải lý thuyết:

- `TickConsumer` xử lý batch tới 1000 bản ghi (`pricing/config/KafkaConfig.java:59`). Ở prod một
  batch có thể trộn tick của **hai partition**, tức hai nhóm symbol khác nhau, xen kẽ trong cùng
  một vòng lặp. Ở dev điều đó **không bao giờ xảy ra**.
- Mọi bug phụ thuộc vào việc một thread phục vụ xen kẽ hai partition — state dùng chung, biến đếm
  per-batch, giả định "một batch = một partition" — sẽ **xanh ở dev, xanh ở staging lúc scale tới
  max, và chỉ đỏ ở prod**.

Tái hiện được sớm nhất ở **staging lúc đang ở `minReplicaCount: 1`** (6 partition / 3 consumer = 2).
Đó là lý do staging phải bật KEDA chứ không phải cố định replica: chính bước co về min mới là trạng
thái giống prod nhất.

> **Không sửa bằng cách cho dev 12 partition.** Trần scale dev khi đó thành 4 pod, mà dev chạy 1
> pod — 9 trong 12 partition không có consumer, lag chỉ tăng. Số partition của dev phải khớp với số
> pod của dev; cái giá là mất vùng phủ nói trên, và cái giá đó phải được biết chứ không phải được
> giấu.

Nhắc lại từ [mục 6](#6-12-partition--keda--scale-theo-đúng-tín-hiệu): **không được "nâng cấp" dev
lên prod bằng cách tăng partition của một topic đang chạy.** `murmur2(key) % numPartitions` đổi mẫu
số là đổi toàn bộ ánh xạ symbol → partition. Đó là lý do Job `kafka-topics-init`
(`k8s/base/infra/kafka-topics-job.yaml`) chỉ **tăng** partition và luôn báo rõ ra log — nó cố ý
không im lặng về một thao tác không an toàn khi hệ đang chạy.

### 10.2. Hash của ConfigMap là cơ chế rollout — và chỗ nó thủng ở prod

`configMapGenerator` gắn hash nội dung vào tên:

```
dev   →  trading-config-6bc9d596dk
prod  →  trading-config-ggtdmhbt5m
```

Đổi một dòng trong `k8s/base/config/trading.env` → hash đổi → `envFrom.configMapRef.name` trong
Deployment đổi → pod template đổi → **rolling restart tự động**. ConfigMap tĩnh thì pod cứ chạy giá
trị cũ cho tới khi có người nhớ ra phải restart; đây là cơ chế biến "sửa config" thành "deploy".

**Chỗ thủng: prod không dùng `secretGenerator`.** Mật khẩu prod do External Secrets Operator dựng
(`k8s/overlays/prod/externalsecret.yaml`), nên Secret `trading-secrets` **không có hash**. Cộng
thêm một tính chất của Kubernetes mà tài liệu hay bỏ qua:

> Secret tiêm qua `env.valueFrom.secretKeyRef` được đọc **đúng một lần lúc container khởi động**.
> Secret thay đổi về sau **không** lan vào tiến trình đang chạy. (Chỉ Secret mount thành *volume*
> mới được kubelet cập nhật.)

Ghép hai điều đó lại: xoay mật khẩu DB trong Vault → ESO cập nhật Secret trong cluster → **không
pod nào restart, không pod nào biết**. Ứng dụng vẫn dùng mật khẩu cũ cho tới lần restart tiếp theo
vì lý do khác — và lúc đó nó mới sập, ở một thời điểm không ai liên hệ với việc xoay mật khẩu nữa.

Vì vậy quy trình xoay mật khẩu prod có **một bước bắt buộc không tự động**:

```bash
kubectl -n trading-prod rollout restart deploy \
  -l app.kubernetes.io/part-of=trading-system
```

### 10.3. Xoay mật khẩu DB ở dev/staging — cái bẫy `initdb`

Ở dev/staging Postgres chạy trong cluster, và lần này hash **có** hoạt động — nhưng kết quả còn tệ
hơn việc nó không hoạt động.

`POSTGRES_PASSWORD` (`k8s/base/infra/postgres.yaml:96`) chỉ được image dùng **một lần duy nhất, lúc
`initdb` tạo cluster dữ liệu**. Lần khởi động sau, `PGDATA` đã tồn tại → biến môi trường bị **bỏ
qua hoàn toàn**. Chuỗi sự kiện khi sửa `secrets.env`:

| Bước | Việc xảy ra |
|---|---|
| 1 | Hash của `trading-secrets` đổi |
| 2 | 3 Deployment ứng dụng **và** StatefulSet `postgres` đều đổi pod template → rolling restart cả bốn |
| 3 | Pod Postgres khởi động lại, thấy `PGDATA` cũ → **giữ nguyên mật khẩu cũ trong DB** |
| 4 | 3 service khởi động với mật khẩu **mới** → `FATAL: password authentication failed` |
| 5 | `wait-for-db` initContainer vẫn pass (`pg_isready` không cần xác thực) → pod vào CrashLoop ở tầng ứng dụng, không phải ở initContainer |

Kết quả: **sửa một dòng trong một file text làm sập toàn bộ môi trường**, và chính cái tính năng
tự-rollout ở [10.2](#102-hash-của-configmap-là-cơ-chế-rollout--và-chỗ-nó-thủng-ở-prod) là thứ biến
nó thành sự cố tức thì thay vì một lỗi âm thầm. Bước 5 là chỗ tốn thời gian nhất khi chẩn đoán: mọi
health check hạ tầng đều xanh.

Thứ tự đúng:

```bash
# 1. doi mat khau TRONG DB truoc
kubectl -n trading-dev exec sts/postgres -- \
  psql -U trading -d trading -c "ALTER ROLE trading WITH PASSWORD 'mat-khau-moi';"
# 2. roi moi sua secrets.env va apply
```

Dev có đường tắt: xoá PVC cho `initdb` chạy lại từ đầu (`kubectl -n trading-dev delete sts postgres
--cascade=orphan && kubectl -n trading-dev delete pvc data-postgres-0`). Staging thì không — dữ
liệu ở đó thường là kết quả của một đợt test dài.

**Prod không dính bẫy này**, vì component `external-postgres` gỡ hẳn StatefulSet: mật khẩu do
managed service quản, `ALTER ROLE` là việc của nhà cung cấp. Đây là một lợi ích của việc đưa DB ra
ngoài mà ít ai kể tên.

### 10.4. Ngân sách tắt pod — ba con số phải cộng lại cho khớp

`terminationGracePeriodSeconds: 60` (`k8s/base/apps/order-service.yaml:61`) **không** phải thời gian
chờ sau `SIGTERM`. Nó là **tổng ngân sách** tính từ lúc pod vào `Terminating`, và `preStop` ăn vào
chính ngân sách đó:

| t | Việc xảy ra |
|---|---|
| 0s | Pod sang `Terminating`. Hai việc chạy **song song**: controller bắt đầu gỡ pod khỏi Endpoints, và kubelet chạy `preStop` |
| 0–5s | `preStop: sleep 5` (`:158`) giữ pod phục vụ trong lúc thông tin gỡ Endpoints còn đang lan tới từng node. Không có bước này, client gặp connection reset chứ không phải 503 |
| 5s | preStop xong → kubelet gửi `SIGTERM` |
| 5–35s | Spring đóng context: Kafka listener container dừng và **commit offset cuối**. Trần là `spring.lifecycle.timeout-per-shutdown-phase: 30s` |
| 35–60s | Dự phòng cho JVM chậm, I/O chậm |
| 60s | `SIGKILL` nếu vẫn còn sống |

Cộng lại: `5 + 30 = 35 ≤ 60`. Nếu ai đó nâng `timeout-per-shutdown-phase` lên 60s mà quên nâng
`terminationGracePeriodSeconds`, Kafka listener sẽ bị `SIGKILL` **giữa lúc đang commit offset** →
batch đó replay ở pod khác → nếu lớp idempotency ([mục 7](#7-idempotency-với-clientorderid)) có lỗ
thì đó là khớp lệnh hai lần. Ba con số này là một hợp đồng, nằm ở **hai repo khác nhau** — giống
hệt cặp `maxReplicaCount` ↔ `setConcurrency` ở [mục 6](#6-12-partition--keda--scale-theo-đúng-tín-hiệu).

**⚠️ Hiện hai trong bốn service chưa dùng hết ngân sách này.** `order-service` và `matching-engine`
có `server.shutdown: graceful` + `spring.lifecycle.timeout-per-shutdown-phase: 30s`;
`market-gateway` và `pricing-risk-service` **không có dòng nào trong hai dòng đó**. Hệ quả chính
xác — cần tách bạch, vì chỗ này dễ nói quá:

- Kafka listener của cả bốn **vẫn** dừng êm. Spring Kafka đăng ký listener container như một
  `SmartLifecycle`, nên việc đóng context vẫn dừng chúng và commit offset cuối.
- Nhưng Tomcat thì **đóng ngay**: mọi request REST đang bay bị cắt giữa chừng. Với
  `pricing-risk-service` đó là `/api/v1/marketdata` — client nhận connection reset ở mỗi lần
  rollout, trong khi `preStop` vừa mới cất công giữ pod sống thêm 5 giây để tránh đúng chuyện đó.

Nói cách khác: `preStop: sleep 5` ở hai service này đang bảo vệ một thứ mà ứng dụng không bảo vệ nốt.

### 10.5. `replicas` trong Git đấu với autoscaler

[Mục 6](#6-12-partition--keda--scale-theo-đúng-tín-hiệu) đã nêu vấn đề: để `replicas` cố định trong
Deployment khi đã có autoscaler thì mỗi lần apply sẽ giằng co. Với GitOps (apply liên tục, không
phải thỉnh thoảng) nó thành giằng co thường trực:

```
KEDA đang giữ 4 pod (lag cao)
  → kubectl apply -k   đặt spec.replicas = 2
  → 2 pod bị xoá, consumer group rebalance
  → HPA do KEDA tạo đọc metric, kéo lại 4 trong ~15s
  → 2 pod mới khởi động (JVM ~40s), rebalance lần nữa
```

Hai lần rebalance và ~1 phút giảm năng lực xử lý, **đúng lúc lag đang cao** — tức đúng lúc không
được phép. (Thiệt hại có nhẹ đi nhờ `CooperativeStickyAssignor`, đã bật ở cả ba service —
`pricing/config/KafkaConfig.java:53`: chỉ partition thật sự đổi chủ mới bị thu hồi, không
stop-the-world. Nhưng nhẹ đi không phải là không có.)

Cách đã chọn: **đặt `replicas` đúng bằng `minReplicaCount`** —
`k8s/overlays/prod/kustomization.yaml:34-35` là `2`, và `k8s/components/keda/*.yaml:21` cũng là `2`.
Khi hệ đang ở min (phần lớn thời gian), apply là no-op hoàn toàn. Khi đang scale lên thì vẫn giật
một nhịp như trên — tức đây là **giảm xác suất, không phải loại bỏ**.

Muốn loại bỏ hẳn thì phải bỏ `replicas` khỏi overlay và bảo công cụ GitOps đừng so sánh trường đó:

```yaml
# Argo CD Application
spec:
  ignoreDifferences:
    - group: apps
      kind: Deployment
      jsonPointers: [/spec/replicas]
```

Chưa làm vì repo hiện chưa chốt công cụ GitOps; `replicas == minReplicaCount` là phương án đúng cho
cả hai trường hợp (có hay không có GitOps), nên nó là mặc định an toàn.

### 10.6. `Recreate` cho market-gateway — cái giá ghi bằng số

`market-gateway` dùng `strategy: Recreate` (`k8s/base/apps/market-gateway.yaml`), khác ba service
còn lại. Đây là lựa chọn có chủ ý và nó **có giá**:

```
downtime = tắt pod cũ (tới 60s)  +  khởi động pod mới cho tới khi Ready
         ≈ 40–70s thực tế với JVM này
         (trần lý thuyết 60 + 36×5 = 240s, theo startupProbe failureThreshold)
```

Trong khoảng đó **không một tick nào vào Kafka**. Với một gateway feed thị trường, mỗi lần deploy là
một lỗ hổng dữ liệu thấy được trên biểu đồ.

Vì sao vẫn chọn: `RollingUpdate` nghĩa là có lúc **hai pod cùng mở một session feed**. Hậu quả không
phải "tốn tài nguyên" mà là **tick trùng** — và tick trùng đi thẳng vào EMA/volatility ở
[mục 2](#2-vì-sao-thứ-tự-theo-mã-là-bắt-buộc-ema--volatility), nơi mỗi bản ghi làm dịch chuyển một
giá trị tích luỹ. Dữ liệu sai âm thầm tệ hơn dữ liệu thiếu ồn ào: thiếu thì thấy ngay trên biểu đồ,
sai thì không.

Lối ra thật không nằm trong YAML: **leader election ở tầng ứng dụng** (lease qua Kubernetes API,
hoặc một khoá trong Redis/DB). Khi đó mới chạy được nhiều replica — một pod giữ feed, các pod khác
chờ sẵn — và downtime tụt xuống còn thời gian chuyển lease. Chừng nào chưa có, `replicas: 1` +
`Recreate` là cấu hình **đúng**, và cảnh báo ở đầu file manifest là để không ai "tối ưu" nó đi mà
không biết mình đang đánh đổi gì.

---

## 11. Tổng hợp các điểm nên xem lại

Bảng dưới gom toàn bộ phát hiện trong tài liệu này. Cột "Loại": **Bug** = sai hành vi;
**Gap** = cơ chế không phủ được tình huống nó định phủ; **Doc** = tài liệu/comment lệch code;
**Ops** = vận hành lâu dài.

| # | Loại | Vị trí | Vấn đề | Hướng sửa |
|---|---|---|---|---|
| 1 | Gap | `pricing/config/KafkaConfig.java:44` | Không có `ErrorHandlingDeserializer` → lỗi deserialize xảy ra trong `poll()`, DLT không bắt được | Bọc `KafkaAvroDeserializer` bằng `ErrorHandlingDeserializer`; viết IT bơm byte rác để xác nhận |
| 2 | Gap | `pricing/consumer/TickConsumer.java:52` | Batch listener không ném `BatchListenerFailedException` → 1 tick lỗi đẩy cả lô 1000 tick vào DLT | Bọc vòng lặp, ném `BatchListenerFailedException(msg, e, i)` |
| 3 | Ops | `pricing/service/SnapshotStore.java:48-68` | `latest` không bao giờ xoá → flush ghi lại mọi symbol kể cả khi không đổi (~17.280 row/symbol/ngày) | Flush theo diff với `lastFlushed`; thêm retention/partition cho bảng |
| 4 | Bug | `order/service/OrderService.java:97` | `orElseThrow()` biến mọi `DataIntegrityViolationException` khác thành `NoSuchElementException`, che lỗi gốc | Kiểm tra tên constraint, hoặc `orElseThrow(() -> dup)` |
| 5 | Ops | `order/service/OrderService.java:112` | Staleness 10s là `@Value` toàn cục → mã thanh khoản thấp bị từ chối oan | Ngưỡng theo nhóm mã |
| 6 | Gap | `order/service/OrderService.java:112` | Đồng hồ pricing chạy nhanh → hiệu số âm → check bị vô hiệu hoá âm thầm | Thêm phòng vệ cho nhánh âm bất thường |
| 7 | Bug | `k8s/legacy/08-keda-scaledobject.yaml:29` | Comment hiểu `lagThreshold` là per-partition; thực tế là per-replica (tổng lag / threshold) | ✅ Đã sửa — `k8s/components/keda/scaledobject-pricing-risk.yaml:38-41` |
| 8 | Doc | `k8s/legacy/08-keda-scaledobject.yaml:23,47` | `cooldownPeriod` chỉ áp cho scale về 0 — không chống flapping như comment nói | ✅ Đã sửa — `cooldownPeriod` bỏ hẳn, thay bằng `behavior.scaleDown.stabilizationWindowSeconds` (`components/keda/*.yaml:26-29`) |
| 9 | Doc | `k8s/legacy/08-keda-scaledobject.yaml:30` | `activationLagThreshold` vô tác dụng vì `minReplicaCount: 2` | ✅ Đã ghi rõ là chỉ dùng cho ngưỡng 0 → 1, giữ lại cho trường hợp sau này bật scale-to-zero (`:42-45`) |
| 10 | Doc | `k8s/legacy/04-pricing-risk.yaml:7,62-64` | Comment còn nói "6 partition", đã là 12 | ✅ Partition thành tham số môi trường (3/6/12); `replicas` đặt bằng `minReplicaCount` — xem #21 cho phần còn lại |
| 11 | Doc | `DESIGN-NOTES.md:158-165` (V2.7) | Mô tả bản v2 (map per-pod); code đã là v3 (Redis + UNIQUE constraint) | Viết lại mục V2.7 theo 3 lớp hiện tại |
| 12 | **Gap** | `matching-engine` `processed_event` | Bảng phình vô hạn — nhưng dọn **sớm hơn retention của `stock.orders`** sẽ mở cửa cho khớp lệnh hai lần khi replay (xem #15) | Job dọn theo `processed_at`, giữ **lâu hơn** retention topic; khai `retention.ms` tường minh ở `ordersTopic()` kèm comment chéo |
| 13 | Ops | `stock.ticks.DLT` | Không có alert → DLT không ai đọc thì tương đương skip | Alert khi message count `> 0`; viết runbook replay |
| 14 | Ops | KEDA ↔ `setConcurrency(3)` | `maxReplicaCount: 4` phải khớp `12 / concurrency` nhưng ở hai repo khác nhau, không gì cưỡng chế | ✅ CI chặn merge nếu `maxReplicaCount > KAFKA_PARTITIONS / 3` (`.github/workflows/k8s-validate.yml`) |
| 15 | **Gap** | `matching/entity/ExecutionEntity.java:24` | `client_order_id` **không** có `unique` → `processed_event` là lớp chặn khớp-trùng **duy nhất**, không có lưới an toàn phía sau (khác hẳn `OrderEntity.java:28` ở order-service) | Thêm `unique = true` + migration. Khi đó job dọn ở #12 chỉ còn là chuyện dung lượng, không còn là chuyện đúng/sai |
| 16 | Gap | Mọi `TopicBuilder` (6 chỗ) + `ACKS_CONFIG="all"` (3 chỗ) | Tất cả topic khai `.replicas(1)` và cluster chỉ 1 broker → `acks=all` ≡ `acks=1`, không có bản sao nào để chờ. Độ bền của outbox producer (`matching/KafkaConfig.java:63`, `order/KafkaConfig.java:63`) và DLT producer (`pricing/KafkaConfig.java:95`) hiện chỉ là danh nghĩa | Multi-broker + `min.insync.replicas=2`. `DESIGN-NOTES.md:401` có thừa nhận nhưng ở dòng cuối file, dễ bỏ sót khi đọc mục 9/22 |
| 17 | Doc | `DESIGN-NOTES.md:209` (bảng crash mục 9) | Dòng "Sau publish, trước mark published → Event bị gửi LẠI" đúng nhưng nói nhẹ: `OutboxRelay` rollback **cả batch**, nên là **mọi event đã gửi trong batch** (tới 100) bị gửi lại, không phải một | Ghi rõ quy mô; hành vi là cố ý (giữ thứ tự) nên chỉ cần sửa mô tả |
| 18 | **Bug** | `k8s/base/infra/postgres.yaml:96` (dev/staging) | `POSTGRES_PASSWORD` chỉ có tác dụng lúc `initdb`. Sửa `secrets.env` → hash đổi → restart cả DB lẫn 3 service → app dùng mật khẩu mới, DB giữ mật khẩu cũ → **sập cả môi trường**. `wait-for-db` vẫn pass nên triệu chứng chỉ lộ ở tầng ứng dụng ([mục 10.3](#103-xoay-mật-khẩu-db-ở-devstaging--cái-bẫy-initdb)) | Đã thêm cảnh báo tại chỗ trong manifest. Quy trình: `ALTER ROLE` trong DB **trước**, rồi mới sửa `secrets.env`. Triệt để: tách Secret của Postgres khỏi Secret của ứng dụng, hoặc dùng operator (CloudNativePG) biết xoay mật khẩu |
| 19 | Gap | `k8s/overlays/prod/externalsecret.yaml` | Secret do ESO dựng **không có hash** → ESO cập nhật xong không pod nào restart; mà `env.valueFrom.secretKeyRef` chỉ đọc **một lần lúc container start** → ứng dụng vẫn dùng mật khẩu cũ tới lần restart ngẫu nhiên sau đó ([mục 10.2](#102-hash-của-configmap-là-cơ-chế-rollout--và-chỗ-nó-thủng-ở-prod)) | Trước mắt: `rollout restart` thủ công, đã ghi vào runbook `k8s/README.md`. Triệt để: Reloader (stakater) watch Secret, hoặc mount Secret thành volume thay vì env |
| 20 | Gap | `market-gateway`, `pricing-risk-service` `application.yml` | Thiếu `server.shutdown: graceful` + `spring.lifecycle.timeout-per-shutdown-phase` (hai service kia có). Kafka listener vẫn dừng êm qua `SmartLifecycle`, nhưng **Tomcat đóng ngay** → request REST đang bay bị cắt, đúng thứ mà `preStop: sleep 5` vừa cất công bảo vệ ([mục 10.4](#104-ngân-sách-tắt-pod--ba-con-số-phải-cộng-lại-cho-khớp)) | Thêm hai dòng đó vào `application.yml` của cả hai; giữ tổng `preStop + timeout ≤ terminationGracePeriodSeconds` |
| 21 | Ops | `k8s/overlays/*/kustomization.yaml` ↔ `components/keda` | `replicas == minReplicaCount` chỉ **giảm xác suất** giằng co với autoscaler, không loại bỏ: apply đúng lúc KEDA đang giữ 4 pod vẫn gây 2 lần rebalance ([mục 10.5](#105-replicas-trong-git-đấu-với-autoscaler)) | Khi chốt công cụ GitOps: bỏ `replicas` khỏi overlay + `ignoreDifferences` trên `/spec/replicas` |
| 22 | Gap | Vùng phủ của dev | dev có 3 partition / 1 pod / 3 consumer → **một consumer không bao giờ ôm quá 1 partition**. Mọi bug phụ thuộc việc một thread xử lý xen kẽ hai partition (batch 1000 bản ghi của `TickConsumer` trộn hai nhóm symbol) không lộ ra ở dev ([mục 10.1](#101-số-partition-là-tham-số-môi-trường--và-nó-phá-vỡ-tính-giống-nhau)) | Không sửa bằng cách tăng partition dev (sẽ thừa partition không consumer). Dựa vào staging lúc ở `minReplicaCount: 1` (6/3 = 2 partition mỗi consumer); thêm integration test Testcontainers với 2 partition trên 1 consumer |

### Nguyên tắc xuyên suốt rút ra từ các mục trên

> Bốn nguyên tắc riêng của tiến trình V3 → V4 nằm ở [mục 9.9](#99-mạch-xuyên-suốt-v3--v4).
> Sáu nguyên tắc dưới đây áp cho toàn hệ.

1. **Thứ tự là một ràng buộc đúng đắn, không phải tối ưu hoá.** Key = symbol không phải để nhanh,
   mà để EMA/volatility ra đúng số ([mục 2](#2-vì-sao-thứ-tự-theo-mã-là-bắt-buộc-ema--volatility)).

2. **Fail-safe thay vì fail-open với tiền.** Không chắc chắn → từ chối. Staleness check
   ([mục 4](#4-staleness-check--max-data-age-ms)) và `systemUnavailable()` cùng một triết lý.

3. **Lỗi im lặng nguy hiểm hơn lỗi ồn ào.** Cache không TTL, DLQ không alert, `catch` nuốt
   exception — cả ba đều biến sự cố thành thứ không ai nhìn thấy. Mỗi cơ chế "degrade gracefully"
   phải đi kèm một cơ chế phát hiện.

4. **DB constraint là chốt chặn cuối, cache chỉ là tăng tốc.** Mọi lớp phòng thủ phía trên có thể
   chết mà hệ vẫn đúng ([mục 7](#7-idempotency-với-clientorderid)).

5. **Đo đúng thứ cần phản ứng.** CPU không phải lag; lag mới là lag
   ([mục 6](#6-12-partition--keda--scale-theo-đúng-tín-hiệu)).

6. **Ràng buộc giữa hai con số ở hai file khác nhau thì sớm muộn cũng lệch.** `maxReplicaCount` ↔
   `setConcurrency(3)`, `timeout-per-shutdown-phase` ↔ `terminationGracePeriodSeconds`, retention
   của `stock.orders` ↔ lịch dọn `processed_event` — cả ba đều đúng ở thời điểm viết và không có gì
   giữ cho chúng đúng về sau. Cặp thứ nhất giờ đã có CI canh
   ([mục 6](#6-12-partition--keda--scale-theo-đúng-tín-hiệu)); hai cặp còn lại vẫn đang dựa vào trí
   nhớ, tức là đã hỏng rồi mà chưa ai biết.
