# Chương 02 — Kafka Messaging & Schema Registry

> **Câu hỏi trung tâm:** Kafka ở đây không phải "hàng đợi" — nó là **xương sống về thứ tự và độ bền** của toàn hệ. Chương này giải thích từng tham số cấu hình và vì sao đổi nó sẽ phá vỡ tính đúng đắn.
>
> **Tài liệu đối chiếu:** `DESIGN-NOTES.md` mục 2-4, `DEEP-DIVE.md` mục 1-2 & 5.

---

## 1. Nó là gì?

### 1.1. Kafka ở chế độ KRaft

```yaml
# docker-compose.yml
kafka:
  image: apache/kafka:3.7.0
  ports: ["9092:9092"]
  environment:
    KAFKA_NODE_ID: 1
    KAFKA_PROCESS_ROLES: broker,controller       # một process đóng cả hai vai
    KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
    KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:19092,CONTROLLER://0.0.0.0:9093,EXTERNAL://0.0.0.0:9092
    KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:19092,EXTERNAL://localhost:9092
    KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT
    KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
    KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
    KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
```

**KRaft (Kafka Raft) thay Zookeeper** từ Kafka 3.3+ (production-ready), bắt buộc từ 4.0. Metadata cluster (danh sách topic, partition, ISR, cấu hình) được lưu trong chính một topic nội bộ `__cluster_metadata`, quản lý bằng thuật toán Raft, thay vì nằm ở một hệ thống riêng.

| | Zookeeper | KRaft |
|---|---|---|
| Số hệ thống phải vận hành | 2 (Kafka + ZK ensemble) | **1** |
| Thời gian failover controller | ~vài chục giây | ~dưới một giây |
| Giới hạn số partition | ~200.000 | hàng triệu |
| Nguồn sự thật metadata | ZK znode | Topic nội bộ của chính Kafka |

🔑 **Hai listener là chi tiết dễ sai nhất ở local.** `PLAINTEXT://kafka:19092` dùng cho container-tới-container (DNS của Docker network phân giải tên `kafka`); `EXTERNAL://localhost:9092` dùng cho process trên máy host (IDE debug). Nếu chỉ khai một listener `localhost:9092`, container sẽ nhận `advertised.listeners = localhost` và tự kết nối vào **chính nó** thay vì vào broker ⇒ lỗi `Connection refused` khó hiểu.

💡 Vì vậy biến `KAFKA_BOOTSTRAP` có hai giá trị tuỳ ngữ cảnh:
- Trong Docker Compose: `kafka:19092` (xem `environment` của từng service).
- Khi chạy từ IDE: `localhost:9092` (mặc định trong `application.yml`).

### 1.2. Tám topic

```yaml
# k8s/base/config/trading.env
KAFKA_PARTITIONS=6               # dev 3 / staging 6 / prod 12
KAFKA_TOPIC_REPLICAS=1
TOPIC_RETENTION_MS=604800000     # 7 ngày
DLT_RETENTION_MS=2592000000      # 30 ngày
```

| Topic | Vai trò | Serialization | Producer acks | Retention |
|---|---|---|:--:|:--:|
| `stock.ticks` | Giá thô từ sàn | **Avro** + Schema Registry | `1` | 7 ngày |
| `stock.enriched` | Giá + chỉ số rủi ro | JSON | `1` | 7 ngày |
| `stock.orders` | Lệnh đã nhận (qua outbox) | JSON | **`all`** | 7 ngày |
| `stock.executions` | Kết quả khớp (qua outbox) | JSON | **`all`** | 7 ngày |
| `stock.*.DLT` (×4) | Message xử lý thất bại | theo topic gốc | `all` | **30 ngày** |

Topic được tạo ở hai nơi:
1. **`@Bean NewTopic`** trong code — dùng cho local/test, Spring Kafka tự gọi AdminClient lúc khởi động.
2. **Job `kafka-topics-init`** trong K8s — nguồn sự thật cho cluster.

```yaml
# k8s/base/infra/kafka-topics-job.yaml
# Tạo topic với SỐ PARTITION TƯỜNG MINH thay vì để auto-create.
#
# Vì sao quan trọng: trần scale của consumer = partition / concurrency.
# Cả 3 service consumer đều đặt concurrency = 3 (KafkaConfig.java), nên:
#     dev      3 partition  -> 1 pod là đủ
#     staging  6 partition  -> trần 2 pod
#     prod    12 partition  -> trần 4 pod  (khớp maxReplicaCount của KEDA)
# Auto-create đẻ topic 1 partition, mọi con số trên sập về 1 mà không ai hay.
```

🔑 **Vì sao `auto.create.topics.enable` nguy hiểm trong production.** Khi một consumer subscribe một topic chưa tồn tại, broker tự tạo nó với `num.partitions` mặc định = **1**. Hệ thống vẫn chạy, không có lỗi nào, nhưng trần scale sập từ 4 pod xuống 1 pod. Không alert nào bắt được — chỉ phát hiện khi lag tăng mà thêm pod không giúp gì. Local bật `true` cho tiện; cluster **phải** tạo tường minh.

Và job này idempotent có chủ đích:

```sh
create_topic() {
  T=$1; RET=$2
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BS" \
    --create --if-not-exists --topic "$T" \
    --partitions "$P" --replication-factor "$RF" \
    --config retention.ms="$RET"

  # Topic da ton tai thi --create khong dong gi: ap lai config va partition cho khop khai bao.
  /opt/kafka/bin/kafka-configs.sh --bootstrap-server "$BS" \
    --entity-type topics --entity-name "$T" \
    --alter --add-config retention.ms="$RET" >/dev/null

  CUR=$(/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BS" \
          --describe --topic "$T" | grep -c "Partition:" || true)
  if [ "$CUR" -lt "$P" ]; then
    echo "  tang partition $T: $CUR -> $P"
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BS" --alter --topic "$T" --partitions "$P"
  fi
}
```

Chi tiết tinh tế: `--create --if-not-exists` **không** sửa topic đã tồn tại, kể cả khi config khai báo đã đổi. Vì vậy hai lệnh tiếp theo (`kafka-configs --alter` và kiểm tra/tăng partition) tồn tại để **hội tụ** topic đang có về đúng khai báo. Và chỉ **tăng** partition — Kafka không cho giảm.

🔑 **DLT giữ cùng số partition với topic gốc** — ghi rõ trong job:
```sh
# DLT giu CUNG so partition voi topic goc: DeadLetterPublishingRecoverer
# gui vao dung partition cua ban ghi loi, DLT it partition hon se publish that bai.
```
`DeadLetterPublishingRecoverer` mặc định gửi bản ghi lỗi vào **đúng chỉ số partition** của bản ghi gốc. Nếu `stock.ticks` có 12 partition còn `stock.ticks.DLT` chỉ có 1, thì một bản ghi lỗi ở partition 7 sẽ cố gửi vào partition 7 của DLT — không tồn tại ⇒ publish thất bại ⇒ **mất message lỗi**, đúng thứ mà DLT sinh ra để chống.

⚠️ Lưu ý: `@Bean ticksDltTopic()` trong `pricing/config/KafkaConfig.java:84-88` lại khai `.partitions(1)`. Ở local nơi topic chính cũng do code tạo với 12 partition, đây **là** vấn đề tiềm tàng. Trên cluster, Job mới là nguồn sự thật và nó tạo DLT đúng số partition, nên bean này chỉ là dự phòng.

### 1.3. Schema Registry + Avro

```json
// common-contracts/src/main/avro/TickEventAvro.avsc
{
  "type": "record",
  "name": "TickEventAvro",
  "namespace": "com.trading.common.avro",
  "doc": "Wire format cho topic stock.ticks. Schema evolution được Schema Registry cưỡng chế: thêm field mới phải có default, không được xoá/đổi kiểu field cũ (BACKWARD compatibility).",
  "fields": [
    { "name": "symbol",    "type": "string" },
    { "name": "price",     "type": { "type": "bytes", "logicalType": "decimal", "precision": 18, "scale": 4 } },
    { "name": "volume",    "type": "long" },
    { "name": "eventTime", "type": "long" }
  ]
}
```

Cấu trúc một message Avro trên dây:

```
┌──────────┬──────────────────┬─────────────────────────────┐
│ 0x00     │ schema ID (4B BE)│ payload Avro binary          │
│ magic    │ ví dụ 0x00000005 │ không chứa tên field         │
└──────────┴──────────────────┴─────────────────────────────┘
```

Consumer đọc 4 byte ID → tra Schema Registry (cache local, chỉ gọi mạng lần đầu cho mỗi ID) → lấy **writer schema** → deserialize theo **reader schema** của chính nó. Avro xử lý chênh lệch giữa hai schema theo quy tắc resolution.

---

## 2. Vai trò trong hệ thống

| Vai trò | Vì sao chỉ Kafka làm được |
|---|---|
| **Tách thời gian (temporal decoupling)** | Producer không cần consumer đang sống. matching-engine chết 10 phút, lệnh nằm chờ trong topic, xử lý khi nó trở lại. |
| **Đảm bảo thứ tự theo khoá** | 🔑 Nền tảng cho tính đúng đắn của EMA/volatility. Xem mục 3.2. |
| **Replay** | Retention 7 ngày ⇒ dựng lại snapshot, điều tra sự cố, hâm nóng consumer group mới. |
| **Fan-out** | `stock.enriched` hiện có 1 consumer group; thêm UI/analytics chỉ cần thêm group mới, không ai phải sửa gì. |
| **Đệm (back-pressure buffer)** | Tick dồn 5× đột biến ⇒ lag tăng ⇒ KEDA scale ⇒ hệ tiêu hoá dần, thay vì sập. |

---

## 3. Cơ chế hoạt động

### 3.1. Partition — trần scale cứng

**Luật của Kafka:** trong một consumer group, **một partition chỉ được gán cho đúng một consumer tại một thời điểm**. Consumer thừa ngồi không.

```java
// pricing-risk-service/config/KafkaConfig.java:61-66
@Bean
public ConcurrentKafkaListenerContainerFactory<String, TickEventAvro> tickListenerFactory(...) {
    var factory = new ConcurrentKafkaListenerContainerFactory<String, TickEventAvro>();
    factory.setConsumerFactory(tickConsumerFactory());
    factory.setBatchListener(true);
    // 12 partition / concurrency 3 = tối đa 4 pod consumer hiệu quả (khớp KEDA max)
    factory.setConcurrency(3);
    ...
}
```

`setConcurrency(3)` nghĩa là **mỗi pod tạo 3 consumer thread**, mỗi thread là một thành viên độc lập của consumer group.

$$\text{Trần pod hiệu quả} = \frac{\text{số partition}}{\text{concurrency}} = \frac{12}{3} = 4$$

Pod thứ 5 join group, nhận về **0 partition**, chiếm RAM và không làm gì. Tệ hơn: việc nó join gây **rebalance** — toàn bộ group tạm dừng xử lý.

| Môi trường | Partition | Trần pod | `maxReplicaCount` KEDA |
|---|:--:|:--:|:--:|
| dev | 3 | 1 | — (KEDA tắt) |
| staging | 6 | 2 | 2 |
| prod | 12 | 4 | 4 |

🔑 **Ba con số này nằm ở ba file khác nhau** (`trading.env`, `KafkaConfig.java`, `scaledobject-*.yaml`) và không có gì trong runtime giữ chúng khớp nhau. Repo bịt lỗ hổng này bằng CI:

```yaml
# .github/workflows/k8s-validate.yml:59-78
- name: KEDA maxReplicaCount khong vuot partition/3
  run: |
    P=$(grep -E '^\s+KAFKA_PARTITIONS:' /tmp/${{ matrix.overlay }}.yaml \
         | head -1 | tr -d ' "' | cut -d: -f2)
    if [ -z "$P" ]; then echo "::error::khong tim thay KAFKA_PARTITIONS"; exit 1; fi
    CEILING=$((P / 3))
    echo "overlay=${{ matrix.overlay }} partitions=$P -> tran pod consumer=$CEILING"
    FAIL=0
    for M in $(grep -E '^\s+maxReplicaCount:' /tmp/${{ matrix.overlay }}.yaml \
                | tr -d ' ' | cut -d: -f2); do
      if [ "$M" -gt "$CEILING" ]; then
        echo "::error::maxReplicaCount=$M vuot tran $CEILING (= $P partition / concurrency 3)"
        FAIL=1
      fi
    done
    exit $FAIL
```

💡 Đây là một mẫu đáng học: **khi một ràng buộc đúng đắn trải qua nhiều file, hãy cưỡng chế nó ở CI.** Comment trong mã chỉ nhắc được người đang đọc đúng file đó.

⚠️ Mặt trái: `concurrency = 3` bị **hardcode** trong CI (`P / 3`). Nếu ai đó đổi `setConcurrency(4)` trong Java, CI vẫn chia cho 3 và vẫn pass. Ràng buộc được bịt một nửa.

### 3.2. 🔑 `key = symbol` — ràng buộc đúng đắn, không phải tối ưu hoá

Khoá được đặt ở **ba nơi**, và ba nơi đó phải nhất quán:

```java
// 1) market-gateway/kafka/TickProducer.java:36 — trực tiếp
kafkaTemplate.send("stock.ticks", tick.symbol(), avro)
```
```java
// 2) pricing-risk-service/consumer/TickConsumer.java:63 — trực tiếp
enrichedTemplate.send("stock.enriched", enriched.symbol(), enriched);
```
```java
// 3) order-service/service/OrderPersistence.java:62 — gián tiếp, qua cột msg_key
event.setMsgKey(result.symbol());   // key = symbol giữ thứ tự theo mã
```
```java
// 4) matching-engine/service/MatchingService.java:101 — gián tiếp
event.setMsgKey(report.symbol());   // cùng key với stock.orders -> giữ thứ tự theo mã
```

Rồi relay đọc cột đó ra làm khoá Kafka:

```java
// outbox-support/OutboxRelay.java:58
kafkaTemplate.send(event.getTopic(), event.getMsgKey(), event.getPayload())
```

💡 Điểm đáng chú ý về mặt thiết kế: với outbox, khoá Kafka **được quyết định lúc ghi DB**, không phải lúc publish. Nghĩa là nó nằm trong cùng transaction với dữ liệu nghiệp vụ, và relay trở thành một bộ chuyển tiếp hoàn toàn vô tri — không cần biết gì về nghiệp vụ để chọn khoá.

**Vì sao thứ tự bắt buộc:**

```java
// pricing-risk-service/service/RiskCalculator.java:49-62
// 1) EMA giá
ema = Double.isNaN(ema) ? px : EMA_ALPHA * px + (1 - EMA_ALPHA) * ema;

// 2) Log-return + sliding window variance (O(1))
if (!Double.isNaN(lastPrice) && lastPrice > 0) {
    double r = Math.log(px / lastPrice);
    ...
}
lastPrice = px;
```

Hai công thức đọc trạng thái của tick **liền trước**. Đảo thứ tự:

| Thứ tự đúng | Thứ tự đảo |
|---|---|
| `100 → 101 → 102` ⇒ `r₁=+0,00995`, `r₂=+0,00985` | `100 → 102 → 101` ⇒ `r₁=+0,0198`, `r₂=−0,00985` |
| Variance nhỏ ⇒ volatility thấp ⇒ `riskLevel = LOW` | Variance lớn hơn đáng kể ⇒ có thể vọt lên `MEDIUM`/`HIGH` |

Và `riskLevel` là thứ `order-service` dùng để **từ chối lệnh của khách** (`OrderService.java:115-117`). Sai thứ tự ⇒ từ chối oan, hoặc tệ hơn, cho qua lệnh đáng ra phải chặn.

**Kafka chỉ đảm bảo thứ tự trong một partition.** `key = symbol` + `DefaultPartitioner` (murmur2 hash của khoá, modulo số partition) ⇒ mọi tick `HPG` vào cùng partition ⇒ đọc theo thứ tự ⇒ xử lý bởi đúng một thread.

**Lợi ích kèm theo — lock luôn uncontended:**

```java
// RiskCalculator.java:35-40
public EnrichedMarketData onTick(TickEvent tick) {
    SymbolState st = states.computeIfAbsent(tick.symbol(), s -> new SymbolState());
    synchronized (st) {
        return st.update(tick);
    }
}
```

Vì mỗi symbol chỉ có một thread xử lý, khối `synchronized` gần như không bao giờ bị tranh chấp — JVM tối ưu thành biased/thin lock, chi phí vài nanosecond. Nó tồn tại như mạng an toàn cho khoảnh khắc rebalance (partition đổi chủ, thread cũ và mới có thể chồng lấn cực ngắn), không phải cho luồng chạy bình thường.

⚠️ **Cạm bẫy: thêm partition phá vỡ thứ tự lịch sử.** `murmur2(key) % numPartitions` — đổi `numPartitions` thì cùng một `symbol` băm sang partition khác. Tick cũ của `HPG` ở partition 3, tick mới ở partition 7, và hai partition này được hai thread đọc song song ⇒ một khoảng thời gian ngắn thứ tự `HPG` không còn đảm bảo. Vì vậy Job `kafka-topics-init` chỉ tăng partition và phải làm vào lúc **không có tải**, hoặc chấp nhận một khoảng nhiễu EMA.

⚠️ **Cạm bẫy: phân bố không đều (hot partition).** Nếu `VN30F2607` chiếm 60% lưu lượng, partition chứa nó sẽ quá tải trong khi các partition khác nhàn rỗi. Thêm pod không giúp — consumer của partition đó vẫn là một. Giải pháp duy nhất mà vẫn giữ thứ tự: khoá mịn hơn (ví dụ `symbol` + một bucket theo `orderId`) và chấp nhận chỉ giữ thứ tự trong bucket — chỉ làm được nếu nghiệp vụ cho phép.

### 3.3. Hai profile producer

**Profile TỐC ĐỘ** — dữ liệu giá:

```java
// market-gateway/config/KafkaProducerConfig.java:52-58
// === TUNING TỐC ĐỘ ===
props.put(ProducerConfig.ACKS_CONFIG, "1");
props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
props.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536);
props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67108864);
```

**Profile ĐỘ BỀN** — luồng lệnh:

```java
// order-service/config/KafkaConfig.java:65-70  (matching-engine giống hệt)
props.put(ProducerConfig.ACKS_CONFIG, "all");
props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
```

Giải thích từng tham số:

| Tham số | Giá trị | Ý nghĩa thật |
|---|---|---|
| `acks=0` | (không dùng) | Gửi rồi quên. Mất dữ liệu kể cả khi broker khoẻ. |
| `acks=1` | tick, enriched | Leader ghi xong là trả ack. Leader chết trước khi replica kịp sao chép ⇒ mất. Chấp nhận cho giá: bản kế tiếp sửa lại. |
| `acks=all` | lệnh, execution, DLT | Chờ **toàn bộ ISR** (in-sync replicas) xác nhận. Chậm hơn nhưng không mất. |
| `linger.ms=5` | tick | Producer **chờ 5ms** để gom thêm bản ghi vào cùng batch. Đánh đổi 5ms latency lấy throughput gấp nhiều lần. |
| `batch.size=65536` | tick | Batch tối đa 64KB. Gửi khi đầy batch **hoặc** hết `linger.ms`, tuỳ cái nào đến trước. |
| `compression=lz4` | tick | Tick rất lặp (cùng symbol, giá gần nhau) ⇒ nén tốt. lz4 nhanh hơn gzip, tỉ lệ nén kém hơn một chút — đánh đổi đúng cho hot path. |
| `buffer.memory=64MB` | tick | Bộ đệm phía client. Đầy ⇒ `send()` **chặn** (back-pressure tự nhiên thay vì mất dữ liệu). |
| `enable.idempotence=true` | lệnh | 🔑 Mục dưới |
| `retries=MAX_VALUE` | lệnh | Thử lại vô hạn. An toàn **chỉ vì** có idempotence; không có nó thì retry sinh bản sao. |

🔑 **`enable.idempotence` giải quyết chính xác vấn đề gì.** Kịch bản: producer gửi, broker ghi thành công, nhưng ack bị mất trên đường về. Producer tưởng thất bại và gửi lại ⇒ **hai bản ghi giống hệt trong partition**. Với idempotence bật, Kafka gán cho mỗi producer một `PID` và một sequence number tăng dần cho mỗi partition; broker nhớ sequence cuối cùng đã nhận và **loại bỏ bản trùng**.

Phạm vi: chỉ trong *một phiên producer*, *một partition*. Nó không chống được trùng do relay gửi lại sau khi pod restart — đó là việc của idempotent consumer.

Khi bật `enable.idempotence=true`, Kafka tự ép: `acks=all`, `retries>0`, `max.in.flight.requests.per.connection ≤ 5`. Nếu bạn khai mâu thuẫn, producer ném `ConfigException` lúc khởi tạo.

### 3.4. Tuning consumer

```java
// pricing-risk-service/config/KafkaConfig.java:53-57
// === TUNING TỐC ĐỘ (throughput vs latency) ===
props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 65536);
props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 50);
props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1000);
props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
```

| Tham số | Giá trị | Ý nghĩa |
|---|---|---|
| `fetch.min.bytes=64KB` | | Broker **giữ lại** response cho tới khi gom đủ 64KB dữ liệu. Ít round-trip hơn nhiều so với mặc định 1 byte. |
| `fetch.max.wait.ms=50` | | Trần cho việc chờ trên: dù chưa đủ 64KB, sau 50ms vẫn trả về. **Đây là trần latency** của cặp tham số này. |
| `max.poll.records=1000` | | Một lần `poll()` trả tối đa 1000 bản ghi. Với `setBatchListener(true)`, listener được gọi **một lần** với cả `List`. |

Cặp `fetch.min.bytes` + `fetch.max.wait.ms` là núm vặn throughput↔latency rõ ràng nhất trong hệ:
- Tăng `min.bytes` ⇒ throughput cao hơn, latency xấu hơn.
- Giảm `max.wait.ms` ⇒ latency tốt hơn, nhiều round-trip hơn.

⚠️ **Mối nguy của `max.poll.records` lớn:** nếu xử lý 1000 bản ghi mất lâu hơn `max.poll.interval.ms` (mặc định 5 phút), broker coi consumer đã chết và kích hoạt rebalance — trong khi nó vẫn đang làm việc. Ở đây an toàn vì hot path không có I/O chặn; nhưng nếu ai đó thêm một lệnh gọi DB vào `onTicks()`, con số 1000 lập tức trở thành bom hẹn giờ.

### 3.5. CooperativeStickyAssignor — rebalance không stop-the-world

```java
// pricing-risk-service/config/KafkaConfig.java:41-49
// CooperativeStickyAssignor - KHẮC PHỤC nhược điểm rebalance:
// Assignor mặc định (eager) khi rolling update: MỌI consumer nhả HẾT partition
// rồi chia lại -> stop-the-world vài giây, tick dồn lag.
// Cooperative: chỉ partition thật sự phải chuyển chủ mới bị thu hồi,
// các consumer còn lại XỬ LÝ TIẾP không gián đoạn (incremental rebalance).
props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
        CooperativeStickyAssignor.class.getName());
```

Đặt ở **cả bốn** consumer factory: pricing (tick), order (enriched), order (executions), matching (orders).

Minh hoạ với 12 partition, 4 consumer, rolling update loại bỏ consumer thứ 4:

| | Eager (`RangeAssignor` mặc định) | **Cooperative Sticky** |
|---|---|---|
| Bước 1 | Cả 4 consumer **nhả hết** 12 partition | Chỉ consumer 4 nhả 3 partition của nó |
| Bước 2 | Group đứng im ~vài giây | Consumer 1-3 **vẫn đang xử lý** |
| Bước 3 | Chia lại 12 partition cho 3 consumer | 3 partition mồ côi được chia cho 1-3 |
| Thời gian ngừng xử lý | **vài giây × toàn bộ partition** | gần như 0 cho 9/12 partition |

Trong phiên giao dịch, "vài giây × toàn bộ" nghĩa là hàng chục nghìn tick dồn lại — và vì `fetch.min.bytes=64KB` đang chờ, khi group phục hồi nó sẽ phải tiêu hoá một đợt dồn ứ. Rolling update với 4 pod gây 4 lần như vậy.

💡 `Sticky` còn giữ cho partition **ở lại với consumer cũ** khi có thể. Điều này quan trọng với `RiskCalculator`: `SymbolState` (EMA, sliding window) nằm trong RAM của thread đang xử lý. Partition chuyển chủ = state bị bỏ lại = consumer mới phải hâm nóng lại 120 tick cho mỗi mã. Sticky giảm số lần phải hâm nóng.

### 3.6. Dead Letter Topic

```java
// pricing-risk-service/config/KafkaConfig.java:68-81
// DLQ - KHẮC PHỤC nhược điểm "retry 3 lần rồi skip mất message":
// Hết retry -> message được publish sang "stock.ticks.DLT" kèm header
// (exception, stacktrace, offset gốc) -> alert trên lag của DLT,
// xem xét/sửa rồi replay lại. KHÔNG BAO GIỜ mất dữ liệu âm thầm.
var recoverer = new DeadLetterPublishingRecoverer(dltTemplate);
factory.setCommonErrorHandler(new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 3)));
```

Ba lựa chọn khi một message không xử lý được:

| Chiến lược | Hậu quả |
|---|---|
| Retry mãi mãi | **Poison pill**: một message hỏng chặn vĩnh viễn cả partition. Lag tăng vô hạn. |
| Retry N lần rồi skip | Mất dữ liệu **âm thầm** — không ai biết đã mất gì. |
| **Retry N lần rồi chuyển DLT** | Dữ liệu được bảo toàn kèm ngữ cảnh chẩn đoán; partition thông trở lại. |

Producer cho DLT dùng `acks=all`:

```java
// pricing-risk-service/config/KafkaConfig.java:88-98
// DLT là dữ liệu chẩn đoán quan trọng -> acks=all cho chắc
props.put(ProducerConfig.ACKS_CONFIG, "all");
```

Nghịch lý thú vị: topic chính `stock.ticks` dùng `acks=1` (mất một tick không sao), nhưng DLT của nó dùng `acks=all`. Lý do: một tick bình thường là một trong hàng triệu; một tick **lỗi** là bằng chứng duy nhất về một bug.

Header mà `DeadLetterPublishingRecoverer` gắn vào:

| Header | Nội dung |
|---|---|
| `kafka_dlt-original-topic` | `stock.ticks` |
| `kafka_dlt-original-partition` | partition gốc |
| `kafka_dlt-original-offset` | offset gốc — 🔑 dùng để replay chính xác |
| `kafka_dlt-exception-fqcn` | tên class exception |
| `kafka_dlt-exception-message` | thông điệp lỗi |
| `kafka_dlt-exception-stacktrace` | stacktrace đầy đủ |

⚠️ **Gap #1 (chương 14) — DLT không bắt được lỗi deserialize.** `KafkaAvroDeserializer` chạy **bên trong `poll()`**, trước khi error handler của listener container có cơ hội can thiệp. Một byte rác trong topic (hoặc schema ID trỏ tới schema không fetch được) sẽ ném exception ở tầng `poll()` ⇒ consumer vào vòng lặp lỗi vô hạn, **không có message nào vào DLT**. Cách sửa chuẩn:

```java
// Bọc deserializer để lỗi trở thành một bản ghi có giá trị null thay vì exception
props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, KafkaAvroDeserializer.class);
```

⚠️ **Gap #2 — batch listener đẩy cả lô vào DLT.** `TickConsumer.onTicks()` nhận `List` 1000 bản ghi. Nếu bản ghi thứ 500 ném exception, `DefaultErrorHandler` **không biết** bản ghi nào lỗi ⇒ sau khi hết retry, nó đẩy **toàn bộ 1000 bản ghi** vào DLT. 999 tick hoàn toàn hợp lệ bị coi là độc hại. Cách sửa:

```java
public void onTicks(List<TickEventAvro> avroTicks) {
    for (int i = 0; i < avroTicks.size(); i++) {
        try {
            process(avroTicks.get(i));
        } catch (Exception e) {
            // Chỉ bản ghi thứ i vào DLT; các bản ghi trước đó coi như đã xử lý xong
            throw new BatchListenerFailedException("tick loi tai index " + i, e, i);
        }
    }
}
```

⚠️ **Ops #13 — DLT không có alert nghĩa là tương đương skip.** Repo đã bù bằng rule trong `k8s/components/monitoring/prometheusrule.yaml`:

```yaml
- alert: DeadLetterTopicGrowing
  expr: sum(rate(kafka_consumer_fetch_manager_records_consumed_total{topic=~"stock\..*\.DLT"}[5m])) > 0
  for: 5m
  labels: { severity: warning }
```

Nhưng rule này đo **records_consumed** — tức là chỉ kêu khi *có ai đó đang consume* DLT. Nếu không có consumer nào đọc DLT (trường hợp hiện tại), metric này luôn bằng 0 và alert không bao giờ bắn. Alert đúng phải dựa trên `kafka_exporter` đo số message trong topic, hoặc trên counter phía producer của recoverer.

---

## 4. Vấn đề được giải quyết

| Vấn đề nếu không có | Cơ chế của Kafka |
|---|---|
| Service gọi REST trực tiếp ⇒ chết dây chuyền | Temporal decoupling |
| EMA/volatility sai do xử lý song song | `key = symbol` ⇒ một thread một mã |
| Mất lệnh khi pod chết | Persistent log + `acks=all` + replay |
| Thêm consumer phải sửa producer | Fan-out qua consumer group |
| Đột biến tải làm sập hệ | Lag đóng vai trò bộ đệm + tín hiệu scale |
| Một message hỏng chặn cả stream | DLT |
| Đổi schema phá consumer âm thầm | Schema Registry (mục 5) |

---

## 5. Avro & Schema Registry chi tiết

### 5.1. Vì sao Avro thay JSON

```java
// market-gateway/config/KafkaProducerConfig.java:20-28
/**
 * Producer AVRO + SCHEMA REGISTRY thay cho JSON:
 * - Message nhỏ hơn 3-5x (binary, không lặp tên field mỗi message)
 * - Schema evolution ĐƯỢC CƯỠNG CHẾ: producer đăng ký schema mới với Registry,
 *   Registry kiểm tra tương thích (BACKWARD mặc định) - deploy phá vỡ consumer
 *   sẽ FAIL NGAY LÚC PRODUCE đầu tiên chứ không âm thầm làm consumer chết.
 * - Message chỉ mang schema ID (4 byte), consumer tự fetch schema từ Registry (có cache).
 */
```

So sánh cụ thể cho một tick:

```
JSON  (~95 byte): {"symbol":"HPG","price":28500.0000,"volume":123,"eventTime":1764057600123}
Avro  (~30 byte): [00][00 00 00 05][03 48 50 47][...decimal bytes...][f6 01][...]
```

Ở 100.000 tick/giây: JSON ≈ 9,5 MB/s, Avro ≈ 3 MB/s. Nhân với retention 7 ngày: **5,7 TB vs 1,8 TB** cho cùng một lượng thông tin.

### 5.2. Decimal logical type — cạm bẫy scale

```java
// market-gateway/kafka/TickProducer.java:31-33
// Decimal logical type yêu cầu ĐÚNG scale=4 như khai báo trong .avsc.
.setPrice(tick.price().setScale(4, RoundingMode.HALF_UP))
```

Avro mã hoá `decimal` thành `bytes` chứa **unscaled value**; `scale` là hằng số trong schema, không đi theo từng giá trị. Nên:
- `BigDecimal("28500.00")` có scale 2 ⇒ Avro ném `AvroTypeException` lúc serialize.
- Nếu ép scale bằng `setScale(4)` mà không chỉ định `RoundingMode`, giá trị cần làm tròn sẽ ném `ArithmeticException`.

`SimulatedFeed.java:45` tạo giá với `setScale(2)`, nên dòng `setScale(4, HALF_UP)` ở producer là **bắt buộc**, không phải trang trí.

🔑 **Vì sao `BigDecimal` chứ không `double` cho tiền:** `0.1 + 0.2 = 0.30000000000000004` trong IEEE 754. Với giá cổ phiếu và khối lượng hàng triệu, sai số tích luỹ thành tiền thật. `TickEvent` ghi rõ: `BigDecimal price,    // BigDecimal, KHÔNG dùng double cho tiền tệ`.

💡 Đáng chú ý: `RiskCalculator` **có** dùng `double` — `double px = tick.price().doubleValue()` (`:46`). Điều đó đúng, vì ở đó ta tính *thống kê* (log-return, variance) chứ không tính *tiền*. Sai số `1e-16` trong volatility không ai quan tâm. Ranh giới "dùng BigDecimal ở đâu" nằm đúng chỗ: tiền thì BigDecimal, thống kê thì double.

### 5.3. Quy tắc tương thích

Schema Registry mặc định `BACKWARD`: **consumer dùng schema MỚI phải đọc được dữ liệu ghi bằng schema CŨ**.

| Thay đổi | BACKWARD? | Giải thích |
|---|:--:|---|
| Thêm field **có** `default` | ✅ | Consumer mới đọc dữ liệu cũ thiếu field đó ⇒ dùng default |
| Thêm field **không** default | ❌ | Consumer mới không biết điền gì |
| Xoá field có default | ✅ | Consumer mới bỏ qua field thừa |
| Xoá field không default | ❌ | |
| Đổi kiểu `long → string` | ❌ | Không có quy tắc chuyển đổi |
| Mở rộng `int → long` | ✅ | Avro promote được |
| Đổi tên field | ❌ | Tương đương xoá + thêm. Dùng `aliases` để sửa. |

Các chế độ khác: `FORWARD` (consumer cũ đọc được dữ liệu mới — hợp khi nâng cấp producer trước), `FULL` (cả hai chiều), `NONE` (tắt kiểm tra — đừng dùng ở production).

🔑 **Giá trị thực:** không có Registry, một dev đổi `volume` từ `long` sang `string`, deploy gateway, và `pricing-risk-service` bắt đầu ném exception trên **mọi** message — một sự cố production lúc 2 giờ sáng. Có Registry, lần `send()` đầu tiên của gateway thất bại ngay với `Schema being registered is incompatible with an earlier schema`. Lỗi chuyển từ *runtime của consumer* sang *deploy time của producer*.

### 5.4. ⚠️ `auto.register.schemas` trong production

```java
// market-gateway/config/KafkaProducerConfig.java:48-50
// demo: tự đăng ký schema; PRODUCTION nên false - schema đăng ký qua CI/CD
// có review, producer chỉ được dùng schema đã duyệt
props.put("auto.register.schemas", true);
```

Với `true`, bất kỳ producer nào cũng tự đăng ký schema mới khi khởi động. Ở production điều này nghĩa là: một dev deploy nhầm một build cũ ⇒ schema cũ được đăng ký ⇒ subject có một version lạ. Hoặc tệ hơn, với `compatibility = NONE`, một schema phá vỡ được đăng ký thành công.

Quy trình đúng cho production:
1. `.avsc` nằm trong Git, thay đổi phải qua pull request.
2. CI chạy `mvn schema-registry:test-compatibility` (Confluent Maven plugin) để kiểm tra trước khi merge.
3. CI đăng ký schema sau khi merge.
4. Producer đặt `auto.register.schemas=false` và `use.latest.version=true`.

### 5.5. Mock Schema Registry trong test

```java
// pricing-risk-service/src/test/java/com/trading/pricing/TickToMarketDataIT.java:65
registry.add("schema.registry.url", () -> "mock://pricing-it");
```

Scheme `mock://` kích hoạt `MockSchemaRegistryClient` — một registry in-memory theo *scope* (phần sau `mock://`). Producer của test và consumer của ứng dụng chạy **cùng JVM** nên chia sẻ cùng instance ⇒ không cần container Schema Registry. Tiết kiệm ~10 giây khởi động mỗi lần chạy test.

⚠️ Đánh đổi: test không kiểm chứng hành vi *thật* của Confluent Schema Registry (kiểm tra tương thích, HTTP retry, cache eviction). Nên có ít nhất một test dùng container thật nếu schema evolution là rủi ro chính.

---

## 6. Đánh đổi

### 6.1. Ưu

1. Thứ tự theo khoá — nền tảng tính đúng đắn.
2. Độ bền + replay 7 ngày.
3. Tách thời gian hoàn toàn.
4. Lag là **tín hiệu scale đúng** (chương 10).
5. Avro: nhỏ hơn 3-5×, schema evolution được cưỡng chế.
6. Fan-out miễn phí cho consumer mới.

### 6.2. Nhược

1. **Trần scale cứng** = partition / concurrency. Chỉ tăng được, không giảm.
2. **Rebalance làm dừng xử lý** — giảm được bằng Cooperative, không triệt tiêu được.
3. **Hot partition** không chữa được bằng thêm pod.
4. **Thêm partition phá thứ tự lịch sử**.
5. Schema Registry là **điểm hỏng mới** — nó chết, producer không produce được schema mới.
6. Avro khó debug bằng mắt: `kafka-console-consumer` trả ra byte rác, phải dùng `kafka-avro-console-consumer`.
7. **Vận hành Kafka là một nghề riêng**: ISR, unclean leader election, log compaction, disk sizing, rack awareness.

---

## 7. Núm vặn cấu hình

| Tham số | Nơi khai | Mặc định | Hệ quả khi đổi |
|---|---|---|---|
| `KAFKA_PARTITIONS` | `trading.env` / overlay | 3/6/12 | Đổi trần scale; tăng phá thứ tự lịch sử |
| `KAFKA_TOPIC_REPLICAS` | như trên | 1 | ⚠️ 1 ⇒ `acks=all` vô nghĩa |
| `setConcurrency(n)` | 4 × `KafkaConfig.java` | 3 | Đổi trần pod; CI hardcode 3 |
| `max.poll.records` | `pricing/KafkaConfig.java:56` | 1000 | Lớn ⇒ throughput cao, nguy cơ `max.poll.interval` |
| `fetch.min.bytes` / `max.wait.ms` | `:54-55` | 64KB / 50ms | Núm throughput↔latency rõ nhất |
| `linger.ms` | `KafkaProducerConfig.java:54` | 5 | Trực tiếp cộng vào latency tick |
| `acks` | mỗi producer | 1 hoặc all | Độ bền ↔ tốc độ |
| `auto.register.schemas` | `KafkaProducerConfig.java:50` | `true` | ⚠️ Phải `false` ở prod |
| `TOPIC_RETENTION_MS` | `trading.env` | 7 ngày | 🔑 Có **hợp đồng ngầm** với lịch dọn `processed_event` — xem [chương 06](./06-idempotency-va-consumer-deduplication.md) |
| `DLT_RETENTION_MS` | `trading.env` | 30 ngày | Dài hơn topic chính có chủ đích |

---

## 8. Cách kiểm chứng

```bash
# Liệt kê topic và số partition
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:19092 --describe

# Lag của từng consumer group — chỉ số sức khoẻ quan trọng nhất
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:19092 --describe --all-groups

# Xem message Avro (PHẢI dùng avro console consumer, không dùng console-consumer thường)
docker compose exec schema-registry kafka-avro-console-consumer \
  --bootstrap-server kafka:19092 --topic stock.ticks \
  --property schema.registry.url=http://localhost:8081 --max-messages 3

# Xem message JSON
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:19092 --topic stock.orders \
  --from-beginning --max-messages 3

# Chứng minh key = symbol giữ thứ tự: in cả key, partition, offset
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:19092 --topic stock.enriched \
  --property print.key=true --property print.partition=true \
  --property print.offset=true --max-messages 20
#  -> mọi dòng có key=HPG đều cùng một partition

# Schema Registry: liệt kê subject và kiểm tra tương thích
curl -s http://localhost:8085/subjects | jq
curl -s http://localhost:8085/subjects/stock.ticks-value/versions | jq
curl -s http://localhost:8085/config | jq            # chế độ compatibility hiện tại

# Thử đăng ký một schema phá vỡ -> phải bị từ chối
curl -s -X POST http://localhost:8085/compatibility/subjects/stock.ticks-value/versions/latest \
  -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
  -d '{"schema":"{\"type\":\"record\",\"name\":\"TickEventAvro\",\"namespace\":\"com.trading.common.avro\",\"fields\":[{\"name\":\"symbol\",\"type\":\"string\"}]}"}' | jq
#  -> {"is_compatible": false}   (đã xoá field không có default)

# DLT có message không?
docker compose exec kafka /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
  --bootstrap-server localhost:19092 --topic stock.ticks.DLT
```

---

## 9. Hạn chế đã biết

| # | Loại | Vị trí | Vấn đề |
|:--:|---|---|---|
| 1 | **Gap** | `pricing/config/KafkaConfig.java:44` | Không có `ErrorHandlingDeserializer` ⇒ lỗi deserialize xảy ra trong `poll()`, **DLT không bắt được**, consumer vào vòng lặp lỗi vô hạn |
| 2 | **Gap** | `pricing/consumer/TickConsumer.java:52` | Batch listener không ném `BatchListenerFailedException` ⇒ 1 tick lỗi đẩy **cả lô 1000 tick** vào DLT |
| 13 | Ops | `stock.ticks.DLT` | Alert hiện tại đo `records_consumed` nên không bắn khi không ai consume DLT; cần `kafka_exporter` hoặc counter phía recoverer |
| 16 | **Gap** | Mọi `TopicBuilder` (6 chỗ) | `.replicas(1)` + 1 broker ⇒ `acks=all` ≡ `acks=1`, **không có replica nào để chờ**. Độ bền chỉ là danh nghĩa. |
| — | Gap | `pricing/config/KafkaConfig.java:84-88` | `ticksDltTopic()` khai `.partitions(1)` trong khi topic gốc 12 partition ⇒ recoverer có thể publish thất bại (chỉ ảnh hưởng local, cluster dùng Job) |
| — | Ops | `KafkaProducerConfig.java:50` | `auto.register.schemas=true` không phù hợp production |

---

## 10. Câu hỏi phỏng vấn

**Q: Vì sao `key = symbol` mà không phải `orderId` hay round-robin?**

A: Vì thứ tự theo mã là **ràng buộc đúng đắn**, không phải tối ưu hoá. `RiskCalculator` tính `ema = α·px + (1−α)·ema` và `r = ln(px / lastPrice)` — cả hai đọc trạng thái của tick liền trước. Round-robin phân tick `HPG` ra 12 partition, 12 thread xử lý song song, EMA và log-return ra số vô nghĩa, và `riskLevel` sai là thứ dùng để từ chối lệnh khách. `orderId` thì mỗi lệnh một partition ngẫu nhiên — mất thứ tự theo mã mà chẳng được gì. Hệ quả kèm theo của `symbol`: `synchronized` trong `RiskCalculator` luôn uncontended vì một mã chỉ có một thread.

**Q: Vì sao `stock.ticks` dùng `acks=1` còn `stock.orders` dùng `acks=all`? Sao không dùng `all` cho cả hai cho an toàn?**

A: Vì bản chất dữ liệu khác nhau. Mất một tick: tick kế tiếp 200ms sau sửa lại bức tranh; mất một lệnh: mất tiền của khách và không có gì cứu. `acks=all` thêm một round-trip chờ toàn bộ ISR — ở 100.000 tick/giây, cái giá đó là thật. Dùng `all` cho cả hai nghĩa là trả giá độ bền cho dữ liệu không cần độ bền. Nhưng tôi cũng phải nói thẳng: trong repo này mọi topic khai `.replicas(1)` và cluster có 1 broker, nên `acks=all` hiện **không có replica nào để chờ** — nó chỉ có ý nghĩa thật sau khi lên multi-broker với `min.insync.replicas=2`.

**Q: Hệ đang có 12 partition, 4 pod. Tải tăng gấp 3. Bạn làm gì?**

A: Thêm pod **không giúp** — pod thứ 5 nhận 0 partition và việc nó join còn gây rebalance. Thứ tự việc phải làm: (1) tăng partition qua Job `kafka-topics-init` (chỉ tăng được, không giảm); (2) nâng `maxReplicaCount` của KEDA tương ứng — CI sẽ chặn nếu vượt `partition/3`; (3) scale pod. Nhưng trước tất cả, tôi sẽ kiểm tra **phân bố lag theo partition**: nếu lag tập trung ở một partition (hot symbol), tăng partition chẳng giải quyết gì vì mã đó vẫn chỉ có một consumer — khi đó phải xét khoá mịn hơn, và phải hỏi nghiệp vụ xem có chấp nhận chỉ giữ thứ tự trong bucket không. Và tôi sẽ cảnh báo trước: tăng partition làm `murmur2(key) % n` đổi kết quả, nên có một khoảng ngắn thứ tự lịch sử của mỗi mã không còn đảm bảo — nên làm ngoài phiên giao dịch.

**Q: Schema Registry chết lúc 2 giờ sáng. Chuyện gì xảy ra?**

A: Service đã chạy và đã cache schema ID vẫn hoạt động bình thường — `KafkaAvroSerializer`/`Deserializer` cache theo ID. Hỏng xảy ra ở ba chỗ: (1) producer cần đăng ký schema **mới** sẽ fail; (2) consumer gặp một schema ID **chưa từng thấy** sẽ fail; (3) pod **mới khởi động** có cache rỗng sẽ fail ngay từ message đầu — nghĩa là một rolling update lúc Registry chết sẽ làm sập consumer. Vì vậy Schema Registry ở production phải chạy nhiều instance sau load balancer, và nó cũng cần probe + alert như một service nghiệp vụ.

**Q: Một message trong `stock.ticks` bị hỏng (byte rác). Hệ thống phản ứng thế nào?**

A: Theo thiết kế: retry 3 lần cách nhau 1s, rồi `DeadLetterPublishingRecoverer` đẩy sang `stock.ticks.DLT` kèm header chứa topic/partition/offset gốc và stacktrace. Theo thực tế của repo hiện tại: **không như vậy** — vì thiếu `ErrorHandlingDeserializer`, lỗi deserialize xảy ra bên trong `poll()` trước khi error handler có cơ hội chạy, nên consumer rơi vào vòng lặp lỗi vô hạn và không có gì vào DLT. Đây là Gap #1 trong danh sách vấn đề đã biết. Sửa bằng cách bọc `KafkaAvroDeserializer` trong `ErrorHandlingDeserializer`, và nên có một integration test bơm byte rác để xác nhận.

---

## 11. Bảng tổng kết

| Khía cạnh | Lựa chọn | Lý do | Cái giá |
|---|---|---|---|
| Broker | Kafka 3.7 KRaft | Bớt một hệ thống; failover nhanh | Tooling quanh ZK không dùng được |
| Partition | 3 / 6 / 12 theo môi trường | Trần scale = partition/3 | Chỉ tăng được; tăng phá thứ tự lịch sử |
| Khoá | `symbol` ở cả 4 topic | 🔑 Thứ tự theo mã = tính đúng đắn | Hot partition không chữa bằng thêm pod |
| Producer giá | `acks=1`, linger 5ms, lz4, batch 64KB | Throughput; mất tick vô hại | +5ms latency |
| Producer lệnh | `acks=all`, idempotence, retries ∞, gửi sync | Không được mất lệnh | Chậm hơn; cần multi-broker mới có ý nghĩa |
| Consumer | batch 1000, fetch 64KB/50ms, Cooperative | Throughput + rebalance êm | Nguy cơ `max.poll.interval` nếu thêm I/O |
| `auto.offset.reset` | `latest` giá / `earliest` lệnh | Giá cũ vô nghĩa; lệnh sót là mất tiền | — |
| Xử lý lỗi | retry 3× + DLT, retention 30 ngày | Không mất dữ liệu âm thầm | ⚠️ Chưa bắt được lỗi deserialize |
| Serialization | Avro cho `stock.ticks`, JSON cho 3 topic còn lại | Di cư từng topic một, bắt đầu từ topic lưu lượng cao nhất | Hai cơ chế song song tạm thời |
| Tạo topic | Job tường minh, idempotent | Auto-create đẻ 1 partition, sập trần scale âm thầm | Thêm một Job vào vòng deploy |

> **Đọc tiếp:** [Chương 03 — Pricing, Real-time Risk & Write-Behind Cache](./03-pricing-realtime-risk-va-write-behind-cache.md).
