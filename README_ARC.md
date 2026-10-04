# DESIGN NOTES — Giải thích thiết kế & Ưu nhược điểm

## 1. Vì sao chia 3 microservices như vậy?

Chia theo **nhịp thay đổi và nhịp scale khác nhau**, không chia theo "mỗi bảng một service":

| Service | Trách nhiệm | Đặc tính |
|---|---|---|
| market-gateway | Nhận feed ngoài, đẩy vào Kafka | I/O-bound, gắn với session feed |
| pricing-risk-service | Tính toán giá + rủi ro, lưu DB | CPU-bound, cần scale theo lưu lượng tick |
| order-service | Pre-trade check + đặt lệnh | Latency-critical, cần độ bền dữ liệu tuyệt đối |

**Ưu điểm:**
- Scale độc lập: phiên ATC lưu lượng tick tăng vọt → chỉ scale pricing-risk-service, order-service không bị ảnh hưởng.
- Cô lập lỗi: pricing-risk-service chết → order-service vẫn nhận lệnh (dùng cache cuối cùng, có staleness check để fail-safe).
- Deploy độc lập: sửa công thức risk không cần deploy lại phần đặt lệnh (giảm rủi ro thay đổi trên đường đi của lệnh).

**Nhược điểm (cái giá phải trả):**
- Độ phức tạp vận hành tăng: 3 service + Kafka + DB + monitoring thay vì 1 JVM.
- Latency mạng giữa các hop: monolith gọi method nội bộ mất nanoseconds; ở đây mỗi hop qua Kafka tốn 1–10ms. Tổng E2E khoảng 10–30ms — quá đủ cho back-office/pre-trade risk, nhưng **không phù hợp HFT** (HFT dùng single-process + LMAX Disruptor, ring buffer nội bộ như bạn đã tìm hiểu trước đây).
- Data consistency: dữ liệu order-service nhìn thấy luôn "trễ" vài ms so với pricing-risk-service (eventual consistency). Phải chấp nhận và thiết kế check staleness thay vì giả định đồng bộ.
- Debug khó hơn: 1 lệnh đi qua 3 service — production cần thêm distributed tracing (Zipkin/Tempo + Micrometer Tracing).

## 2. Các quyết định Kafka (điểm mấu chốt của tốc độ)

### 2.1. Hai profile producer khác nhau — đây là quyết định quan trọng nhất

| | Tick data (market-gateway) | Order (order-service) |
|---|---|---|
| acks | `1` | `all` |
| idempotence | tắt | **bật** |
| Lý do | Tick sau ghi đè tick trước, mất 1 tick không sao. Đổi lấy latency thấp nhất. | Mất 1 lệnh = sự cố nghiêm trọng. Duplicate lệnh khi retry = nghiêm trọng hơn. Chấp nhận chậm hơn vài ms. |

Bài học: **không có một cấu hình Kafka "đúng" cho mọi topic** — độ bền phải khớp với giá trị nghiệp vụ của message.

### 2.2. Partition key = symbol
- **Ưu:** mọi tick của một mã luôn vào cùng partition → consumer xử lý đúng thứ tự thời gian. Bắt buộc vì volatility/EMA là phép tính phụ thuộc thứ tự. Bonus: mỗi symbol chỉ do 1 thread xử lý → state per-symbol gần như không có lock contention.
- **Nhược:** hot partition — nếu VN30F chiếm 80% lưu lượng thì 1 partition gánh 80% tải, các partition khác nhàn rỗi. Scale consumer thêm không giúp gì cho mã nóng. Giải pháp khi cần: composite key (symbol + bucket) và chấp nhận merge thứ tự ở consumer, hoặc tách mã nóng ra topic riêng.

### 2.3. Batching (linger.ms=5, batch.size=64KB, max.poll.records=1000, batch listener)
- **Ưu:** throughput tăng 5–10 lần, giảm số network round-trip và GC pressure.
- **Nhược:** cộng thêm ~5–50ms latency ở biên. Trade-off kinh điển throughput vs latency — với pre-trade risk chấp nhận được; nếu cần <1ms thì phải bỏ batching, thậm chí bỏ Kafka khỏi hot path.

### 2.4. Nén lz4
- **Ưu:** JSON nén tốt (~4x), lz4 là codec nhanh nhất (nhanh hơn gzip nhiều, nhẹ hơn zstd về CPU).
- **Nhược:** tốn CPU hai đầu. Bước tối ưu tiếp theo là bỏ JSON, dùng **Avro/Protobuf + Schema Registry**: message nhỏ hơn 3–5x, có schema evolution — JSON hiện tại là "contract ngầm" giữa các service (record `TickEvent` phải khớp bằng tay ở 2 nơi), đây là điểm yếu thật sự của demo này.

## 3. Write-behind cache ở pricing-risk-service (hot path vs cold path)

Luồng tick **không bao giờ chạm database**. Kết quả tính xong chỉ `put` vào `ConcurrentHashMap`; một scheduler riêng flush snapshot xuống Postgres mỗi 5 giây bằng batch insert (`hibernate.jdbc.batch_size=100`).

- **Ưu:** DB chậm, nghẽn, thậm chí chết → luồng tính toán realtime không hề hấn gì. REST đọc giá realtime <1ms vì đọc RAM. Ghi DB theo lô rẻ hơn ghi từng tick hàng trăm lần.
- **Nhược:**
  - Pod chết → mất tối đa 5 giây snapshot chưa flush. Chấp nhận được vì **Kafka giữ raw tick, replay được** để rebuild — Kafka đóng vai trò source-of-truth, DB chỉ là materialized view.
  - Dữ liệu trong DB không phải "mọi tick" mà là snapshot 5s/lần. Nếu nghiệp vụ cần full tick history → thêm consumer riêng ghi vào time-series DB (TimescaleDB/ClickHouse), đừng nhét vào service tính toán.
  - `ddl-auto: update` chỉ dành cho demo; production phải dùng Flyway/Liquibase (bạn đã quá quen với đau đớn schema mismatch khi migrate DB2→MySQL — version-control schema ngay từ đầu là cách tránh).

## 4. Order-service dùng local cache thay vì gọi REST — vì sao?

Cách "ngây thơ": mỗi lệnh đến, order-service gọi REST sang pricing-risk-service lấy giá. Cách này có 3 vấn đề: +2–10ms mỗi lệnh, coupling runtime (pricing chết → không đặt được lệnh nào), và pricing-risk phải chịu tải REST tỉ lệ thuận số lệnh.

Cách trong code: subscribe `stock.enriched`, giữ bản mới nhất trong `ConcurrentHashMap` (pattern **local materialized view / CQRS read model**).

- **Ưu:** risk check = 1 lần đọc map (micro-giây); không phụ thuộc runtime vào service khác; tải đọc không đè lên pricing-risk.
- **Nhược:** dữ liệu eventual consistent (trễ vài ms). Phải có **staleness check** (`max-data-age-ms=10000`): nếu cache quá cũ thì *từ chối lệnh* chứ không đặt bừa — nguyên tắc fail-safe của hệ thống tài chính: khi không chắc chắn, chọn phương án an toàn (không khớp) thay vì phương án tiện (khớp với data cũ).

Ba lớp pre-trade check trong `OrderService`: staleness → risk level (volatility HIGH chặn) → fat-finger (giá lệch >5% giá tham chiếu chặn). Tất cả in-memory, không I/O.

## 5. Monitoring — Micrometer + Prometheus + Grafana

- Mỗi service expose `/actuator/prometheus`; Prometheus **pull** mỗi 10s (mặc định 1m quá thưa cho trading).
- Metric tự tạo có chủ đích, không chỉ dùng metric mặc định:
  - `pricing.e2e.latency` (p50/p95/p99): latency từ tick sinh ra ở nguồn tới lúc tính xong — **KPI số 1** của hệ thống. Nhìn p99, đừng nhìn average (average che giấu tail latency).
  - `pricing.batch.process`: phát hiện hot path chậm dần (GC, lock contention).
  - `orders.accepted/rejected`: reject tăng đột biến = tín hiệu thị trường biến động hoặc bug — đã có alert rule mẫu trong `monitoring/alert-rules.yml`.
- **Ưu điểm mô hình pull của Prometheus:** service không cần biết monitoring ở đâu; `up == 0` tự thành tín hiệu chết; scale monitoring độc lập.
- **Nhược điểm:** scrape 10s nghĩa là sự kiện <10s có thể "lọt khe" giữa 2 lần scrape; Prometheus lưu local, không HA — production dùng kube-prometheus-stack + Thanos/Mimir nếu cần long-term storage. Metrics chỉ trả lời "cái gì chậm", không trả lời "chậm ở đâu trong luồng" — mảnh ghép còn thiếu là **distributed tracing** (Micrometer Tracing + Zipkin/Tempo) và **centralized logging** (Loki/ELK).

## 6. Kubernetes — các quyết định đáng chú ý

- **Liveness ≠ Readiness:** liveness = "JVM còn sống?" (fail → restart pod); readiness = "sẵn sàng nhận traffic?" (fail → rút khỏi Service endpoint, không restart). Nhầm hai cái này → pod bị restart vòng lặp khi chỉ đang chậm khởi động.
- **HPA có bẫy với Kafka consumer:** HPA scale theo CPU, nhưng số consumer hiệu quả bị chặn trên bởi **số partition**. 6 partition / concurrency 3 = tối đa 2 pod tiêu thụ hết; pod thứ 3–4 chỉ gánh REST API, consumer của nó ngồi không. Muốn scale consumer thật sự → tăng partition trước, hoặc dùng **KEDA** scale theo consumer lag (chính xác hơn CPU rất nhiều cho workload Kafka).
- **`MaxRAMPercentage=75`** trong Dockerfile: JVM tự tính heap theo memory limit của container. Không set → JVM cũ nhìn RAM của node → OOMKilled.
- **Kafka/Postgres chạy StatefulSet chỉ để demo.** Production: Kafka dùng **Strimzi Operator**, Postgres dùng managed service hoặc CloudNativePG. Chạy stateful workload "tay bo" trên K8s là nguồn sự cố lớn.
- Secret trong manifest là plaintext base64 — production dùng SealedSecrets/ExternalSecrets/Vault.
- `scaleDown.stabilizationWindowSeconds=300`: tránh flapping khi tải dao động quanh ngưỡng.

**Ưu điểm K8s cho hệ này:** self-healing (pod chết tự lên lại, consumer group tự rebalance), rolling update không downtime, autoscaling, resource isolation, cấu hình qua env/Secret tách khỏi image.
**Nhược điểm:** learning curve + chi phí vận hành cluster; rebalance consumer khi rolling update gây pause vài giây (giảm bằng `CooperativeStickyAssignor`); network overlay cộng thêm chút latency so với bare metal.

## 7. Những gì demo này CHƯA có (roadmap production)

1. **Schema Registry + Avro/Protobuf** — thay JSON, có schema evolution.
2. **Dead Letter Queue** — thay `DefaultErrorHandler` skip bằng `DeadLetterPublishingRecoverer` (bạn đã quen pattern `@RetryableTopic` + DLQ).
3. **Distributed tracing + centralized logging.**
4. **Security:** mTLS giữa service (service mesh hoặc Kafka SASL/TLS), authn/authz cho API đặt lệnh.
5. **Transactional Outbox** cho order-service nếu sau này lệnh phải ghi DB + publish Kafka nguyên tử.
6. **Kafka Streams / exactly-once** nếu phép tính risk trở thành stateful aggregation phức tạp cần chính xác tuyệt đối khi replay.
7. **Idempotent consumer** ở hệ thống khớp lệnh phía sau (orderId là key dedupe).

---

# PHIÊN BẢN 2 — Các nâng cấp khắc phục nhược điểm

## V2.1. `common-contracts` — xoá "contract ngầm"

**Vấn đề cũ:** `TickEvent`, `EnrichedMarketData` được copy-paste ở mỗi service. Đổi 1 field ở gateway mà quên sửa ở pricing → lỗi deserialize runtime, khó tìm.

**Giải pháp:** module Maven `common-contracts` chứa toàn bộ record đi qua Kafka. Đổi schema = đổi 1 chỗ, **compiler bắt lỗi ở compile-time** thay vì production.

- **Ưu:** rẻ, an toàn kiểu, refactor bằng IDE được, không thêm hạ tầng.
- **Nhược:** chỉ hoạt động khi mọi consumer đều là Java trong cùng repo/registry Maven; deploy không đồng bộ version vẫn có thể lệch schema (JSON tha thứ field thừa/thiếu nên đỡ, nhưng không có kiểm soát). **Bước tiếp theo khi hệ lớn/đa ngôn ngữ:** Avro/Protobuf + Schema Registry — registry cưỡng chế backward/forward compatibility ngay lúc producer đăng ký schema, chặn deploy phá vỡ consumer.

## V2.2. Dead Letter Queue — không mất message âm thầm

**Vấn đề cũ:** `DefaultErrorHandler` retry 3 lần rồi **skip** — message hỏng biến mất không dấu vết.

**Giải pháp:** `DeadLetterPublishingRecoverer` → hết retry, message được publish sang `stock.ticks.DLT` kèm header chẩn đoán (exception class, message, topic/partition/offset gốc). Alert theo lag của DLT, xem xét, sửa, replay.

- **Ưu:** zero data loss âm thầm; tách "poison message" khỏi luồng chính nên 1 message hỏng không chặn cả partition.
- **Nhược:** phải có quy trình vận hành DLT (ai xem? replay thế nào?) — DLT không ai đọc thì cũng như skip; message trong DLT mất thứ tự so với luồng chính khi replay (với tick data chấp nhận được).

## V2.3. Distributed tracing — trả lời "chậm ở đâu"

**Giải pháp:** Micrometer Tracing (Brave) + Zipkin. `setObservationEnabled(true)` trên cả KafkaTemplate lẫn listener container → traceId đi trong **Kafka header**, xuyên suốt gateway → pricing → order. Log pattern in traceId → grep 1 traceId ra log cả 3 service.

- **Ưu:** nhìn thấy từng hop tốn bao nhiêu ms trên Zipkin UI; metrics nói "p99 tăng", trace nói "tăng ở đoạn nào".
- **Nhược & trade-off quan trọng:** tracing có phí trên hot path (tạo span, inject header mỗi record). Vì vậy **sampling 10%** (`TRACING_PROBABILITY=0.1`) — đủ thấy hình dạng latency, khi điều tra sự cố nâng tạm lên 1.0 qua env var không cần build lại. Zipkin in-memory mất trace khi restart — production dùng Tempo/Elasticsearch backend.

## V2.4. CooperativeStickyAssignor — rolling update không còn stop-the-world

**Vấn đề cũ:** assignor eager mặc định: 1 pod restart → **mọi** consumer nhả hết partition rồi chia lại → toàn bộ consumption khựng vài giây, lag dồn đúng lúc nhạy cảm.

**Giải pháp:** `CooperativeStickyAssignor` — incremental rebalance, chỉ partition phải đổi chủ mới bị thu hồi, các consumer khác chạy tiếp.

- **Ưu:** rolling update/scale gần như không gián đoạn; cấu hình 1 dòng.
- **Nhược:** chuyển từ eager sang cooperative trên cluster **đang chạy** cần 2 bước rolling (giao thức không tương thích trực tiếp trong cùng group) — hệ mới thì set ngay từ đầu như ở đây là xong.

## V2.5. 12 partition + KEDA — scale theo đúng tín hiệu

Hai thay đổi phối hợp:

1. **Partition 6 → 12.** Số partition là trần cứng của scale-out và **chỉ tăng được, không giảm được**; tăng partition về sau còn làm vỡ mapping key→partition (tick cùng symbol trước/sau nằm khác partition → thứ tự lịch sử lẫn). Provision dư từ đầu rẻ hơn nhiều. 12 partition / concurrency 3 = trần 4 pod.
2. **KEDA thay HPA CPU.** HPA nhìn CPU — sai tín hiệu với Kafka workload: consumer có thể lag hàng triệu message mà CPU thấp (nghẽn I/O), hoặc CPU cao vì GC mà chẳng lag gì. KEDA query thẳng **consumer lag** của group — đúng thứ cần phản ứng. `lagThreshold=1000`/partition.

- **Ưu:** scale đúng lúc hệ thật sự tụt hậu; scale-to-min khi thị trường đóng cửa.
- **Nhược:** thêm 1 operator phải cài và vận hành (KEDA); vẫn bị trần partition (không công cụ nào vượt được quy luật này); lag threshold phải tune theo throughput thực tế.

## V2.6. Flyway — schema là code, có version

**Vấn đề cũ:** `ddl-auto: update` để Hibernate tự sửa schema — không kiểm soát được nó sửa gì, không rollback được, các môi trường trôi dạt khác nhau (đúng loại đau đớn của mọi cuộc migration DB2→MySQL: schema drift).

**Giải pháp:** migration SQL nằm trong Git (`db/migration/V1__init_market_snapshot.sql`), Flyway chạy lúc khởi động, Hibernate chuyển sang `ddl-auto: validate` (chỉ kiểm tra khớp, cấm sửa).

- **Ưu:** schema có lịch sử, review được qua PR, mọi môi trường giống nhau tuyệt đối, entity lệch schema → fail-fast ngay lúc boot thay vì lỗi ngầm lúc chạy.
- **Nhược:** thêm kỷ luật — mọi thay đổi schema phải viết file migration mới (không sửa file cũ đã chạy); nhiều pod cùng boot thì Flyway lock bảng history, các pod khác chờ (vài giây, chấp nhận được).

## V2.7. Idempotency với `clientOrderId` — chống duplicate lệnh

**Vấn đề cũ:** client gọi POST /orders, timeout, retry → **2 lệnh giống nhau được đặt**. Với hệ tài chính đây là lỗi loại nghiêm trọng nhất.

**Giải pháp:** client bắt buộc gửi `clientOrderId` (UUID tự sinh). Service cache `clientOrderId → kết quả` (TTL 10 phút): retry trùng id → trả lại **đúng kết quả cũ**, không xử lý lại.

- **Ưu:** client retry thoải mái (đúng tinh thần "retry an toàn" của mọi API tài chính — cùng pattern với Kafka idempotent producer, chỉ khác tầng).
- **Nhược (phải hiểu rõ):** cache này **per-pod** — 2 retry rơi vào 2 pod khác nhau qua load balancer vẫn lọt. Kiến trúc phòng thủ nhiều lớp: (1) cache per-pod chặn đa số, (2) Kafka producer idempotent chặn duplicate do producer retry, (3) hệ khớp lệnh downstream dedupe theo `clientOrderId` (idempotent consumer) là chốt chặn cuối. Muốn dedupe tập trung ngay tại order-service: thay map bằng Redis `SETNX` — đổi lấy +1 network hop (~1ms) mỗi lệnh.

## V2.8. API key filter — sàn bảo mật, không phải trần

Header `X-API-KEY` bắt buộc cho `/api/**` (actuator để mở cho probe/Prometheus).

- **Ưu:** 0 dependency, ~0 latency, chặn truy cập vu vơ, cấu hình qua Secret.
- **Nhược:** key tĩnh — lộ là hết, không phân quyền theo user/account, không audit được ai đặt lệnh. Production: OAuth2/JWT + Spring Security (quyền theo account), mTLS service-to-service, rate limit per-account. Để ở mức filter đơn giản vì demo — nhưng có sàn vẫn hơn không có gì.

## Bảng tổng kết: nhược điểm cũ → giải pháp → cái giá mới

| Nhược điểm cũ | Giải pháp V2 | Cái giá phải trả |
|---|---|---|
| Contract JSON trùng lặp | common-contracts module | Chưa cưỡng chế schema evolution (cần SR khi lớn) |
| Retry hết là mất message | DLQ `stock.ticks.DLT` | Cần quy trình vận hành/replay DLT |
| Không biết chậm ở đâu | Tracing + Zipkin | Overhead hot path → phải sample 10% |
| Rebalance stop-the-world | CooperativeStickyAssignor | Migration 2 bước nếu cluster đang chạy eager |
| HPA scale sai tín hiệu | KEDA theo consumer lag | Thêm operator; vẫn bị trần partition |
| Trần scale 6 partition | 12 partition | Nhiều file segment hơn trên broker (không đáng kể) |
| Hibernate tự sửa schema | Flyway + validate | Kỷ luật viết migration cho mọi thay đổi |
| Duplicate lệnh khi retry | clientOrderId idempotency | Per-pod; cần dedupe downstream làm chốt cuối |
| API đặt lệnh mở toang | API key filter | Mới là sàn; cần OAuth2/mTLS cho production |

## Còn lại chưa làm (có chủ đích)

- **Transactional Outbox** ở order-service: chỉ cần khi lệnh phải ghi DB + publish Kafka nguyên tử. Hiện lệnh chỉ đi Kafka (acks=all) nên chưa có bài toán dual-write. Thêm DB cho order là lúc phải thêm outbox (pattern Debezium CDC như đã nghiên cứu).
- **Avro + Schema Registry**: nâng cấp tự nhiên của common-contracts khi có consumer ngoài Java hoặc nhiều team.
- **Hot partition** (VN30F chiếm phần lớn lưu lượng): chưa xử lý vì mọi giải pháp (composite key, topic riêng cho mã nóng) đều hy sinh tính đơn giản của "key = symbol → thứ tự tuyệt đối". Chỉ đụng vào khi metric `records-lag` per-partition cho thấy lệch thật.

---

# V3 — Distributed transaction, schema evolution, và chất lượng production

V3 trả lời trực diện câu hỏi "đã có distributed transaction chưa?" và bịt các lỗ hổng chất lượng còn lại (test, resilience, CI/CD).

## 9. Transactional Outbox — distributed transaction không cần XA/2PC

**Vấn đề dual-write:** `order-service` phải làm 2 việc khi nhận lệnh: ghi DB (audit, dedupe) và publish Kafka (cho matching-engine). Ghi DB xong mà publish fail (hoặc ngược lại) → hai hệ thống lệch nhau vĩnh viễn. Đây chính là bài toán distributed transaction.

**Vì sao không dùng XA/2PC:** Kafka không hỗ trợ XA. Kể cả khi có, 2PC là giao thức blocking — coordinator chết giữa chừng thì participant giữ lock chờ vô hạn, throughput sụp. Không hệ thống trading hiện đại nào chọn 2PC cho đường nóng.

**Giải pháp:** không ghi 2 hệ thống nữa. Bảng `orders` + bảng `outbox_event` được INSERT trong **cùng một local ACID transaction** của Postgres — nguyên tử tuyệt đối, giống như bạn quen với DB2: một COMMIT, hoặc tất cả hoặc không gì. `OutboxRelay` (@Scheduled 500ms) sau đó đọc event chưa publish → gửi Kafka đồng bộ (acks=all) → đánh dấu `published_at`.

**Phân tích crash tại từng điểm:**
| Crash tại | Hậu quả | Vì sao vẫn đúng |
|---|---|---|
| Trước COMMIT | Không có lệnh, không có event | Client retry với cùng clientOrderId |
| Sau COMMIT, trước publish | Event nằm chờ trong outbox | Relay lần sau gửi — không mất |
| Sau publish, trước mark published | Event bị gửi LẠI lần sau | At-least-once → idempotent consumer lo |

**Polling relay vs Debezium CDC:** polling đơn giản (không thêm Kafka Connect + connector), độ trễ ~500ms đủ dùng; nhược là tải query định kỳ lên DB. Khi cần realtime hơn: chuyển sang Debezium đọc WAL — **không cần đổi schema**, Debezium outbox event router đọc đúng bảng `outbox_event` này.

**Chi tiết đáng chú ý:** `FOR UPDATE SKIP LOCKED` cho phép nhiều pod relay chạy song song không giẫm chân nhau; partial index `WHERE published_at IS NULL` giữ index quét luôn nhỏ; gửi đồng bộ `.get(5s)` có chủ đích để giữ thứ tự và chắc chắn Kafka nhận trước khi mark — trần ~200 event/s/pod, quá đủ, cần hơn thì CDC.

## 10. Idempotent Consumer — nửa còn lại của bức tranh

Outbox cho **at-least-once**. `matching-engine` biến nó thành **effectively-once**: trong cùng một DB transaction, `INSERT INTO processed_event ... ON CONFLICT DO NOTHING` (atomic check-and-set bằng chính DB, không cần lock ứng dụng) — trả 0 row nghĩa là đã xử lý → bỏ qua; trả 1 row thì ghi execution.

Offset Kafka commit **sau** khi DB transaction commit → crash giữa chừng chỉ gây redelivery, và redelivery bị dedupe nuốt. Đây là cách chuẩn công nghiệp đạt "exactly-once nghiệp vụ" mà không dùng Kafka transactions (vốn chỉ exactly-once trong phạm vi Kafka→Kafka, không phủ được DB).

**Dedupe 3 lớp của toàn hệ:** Redis fast-path (cache) → UNIQUE(client_order_id) ở order-service (chốt tuyệt đối) → processed_event PK ở matching-engine (chốt phía consumer). Mỗi lớp hỏng, lớp sau đỡ.

## 11. Redis idempotency tập trung — cache, KHÔNG phải source of truth

Thay cache per-pod cũ (lỗ hổng: retry rơi vào pod khác là lọt). Redis GET/SET với TTL 10' cho mọi pod nhìn chung một trạng thái. Điểm thiết kế quan trọng nhất: **dedupe đúng-sai nằm ở DB constraint, Redis chỉ tăng tốc**. Hệ quả:
- Redis chết → hệ vẫn ĐÚNG, chỉ chậm hơn chút (mọi retry đâm xuống DB)
- Vì vậy `management.health.redis.enabled=false` — Redis chết không được kéo readiness fail, nếu không K8s rút pod khỏi Service dù pod vẫn phục vụ tốt
- Timeout Redis 200ms — cache chậm không được kéo chậm lệnh

## 12. Avro + Schema Registry — schema evolution có cưỡng chế

JSON của V1/V2 có nhược điểm: đổi contract là "thoả thuận miệng" — producer đổi field, consumer chết lúc runtime, đúng loại schema drift bạn từng vật lộn trong migration DB2→MySQL. Avro + Registry biến contract thành thứ **máy cưỡng chế được**:
- Producer đăng ký schema với Registry; Registry check tương thích BACKWARD → deploy phá vỡ consumer **fail ngay lần produce đầu tiên**, không âm thầm giết consumer
- Message binary chỉ mang schema ID 4 byte → nhỏ hơn JSON 3–5 lần (không lặp tên field mỗi message) — đáng kể với 12 partition tick chảy liên tục
- **Trade-off:** thêm 1 hạ tầng phải vận hành (Registry là SPOF nếu 1 replica — nhưng client cache schema nên Registry chết ngắn hạn không chặn traffic đang chạy); build phụ thuộc Confluent repo; debug khó hơn (không `kafka-console-consumer` đọc mắt thường được, cần `kafka-avro-console-consumer`)
- **Phạm vi có chủ đích:** chỉ `stock.ticks` chuyển Avro làm mẫu; `stock.enriched`/`stock.orders` vẫn JSON — migrate từng topic một là cách làm thực tế, và code minh hoạ luôn pattern "tách wire format khỏi domain model, map tại biên" (TickEventAvro ↔ TickEvent record)
- `auto.register.schemas=true` chỉ cho demo; production đặt false, schema đăng ký qua CI/CD có review

## 13. Resilience4j — fail-fast thay vì chết chùm

- **RateLimiter (controller, 200 req/s, timeout 0):** client bug retry vô hạn bị chặn bằng HTTP 429 trước khi đè chết DB/Kafka. `timeout-duration: 0` = không xếp hàng — với lệnh trading, chờ trong queue rồi khớp trễ còn tệ hơn từ chối ngay.
- **CircuitBreaker "orders-db" (service):** DB chết → sau ngưỡng 50% lỗi breaker MỞ, request fail-fast ngay thay vì mỗi request treo chờ timeout DB đến cạn thread pool (cascading failure). Fallback trả REJECTED SYSTEM_UNAVAILABLE — fail-safe: không chắc chắn thì không nhận lệnh.
- Hai annotation đặt ở **hai tầng khác nhau** (RateLimiter ở controller, CircuitBreaker ở service) có chủ đích — tránh phải cấu hình aspect order phức tạp khi chồng annotation.
- DataIntegrityViolationException được catch **bên trong** placeOrder → breaker chỉ nhìn thấy lỗi hạ tầng thật, duplicate lệnh không làm breaker mở nhầm.

## 14. Testcontainers — test bằng Kafka thật, Postgres thật

Mock Kafka không bắt được lỗi thật (serialization, partition assignment, rebalance, timing). Testcontainers spin container thật trong test:
- `TickToMarketDataIT`: bơm 50 tick Avro qua Kafka thật → assert REST trả enriched data có volatility/riskLevel. Registry dùng `mock://scope` — producer test và consumer app cùng JVM chia sẻ registry in-memory, khỏi cần container Registry.
- `IdempotentConsumerIT`: **test quan trọng nhất hệ thống** — gửi cùng 1 order 3 lần, khẳng định đúng 1 execution, và giữ khẳng định thêm 3 giây đề phòng message trễ.
- **Trade-off:** test chậm (~30–60s vì kéo image + khởi động container) và cần Docker trên máy chạy test. Đổi lại là niềm tin thật sự vào pipeline. Chạy: `mvn verify` (máy có Docker).

## 15. Grafana dashboard as code

Dashboard JSON nằm trong repo, Grafana tự provision lúc khởi động — không còn "dashboard ai đó vẽ tay rồi mất khi container chết". 6 panel: E2E latency p50/95/99, tick throughput, orders accepted/rejected/duplicate, outbox+matching rate, HTTP p99 theo service, JVM heap. Nguyên tắc chọn metric: mỗi panel trả lời một câu hỏi vận hành cụ thể ("outbox có tắc không?" = published/s so với accepted/s).

## 16. CI/CD GitHub Actions

`mvn verify` chạy cả integration test (GitHub runner có sẵn Docker cho Testcontainers) trên mọi PR — lỗi dual-write hay dedupe hỏng bị bắt trước khi merge. Merge vào main → build 4 Docker image song song (matrix). Push registry để sẵn dạng comment vì cần cấu hình secret.

## Bảng tổng: nhược điểm V2 → giải pháp V3 → cái giá mới

| Nhược điểm V2 | Giải pháp V3 | Cái giá phải trả |
|---|---|---|
| Dual-write DB/Kafka không nguyên tử | Transactional Outbox | Thêm bảng + relay; độ trễ publish ~500ms |
| Consumer xử lý trùng khi redelivery | Idempotent Consumer (processed_event) | Thêm 1 INSERT mỗi message |
| Idempotency per-pod, retry lọt pod khác | Redis tập trung + DB UNIQUE | Thêm Redis phải vận hành (nhưng chết không sao) |
| Contract JSON = thoả thuận miệng | Avro + Schema Registry (stock.ticks) | Thêm Registry; debug khó hơn; phụ thuộc Confluent repo |
| DB chết → thread pool cạn dần | CircuitBreaker fail-fast | Cấu hình ngưỡng cần tinh chỉnh theo tải thật |
| Client loạn đè chết hệ thống | RateLimiter 429 | Ngưỡng per-pod, scale pod thì tổng ngưỡng tăng theo |
| Không có test | 2 integration test Testcontainers | Test chậm, cần Docker |
| Build/test thủ công | GitHub Actions CI | — |

**Điểm dừng có chủ đích (để bạn biết còn gì phía trước):** Debezium CDC thay polling relay; Avro cho 2 topic còn lại; Kafka ACL + TLS; Redis HA; Registry HA; saga bồi hoàn khi matching fail (hiện matching stub không fail nghiệp vụ).

---

# V4 — Vòng đời lệnh trọn vẹn, CDC, bảo mật, và kiểm chứng

V3 giải quyết "làm sao ghi DB và Kafka nguyên tử". V4 trả lời câu hỏi kế tiếp, khó hơn: **lệnh sau khi ra khỏi order-service thì kết thúc ở đâu?** Cùng với đó là ba mảng một hệ thống trading thật không thể thiếu: bảo mật đường truyền, khả năng đổi cơ chế publish, và số đo hiệu năng thay cho lời hứa.

## 17. Saga choreography — chiều về của lệnh

**Lỗ hổng nghiêm trọng nhất của V3:** order-service trả `ACCEPTED` rồi... hết. Lệnh vào Kafka, matching-engine xử lý, nhưng order-service **không bao giờ biết kết quả**. Bảng `orders` mãi mãi ở trạng thái ACCEPTED. Với hệ thống trading, đây không phải thiếu sót thẩm mỹ: khách hàng bị giữ ký quỹ cho một lệnh mà hệ thống không biết đã khớp hay chưa.

V4 khép vòng bằng **saga choreography** (không có orchestrator trung tâm, mỗi service tự phản ứng với event):

```
order-service --stock.orders--> matching-engine --stock.executions--> order-service
     (outbox)                        (outbox)                      (state machine)
```

Điểm quan trọng: **matching-engine cũng có outbox của riêng nó**. Nếu execution report được publish ngoài transaction, sẽ có trường hợp lệnh đã ghi `execution` mà report mất — order-service treo ACCEPTED vĩnh viễn, đúng cái ta vừa đi sửa. Ba việc (`processed_event` + `execution` + `outbox_event`) nằm trong **một transaction duy nhất**.

**Choreography vs Orchestration:**

| | Choreography (đã chọn) | Orchestration (saga coordinator) |
|---|---|---|
| Coupling | Thấp — service chỉ biết event | Cao — coordinator biết mọi bước |
| Thêm bước mới | Thêm consumer, không sửa ai | Sửa coordinator |
| Nhìn thấy toàn cảnh | Khó — phải ghép trace | Dễ — một chỗ định nghĩa flow |
| Hợp với | Luồng ngắn 2–3 bước | Luồng dài, nhiều nhánh bồi hoàn |

Luồng này chỉ 2 chặng nên choreography thắng rõ. **Nhưng cần nói thẳng cái giá:** khi thêm chặng thứ ba và thứ tư (khớp → thanh toán → cập nhật vị thế → thông báo), choreography biến thành "event spaghetti" — không ai đọc code mà biết được toàn bộ luồng, phải dựng lại từ Zipkin trace. Đó là điểm nên chuyển sang orchestration.

## 18. Máy trạng thái lệnh + optimistic locking

`ACCEPTED → FILLED | REJ_MATCH`, các trạng thái cuối không có đường ra.

Lợi ích kép thú vị: **máy trạng thái tự nó là một lớp idempotency miễn phí**. Execution report tới lần thứ hai thấy lệnh đã ở trạng thái cuối → transition không hợp lệ → bỏ qua. Không cần bảng `processed_event` ở phía order-service. Một cơ chế, hai công dụng.

`@Version` (optimistic locking) chống **lost update**: hai report tới song song trên hai pod cùng UPDATE một lệnh, pod thua nhận `OptimisticLockingFailureException` và retry trên dữ liệu mới thay vì âm thầm ghi đè.

- **Vì sao optimistic chứ không pessimistic:** xung đột ở đây *hiếm* (cùng một `clientOrderId` được key theo symbol nên thường vào cùng partition, cùng consumer). Khoá bi quan bắt mọi update trả giá lock chỉ để phòng trường hợp hiếm. Optimistic trả giá đúng lúc có xung đột thật.
- **Cái giá:** retry phải nằm **ngoài** `@Transactional` — transaction đã rollback rồi mới ném exception, nên phải mở transaction mới. Đây là lỗi kinh điển: gắn `@Retryable` chung chỗ với `@Transactional` sẽ retry trong một transaction chết.
- **Giới hạn có chủ đích:** chưa hỗ trợ khớp một phần (partial fill). Muốn có thì cần `PARTIALLY_FILLED` với self-loop và cộng dồn `filledQty` — lúc đó máy trạng thái không còn "terminal thì đóng" đơn giản nữa.

## 19. Luật sàn ở matching-engine — vì sao cố tình kiểm tra hai lần

| Tầng | Bảo vệ ai | Ví dụ |
|---|---|---|
| order-service pre-trade | Khách hàng | fat-finger 5%, dữ liệu cũ, mã rủi ro cao |
| matching-engine `TradingRules` | Sàn (nguồn sự thật) | biên độ 7%, lô chẵn 100, phiên giao dịch |

Trùng lặp là **có chủ đích**, không phải thừa. Kiểm tra ở order-service từ chối sớm cho nhanh và tiết kiệm tài nguyên, nhưng **không được tin tưởng**: client có thể publish thẳng vào topic, hoặc tham số hai bên lệch nhau sau một lần deploy. Sàn luôn kiểm tra lại. Chính vì lệnh qua được cửa một vẫn có thể chết ở cửa hai, saga mới cần chiều về.

`session-check-enabled: false` mặc định để demo chạy được 24/7 — một ví dụ nhỏ về việc **cấu hình nên phục vụ được cả môi trường dev lẫn thật**, thay vì hardcode giờ giao dịch rồi tự hỏi sao test đêm nào cũng đỏ.

## 20. Module `outbox-support` — chia sẻ hành vi, không chia sẻ cấu trúc

Hai service giờ cùng có outbox. Cách dễ nhất là tạo một `@Entity` dùng chung. **Đã cố tình không làm vậy.** Shared JPA entity kéo theo ràng buộc schema, cấu hình Hibernate và vòng đời migration chung giữa các service — đúng loại coupling mà microservices sinh ra để tránh; đổi một cột là hai service phải deploy cùng lúc.

Thay vào đó: module chung chỉ chứa **port** (`OutboxRecord`, `OutboxStore`) và **hành vi** (`OutboxRelay`). Mỗi service tự sở hữu bảng, entity, native query của mình và cài đặt adapter. Chia sẻ hành vi, không chia sẻ cấu trúc dữ liệu.

- **Ưu:** hết trùng code relay; mỗi service tự do đổi schema outbox
- **Nhược:** thêm một tầng gián tiếp (đọc code phải nhảy qua interface); và một module chung vẫn là một điểm phối hợp — sửa interface là phải build lại tất cả. Đây là lý do module này cố tình giữ **rất nhỏ**: ba file, không logic nghiệp vụ.

## 21. Debezium CDC — đổi cơ chế publish mà không đổi schema

`outbox.relay.enabled=false` là tắt polling relay; Debezium `EventRouter` đọc thẳng WAL của Postgres và phát ra Kafka **message giống hệt** — consumer không sửa một dòng. Đây chính là phần thưởng thật sự của outbox pattern: nó tách *ghi sự kiện* khỏi *cách chuyển sự kiện đi*.

| | Polling relay | Debezium CDC |
|---|---|---|
| Độ trễ | ~500ms | ~mili-giây |
| Tải DB | Query định kỳ + UPDATE mark | Chỉ đọc WAL |
| Hạ tầng | Không thêm gì | Kafka Connect + connector |
| Rủi ro vận hành | Thấp, debug bằng SQL | **Replication slot** |

**Rủi ro lớn nhất của CDC** đáng nhắc riêng: replication slot đảm bảo không mất event, nhưng nếu connector chết mà không ai để ý, Postgres giữ lại toàn bộ WAL từ điểm đó và **đĩa đầy dần cho tới khi DB dừng**. Bắt buộc phải alert trên độ trễ của `pg_replication_slots`. Với hệ này polling 500ms là đủ; CDC đáng chuyển khi outbox đủ lớn để query polling gây tải, hoặc khi cần độ trễ dưới 100ms.

Cảnh báo cấu hình: bật CDC mà **quên tắt relay** thì cả hai cùng publish → event nhân đôi. Consumer dedupe được nên không sai kết quả, nhưng tốn băng thông và làm nhiễu mọi metric.

## 22. Bảo mật Kafka — ba lớp, ba mục đích

Dev chạy PLAINTEXT. Production thiếu bảo mật nghĩa là *bất kỳ ai vào được mạng cluster đều đọc được toàn bộ dòng lệnh của khách hàng*.

| Lớp | Chống gì | Thiếu nó thì sao |
|---|---|---|
| TLS | Nghe lén đường truyền | Lệnh đi dạng rõ trong mạng |
| SASL/SCRAM | Không biết client là ai | Ai kết nối được là dùng được |
| ACL | Biết là ai nhưng làm quá quyền | Một service bị chiếm = mất tất cả |

Nguyên tắc áp dụng trong `security/create-acls.sh`: **mỗi service một principal, đặc quyền tối thiểu**. `market-gateway` chỉ có quyền Write `stock.ticks` — gateway bị chiếm cũng không đọc được lệnh của ai. Không service nào được `--operation All`.

**Cái giá phải trả, nói thẳng:** TLS làm broker **mất zero-copy** (dữ liệu phải qua tầng mã hoá thay vì đi thẳng từ page cache ra socket) → throughput topic tick giảm đáng kể. Phải đo lại trước khi cam kết SLA, đừng bật TLS xong vẫn hứa con số cũ. Thêm nữa: xoay vòng chứng chỉ thành việc định kỳ, và cert hết hạn = cả hệ dừng — một dạng sự cố hẹn giờ.

## 23. Load test k6 — biến hiệu năng thành hợp đồng

Suốt V1–V3, mọi con số latency đều là *lý luận thiết kế*, chưa cái nào được đo. `loadtest/order-load.js` đặt threshold `p95<200ms, p99<500ms` — vi phạm là **fail**, nên hiệu năng trở thành hợp đồng kiểm chứng được chứ không phải kỳ vọng.

Kịch bản cố tình đẩy lên 200 VU để **vượt** RateLimiter và quan sát 429 xuất hiện đúng lúc. `clientOrderId` duy nhất mỗi request để đo đường đặt lệnh thật, không phải đo tốc độ Redis trả cache — một cái bẫy dễ mắc khiến kết quả đẹp giả tạo.

**Giới hạn phải thừa nhận:** chạy trên một máy dev, Kafka + Postgres + 4 service + cả k6 tranh nhau CPU. Số này dùng để **so sánh trước/sau một thay đổi code**, không dùng cam kết SLA. Số thật cần môi trường tách riêng, dữ liệu tick thật, và chạy đủ lâu để thấy ảnh hưởng GC.

## 24. Vận hành: graceful shutdown, PDB, KEDA

- **`server.shutdown: graceful` + `terminationGracePeriodSeconds: 45`**: mặc định K8s cho 30s rồi SIGKILL — sát nút với Kafka listener đang cần commit offset cuối. Bị giết giữa chừng không làm *sai* (dedupe đỡ) nhưng tạo redelivery vô ích và làm nhiễu metric.
- **PodDisruptionBudget**: không có PDB, một lệnh `kubectl drain` lúc nâng cấp cluster có thể rút sạch consumer của một group — dừng xử lý lệnh giữa phiên giao dịch.
- **KEDA cho matching-engine** với `lagThreshold: 500` (thấp hơn 1000 của tick): lệnh nhạy cảm hơn tick, chậm một giây là khách thấy. `maxReplicaCount: 4` vì 12 partition / concurrency 3 = 4 pod là trần hữu ích — pod thứ 5 ngồi không.

## 25. Hai thứ cố tình KHÔNG làm (và vì sao)

Nói rõ để bạn biết đây là lựa chọn, không phải bỏ sót.

**Avro cho `stock.orders` và `stock.executions`:** payload outbox nằm trong cột `TEXT` của Postgres. Nhét Avro binary vào cột text đòi hỏi base64 (phình ~33%, mất khả năng đọc bằng mắt khi debug SQL) hoặc để relay serialize lúc publish (relay phải biết kiểu dữ liệu — mất tính tổng quát vừa xây ở mục 20). JSON trong outbox là lựa chọn đúng ở đây. `stock.enriched` thì khác — không vướng outbox — nhưng hiện chỉ có **một** consumer, chi phí governance chưa đáng; khi có consumer thứ hai, thứ ba thì Avro mới trả lãi.

**Kafka transactions (`exactly-once semantics`):** hấp dẫn trên giấy nhưng chỉ exactly-once trong phạm vi Kafka→Kafka. Cặp Outbox + Idempotent Consumer đã phủ được cả DB lẫn Kafka, và không trả giá throughput của transaction coordinator.

## Bảng tổng: nhược điểm V3 → giải pháp V4 → cái giá mới

| Nhược điểm V3 | Giải pháp V4 | Cái giá phải trả |
|---|---|---|
| Lệnh treo ACCEPTED vĩnh viễn, không có chiều về | Saga choreography + outbox ở matching-engine | Thêm topic, thêm bảng; luồng dài hơn sẽ thành event spaghetti |
| Không có vòng đời lệnh, ghi đè trạng thái tuỳ tiện | State machine + `@Version` | Retry phải ở ngoài transaction; chưa hỗ trợ partial fill |
| Matching-engine nhận mọi lệnh không kiểm tra | `TradingRules` (biên độ, lô, phiên) | Luật trùng một phần với pre-trade → phải đồng bộ tham số hai nơi |
| Code relay trùng lặp giữa các service | Module `outbox-support` (port/adapter) | Thêm tầng gián tiếp; module chung là điểm phối hợp |
| Relay polling là cách publish duy nhất | Debezium CDC profile, đổi bằng 1 property | Kafka Connect phải vận hành; **replication slot có thể làm đầy đĩa** |
| Kafka mở toang, không auth | SASL/SCRAM + TLS + ACL đặc quyền tối thiểu | TLS mất zero-copy → giảm throughput; cert hết hạn = hệ dừng |
| Latency chỉ là lý luận | k6 với threshold fail-build | Số trên máy dev chỉ để so sánh tương đối |
| Pod bị giết đột ngột khi drain/deploy | Graceful shutdown + PDB + KEDA | Deploy chậm hơn; PDB có thể chặn drain nếu đặt quá chặt |

**Còn lại phía trước:** partial fill + order book thật (hiện matching là stub fill toàn bộ); position/buying-power service để pre-trade check dựa trên sức mua thật; Redis HA và Schema Registry HA; multi-broker Kafka với `min.insync.replicas=2` (hiện 1 broker — mọi bàn về `acks=all` chỉ thật sự có ý nghĩa khi có replica); job dọn bảng outbox; và chaos testing (giết pod giữa phiên để chứng minh những gì tài liệu này khẳng định).
