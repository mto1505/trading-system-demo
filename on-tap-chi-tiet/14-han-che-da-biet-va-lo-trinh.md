# Chương 14 — Hạn Chế Đã Biết & Lộ Trình

> **Vì sao chương này quan trọng nhất khi phỏng vấn:** ai cũng kể được điều hệ thống của mình làm tốt. Thứ phân biệt một kỹ sư senior là khả năng **chỉ ra chính xác chỗ hệ thống của mình chưa đủ, vì sao, và sửa thế nào** — bằng vị trí dòng mã, không phải bằng cảm giác.
>
> **Nguồn:** `DEEP-DIVE.md` mục 11 (22 điểm gốc) + các phát hiện bổ sung khi đối chiếu lại toàn bộ mã nguồn cho bộ tài liệu này.

---

## 1. Phân loại

| Loại | Nghĩa | Mức nguy hiểm |
|---|---|---|
| **Bug** | Sai hành vi — code làm khác điều nó định làm | 🔴 Cao |
| **Gap** | Cơ chế **không phủ được tình huống nó định phủ** | 🔴 Cao (vì tạo cảm giác an toàn giả) |
| **Doc** | Tài liệu / comment lệch mã | 🟡 Trung bình |
| **Ops** | Vấn đề tích luỹ theo thời gian vận hành | 🟡 Trung bình → 🔴 Cao theo thời gian |

🔑 **Gap nguy hiểm hơn Bug.** Một bug gây lỗi thấy được; một gap làm ta *tin* là đã được bảo vệ trong khi không. Ví dụ rõ nhất là #1: DLT được cấu hình đầy đủ, trông như đã chống poison pill, nhưng lỗi deserialize không bao giờ đi qua nó.

---

## 2. Bảng tổng hợp — 22 điểm gốc

| # | Loại | Vị trí | Vấn đề | Hướng sửa | Trạng thái |
|:--:|:--:|---|---|---|:--:|
| 1 | **Gap** | `pricing/config/KafkaConfig.java:44` | Không có `ErrorHandlingDeserializer` → lỗi deserialize xảy ra trong `poll()`, DLT không bắt được | Bọc `KafkaAvroDeserializer`; viết IT bơm byte rác | ⬜ |
| 2 | **Gap** | `pricing/consumer/TickConsumer.java:52` | Batch listener không ném `BatchListenerFailedException` → 1 tick lỗi đẩy cả lô 1000 tick vào DLT | Bọc vòng lặp, ném `BatchListenerFailedException(msg, e, i)` | ⬜ |
| 3 | **Ops** | `pricing/service/SnapshotStore.java:48-68` | `latest` không bao giờ xoá → flush ghi lại mọi symbol kể cả khi không đổi (~17.280 row/symbol/ngày) | Flush theo diff với `lastFlushed`; thêm retention/partition | ⬜ |
| 4 | **Bug** | `order/service/OrderService.java:97` | `orElseThrow()` biến mọi `DataIntegrityViolationException` khác thành `NoSuchElementException`, che lỗi gốc | `orElseThrow(() -> dup)`, hoặc kiểm tên constraint | ⬜ |
| 5 | **Ops** | `order/service/OrderService.java:112` | Staleness 10s là `@Value` toàn cục → mã thanh khoản thấp bị từ chối oan | Ngưỡng theo nhóm mã | ⬜ |
| 6 | **Gap** | `order/service/OrderService.java:112` | Đồng hồ pricing chạy nhanh → hiệu số âm → check bị vô hiệu hoá âm thầm | Counter + từ chối khi lệch bất thường | ⬜ |
| 7 | Bug | `k8s/legacy/08-keda-scaledobject.yaml:29` | Comment hiểu `lagThreshold` là per-partition; thực tế là per-replica | — | ✅ Đã sửa (`components/keda/scaledobject-pricing-risk.yaml:38-41`) |
| 8 | Doc | `k8s/legacy/08-keda-scaledobject.yaml:23,47` | `cooldownPeriod` chỉ áp cho scale về 0 — không chống flapping như comment nói | — | ✅ Đã sửa (thay bằng `behavior.scaleDown.stabilizationWindowSeconds`) |
| 9 | Doc | `k8s/legacy/08-keda-scaledobject.yaml:30` | `activationLagThreshold` vô tác dụng vì `minReplicaCount: 2` | — | ✅ Đã ghi rõ (`:42-45`) |
| 10 | Doc | `k8s/legacy/04-pricing-risk.yaml:7,62-64` | Comment còn nói "6 partition", đã là 12 | — | ✅ Partition thành tham số môi trường (3/6/12) |
| 11 | Doc | `DESIGN-NOTES.md:158-165` | Mô tả bản v2 (map per-pod); code đã là v3 (Redis + UNIQUE constraint) | Viết lại mục V2.7 theo 3 lớp hiện tại | ⬜ |
| 12 | **Gap** | `matching-engine` `processed_event` | Bảng phình vô hạn — nhưng dọn **sớm hơn retention của `stock.orders`** sẽ mở cửa cho khớp lệnh hai lần khi replay | Job dọn theo `processed_at`, giữ **lâu hơn** retention topic; comment chéo | ⬜ |
| 13 | **Ops** | `stock.ticks.DLT` | Không có alert → DLT không ai đọc thì tương đương skip | Alert khi message count > 0; runbook replay | 🟡 Một phần — rule có nhưng **không bắn** (mục 3.2) |
| 14 | Ops | KEDA ↔ `setConcurrency(3)` | `maxReplicaCount: 4` phải khớp `12 / concurrency` nhưng ở hai file khác nhau | — | ✅ CI chặn merge (`k8s-validate.yml:59-78`) — nhưng `/3` hardcode |
| 15 | **Gap** | `matching/entity/ExecutionEntity.java:24-25` | `client_order_id` **không** có `unique` → `processed_event` là lớp chặn khớp-trùng **duy nhất**, khác hẳn `OrderEntity.java:27-28` | Thêm `unique = true` + migration `V3` | ⬜ |
| 16 | **Gap** | Mọi `TopicBuilder` (6 chỗ) + `ACKS_CONFIG="all"` (3 chỗ) | Tất cả topic khai `.replicas(1)` và cluster chỉ 1 broker → `acks=all` ≡ `acks=1`. Độ bền của outbox producer và DLT producer hiện chỉ là **danh nghĩa** | Multi-broker + `min.insync.replicas=2` | ⬜ |
| 17 | Doc | `OutboxRelay.java:62-67` | Rollback **cả batch** nên là **mọi event đã gửi trong batch** (tới 100) bị gửi lại, không phải một | Ghi rõ quy mô; hành vi là cố ý | ⬜ |
| 18 | **Bug** | `k8s/base/infra/postgres.yaml:96` | `POSTGRES_PASSWORD` chỉ có tác dụng lúc `initdb`. Sửa `secrets.env` → hash đổi → restart cả DB lẫn service → app dùng mật khẩu mới, DB giữ cũ → **sập cả môi trường**. `wait-for-db` vẫn pass nên triệu chứng chỉ lộ ở tầng ứng dụng | `ALTER ROLE` trong DB **trước**, rồi mới sửa `secrets.env`. Triệt để: tách Secret, hoặc CloudNativePG | 🟡 Đã thêm cảnh báo tại chỗ |
| 19 | **Gap** | `k8s/overlays/prod/externalsecret.yaml` | Secret do ESO dựng **không có hash** → cập nhật xong không pod nào restart; `secretKeyRef` chỉ đọc **một lần lúc start** | Trước mắt: `rollout restart` (đã vào runbook). Triệt để: Reloader, hoặc mount volume | 🟡 Một phần |
| 20 | **Gap** | `market-gateway`, `pricing-risk-service` `application.yml` | Thiếu `server.shutdown: graceful` + `spring.lifecycle.timeout-per-shutdown-phase`. Kafka listener vẫn dừng êm, nhưng **Tomcat đóng ngay** → request REST đang bay bị cắt, vô hiệu hoá `preStop: sleep 5` | Thêm hai dòng; giữ `preStop + timeout ≤ terminationGracePeriodSeconds` | ⬜ |
| 21 | **Ops** | `k8s/overlays/*/kustomization.yaml` ↔ `components/keda` | `replicas == minReplicaCount` chỉ **giảm xác suất** giằng co với autoscaler: apply đúng lúc KEDA giữ 4 pod vẫn gây 2 lần rebalance | Bỏ `replicas` khỏi overlay + `ignoreDifferences` trên `/spec/replicas` | ⬜ |
| 22 | **Gap** | Vùng phủ của dev | dev có 3 partition / 1 pod / 3 consumer → một consumer **không bao giờ ôm quá 1 partition**. Mọi bug phụ thuộc việc một thread xử lý xen kẽ hai partition (batch 1000 bản ghi trộn hai nhóm symbol) không lộ ra ở dev | Không tăng partition dev; dựa vào staging ở `minReplicaCount: 1` (6/3 = 2 partition/consumer); thêm IT với 2 partition trên 1 consumer | ⬜ |

**Thống kê:** 4 đã sửa hoàn toàn ✅ · 3 sửa một phần 🟡 · 15 còn mở ⬜

---

## 3. Phát hiện bổ sung khi viết bộ tài liệu này

Các điểm dưới đây **không** có trong 22 điểm gốc, được phát hiện khi đối chiếu lại toàn bộ mã nguồn.

### 3.1. Gap — `stock.executions` không có DLT thật

| | |
|---|---|
| **Vị trí** | `order-service/consumer/ExecutionReportConsumer.java:36` ↔ `order/config/KafkaConfig.java:97-104` |
| **Vấn đề** | Comment nói *"Ném ra → error handler retry rồi đẩy DLT (đã cấu hình ở container factory)"*. Nhưng `executionListenerFactory` **không** đặt `setCommonErrorHandler` với `DeadLetterPublishingRecoverer` — chỉ `pricing-risk-service` có. Thực tế: `DefaultErrorHandler` mặc định retry 10 lần không backoff rồi **log và bỏ qua**. Topic `stock.executions.DLT` được Job tạo ra nhưng **không ai ghi vào** |
| **Hậu quả** | Execution report không xử lý được (ví dụ optimistic lock thất bại 3 lần liên tiếp) sẽ **mất âm thầm** ⇒ lệnh treo `ACCEPTED` vĩnh viễn |
| **Sửa** | Thêm recoverer cho `executionListenerFactory` (và cho `orderListenerFactory` của matching-engine), hoặc sửa comment cho đúng |

### 3.2. Gap — Alert DLT không bao giờ bắn

| | |
|---|---|
| **Vị trí** | `k8s/components/monitoring/prometheusrule.yaml` |
| **Vấn đề** | `expr: sum(rate(kafka_consumer_fetch_manager_records_consumed_total{topic=~"stock\..*\.DLT"}[5m])) > 0` — metric này chỉ tồn tại khi **có consumer đang đọc** topic đó. Hiện không service nào consume DLT ⇒ metric luôn 0 ⇒ alert không bao giờ bắn |
| **Hậu quả** | 🔑 **Alert tồn tại nhưng không chạy còn nguy hiểm hơn không có alert** — nó tạo cảm giác an toàn giả. Đây chính là điểm #13 chưa được đóng thật sự |
| **Sửa** | `kafka_exporter` đo số message trong topic từ phía broker, hoặc thêm counter ở phía `DeadLetterPublishingRecoverer` |

### 3.3. Gap — Không có Alertmanager

| | |
|---|---|
| **Vị trí** | Toàn hệ monitoring |
| **Vấn đề** | 6 alert rule được Prometheus đánh giá và hiển thị ở UI của nó, nhưng **không có Alertmanager** ⇒ không routing, không dedupe, không silence, không escalation, **không ai được thông báo** |
| **Hậu quả** | Về thực chất là *có alert mà không có cảnh báo*. `SagaStuckOrders` bắn lúc 3 giờ sáng thì không ai biết cho tới sáng hôm sau |
| **Sửa** | Thêm Alertmanager + routing tới Slack/PagerDuty/email |

### 3.4. Ops — Không có dashboard Grafana

| | |
|---|---|
| **Vị trí** | `monitoring/grafana-dashboards.yml` |
| **Vấn đề** | Provider khai `path: /var/lib/grafana/dashboards`, `docker-compose.yml` mount `./monitoring/dashboards`, nhưng **thư mục đó không tồn tại trong repo** |
| **Hậu quả** | Grafana khởi động được nhưng không có dashboard nào; phải tự dựng từ đầu |
| **Sửa** | Thêm dashboard JSON (ít nhất: saga health, latency p99, outbox pending, consumer lag) |

### 3.5. Gap — `SagaStuckOrders` dùng phép trừ counter thô

| | |
|---|---|
| **Vị trí** | `monitoring/alert-rules.yml:27`, `prometheusrule.yaml` |
| **Vấn đề** | `(orders_accepted_total - (orders_filled_total + orders_rejected_by_matching_total)) > 10`. Micrometer counter **reset về 0 khi pod restart**. `rate()`/`increase()` xử lý được counter reset, nhưng **phép trừ hai counter thô thì không** |
| **Hậu quả** | Sau rolling update, ba counter reset ở thời điểm hơi khác nhau ⇒ hiệu số có thể âm hoặc dương giả ⇒ alert kêu oan hoặc **im lặng sai** |
| **Sửa** | `sum(increase(...[10m]))` cho cả ba, hoặc tốt nhất là một **gauge** đếm trực tiếp lệnh `ACCEPTED` quá hạn từ `idx_orders_pending` |

### 3.6. Ops — Counter `ignored` gộp hai tình huống khác hẳn nhau

| | |
|---|---|
| **Vị trí** | `order/service/OrderStatusService.java:44` và `:61` |
| **Vấn đề** | Cùng counter `orders.report.ignored` tăng cho *"lệnh không tồn tại"* (bất thường — nghĩa là ai đó publish thẳng vào Kafka) và *"report trùng"* (hoàn toàn bình thường) |
| **Hậu quả** | Không thể đặt alert cho tình huống bất thường vì nó lẫn với tình huống bình thường |
| **Sửa** | Tách `orders.report.orphan` và `orders.report.duplicate` |

### 3.7. Ops — Hai cơ chế nuốt lỗi không có counter

| | |
|---|---|
| **Vị trí** | `pricing/service/SnapshotStore.java:65` và `order/service/OrderService.java:84` |
| **Vấn đề** | Cả hai `catch (Exception)` rồi `log.warn/error` mà **không tăng counter nào** |
| **Hậu quả** | DB chết ba ngày thì flush thất bại liên tục, service vẫn báo `UP`, API realtime vẫn trả dữ liệu, và **không ai biết lịch sử đã mất ba ngày**. Tương tự với Redis |
| **Sửa** | `snapshot_flush_failed_total`, `redis_unavailable_total` + alert. Đây là mẫu mà `OutboxRelay` đã làm đúng với `outbox.publish.failed` |

### 3.8. Ops — Bốn bảng chỉ tăng, không có job dọn

| Bảng | Tốc độ tăng | Dọn là vấn đề gì |
|---|---|---|
| `public.market_snapshot` | ~864.000 row/ngày (50 mã) | Dung lượng |
| `ordersvc.outbox_event` | 1 row/lệnh ACCEPTED | Dung lượng |
| `matchsvc.outbox_event` | 1 row/lệnh xử lý | Dung lượng |
| `matchsvc.processed_event` | 1 row/lệnh | 🔑 **Tính đúng đắn** (#12) |
| `ordersvc.orders` | 1 row/lệnh (giữ cả REJECTED) | Audit — **không được dọn** |

🔑 Lưu ý thứ tự: nên sửa **#15 trước** (thêm `UNIQUE` trên `execution`), vì khi đó dọn `processed_event` trở lại thành chuyện dung lượng thuần tuý thay vì chuyện đúng/sai.

### 3.9. Ops — Thiếu index cho endpoint liệt kê

| | |
|---|---|
| **Vị trí** | `ordersvc.orders` |
| **Vấn đề** | `OrderController.list` lọc theo `account_id`/`status` với `ORDER BY created_at DESC`, nhưng chỉ có `idx_orders_pending` (partial, `WHERE status='ACCEPTED'`) |
| **Hậu quả** | Seq scan + sort trên bảng chỉ tăng |
| **Sửa** | `CREATE INDEX idx_orders_account_created ON orders (account_id, created_at DESC);` và tương tự cho `status` |

### 3.10. Gap — `MarketDataController.history` không chặn `limit`

| | |
|---|---|
| **Vị trí** | `pricing/controller/MarketDataController.java:44` |
| **Vấn đề** | `@RequestParam(defaultValue = "50") int limit` không có trần, khác hẳn `OrderController.MAX_PAGE_SIZE = 200` |
| **Hậu quả** | `?limit=10000000` kéo hàng triệu row vào heap ⇒ OOM |
| **Sửa** | `Math.min(Math.max(limit, 1), 500)` |

### 3.11. Gap — Hai controller trả thẳng entity

| | |
|---|---|
| **Vị trí** | `MarketDataController.history()`, `ExecutionController.byClientOrderId()` |
| **Vấn đề** | Trả `MarketSnapshotEntity` / `ExecutionEntity`, trong khi `OrderView` đã đặt ra chuẩn DTO và giải thích rõ ba lý do |
| **Hậu quả** | Thêm cột nội bộ là tự động lọt ra API; đổi tên cột âm thầm phá hợp đồng |
| **Sửa** | `SnapshotView`, `ExecutionView` |

### 3.12. Doc — `ANNUALIZE` giả định sai tần suất tick

| | |
|---|---|
| **Vị trí** | `pricing/service/RiskCalculator.java:29` |
| **Vấn đề** | `Math.sqrt(252 * 6.5 * 3600)` giả định tick cách nhau ~1 giây. `SimulatedFeed` phát `@Scheduled(fixedRate = 200)` = 5 tick/giây/mã ⇒ hệ số năm hoá bị thiếu $\sqrt 5 \approx 2{,}24$ lần |
| **Hậu quả** | Volatility năm hoá **bị thổi lên ~2,24 lần**. Phân lớp tương đối vẫn có nghĩa (ngưỡng 15/35 được hiệu chỉnh cho dải này), nhưng con số tuyệt đối không dùng cho quyết định tài chính thật |
| **Sửa** | Đo khoảng cách tick trung bình trong cửa sổ và năm hoá theo nó; hoặc đưa `ANNUALIZE` thành `@Value` |

### 3.13. Ops — Ngưỡng `riskLevel` hardcode ở service khác

| | |
|---|---|
| **Vị trí** | `pricing/service/RiskCalculator.java:75` ↔ `order/service/OrderService.java:115` |
| **Vấn đề** | Ngưỡng 15/35 hardcode trong `RiskCalculator`, nhưng **hệ quả** của nó (từ chối lệnh) xảy ra ở `OrderService` — hai service, hai nhịp deploy |
| **Hậu quả** | Đội quant đổi ngưỡng "để phân loại chính xác hơn" có thể vô tình chặn đặt lệnh trên toàn thị trường |
| **Sửa** | `@Value` + cấu hình chung; hoặc truyền ngưỡng theo event |

### 3.14. Ops — `states` trong `RiskCalculator` không bao giờ xoá

| | |
|---|---|
| **Vị trí** | `pricing/service/RiskCalculator.java:33` |
| **Vấn đề** | `ConcurrentHashMap<String, SymbolState>` chỉ có `computeIfAbsent`, không bao giờ `remove` — cùng mẫu với `SnapshotStore.latest` (#3) |
| **Hậu quả** | Với số mã cố định thì chấp nhận được (~3KB/mã). Nếu ai đó bơm symbol sinh ngẫu nhiên vào `stock.ticks` thì map phình tới **OOM** |
| **Sửa** | Caffeine cache với `expireAfterAccess` |

### 3.15. Gap — Bất biến hai ngưỡng giá không ai cưỡng chế

| | |
|---|---|
| **Vị trí** | `order.max-price-deviation-pct` (5) ↔ `matching.price-band-pct` (7) |
| **Vấn đề** | Phải giữ `order ≤ matching`, nếu không hàng loạt lệnh qua cửa 1 rồi chết ở cửa 2. Không có gì cưỡng chế |
| **Hậu quả** | Client nhận `201 ACCEPTED` rồi 2 giây sau thành `REJ_MATCH` hàng loạt |
| **Sửa** | Kiểm ở CI hoặc ở startup |

### 3.16. Gap — ACL Kafka ↔ `GROUP_ID_CONFIG` không ai cưỡng chế

| | |
|---|---|
| **Vị trí** | `security/create-acls.sh` ↔ 4 chỗ `GROUP_ID_CONFIG` trong mã |
| **Vấn đề** | ACL cấp quyền `Read` trên group cụ thể. Đổi group ID trong Java mà quên sửa ACL ⇒ `GroupAuthorizationException` |
| **Hậu quả** | Service không consume được, lỗi không nói rõ nguyên nhân |
| **Sửa** | Sinh ACL script từ một nguồn khai báo chung; hoặc kiểm ở CI |

### 3.17. Ops — CI không push image ⇒ chuỗi CI→CD chưa khép kín

| | |
|---|---|
| **Vị trí** | `.github/workflows/ci.yml:37-41` |
| **Vấn đề** | Bước `docker push` bị comment ⇒ pipeline **build image rồi vứt đi** |
| **Hậu quả** | Không có artifact để deploy. Và overlay prod yêu cầu ghim **digest** (CI `k8s-validate` chặn merge nếu dùng tag), mà digest chỉ có sau khi push |
| **Sửa** | Bỏ comment + `permissions: packages: write` |

### 3.18. Ops — CI không validate Helm

| | |
|---|---|
| **Vị trí** | `.github/workflows/` |
| **Vấn đề** | `k8s-validate.yml` validate 3 overlay Kustomize rất kỹ, nhưng **không** có job nào `helm lint` / `helm template` |
| **Hậu quả** | Cạm bẫy `nil pointer` của Helm (xem [chương 11](./11-trien-khai-helm-va-ha-tang-production.md) mục 3.3) không bị bắt. Và hai bộ manifest đã lệch nhau mà không gì phát hiện |
| **Sửa** | Thêm job `helm lint` + `helm template` + `kubeconform` cho cả 4 file values |

### 3.19. Ops — Duy trì song song Helm và Kustomize

| | |
|---|---|
| **Vị trí** | `helm/` ↔ `k8s/` |
| **Vấn đề** | Hai bộ manifest cho **cùng một hệ thống** ⇒ mọi thay đổi phải làm hai lần |
| **Bằng chứng đã lệch** | Helm `values.yaml` thiếu route `/api/v1/executions` mà `k8s/base/apps/ingress.yaml` có |
| **Sửa** | Chọn một; hoặc Helm render + Kustomize patch output |

### 3.20. Bug nhẹ — `ticksDltTopic()` khai 1 partition

| | |
|---|---|
| **Vị trí** | `pricing/config/KafkaConfig.java:84-88` |
| **Vấn đề** | `TopicBuilder.name("stock.ticks.DLT").partitions(1)` trong khi topic gốc có 12. `DeadLetterPublishingRecoverer` gửi vào **đúng chỉ số partition** của bản ghi gốc ⇒ bản ghi ở partition 7 không có đích ⇒ **publish thất bại** |
| **Phạm vi** | Chỉ ảnh hưởng local (nơi code tạo topic). Trên cluster, Job `kafka-topics-init` tạo DLT cùng số partition và có comment giải thích đúng vấn đề này |
| **Sửa** | Đồng bộ `partitions` với topic gốc |

### 3.21. Ops — Bảo mật lớp ứng dụng

Tổng hợp từ [chương 12](./12-bao-mat-he-thong.md):

| Mức | Vấn đề |
|:--:|---|
| 🔴 | Kafka `PLAINTEXT`, không ACL ⇒ pod bị chiếm = **giả mạo được execution report** |
| 🔴 | `accountId` là bộ lọc, không phải ràng buộc quyền ⇒ xem được lệnh của mọi account |
| 🔴 | `POST /api/v1/ticks` không có filter ⇒ bơm giá giả ⇒ thao túng `refPrice` |
| 🟠 | API key so sánh bằng `.equals()` — không constant-time |
| 🟠 | Mặc định `demo-key`, không cảnh báo khi quên đặt |
| 🟠 | `@CrossOrigin("*")` trên cả 4 controller |
| 🟠 | `auto.register.schemas=true` |
| 🟡 | `/actuator/**` không được filter bảo vệ |
| 🟡 | Không có audit log |

---

## 4. Lộ trình ưu tiên

### 4.1. 🔴 P0 — Sai tính đúng đắn hoặc có lỗ hổng nghiêm trọng

| Việc | Điểm | Công sức | Vì sao ưu tiên cao nhất |
|---|:--:|---|---|
| **Thêm `UNIQUE` trên `execution.client_order_id`** | #15 | **Thấp** (1 migration) | `processed_event` đang là lớp chặn khớp-trùng duy nhất. Sửa xong thì #12 hạ từ "đúng/sai" xuống "dung lượng" |
| **`ErrorHandlingDeserializer`** | #1 | Thấp | DLT hiện **không bắt được** lỗi deserialize ⇒ consumer vào vòng lặp lỗi vô hạn |
| **`BatchListenerFailedException`** | #2 | Thấp | 1 tick lỗi đẩy **cả lô 1000** vào DLT |
| **Bật SASL_SSL + SCRAM + ACL Kafka** | 3.21 | Trung bình | Pod bị chiếm = giả mạo execution report. Mẫu đã có sẵn |
| **JWT + phân quyền theo `accountId`** | 3.21 | Trung bình | Lộ dữ liệu khách hàng |
| **Sửa `orElseThrow()`** | #4 | **1 dòng** | Che lỗi gốc, biến bug dễ sửa thành cuộc điều tra |
| **Filter cho `market-gateway`** | 3.21 | Thấp | Đường thao túng giá |
| **`server.shutdown: graceful` cho 2 service** | #20 | **2 dòng/service** | Cắt request mỗi lần rolling update |

### 4.2. 🟠 P1 — Mất khả năng quan sát hoặc tích luỹ nợ nhanh

| Việc | Điểm | Công sức |
|---|:--:|---|
| **Alertmanager** | 3.3 | Trung bình |
| **Sửa alert DLT** (kafka_exporter hoặc counter) | #13, 3.2 | Thấp |
| **Sửa công thức `SagaStuckOrders`** (gauge/increase) | 3.5 | Thấp |
| **Counter cho flush thất bại + lệch đồng hồ** | 3.7, #6 | Thấp |
| **Tách counter `ignored`** | 3.6 | Thấp |
| **Dashboard Grafana** | 3.4 | Trung bình |
| **Consumer lag metric** (`kafka_exporter`) | — | Thấp |
| **Flush theo diff cho `SnapshotStore`** | #3 | Thấp |
| **Job dọn 3 bảng outbox/processed** | #12, 3.8 | Trung bình |
| **Index cho endpoint liệt kê** | 3.9 | Thấp |
| **Chặn `limit` ở `history`** | 3.10 | 1 dòng |
| **DLT thật cho `stock.executions`** | 3.1 | Thấp |

### 4.3. 🟡 P2 — Nợ kỹ thuật và cải thiện chất lượng

| Việc | Điểm |
|---|:--:|
| Multi-broker Kafka + `min.insync.replicas=2` | #16 |
| Bỏ `replicas` khỏi overlay + `ignoreDifferences` | #21 |
| Reloader cho ESO secret | #19 |
| Tách Secret của Postgres khỏi Secret ứng dụng | #18 |
| IT với 2 partition trên 1 consumer | #22 |
| IT cho `OutboxRelay` (publish, rollback batch, `SKIP LOCKED` đa pod) | — |
| CI push image + `packages: write` | 3.17 |
| CI validate Helm | 3.18 |
| Chọn một giữa Helm và Kustomize | 3.19 |
| `ANNUALIZE` theo tần suất tick thực tế | 3.12 |
| Ngưỡng `riskLevel` và staleness thành cấu hình theo nhóm mã | #5, 3.13 |
| Caffeine cache cho `states` | 3.14 |
| DTO cho 2 controller còn lại | 3.11 |
| CI kiểm bất biến `order ≤ matching` price pct | 3.15 |
| Viết lại `DESIGN-NOTES.md` mục V2.7 | #11 |
| Partition `market_snapshot` theo ngày | 3.8 |
| OPA/Kyverno + quét image Trivy | 3.21 |

### 4.4. 🔵 P3 — Tính năng mới (không phải sửa lỗi)

| Việc | Kéo theo gì |
|---|---|
| **Partial fill** (`PARTIALLY_FILLED`) | 🔑 Phá lớp idempotency miễn phí của `OrderStateMachine` ⇒ cần `execId` + bảng dedupe thật ở order-service; `UNIQUE(client_order_id)` trên `execution` phải đổi thành `(client_order_id, exec_seq)` |
| **Huỷ lệnh** (`CANCELLED`) | Thêm transition; cần idempotency cho lệnh huỷ |
| **Limit order book thật** | Thay đổi bản chất matching-engine; cần state in-memory có thứ tự ưu tiên, và khi đó scale ngang **không còn được** |
| **Timeout saga** (`EXPIRED`) | Cần process manager hoặc scheduler quét `idx_orders_pending` |
| **Bồi hoàn (compensation)** | Cần định nghĩa "huỷ một execution" về mặt nghiệp vụ |
| **Debezium CDC** | Thay polling relay; cần alert trên `pg_replication_slots` |
| **Avro cho 3 topic còn lại** | Di cư từng topic một |
| **Read replica + CQRS đọc** | Tách truy vấn lịch sử khỏi primary |

---

## 5. Bảy nguyên tắc rút ra từ toàn bộ danh sách

> Đây là phần giá trị nhất của chương — các nguyên tắc tổng quát, áp dụng được cho bất kỳ hệ phân tán nào.

### 🔑 1. Thứ tự là một ràng buộc đúng đắn, không phải tối ưu hoá

`key = symbol` không phải để nhanh, mà để EMA và volatility ra **đúng số**. Và `riskLevel` tính từ đó là thứ dùng để từ chối lệnh của khách. Sai thứ tự ⇒ từ chối oan hoặc cho qua nhầm.

*Hệ quả vận hành:* tăng partition làm `murmur2(key) % n` đổi kết quả ⇒ có một khoảng ngắn thứ tự lịch sử không còn đảm bảo ⇒ phải làm ngoài phiên giao dịch.

### 🔑 2. Fail-safe thay vì fail-open với tiền

Không chắc chắn ⇒ **từ chối**. Staleness check, `systemUnavailable()`, và việc `check()` luôn trả `REJECTED` khi gặp bất định đều cùng một triết lý. Từ chối oan gây khó chịu; cho qua nhầm gây mất tiền và trách nhiệm pháp lý.

*Chỗ nguyên tắc này bị thủng:* Gap #6 — lệch đồng hồ làm staleness check **fail-open âm thầm**.

### 🔑 3. Lỗi im lặng nguy hiểm hơn lỗi ồn ào

Cache không TTL, DLQ không alert, `catch` nuốt exception — cả ba đều biến sự cố thành thứ **không ai nhìn thấy**.

> **Mỗi cơ chế "degrade gracefully" phải đi kèm một cơ chế phát hiện.**

`OutboxRelay` làm đúng: nuốt lỗi nhưng tăng `outbox.publish.failed` và có alert. `SnapshotStore` và nhánh Redis của `OrderService` làm thiếu nửa sau.

Và tệ hơn lỗi im lặng là **alert im lặng** (3.2): alert tồn tại nhưng không bao giờ bắn, tạo cảm giác an toàn giả.

### 🔑 4. DB constraint là chốt chặn cuối, cache chỉ là tăng tốc

Mọi lớp phòng thủ phía trên **phải chết được** mà hệ vẫn đúng. Redis chết ⇒ `UNIQUE(client_order_id)` vẫn chặn trùng. Đó là lý do `management.health.redis.enabled: false` quan trọng đến vậy.

*Chỗ nguyên tắc này bị thủng:* Gap #15 — `execution.client_order_id` không có `UNIQUE`, nên ở matching-engine không có lớp dưới cùng.

### 🔑 5. Đo đúng thứ cần phản ứng

CPU không phải lag; **lag mới là lag**. KEDA query thẳng Kafka thay vì suy ra từ CPU, vì CPU sai ở cả hai chiều: nghẽn I/O thì CPU thấp mà lag hàng triệu; GC nặng thì CPU cao mà lag bằng 0.

Cùng nguyên tắc: `pricing_batch_process` và `pricing_e2e_latency` là **hai** metric vì chúng đo hai thứ khác nhau — và chỉ khi có cả hai mới phân biệt được "thuật toán chậm" với "Kafka tắc".

### 🔑 6. Ràng buộc giữa hai con số ở hai file khác nhau thì sớm muộn cũng lệch

Danh sách trong repo này:

| Cặp ràng buộc | Nơi khai | Được cưỡng chế? |
|---|---|:--:|
| `maxReplicaCount` ↔ `KAFKA_PARTITIONS / setConcurrency(3)` | 3 file | ✅ CI — nhưng `/3` hardcode |
| `timeout-per-shutdown-phase` ↔ `terminationGracePeriodSeconds` ↔ `preStop` | yml + manifest | ❌ |
| Retention `stock.orders` ↔ lịch dọn `processed_event` | `trading.env` + (chưa có job) | ❌ |
| `order.max-price-deviation-pct` ≤ `matching.price-band-pct` | 2 yml | ❌ |
| ACL `--group` ↔ `GROUP_ID_CONFIG` | shell + 4 file Java | ❌ |
| `kafka.heapOpts` ↔ `kafka.resources.limits.memory` | cùng file values | ❌ (chỉ comment) |
| Ngưỡng `riskLevel` 15/35 ↔ nơi dùng nó | 2 service | ❌ |

> **Khi một ràng buộc đúng đắn trải qua nhiều file, hãy cưỡng chế nó ở CI.** Comment trong mã chỉ nhắc được người đang mở đúng file đó.

### 🔑 7. Sự cố chỉ lộ ra khi có dữ liệu thật

Cả một nhóm vấn đề **không thể phát hiện bằng unit test**, chỉ xuất hiện khi đã có dữ liệu production:

- OOM do query không giới hạn (#3.9, 3.10 — `findAll()`, `limit` không chặn trần).
- Bảng phình (#3, #12, 3.8).
- Index thiếu (3.9).
- `LazyInitializationException` (lý do `OrderView` tồn tại).
- Vùng phủ môi trường (#22 — dev không bao giờ có 2 partition trên 1 consumer).
- Lệch đồng hồ (#6).

> **Đối phó:** thiết kế phòng thủ *trước* (trần cứng, phân trang bắt buộc, partial index, DTO), và dùng **hạ tầng thật** trong integration test (Testcontainers, không H2).

---

## 6. Cách tự kiểm tra các điểm trên

```bash
PSQL='docker compose exec postgres psql -U trading -d trading'

# #15 — execution thiếu UNIQUE
$PSQL -c "SELECT conname, contype FROM pg_constraint WHERE conrelid='matchsvc.execution'::regclass;"
#  -> chỉ có 'p' (primary key), KHÔNG có 'u'

# #3 — market_snapshot ghi lại row trùng
$PSQL -c "SELECT symbol, last_price, count(*) FROM market_snapshot
          GROUP BY symbol, last_price HAVING count(*) > 1 ORDER BY 3 DESC LIMIT 5;"

# #12, 3.8 — bốn bảng chỉ tăng
$PSQL -c "SELECT 'market_snapshot' t, count(*) FROM market_snapshot
          UNION ALL SELECT 'ordersvc.outbox', count(*) FROM ordersvc.outbox_event
          UNION ALL SELECT 'matchsvc.outbox', count(*) FROM matchsvc.outbox_event
          UNION ALL SELECT 'processed_event', count(*) FROM matchsvc.processed_event;"

# #16 — mọi topic chỉ 1 replica ⇒ acks=all vô nghĩa
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:19092 --describe | grep -E 'ReplicationFactor|Topic:'

# #20 — hai service thiếu graceful shutdown
grep -l 'shutdown: graceful' */src/main/resources/application.yml
#  -> chỉ order-service và matching-engine

# 3.1 — executionListenerFactory không có DLT recoverer
grep -n 'setCommonErrorHandler' */src/main/java/com/trading/*/config/KafkaConfig.java
#  -> chỉ pricing-risk-service

# 3.2 — DLT có message không? (alert hiện tại không phát hiện được)
docker compose exec kafka /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
  --bootstrap-server localhost:19092 --topic stock.ticks.DLT

# 3.4 — thư mục dashboard không tồn tại
ls -la monitoring/dashboards/ 2>&1

# 3.7 — hai cơ chế nuốt lỗi không có counter
grep -n -A3 'catch (Exception' pricing-risk-service/src/main/java/com/trading/pricing/service/SnapshotStore.java
curl -s http://localhost:8082/actuator/prometheus | grep -c snapshot_flush   # -> 0

# 3.9 — thiếu index cho endpoint liệt kê
$PSQL -c "EXPLAIN SELECT * FROM ordersvc.orders
          WHERE account_id='ACC001' ORDER BY created_at DESC LIMIT 20;"
#  -> Seq Scan + Sort

# 3.10 — limit không chặn trần
curl -s 'http://localhost:8082/api/v1/marketdata/HPG/history?limit=1000000' | jq 'length'

# 3.12 — tần suất tick thực tế
grep -n 'fixedRate' market-gateway/src/main/java/com/trading/gateway/feed/SimulatedFeed.java
grep -n 'ANNUALIZE' pricing-risk-service/src/main/java/com/trading/pricing/service/RiskCalculator.java

# 3.15 — bất biến hai ngưỡng giá
grep -n 'max-price-deviation-pct' order-service/src/main/resources/application.yml
grep -n 'price-band-pct'          matching-engine/src/main/resources/application.yml

# 3.17 — CI không push image
grep -n -A3 'Push' .github/workflows/ci.yml

# 3.19 — Helm vs Kustomize đã lệch
diff <(kustomize build k8s/overlays/prod | grep -A2 'path: /api/v1') \
     <(helm template t ./helm/trading-system -f helm/trading-system/values-prod.yaml | grep -A2 'path: /api/v1')

# 3.21 — Kafka PLAINTEXT, ai cũng đọc được lệnh của khách
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:19092 --topic stock.orders --from-beginning --max-messages 2
```

---

## 7. Câu hỏi phỏng vấn

**Q: Kể cho tôi một điểm yếu của hệ thống bạn xây.**

A: Tôi có một danh sách 22 điểm đã được phân loại, cộng thêm khoảng 20 điểm phát hiện khi đối chiếu lại mã nguồn. Nghiêm trọng nhất là **Gap #15**: ở `matching-engine`, cột `client_order_id` của bảng `execution` có INDEX nhưng **không** có UNIQUE constraint — khác hẳn `orders` ở order-service. Nghĩa là bảng `processed_event` đang là lớp chặn khớp-trùng **duy nhất**, không có lưới an toàn phía sau. Nếu bảng đó bị truncate, bị dọn nhầm, hoặc migration lỗi, hệ sẽ khớp lệnh hai lần và không gì chặn lại. Điều đáng chú ý là cách sửa chỉ tốn một migration một dòng — và sửa nó xong còn hạ cấp luôn một vấn đề khác: #12, việc dọn `processed_event` hiện là vấn đề tính đúng đắn, sau đó chỉ còn là vấn đề dung lượng.

**Q: Loại vấn đề nào bạn thấy nguy hiểm nhất?**

A: **Gap**, không phải Bug. Một bug gây lỗi thấy được — ai đó sẽ báo. Một gap làm ta *tin* là đã được bảo vệ trong khi không. Ví dụ rõ nhất là #1: Dead Letter Topic được cấu hình đầy đủ — `DeadLetterPublishingRecoverer`, `FixedBackOff(1000L, 3)`, topic DLT được tạo với retention 30 ngày — trông như đã chống poison pill hoàn chỉnh. Nhưng lỗi deserialize xảy ra **bên trong `poll()`**, trước khi error handler của listener container có cơ hội chạy, nên một byte rác trong topic sẽ làm consumer vào vòng lặp lỗi vô hạn và **không có message nào vào DLT**. Cùng loại với nó là 3.2: alert `DeadLetterTopicGrowing` tồn tại nhưng đo `records_consumed`, mà không ai consume DLT, nên nó không bao giờ bắn. Alert không chạy còn nguy hiểm hơn không có alert.

**Q: Nếu có một tuần, bạn sửa gì?**

A: Theo thứ tự tác động chia cho công sức.

Hai ngày đầu cho những thứ rẻ mà tác động lớn: migration thêm `UNIQUE` trên `execution` (#15), `ErrorHandlingDeserializer` + `BatchListenerFailedException` (#1, #2) để DLT hoạt động thật, `orElseThrow(() -> dup)` (#4, một dòng), hai dòng `server.shutdown: graceful` cho hai service còn thiếu (#20), và `Math.min(limit, 500)` ở `history` (3.10).

Hai ngày tiếp cho khả năng quan sát, vì không có nó thì không biết mình đang hỏng: Alertmanager (3.3) — hiện có alert mà không ai được báo; sửa công thức `SagaStuckOrders` sang gauge đếm trực tiếp lệnh treo (3.5) vì phép trừ counter thô không bền khi pod restart; thêm counter cho flush thất bại và lệch đồng hồ (3.7, #6).

Ba ngày cuối cho bảo mật: bật SASL_SSL + SCRAM + ACL cho Kafka — mẫu đã có sẵn trong `security/`, công sức chủ yếu là vận hành (dựng CA, phát credential, mount truststore). Đó là lỗ hổng có sức sát thương lớn nhất: một pod bị chiếm quyền hiện **giả mạo được execution report**, đánh dấu lệnh FILLED ở giá bất kỳ, và `order-service` không có cách nào phát hiện vì nó tin topic đó theo thiết kế.

**Q: Nguyên tắc nào bạn rút ra được từ việc tự kiểm điểm này?**

A: Nguyên tắc tôi thấy đắt giá nhất là: **ràng buộc giữa hai con số ở hai file khác nhau thì sớm muộn cũng lệch.** Repo này có ít nhất bảy cặp như vậy — `maxReplicaCount` ↔ `partition/concurrency`, `timeout-per-shutdown-phase` ↔ `terminationGracePeriodSeconds`, retention topic ↔ lịch dọn `processed_event`, hai ngưỡng giá 5% ↔ 7%, ACL group ↔ `GROUP_ID_CONFIG`, heap Kafka ↔ memory limit, ngưỡng riskLevel ↔ nơi dùng nó. Chỉ **một** cặp được cưỡng chế bằng CI, và ngay cả cặp đó cũng hardcode `/3` nên chỉ bịt được một nửa. Bài học: comment trong mã chỉ nhắc được người đang mở đúng file đó; khi một ràng buộc đúng đắn trải qua nhiều file, phải cưỡng chế nó ở CI hoặc ở startup.

Nguyên tắc thứ hai, cụ thể hơn: **mỗi cơ chế "degrade gracefully" phải đi kèm một cơ chế phát hiện.** `OutboxRelay` làm đúng — nó nuốt lỗi nhưng tăng counter và có alert. `SnapshotStore.flushToDatabase()` làm thiếu nửa sau: DB chết ba ngày thì service vẫn báo `UP`, API realtime vẫn trả dữ liệu, và không ai biết lịch sử đã mất ba ngày.

**Q: Vì sao bạn giữ cả một danh sách như thế này thay vì cứ sửa đi?**

A: Vì ba lý do. Thứ nhất, **ưu tiên cần dữ liệu**: không có danh sách thì người ta sửa thứ dễ nhìn thấy thay vì thứ nguy hiểm nhất — và trong danh sách này, thứ nguy hiểm nhất (#15) lại là thứ rẻ nhất để sửa, điều chỉ thấy được khi đặt cạnh nhau. Thứ hai, **một số điểm là đánh đổi có ý thức, không phải lỗi** — ví dụ `common-contracts` tạo build-time coupling, hay một Postgres instance cho ba schema; ghi chúng ra kèm lý do và lộ trình thoát tốt hơn là sửa vội theo giáo điều. Thứ ba, và quan trọng nhất về mặt vận hành: **biết hệ thống của mình hỏng ở đâu là điều kiện tiên quyết để vận hành nó**. Khi `SagaStuckOrders` bắn lúc 3 giờ sáng, người trực cần biết ngay bốn nguyên nhân có thể và thứ tự kiểm tra — danh sách này chính là thứ sinh ra runbook đó.

---

## 8. Bảng tổng kết

| Nhóm | Số lượng | Nghiêm trọng nhất |
|---|:--:|---|
| 22 điểm gốc (`DEEP-DIVE.md`) | 4 ✅ · 3 🟡 · 15 ⬜ | #15 (thiếu lưới an toàn chống khớp trùng) |
| Phát hiện bổ sung | ~20 | 3.3 (không có Alertmanager), 3.2 (alert không bắn) |
| Lỗ hổng bảo mật | 3 🔴 · 6 🟠 | Kafka `PLAINTEXT` ⇒ giả mạo execution report |
| Ràng buộc liên-file không cưỡng chế | 7 cặp | 1 được bịt bằng CI (một nửa) |
| Bảng chỉ tăng không có job dọn | 4 | `processed_event` (đụng tính đúng đắn) |

| Nguyên tắc | Chỗ nó bị thủng trong chính repo này |
|---|---|
| 1. Thứ tự là ràng buộc đúng đắn | — (làm đúng) |
| 2. Fail-safe với tiền | #6 — lệch đồng hồ làm staleness check fail-open âm thầm |
| 3. Lỗi im lặng nguy hiểm hơn lỗi ồn ào | 3.7, 3.2 — nuốt lỗi không counter; alert không bắn |
| 4. DB constraint là chốt chặn cuối | #15 — `execution` không có lớp dưới cùng |
| 5. Đo đúng thứ cần phản ứng | — (làm đúng với KEDA); thiếu consumer lag metric |
| 6. Ràng buộc liên-file sẽ lệch | 7 cặp, chỉ 1 được cưỡng chế |
| 7. Sự cố lộ ra khi có dữ liệu thật | #3, #22, 3.9, 3.10 |

> **Đọc tiếp:** [Chương 15 — Runbook vận hành & xử lý sự cố](./15-runbook-van-hanh-va-su-co.md) — biến danh sách này thành thao tác cụ thể khi có sự cố.
