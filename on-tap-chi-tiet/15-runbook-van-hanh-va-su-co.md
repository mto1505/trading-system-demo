# Chương 15 — Runbook Vận Hành & Xử Lý Sự Cố

> **Mục đích:** chương này viết cho **người đang trực lúc 3 giờ sáng**. Không giải thích lý thuyết — chỉ triệu chứng, chẩn đoán, thao tác. Lý thuyết nằm ở các chương trước và được liên kết tới.
>
> **Tài liệu đối chiếu:** `GUIDE.md` mục 6-8, `k8s/README.md`, `monitoring/alert-rules.yml`.

---

## 0. Bộ lệnh chẩn đoán nhanh — chạy trước mọi thứ

```bash
# ======== LOCAL (Docker Compose) ========
alias PSQL='docker compose exec postgres psql -U trading -d trading'
alias KT='docker compose exec kafka /opt/kafka/bin'

# 1. Bốn service có sống không?
for p in 8081 8082 8083 8084; do
  echo -n "$p: "; curl -s --max-time 3 http://localhost:$p/actuator/health | jq -r .status 2>/dev/null || echo "KHONG PHAN HOI"
done

# 2. Dòng giá có chảy không?
curl -s http://localhost:8082/api/v1/marketdata/HPG | jq '{lastPrice, riskLevel, tuoi_ms: (now*1000 - .calcTime | floor)}'

# 3. Outbox có tắc không? (phải ~0)
PSQL -c "SELECT 'order' svc, count(*) cho_gui FROM ordersvc.outbox_event WHERE published_at IS NULL
         UNION ALL SELECT 'match', count(*) FROM matchsvc.outbox_event WHERE published_at IS NULL;"

# 4. Có lệnh nào treo không?
PSQL -c "SELECT status, count(*) FROM ordersvc.orders GROUP BY status ORDER BY 2 DESC;"

# 5. Consumer lag
KT/kafka-consumer-groups.sh --bootstrap-server localhost:19092 --describe --all-groups

# 6. Relay có thất bại không?
curl -s http://localhost:8083/actuator/prometheus | grep outbox_publish_failed_total
curl -s http://localhost:8084/actuator/prometheus | grep outbox_publish_failed_total
```

```bash
# ======== KUBERNETES ========
NS=trading-prod
kubectl -n $NS get pods -o wide
kubectl -n $NS get events --sort-by=.lastTimestamp | tail -20
kubectl -n $NS top pods
kubectl -n $NS get scaledobject,hpa
kubectl -n $NS logs -l app.kubernetes.io/component=order --tail=50
```

---

## 1. Triệu chứng → nguyên nhân (bảng tra nhanh)

| Triệu chứng | Nguyên nhân thường gặp | Đi tới |
|---|---|:--:|
| `422 NO_FRESH_MARKET_DATA` | Feed chưa chạy đủ lâu; pricing chết; `stock.enriched` tắc; **lệch đồng hồ** | [§2.1](#21) |
| `422 RISK_LEVEL_HIGH` | Volatility mô phỏng vọt lên — thường là bình thường | [§2.2](#22) |
| `422 PRICE_DEVIATION` | Giá lệch > 5% giá tham chiếu — fat-finger hoặc giá tham chiếu cũ | [§2.3](#23) |
| `401` khi POST order | Thiếu `X-API-KEY: demo-key` | — |
| `429` | Rate limiter 200 req/s/pod đang làm việc — **đúng thiết kế** | [§2.4](#24) |
| `REJECTED SYSTEM_UNAVAILABLE` | Circuit breaker `orders-db` đã mở ⇒ DB có vấn đề | [§3.2](#32) |
| **Lệnh kẹt mãi ở `ACCEPTED`** | 🔴 Saga đứt — 4 nguyên nhân | [§3.1](#31) |
| `REJ_MATCH / INVALID_LOT_SIZE` | `quantity` không phải bội số 100 — đúng thiết kế | [§2.5](#25) |
| `REJ_MATCH / PRICE_OUT_OF_BAND` | Lệch > 7% biên độ sàn | [§2.5](#25) |
| `REJ_MATCH / MARKET_CLOSED` | `session-check-enabled=true` và ngoài giờ | [§2.5](#25) |
| Event ra Kafka **hai lần** | Bật CDC mà quên `OUTBOX_RELAY_ENABLED=false` | [§3.6](#36) |
| App không khởi động, lỗi schema validate | Thiếu migration Flyway (`ddl-auto: validate` cố ý không tự sửa) | [§4.1](#41) |
| App không khởi động, `FATAL: password authentication failed` | 🔴 Bug #18 — xoay mật khẩu ở dev/staging | [§4.2](#42) |
| App không khởi động, `too many clients already` | Tổng pool vượt `max_connections` | [§4.3](#43) |
| Compose CDC báo trùng cổng **8083** | Kafka Connect trùng cổng order-service | [§3.6](#36) |
| Deserialize lỗi ở consumer | Contract lệch giữa service ⇒ rebuild **tất cả** module | [§4.4](#44) |
| Tick không được tiêu thụ | Schema Registry chưa lên | [§3.4](#34) |
| Consumer lag tăng, thêm pod không giúp | Đã chạm trần `partition / 3`, hoặc **hot partition** | [§3.3](#33) |
| Pod `OOMKilled` | Heap không khớp `limits.memory` | [§4.5](#45) |
| Pod `CrashLoopBackOff` ngay khi start | DB chưa sẵn sàng; Flyway fail; port sai | [§4.1](#41) |
| Dashboard Grafana trống | Thiếu label `release:` trên ServiceMonitor; hoặc chưa có dashboard JSON | [§5.3](#53) |
| `market_snapshot` phình nhanh bất thường | Ops #3 — flush ghi lại cả mã không đổi | [§5.1](#51) |
| Đĩa Postgres đầy (khi chạy CDC) | 🔴 Replication slot bị bỏ quên | [§3.7](#37) |

---

## 2. Sự cố "không phải sự cố" — hệ thống đang hoạt động đúng

<a id="21"></a>
### 2.1. `422 NO_FRESH_MARKET_DATA`

**Chẩn đoán theo thứ tự:**

```bash
# 1. Pricing có dữ liệu không?
curl -s http://localhost:8082/api/v1/marketdata/HPG | jq

#    Có dữ liệu -> kiểm tra TUỔI của nó
curl -s http://localhost:8082/api/v1/marketdata/HPG | \
  jq '{calcTime, now: (now*1000|floor), tuoi_ms: ((now*1000|floor) - .calcTime)}'
#    tuoi_ms > 10000 -> đúng là stale

#    KHÔNG có dữ liệu -> feed hoặc pipeline đứt
curl -s http://localhost:8081/actuator/prometheus | grep gateway_ticks_sent_total
KT/kafka-consumer-groups.sh --bootstrap-server localhost:19092 \
  --describe --group pricing-risk-group

# 2. order-service có nhận được stock.enriched không?
KT/kafka-consumer-groups.sh --bootstrap-server localhost:19092 \
  --describe --group order-service-group

# 3. ⚠️ Kiểm tra LỆCH ĐỒNG HỒ (Gap #6) — nếu tuoi_ms ra số ÂM
#    thì staleness check đang bị vô hiệu hoá âm thầm
docker compose exec pricing-risk-service date -u
docker compose exec order-service date -u
```

**Xử lý:**

| Nguyên nhân | Thao tác |
|---|---|
| Feed chưa chạy đủ lâu | Chờ 5-10 giây; hoặc bơm tick thủ công (dưới) |
| `SIMULATED_FEED=false` mà không có feed thật | Bật lại, hoặc cắm feed |
| pricing-risk-service chết | `docker compose restart pricing-risk-service` |
| Lag `stock.ticks` cao | Xem [§3.3](#33) |
| Mã thanh khoản thấp (Ops #5) | Nới `order.max-data-age-ms` cho nhóm mã đó |
| **Lệch đồng hồ** | Sửa NTP trên node; đây là lỗi nghiêm trọng, xem Gap #6 |

```bash
# Bơm tick thủ công — mẹo: 6 tick GIÁ Y HỆT NHAU -> volatility = 0 -> riskLevel LOW chắc chắn
for i in 1 2 3 4 5 6; do
  curl -s -X POST http://localhost:8081/api/v1/ticks -H 'Content-Type: application/json' \
    -d "{\"symbol\":\"HPG\",\"price\":28500,\"volume\":100,\"eventTime\":$(date +%s%3N)}"
done
sleep 2
curl -s http://localhost:8082/api/v1/marketdata/HPG | jq '{volatility, riskLevel}'
```

<a id="22"></a>
### 2.2. `422 RISK_LEVEL_HIGH`

Volatility năm hoá ≥ 35% (`RiskCalculator.java:75`). Với `SimulatedFeed` random walk, điều này xảy ra ngẫu nhiên.

```bash
curl -s http://localhost:8082/api/v1/marketdata/HPG | jq '{volatility, var95, riskLevel}'
```

| Tình huống | Thao tác |
|---|---|
| Demo, muốn đặt lệnh ngay | Dùng mẹo "6 tick giống nhau" ở §2.1 với một **mã mới** |
| Thị trường thật đang biến động | **Không làm gì** — hệ thống đang bảo vệ khách đúng như thiết kế |
| Ngưỡng không phù hợp | Sửa `RiskCalculator.java:75` (⚠️ hardcode, xem Ops 3.13) |

💡 Nhớ rằng volatility năm hoá đang bị thổi lên ~2,24 lần do `ANNUALIZE` giả định sai tần suất tick (Doc 3.12) — nên ngưỡng 15/35 là con số đã hiệu chỉnh cho feed này, không phải con số tài chính chuẩn.

<a id="23"></a>
### 2.3. `422 PRICE_DEVIATION`

```bash
# So giá đặt với giá tham chiếu
curl -s http://localhost:8082/api/v1/marketdata/HPG | jq '.lastPrice'
# Lệch > 5% -> bị chặn
```

| Tình huống | Thao tác |
|---|---|
| Gõ nhầm số 0 | Đúng — fat-finger check đang làm việc |
| Giá tham chiếu quá cũ | Bơm tick mới (§2.1) |
| Thị trường nhảy gap | Nới `order.max-price-deviation-pct`, nhưng **phải giữ ≤ `matching.price-band-pct` (7)** — xem Gap 3.15 |

<a id="24"></a>
### 2.4. `429 Too Many Requests`

```bash
curl -s http://localhost:8083/actuator/metrics/resilience4j.ratelimiter.available.permissions | jq
```

Rate limiter 200 req/s **per pod**, `timeout-duration: 0s` (từ chối tức thì, không chiếm thread).

| Tình huống | Thao tác |
|---|---|
| Load test | Đúng thiết kế |
| Client bug retry vô hạn | Tìm client, sửa client |
| Tải thật tăng | Scale pod (trần cụm = 200 × số pod), **hoặc** nâng `limit-for-period` |
| Một account đặt loạn làm cả hệ 429 | ⚠️ Hạn chế đã biết — rate limit là per-service, chưa per-account |

<a id="25"></a>
### 2.5. `REJ_MATCH` — matching-engine từ chối

```bash
curl -s http://localhost:8083/api/v1/orders/<clientOrderId> -H 'X-API-KEY: demo-key' | jq '{status, reason}'
docker compose logs --tail=50 matching-engine | grep "TU CHOI"
```

| `reason` | Nguyên nhân | Thao tác |
|---|---|---|
| `INVALID_LOT_SIZE must be multiple of 100` | `quantity` không chia hết lô | Đặt bội số 100; hoặc `matching.lot-size=1` cho phái sinh |
| `PRICE_OUT_OF_BAND x% > 7%` | Vượt biên độ sàn | Đúng thiết kế; HNX thì `price-band-pct=10` |
| `NO_REFERENCE_PRICE` | `refPrice` null và `price` ≤ 0 | Lệnh đến từ nguồn không qua order-service |
| `MARKET_CLOSED` | Ngoài 9:00-11:30 / 13:00-14:45 | `matching.session-check-enabled=false` để demo 24/7 |

🔑 **Client nhận `201 ACCEPTED` rồi 2 giây sau thành `REJ_MATCH` là hành vi ĐÚNG** — `order-service` không biết luật sàn, chỉ sàn mới biết. Xem [chương 07](./07-matching-engine-va-saga-choreography.md) mục 2.3.

---

## 3. Sự cố thật

<a id="31"></a>
### 3.1. 🔴 Lệnh kẹt ở `ACCEPTED` — saga đứt

**Đây là sự cố nghiêm trọng nhất: khách đang bị giữ tiền cho lệnh không bao giờ kết thúc.**

Alert: `SagaStuckOrders` (critical, sau 2 phút).

#### Chẩn đoán — bốn nguyên nhân, theo đúng thứ tự kiểm tra

```bash
# === (1) Relay có publish được không? ===
curl -s http://localhost:8083/actuator/prometheus | grep -E 'outbox_published_total|outbox_publish_failed_total'
curl -s http://localhost:8084/actuator/prometheus | grep -E 'outbox_published_total|outbox_publish_failed_total'
#    outbox_publish_failed_total tăng -> Kafka có vấn đề, hoặc topic không tồn tại
docker compose logs --tail=50 order-service | grep "Publish outbox"

# === (2) matching-engine còn sống? ===
curl -s http://localhost:8084/actuator/health | jq -r .status
docker compose ps matching-engine
docker compose logs --tail=50 matching-engine

# === (3) Consumer lag ===
KT/kafka-consumer-groups.sh --bootstrap-server localhost:19092 --describe \
  --group matching-engine-group
KT/kafka-consumer-groups.sh --bootstrap-server localhost:19092 --describe \
  --group order-service-exec-group

# === (4) Outbox tồn đọng ở cả hai service ===
PSQL -c "SELECT 'order' svc, count(*) FROM ordersvc.outbox_event WHERE published_at IS NULL
         UNION ALL SELECT 'match', count(*) FROM matchsvc.outbox_event WHERE published_at IS NULL;"
```

#### Danh sách lệnh treo (dùng partial index `idx_orders_pending`)

```sql
SELECT client_order_id, symbol, side, price, quantity, created_at,
       round(extract(epoch from (now() - created_at))) AS treo_giay
FROM ordersvc.orders
WHERE status = 'ACCEPTED' AND created_at < now() - interval '1 minute'
ORDER BY created_at;
```

#### Bảng chẩn đoán

| Phát hiện | Nghĩa là | Thao tác |
|---|---|---|
| Outbox `order` tồn đọng, `failed` tăng | Relay không publish được | Kiểm Kafka; kiểm topic `stock.orders` tồn tại; restart order-service |
| Outbox `order` tồn đọng, `failed` = 0, `published` cũng không tăng | 🔑 **Relay không chạy** | Kiểm `OUTBOX_RELAY_ENABLED`; kiểm `scanBasePackages` có `com.trading.outbox` |
| Outbox `order` sạch, lag `matching-engine-group` cao | matching không theo kịp hoặc đã chết | Restart / scale matching-engine |
| Outbox `match` tồn đọng | Relay của matching không publish được | Như hàng 1, cho matching-engine |
| Outbox cả hai sạch, lag `order-service-exec-group` cao | order-service không xử lý report | Restart order-service; kiểm optimistic lock conflict |
| Mọi thứ sạch mà lệnh vẫn treo | Report **đã mất** (Gap 3.1 — không có DLT thật) | Xem "Gỡ thủ công" dưới |

#### 🔑 Nguyên nhân #1 bị bỏ sót nhiều nhất: relay không chạy

```bash
# Kiểm tra bean OutboxRelay có được đăng ký không
curl -s http://localhost:8083/actuator/beans 2>/dev/null | jq '..|.outboxRelay? // empty'
#    (nếu /actuator/beans không expose, xem log khởi động)
docker compose logs order-service | grep -i "outbox"

# Kiểm tra property
docker compose exec order-service printenv | grep OUTBOX
```

Hai nguyên nhân làm relay không chạy:
1. `OUTBOX_RELAY_ENABLED=false` (ví dụ: đã bật CDC rồi quên).
2. 🔑 **`scanBasePackages` thiếu `com.trading.outbox`** — `OutboxRelay` nằm ngoài package của app nên component scan mặc định **không thấy nó**, và **không có lỗi nào được ném ra**. Xem comment ở `OrderServiceApplication.java:20-22`.

#### Gỡ thủ công khi report đã mất

⚠️ Chỉ làm khi đã xác nhận execution tồn tại ở matching-engine nhưng report không bao giờ tới order-service.

```bash
# 1. Xác nhận lệnh ĐÃ được khớp ở matching-engine
curl -s http://localhost:8084/api/v1/executions/<clientOrderId> | jq
PSQL -c "SELECT * FROM matchsvc.execution WHERE client_order_id = '<clientOrderId>';"

# 2. Xem report có trong outbox của matching không
PSQL -c "SELECT id, topic, published_at, payload FROM matchsvc.outbox_event
         WHERE aggregate_id = '<clientOrderId>';"

# 3a. Nếu CÓ trong outbox mà published_at IS NULL -> relay sẽ tự gửi, chỉ cần chờ/restart
docker compose restart matching-engine

# 3b. Nếu ĐÃ published mà order-service không nhận -> publish lại thủ công
PSQL -t -A -c "SELECT payload FROM matchsvc.outbox_event
               WHERE aggregate_id = '<clientOrderId>' LIMIT 1;" | \
  KT/kafka-console-producer.sh --bootstrap-server localhost:19092 --topic stock.executions

# 3c. Lựa chọn cuối: cập nhật trạng thái trực tiếp (GHI LẠI VÀO SỔ SỰ CỐ)
PSQL -c "UPDATE ordersvc.orders
         SET status='FILLED', filled_qty=100, avg_fill_price=28512.0000,
             updated_at=now(), version=version+1
         WHERE client_order_id='<clientOrderId>' AND status='ACCEPTED';"
```

🔑 **Luôn ưu tiên 3b hơn 3c.** Publish lại message giữ nguyên luồng xử lý và mọi cơ chế dedupe/state machine vẫn hoạt động. `UPDATE` trực tiếp bỏ qua state machine và không để lại dấu vết trong luồng sự kiện.

<a id="32"></a>
### 3.2. `REJECTED SYSTEM_UNAVAILABLE` — circuit breaker mở

```bash
curl -s http://localhost:8083/actuator/metrics/resilience4j.circuitbreaker.state | jq
curl -s http://localhost:8083/actuator/prometheus | grep resilience4j_circuitbreaker

# Database có khoẻ không?
docker compose exec postgres pg_isready
PSQL -c "SELECT 1;"
PSQL -c "SELECT count(*) dang_dung, (SELECT setting::int FROM pg_settings
         WHERE name='max_connections') tran FROM pg_stat_activity;"
curl -s http://localhost:8083/actuator/metrics/hikaricp.connections.pending | jq
```

| Phát hiện | Thao tác |
|---|---|
| Postgres không phản hồi | Restart Postgres; kiểm đĩa, kiểm OOM |
| `pending` connection cao | Pool cạn — xem query chậm, hoặc tăng pool |
| `too many clients` | Xem [§4.3](#43) |
| Query chậm | `SELECT * FROM pg_stat_activity WHERE state='active' ORDER BY query_start;` |

Breaker tự đóng lại sau `wait-duration-in-open-state: 10s` nếu 3/3 call thử thành công. **Không cần can thiệp** nếu nguyên nhân gốc đã được xử lý.

<a id="33"></a>
### 3.3. Consumer lag tăng — thêm pod không giúp

```bash
# Lag theo TỪNG PARTITION (quan trọng hơn tổng lag)
KT/kafka-consumer-groups.sh --bootstrap-server localhost:19092 --describe --group pricing-risk-group
```

🔑 **Đọc cột `PARTITION` và `LAG`:**

| Phân bố lag | Chẩn đoán | Thao tác |
|---|---|---|
| **Đều** trên mọi partition | Thiếu pod consumer | Scale — nhưng kiểm trần trước |
| **Tập trung ở 1-2 partition** | 🔑 **Hot partition** (một mã chiếm phần lớn lưu lượng) | Thêm pod **không giúp** — xem dưới |
| `CONSUMER-ID` trống ở vài partition | Không có consumer nào nhận | Pod chết, hoặc đang rebalance |

#### Kiểm trần scale trước khi scale

$$\text{trần pod} = \frac{\text{partition}}{\text{concurrency}(3)}$$

```bash
# Số partition hiện tại
KT/kafka-topics.sh --bootstrap-server localhost:19092 --describe --topic stock.ticks | head -1

# Nếu đã chạm trần: TĂNG PARTITION TRƯỚC, rồi mới nâng maxReplicaCount
KT/kafka-topics.sh --bootstrap-server localhost:19092 --alter --topic stock.ticks --partitions 24
```

⚠️ **Tăng partition phá vỡ thứ tự lịch sử.** `murmur2(key) % n` đổi kết quả ⇒ tick cũ của `HPG` ở partition 3, tick mới ở partition 7, hai partition này được hai thread đọc song song ⇒ một khoảng ngắn thứ tự `HPG` không còn đảm bảo ⇒ EMA/volatility nhiễu. **Làm ngoài phiên giao dịch.**

⚠️ Kafka **không cho giảm** partition. Chỉ tăng được.

#### Hot partition

```bash
# Tìm mã chiếm nhiều lưu lượng
KT/kafka-console-consumer.sh --bootstrap-server localhost:19092 --topic stock.ticks \
  --property print.key=true --property print.partition=true --max-messages 1000 2>/dev/null | \
  awk '{print $1}' | sort | uniq -c | sort -rn | head
```

Thêm pod không giúp vì mã đó vẫn chỉ có **một** consumer. Giải pháp duy nhất giữ được thứ tự: khoá mịn hơn (`symbol` + bucket), và chỉ làm được nếu nghiệp vụ chấp nhận chỉ giữ thứ tự trong bucket.

<a id="34"></a>
### 3.4. Tick không được tiêu thụ

```bash
# 1. Schema Registry lên chưa?
curl -s http://localhost:8085/subjects | jq
#    -> ["stock.ticks-value"]

# 2. Gateway có produce không?
curl -s http://localhost:8081/actuator/prometheus | grep -E 'gateway_ticks_(sent|error)_total'

# 3. Topic có message không?
KT/kafka-run-class.sh kafka.tools.GetOffsetShell --bootstrap-server localhost:19092 --topic stock.ticks

# 4. ⚠️ Consumer có vào vòng lặp lỗi deserialize không? (Gap #1)
docker compose logs --tail=100 pricing-risk-service | grep -iE 'deserial|SerializationException|Avro'
```

| Phát hiện | Thao tác |
|---|---|
| `/subjects` rỗng hoặc lỗi | Schema Registry chưa sẵn sàng — chờ hoặc restart |
| `gateway_ticks_error_total` tăng | Kiểm Kafka; kiểm scale của `BigDecimal` (Avro yêu cầu đúng scale=4) |
| Topic có message, lag tăng, log đầy lỗi deserialize | 🔴 **Gap #1** — DLT không bắt được, consumer kẹt | [§5.4](#54) |
| Topic có message, lag = 0, nhưng không có snapshot | Kiểm `RiskCalculator` có ném exception không |

<a id="36"></a>
### 3.5. Event ra Kafka hai lần

**Nguyên nhân:** bật CDC mà quên `OUTBOX_RELAY_ENABLED=false` ⇒ cả relay lẫn Debezium cùng publish.

```bash
docker compose exec order-service printenv | grep OUTBOX_RELAY
docker compose exec matching-engine printenv | grep OUTBOX_RELAY
curl -s http://localhost:8083/connectors?expand=status | jq   # Kafka Connect
```

**Xử lý:** đặt `OUTBOX_RELAY_ENABLED=false` ở cả hai service có outbox rồi restart.

💡 Hệ vẫn **đúng** nhờ dedupe — chỉ tốn gấp đôi băng thông và làm nhiễu metric (`outbox_published_total` không còn phản ánh số event thật).

**Lỗi trùng cổng 8083:** `docker-compose.cdc.yml` map Kafka Connect vào `8083`, trùng `order-service`. Đổi một trong hai trước khi chạy:

```yaml
# docker-compose.cdc.yml
connect:
  ports: ["8099:8083"]    # đổi cổng host
```

<a id="37"></a>
### 3.6. 🔴 Đĩa Postgres đầy (chỉ khi chạy CDC)

**Nguyên nhân:** replication slot bị bỏ quên ⇒ Postgres giữ lại **toàn bộ WAL** từ điểm connector dừng.

```sql
-- Độ trễ của từng slot
SELECT slot_name, active, restart_lsn,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS wal_lag
FROM pg_replication_slots;

-- Kích thước WAL hiện tại
SELECT pg_size_pretty(sum(size)) FROM pg_ls_waldir();
```

| Phát hiện | Thao tác |
|---|---|
| `active = false` và `wal_lag` lớn | Connector đã chết — khởi động lại, **hoặc** xoá slot nếu không dùng nữa |
| `active = true` nhưng `wal_lag` tăng đều | Connector chạy nhưng không theo kịp — kiểm Kafka Connect |

```sql
-- CHỈ làm khi chắc chắn không cần slot nữa (SẼ MẤT EVENT chưa đọc)
SELECT pg_drop_replication_slot('order_outbox_slot');
```

🔑 **Phòng ngừa bắt buộc:** đặt `max_slot_wal_keep_size` (Postgres 13+) để Postgres **tự vô hiệu hoá slot** thay vì tự sát vì hết đĩa — đổi "mất event" lấy "database còn sống". Và alert trên `pg_replication_slots`.

---

## 4. Sự cố khởi động

<a id="41"></a>
### 4.1. Pod `CrashLoopBackOff` hoặc không khởi động

```bash
# Local
docker compose logs --tail=100 order-service

# Kubernetes
kubectl -n $NS describe pod <pod>
kubectl -n $NS logs <pod> --previous          # log của lần crash TRƯỚC
kubectl -n $NS logs <pod> -c wait-for-db      # log của initContainer
```

| Thông điệp lỗi | Nguyên nhân | Thao tác |
|---|---|---|
| `Schema-validation: missing column [x] in table [y]` | Thiếu migration Flyway | Viết `V<n>__...sql`. **Không bao giờ** đổi `ddl-auto` sang `update` |
| `Schema-validation: wrong column type` | Entity khai precision khác cột DB | Sửa entity hoặc viết migration |
| `Validate failed: Migration checksum mismatch` | Đã sửa một file migration **đã chạy** | Khôi phục file gốc; hoặc `flyway repair`. Quy tắc: migration đã merge thì không sửa |
| `FATAL: password authentication failed` | 🔴 Bug #18 | [§4.2](#42) |
| `FATAL: sorry, too many clients already` | Vượt `max_connections` | [§4.3](#43) |
| `Connection refused` tới Kafka | `advertised.listeners` sai cho ngữ cảnh | Container dùng `kafka:19092`, host dùng `localhost:9092` |
| `OOMKilled` trong `describe pod` | Heap không khớp limit | [§4.5](#45) |
| initContainer treo ở `cho postgres tai ...` | DB chưa sẵn sàng | Kiểm pod postgres |
| Pod Ready nhưng không xử lý message | Relay/listener không chạy | Kiểm `scanBasePackages` |

#### Migration thất bại giữa chừng

```sql
-- 1. Xem migration nào hỏng
SELECT * FROM ordersvc.flyway_schema_history WHERE success = false;

-- 2. Dọn tay phần DDL đã áp dụng dở

-- 3. Xoá bản ghi hỏng (Flyway chặn mọi migration tiếp theo khi có success=false)
DELETE FROM ordersvc.flyway_schema_history WHERE success = false;

-- 4. Restart pod
```

<a id="42"></a>
### 4.2. 🔴 Bug #18 — xoay mật khẩu DB ở dev/staging làm sập môi trường

**Triệu chứng:** sau khi sửa `secrets.env` và `kubectl apply`, mọi service báo `FATAL: password authentication failed`, trong khi initContainer `wait-for-db` **vẫn pass**.

**Nguyên nhân:** `POSTGRES_PASSWORD` **chỉ có tác dụng lúc `initdb`** (lần đầu khởi tạo volume). Sửa Secret ⇒ hash đổi ⇒ restart cả Postgres lẫn service ⇒ app dùng mật khẩu **mới**, DB giữ mật khẩu **cũ**.

**Khôi phục:**

```bash
# Đặt lại mật khẩu TRONG DB cho khớp với Secret mới
kubectl -n $NS exec sts/postgres -- \
  psql -U trading -c "ALTER ROLE trading WITH PASSWORD 'matkhau_moi';"
kubectl -n $NS rollout restart deploy/order-service deploy/matching-engine deploy/pricing-risk-service
```

**Quy trình ĐÚNG cho lần sau:**

```bash
# 1. Đổi mật khẩu TRONG DB TRƯỚC
kubectl -n $NS exec sts/postgres -- \
  psql -U trading -c "ALTER ROLE trading WITH PASSWORD 'matkhau_moi';"

# 2. RỒI mới sửa secrets.env và apply
sed -i 's/^DB_PASSWORD=.*/DB_PASSWORD=matkhau_moi/' k8s/overlays/dev/secrets.env
kubectl apply -k k8s/overlays/dev
```

<a id="43"></a>
### 4.3. `too many clients already`

```sql
SELECT count(*) dang_dung,
       (SELECT setting::int FROM pg_settings WHERE name='max_connections') AS tran
FROM pg_stat_activity;

SELECT application_name, state, count(*) FROM pg_stat_activity GROUP BY 1,2 ORDER BY 3 DESC;
```

🔑 **Phép tính:** tổng pool = Σ (`maximum-pool-size` × số pod). Với pricing 10 + order 20 + matching 10, mỗi service 3 pod = **120 connection** > `max_connections` mặc định **100**.

| Thao tác | Khi nào |
|---|---|
| Giảm `maximum-pool-size` | Pool đang thừa so với số thread thật sự cần DB |
| Tăng `max_connections` | Có RAM (mỗi connection Postgres là một process ~10MB) |
| **PgBouncer transaction pooling** | 🔑 Giải pháp chuẩn khi scale nhiều pod |
| Giảm số pod | Tạm thời |

```sql
-- Dọn connection idle quá lâu (khẩn cấp)
SELECT pg_terminate_backend(pid) FROM pg_stat_activity
WHERE state = 'idle' AND state_change < now() - interval '10 minutes';
```

<a id="44"></a>
### 4.4. Deserialize lỗi — contract lệch giữa service

**Nguyên nhân:** sửa một `record` trong `common-contracts` mà chỉ rebuild một module.

```bash
# Rebuild TẤT CẢ
mvn clean install -DskipTests
docker compose build --no-cache market-gateway pricing-risk-service order-service matching-engine
docker compose up -d
```

🔑 **Quy tắc:** đổi `common-contracts` ⇒ `mvn clean install` toàn repo ⇒ rebuild **mọi** image. Compiler bắt lỗi giúp ở bước build, nhưng chỉ khi mọi module được build lại.

**Với Avro (`TickEventAvro`):** muốn tương thích BACKWARD thì **chỉ thêm field có `default`**, không xoá/đổi kiểu field cũ. Schema Registry sẽ từ chối schema vi phạm ngay lúc producer đăng ký.

<a id="45"></a>
### 4.5. `OOMKilled`

```bash
kubectl -n $NS describe pod <pod> | grep -A5 "Last State"
#  -> Reason: OOMKilled

kubectl -n $NS top pods
curl -s http://localhost:8083/actuator/metrics/jvm.memory.used | jq
```

🔑 **Kubernetes chỉ báo `OOMKilled` chứ không nói heap là thủ phạm.** Ba loại thành phần, ba cơ chế heap:

| Thành phần | Heap theo | Hạ `limits.memory` thì |
|---|---|---|
| 4 service Java | `-XX:MaxRAMPercentage=75` (Dockerfile) | ✅ Heap tự co theo |
| Kafka | `KAFKA_HEAP_OPTS` cố định | ⚠️ **Phải sửa `kafka.heapOpts` cùng lúc** |
| Schema Registry | `-Xmx512M` mặc định của image | ⚠️ **Phải sửa `schemaRegistry.heapOpts`** |

⚠️ **Trên minikube: `kubectl describe node` NÓI DỐI.** kubelet đọc `/proc/meminfo` nên báo RAM của host, trong khi cgroup chặn ở con số thấp hơn nhiều. Con số thật:

```bash
docker stats minikube --no-stream
```

---

## 5. Vận hành định kỳ

<a id="51"></a>
### 5.1. Dọn bảng chỉ tăng

```sql
-- 1. Outbox đã publish (CHỈ xoá published_at IS NOT NULL!)
DELETE FROM ordersvc.outbox_event
WHERE published_at IS NOT NULL AND published_at < now() - interval '7 days';

DELETE FROM matchsvc.outbox_event
WHERE published_at IS NOT NULL AND published_at < now() - interval '7 days';

-- 2. processed_event — 🔑 PHẢI giữ LÂU HƠN retention của stock.orders (7 ngày)
--    Dọn sớm hơn = replay từ offset cũ sẽ khớp lệnh HAI LẦN
DELETE FROM matchsvc.processed_event
WHERE processed_at < now() - interval '10 days';

-- 3. market_snapshot
DELETE FROM market_snapshot WHERE snap_time < now() - interval '30 days';

-- 4. orders: KHÔNG dọn (audit). Archive sang cold storage nếu cần.
```

⚠️ **Xoá hàng triệu row bằng `DELETE` sinh dead tuple cần VACUUM và gây bloat.** Xoá theo lô nhỏ:

```sql
DELETE FROM ordersvc.outbox_event
WHERE id IN (SELECT id FROM ordersvc.outbox_event
             WHERE published_at IS NOT NULL AND published_at < now() - interval '7 days'
             LIMIT 10000);
-- lặp cho tới khi 0 row
```

🔑 **Thứ tự ưu tiên:** nên thêm `UNIQUE` trên `execution.client_order_id` (Gap #15) **trước** khi viết job dọn `processed_event` — khi đó dọn sớm chỉ gây xử lý lại chứ không gây khớp hai lần.

```sql
-- Kiểm tra bloat và VACUUM
SELECT schemaname||'.'||relname AS bang, n_dead_tup, last_autovacuum
FROM pg_stat_user_tables WHERE schemaname IN ('ordersvc','matchsvc','public')
ORDER BY n_dead_tup DESC;

VACUUM ANALYZE ordersvc.outbox_event;
```

### 5.2. Replay từ Dead Letter Topic

```bash
# 1. DLT có message không?
KT/kafka-run-class.sh kafka.tools.GetOffsetShell \
  --bootstrap-server localhost:19092 --topic stock.ticks.DLT

# 2. Xem message và HEADER chẩn đoán
KT/kafka-console-consumer.sh --bootstrap-server localhost:19092 \
  --topic stock.ticks.DLT --from-beginning --max-messages 5 \
  --property print.headers=true --property print.offset=true
#    header: kafka_dlt-original-topic/partition/offset, exception-fqcn, exception-message, stacktrace

# 3. Sửa nguyên nhân gốc (deploy bản vá)

# 4. Replay: copy message từ DLT về topic gốc
KT/kafka-console-consumer.sh --bootstrap-server localhost:19092 \
  --topic stock.ticks.DLT --from-beginning --max-messages 100 2>/dev/null | \
KT/kafka-console-producer.sh --bootstrap-server localhost:19092 --topic stock.ticks
```

⚠️ **Replay vào topic Avro bằng console producer sẽ không hoạt động** (console producer gửi string, consumer chờ Avro binary có schema ID). Với topic Avro, phải viết một công cụ replay bằng Java dùng `KafkaAvroDeserializer`/`Serializer`, hoặc dùng `kafka-avro-console-consumer` + `kafka-avro-console-producer` của Confluent.

⚠️ **Nhớ rằng DLT hiện không nhận được lỗi deserialize** (Gap #1) và **một tick lỗi đẩy cả lô 1000** (Gap #2).

<a id="53"></a>
### 5.3. Dashboard Grafana trống

| Nguyên nhân | Kiểm tra | Sửa |
|---|---|---|
| Không có dashboard JSON (Ops 3.4) | `ls monitoring/dashboards/` | Thêm file JSON |
| ServiceMonitor thiếu label `release:` | `kubectl -n monitoring get prometheus -o yaml \| grep -A5 podMonitorSelector` | Đặt `monitoring.extraLabels.release` |
| Prometheus không scrape được | `curl localhost:9090/api/v1/targets \| jq '.data.activeTargets[].health'` | Kiểm NetworkPolicy, kiểm port |
| Datasource sai | Grafana → Configuration → Data sources | `url: http://prometheus:9090` |

<a id="54"></a>
### 5.4. Consumer kẹt vì lỗi deserialize (Gap #1)

**Triệu chứng:** lag tăng đều, log đầy `SerializationException`, DLT rỗng.

**Xử lý tạm thời — bỏ qua offset hỏng:**

```bash
# 1. Dừng consumer
docker compose stop pricing-risk-service

# 2. Tìm offset hỏng trong log
docker compose logs pricing-risk-service | grep -iE 'offset.*partition' | tail -5

# 3. Nhảy qua offset đó
KT/kafka-consumer-groups.sh --bootstrap-server localhost:19092 \
  --group pricing-risk-group --topic stock.ticks:7 \
  --reset-offsets --shift-by 1 --execute

# 4. Khởi động lại
docker compose start pricing-risk-service
```

**Xử lý triệt để:** thêm `ErrorHandlingDeserializer` (Gap #1).

### 5.5. Tăng partition

```bash
# 1. Số hiện tại
KT/kafka-topics.sh --bootstrap-server localhost:19092 --describe --topic stock.ticks | head -1

# 2. Tăng (CHỈ tăng được, Kafka không cho giảm)
KT/kafka-topics.sh --bootstrap-server localhost:19092 --alter --topic stock.ticks --partitions 24

# 3. DLT phải tăng theo cùng số partition
KT/kafka-topics.sh --bootstrap-server localhost:19092 --alter --topic stock.ticks.DLT --partitions 24

# 4. Cập nhật KAFKA_PARTITIONS trong trading.env/values + nâng maxReplicaCount
#    (CI sẽ chặn nếu maxReplicaCount > partition/3)
```

⚠️ Làm **ngoài phiên giao dịch** — xem cảnh báo ở [§3.3](#33).

### 5.6. Xoay secret ở production (ESO)

```bash
# 1. Đổi ở AWS Secrets Manager
aws secretsmanager put-secret-value --secret-id trading/prod/db \
  --secret-string '{"username":"trading","password":"matkhau_moi"}'

# 2. Chờ ESO đồng bộ (refreshInterval: 1h) hoặc ép ngay
kubectl -n $NS annotate externalsecret trading-secrets force-sync=$(date +%s) --overwrite

# 3. Xác nhận Secret đã đổi
kubectl -n $NS get secret trading-secrets -o jsonpath='{.data.DB_PASSWORD}' | base64 -d

# 4. 🔑 BẮT BUỘC — Gap #19: ESO KHÔNG trigger restart
kubectl -n $NS rollout restart deploy/order-service deploy/matching-engine deploy/pricing-risk-service
kubectl -n $NS rollout status deploy/order-service
```

⚠️ **Bước 4 không được bỏ qua.** `secretKeyRef` chỉ đọc **một lần lúc container start**. Không restart thì ứng dụng dùng mật khẩu cũ tới lần restart ngẫu nhiên sau đó — và nếu mật khẩu cũ đã bị vô hiệu hoá, pod sẽ chết vào một thời điểm hoàn toàn không liên quan tới thao tác xoay, khiến chẩn đoán rất khó.

### 5.7. Rollback

```bash
# Helm — khôi phục chính xác manifest đã deploy
helm history trading -n $NS
helm rollback trading -n $NS                   # về revision trước
helm rollback trading 5 -n $NS                 # về revision cụ thể

# Kustomize / kubectl
kubectl -n $NS rollout history deploy/order-service
kubectl -n $NS rollout undo deploy/order-service
kubectl -n $NS rollout undo deploy/order-service --to-revision=3
```

⚠️ **Rollback chart ≠ rollback hệ thống.** Không undo được: Flyway migration (schema vẫn ở version mới), message đã publish lên Kafka, row đã ghi vào DB. Đó là lý do mọi migration phải tương thích ngược (expand-contract) — xem [chương 13](./13-persistence-flyway-va-mo-hinh-du-lieu.md) mục 2.3.

---

## 6. Kiểm tra sức khoẻ định kỳ

### 6.1. Hằng ngày

```bash
# Phân bố trạng thái lệnh — không được có lệnh ACCEPTED quá cũ
PSQL -c "SELECT status, count(*),
                max(now() - created_at) AS cu_nhat
         FROM ordersvc.orders GROUP BY status;"

# Outbox tồn đọng — phải ~0
PSQL -c "SELECT 'order' svc, count(*) FROM ordersvc.outbox_event WHERE published_at IS NULL
         UNION ALL SELECT 'match', count(*) FROM matchsvc.outbox_event WHERE published_at IS NULL;"

# Lag mọi consumer group
KT/kafka-consumer-groups.sh --bootstrap-server localhost:19092 --describe --all-groups

# DLT
for t in stock.ticks.DLT stock.orders.DLT stock.enriched.DLT stock.executions.DLT; do
  echo -n "$t: "
  KT/kafka-run-class.sh kafka.tools.GetOffsetShell --bootstrap-server localhost:19092 --topic $t 2>/dev/null | \
    awk -F: '{s+=$3} END {print s+0}'
done

# Độ trễ saga thực tế
PSQL -c "SELECT percentile_cont(0.50) WITHIN GROUP (ORDER BY updated_at - created_at) p50,
                percentile_cont(0.99) WITHIN GROUP (ORDER BY updated_at - created_at) p99
         FROM ordersvc.orders WHERE updated_at IS NOT NULL
           AND created_at > now() - interval '1 day';"
```

### 6.2. Hằng tuần

```bash
# Kích thước bảng và tốc độ tăng
PSQL -c "SELECT schemaname||'.'||relname bang, n_live_tup, n_dead_tup,
                pg_size_pretty(pg_total_relation_size(relid)) kich_thuoc
         FROM pg_stat_user_tables WHERE schemaname IN ('ordersvc','matchsvc','public')
         ORDER BY pg_total_relation_size(relid) DESC;"

# Index không bao giờ được dùng
PSQL -c "SELECT schemaname||'.'||relname bang, indexrelname, idx_scan
         FROM pg_stat_user_indexes WHERE schemaname IN ('ordersvc','matchsvc','public')
         ORDER BY idx_scan LIMIT 10;"

# Query chậm (cần pg_stat_statements)
PSQL -c "SELECT round(mean_exec_time::numeric,2) tb_ms, calls, left(query,80)
         FROM pg_stat_statements ORDER BY mean_exec_time DESC LIMIT 10;"

# Connection
PSQL -c "SELECT count(*) dang_dung,
                (SELECT setting::int FROM pg_settings WHERE name='max_connections') tran
         FROM pg_stat_activity;"

# Replication slot (nếu chạy CDC)
PSQL -c "SELECT slot_name, active,
                pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) wal_lag
         FROM pg_replication_slots;"
```

### 6.3. Trước mỗi lần release

```bash
mvn clean verify                                        # gồm cả integration test
kustomize build k8s/overlays/prod > /tmp/prod.yaml
grep -E '^\s+image: ghcr\.io/' /tmp/prod.yaml           # phải là @sha256:
helm lint ./helm/trading-system -f ./helm/trading-system/values-prod.yaml
k6 run loadtest/order-load.js                           # threshold p95<200ms, p99<500ms

# Dry-run với API server thật
helm upgrade --install trading ./helm/trading-system \
  -f ./helm/trading-system/values-prod.yaml -n $NS --dry-run=server
```

---

## 7. Danh sách kiểm tra khi có sự cố

```
□ 1. Bốn service có UP không?            for p in 8081..8084; curl /actuator/health
□ 2. Dòng giá có chảy không?             curl :8082/api/v1/marketdata/HPG
□ 3. Outbox có tắc không?                SELECT count(*) WHERE published_at IS NULL
□ 4. Có lệnh treo không?                 SELECT status, count(*) FROM orders GROUP BY status
□ 5. Consumer lag?                       kafka-consumer-groups --describe --all-groups
□ 6. Relay có thất bại không?            grep outbox_publish_failed_total
□ 7. DB có khoẻ không?                   pg_isready + pg_stat_activity
□ 8. Breaker có mở không?                /actuator/metrics/resilience4j.circuitbreaker.state
□ 9. DLT có message không?               GetOffsetShell --topic stock.*.DLT
□ 10. Lấy traceId từ log → Zipkin        grep '\[service,' | mở :9411
□ 11. Alert nào đang firing?             curl :9090/api/v1/alerts
□ 12. Event gần đây của pod?             kubectl get events --sort-by=.lastTimestamp
```

---

## 8. Liên kết nhanh tới chương giải thích

| Chủ đề | Chương |
|---|:--:|
| Đường đi của một lệnh, bảng thời gian từng chặng | [00](./00-duong-di-cua-mot-lenh-end-to-end.md) |
| Vì sao cô lập lỗi hoạt động như vậy | [01](./01-kien-truc-tong-the-va-microservices-boundary.md) §4.3 |
| Partition, lag, rebalance, DLT | [02](./02-kafka-messaging-va-schema-registry.md) |
| Thuật toán risk, write-behind | [03](./03-pricing-realtime-risk-va-write-behind-cache.md) |
| Pre-trade check, breaker, rate limit | [04](./04-order-service-pretrade-risk-va-resilience.md) |
| Outbox, relay, CDC, replication slot | [05](./05-distributed-transactions-outbox-va-cdc.md) |
| Ba lớp dedupe, hợp đồng retention | [06](./06-idempotency-va-consumer-deduplication.md) |
| Saga, state machine, alert kẹt saga | [07](./07-matching-engine-va-saga-choreography.md) |
| Metrics, tracing, alert, probe | [08](./08-observability-metrics-tracing-va-alerting.md) |
| Docker Compose, hybrid, test | [09](./09-trien-khai-local-va-integration-testing.md) |
| Kustomize, KEDA, trần pod | [10](./10-trien-khai-kubernetes-kustomize-va-keda.md) |
| Helm, ESO, rollback, ngân sách RAM | [11](./11-trien-khai-helm-va-ha-tang-production.md) |
| NetworkPolicy, SASL, ACL | [12](./12-bao-mat-he-thong.md) |
| Flyway, index, pool, migration | [13](./13-persistence-flyway-va-mo-hinh-du-lieu.md) |
| **Toàn bộ hạn chế đã biết** | [14](./14-han-che-da-biet-va-lo-trinh.md) |
