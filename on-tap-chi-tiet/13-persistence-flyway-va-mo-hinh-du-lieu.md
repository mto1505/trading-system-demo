# Chương 13 — Persistence, Flyway & Mô Hình Dữ Liệu

> **Câu hỏi trung tâm:** ba service chia sẻ một PostgreSQL instance mà không giẫm chân nhau; schema được version-control; và mọi quyết định JPA ở đây đều có lý do liên quan tới một sự cố cụ thể.
>
> **Tài liệu đối chiếu:** các file `db/migration/*.sql`, `*/entity/*.java`, `DEEP-DIVE.md` mục 3 & 7.

---

## 1. Nó là gì?

### 1.1. Một instance, ba schema

| Schema | Sở hữu bởi | Bảng | `flyway_schema_history` riêng |
|---|---|---|:--:|
| `public` | `pricing-risk-service` | `market_snapshot` | ✅ |
| `ordersvc` | `order-service` | `orders`, `outbox_event` | ✅ |
| `matchsvc` | `matching-engine` | `processed_event`, `execution`, `outbox_event` | ✅ |

```sql
-- order-service/src/main/resources/db/migration/V1__orders_and_outbox.sql
-- Schema riêng "ordersvc": nhiều service dùng chung 1 Postgres instance (demo)
-- nhưng KHÔNG đụng bảng của nhau, mỗi service 1 flyway_schema_history riêng.
```

```yaml
# order-service/application.yml
spring:
  flyway:
    enabled: true
    schemas: ordersvc              # Flyway tự tạo schema + chạy migration trong đó
    default-schema: ordersvc
```

```yaml
# matching-engine/application.yml
spring:
  flyway:
    schemas: matchsvc
    default-schema: matchsvc
```

🔑 **Hai bảng cùng tên `outbox_event` ở hai schema khác nhau** — đó chính là bằng chứng của sự cô lập. Chúng có lược đồ **khác nhau** (`ordersvc.outbox_event` có `aggregate_type`, `matchsvc.outbox_event` không có) và tiến hoá độc lập, mà vẫn dùng chung một `OutboxRelay`.

### 1.2. Ba mức độ cô lập dữ liệu

| | Database riêng | **Schema riêng, instance chung** | Bảng chung |
|---|---|---|---|
| Cô lập logic | Hoàn toàn | ✅ **Hoàn toàn** | ❌ |
| Cô lập tài nguyên | Hoàn toàn | ❌ Chung connection, disk, WAL, autovacuum | ❌ |
| Migration độc lập | ✅ | ✅ | ❌ |
| Chi phí | 3 instance | ✅ **1 instance** | 1 instance |
| Nâng cấp lên mức cao hơn | — | ✅ **Chỉ đổi `DB_HOST`** | Phải refactor |

🔑 Điểm quan trọng: nâng từ "schema riêng" lên "database riêng" **không cần đổi một dòng mã nào** — mỗi service đã đọc `DB_HOST` riêng từ biến môi trường. Đó là lý do điểm dừng này hợp lý: nó không khoá ta vào một kiến trúc.

⚠️ Nhưng cô lập tài nguyên thì không có: một service quét bảng lớn sẽ làm chậm service kia. Và autovacuum chạy trên toàn instance.

---

## 2. Flyway — schema là mã nguồn

### 2.1. `ddl-auto: validate` — quyết định quan trọng nhất

```yaml
# pricing-risk-service/application.yml (order & matching giống hệt)
spring:
  flyway:
    enabled: true                # schema do Flyway quản lý, version-control trong Git
  jpa:
    hibernate:
      ddl-auto: validate         # Hibernate CHỈ kiểm tra khớp schema, không được sửa
```

```sql
-- pricing-risk-service/src/main/resources/db/migration/V1__init_market_snapshot.sql
-- Flyway migration V1: schema được version-control, KHÔNG để Hibernate tự sửa.
-- Bài học từ mọi cuộc migration DB (DB2->MySQL...): schema drift là kẻ thù số 1.
```

| `ddl-auto` | Hành vi | Dùng được ở đâu |
|---|---|---|
| `create-drop` | **Xoá và tạo lại** mỗi lần khởi động | Chỉ unit test |
| `create` | Xoá và tạo lại lúc start | Không bao giờ |
| `update` | Tự thêm cột/bảng thiếu | 🔴 **Nguy hiểm nhất** — xem dưới |
| **`validate`** | **Chỉ so sánh**, không sửa; lệch thì **fail khởi động** | ✅ Mọi môi trường |
| `none` | Không làm gì | Được, nhưng mất lớp kiểm tra |

🔑 **Vì sao `update` nguy hiểm nhất** — nguy hiểm hơn cả `create-drop`:

| Vấn đề | Giải thích |
|---|---|
| Chỉ **thêm**, không bao giờ **xoá/sửa** | Đổi tên cột ⇒ Hibernate tạo cột **mới**, cột cũ vẫn còn với toàn bộ dữ liệu ⇒ không ai biết cái nào là thật |
| Không version | Không biết schema hiện tại từ đâu ra, không reproduce được |
| Không rollback | Không có đường lùi |
| **Kết quả phụ thuộc thứ tự deploy** | Pod cũ và pod mới cùng chạy trong rolling update ⇒ schema cuối cùng tuỳ thuộc pod nào khởi động sau |
| Không review được | Thay đổi schema không qua pull request |

🔑 **`validate` biến schema drift thành lỗi khởi động — ồn ào, ngay lập tức, không thể bỏ qua.** Nếu một dev thêm field vào entity mà quên viết migration, pod **không khởi động được**:

```
Schema-validation: missing column [new_field] in table [ordersvc.orders]
```

Thà fail lúc deploy còn hơn chạy được rồi hỏng lúc có dữ liệu thật.

💡 Và nó bắt được **cả hai chiều**: entity có field mà DB thiếu cột, **hoặc** kiểu dữ liệu không khớp (ví dụ entity khai `BigDecimal precision=18 scale=4` mà cột là `NUMERIC(10,2)`).

### 2.2. Thứ tự khởi động

```
1. Container start
2. initContainer wait-for-db: pg_isready cho tới khi DB nhận kết nối
3. Spring context bắt đầu
4. DataSource (HikariCP) khởi tạo
5. ─── FLYWAY chạy migration ───       ← tạo/sửa schema
6. Hibernate validate                   ← so sánh entity với schema
7. Phần còn lại của context
8. Kafka listener bắt đầu poll
9. /actuator/health/readiness trả UP
```

🔑 **Flyway phải chạy TRƯỚC Hibernate validate** — Spring Boot bảo đảm điều này qua `@DependsOn` nội bộ (bean `flywayInitializer` là dependency của `entityManagerFactory`). Nếu thứ tự ngược lại, validate sẽ luôn thất bại ở lần deploy đầu tiên vì bảng chưa tồn tại.

💡 Đây cũng là lý do `startupProbe` có `failureThreshold: 36` (180 giây): bước 2 có thể chờ lâu, bước 5 có thể chạy migration nặng.

### 2.3. Năm migration hiện có

| Service | File | Nội dung |
|---|---|---|
| pricing | `V1__init_market_snapshot.sql` | `market_snapshot` + index `(symbol, snap_time)` |
| order | `V1__orders_and_outbox.sql` | `orders` + `outbox_event` + partial index |
| order | `V2__order_state_machine.sql` | `version`, `updated_at`, `filled_qty`, `avg_fill_price` + partial index |
| matching | `V1__matching_tables.sql` | `processed_event` + `execution` + index |
| matching | `V2__matching_outbox.sql` | `outbox_event` + partial index |

🔑 **`V2__order_state_machine.sql` là ví dụ về migration tương thích ngược:**

```sql
-- Vòng đời lệnh: ACCEPTED -> FILLED | REJ_MATCH (do matching-engine phản hồi)
ALTER TABLE orders ADD COLUMN version        BIGINT        NOT NULL DEFAULT 0;
ALTER TABLE orders ADD COLUMN updated_at     TIMESTAMPTZ;
ALTER TABLE orders ADD COLUMN filled_qty     BIGINT;
ALTER TABLE orders ADD COLUMN avg_fill_price NUMERIC(18,4);
```

Bốn cột **thêm mới**, không đổi, không xoá. `version NOT NULL DEFAULT 0` cho phép row cũ có giá trị hợp lệ. Nghĩa là trong rolling update, pod chạy mã **V1** vẫn hoạt động bình thường với schema **V2** — nó chỉ không biết bốn cột mới tồn tại.

🔑 Đây chính là nguyên tắc **expand–contract**:

| Giai đoạn | Việc | An toàn vì |
|---|---|---|
| **Expand** | Thêm cột mới (nullable hoặc có default) | Mã cũ bỏ qua cột mới |
| **Migrate** | Deploy mã mới đọc/ghi cột mới; backfill dữ liệu | Cả hai phiên bản mã cùng chạy được |
| **Contract** | Xoá cột cũ ở một migration **sau** | Không còn mã nào dùng cột cũ |

Những thao tác **không** tương thích ngược và phải tránh trong rolling update: đổi tên cột, đổi kiểu dữ liệu, thêm `NOT NULL` không default, xoá cột, thêm constraint mà dữ liệu hiện có vi phạm.

⚠️ Và nhớ: **`helm rollback` không undo Flyway migration.** Rollback chart về revision cũ thì mã cũ chạy với schema mới — và điều đó chỉ an toàn nếu mọi migration đều tương thích ngược. Đây là lý do nguyên tắc expand-contract không phải tuỳ chọn.

### 2.4. Vận hành Flyway

```bash
PSQL='docker compose exec postgres psql -U trading -d trading'

# Lịch sử migration của từng service
$PSQL -c "SELECT installed_rank, version, description, success, installed_on
          FROM ordersvc.flyway_schema_history ORDER BY installed_rank;"
$PSQL -c "SELECT version, description, success FROM matchsvc.flyway_schema_history;"
$PSQL -c "SELECT version, description, success FROM public.flyway_schema_history;"
```

⚠️ **Nếu một migration thất bại giữa chừng**, Flyway ghi `success = false` và **chặn mọi migration tiếp theo**. Phải:

```sql
-- 1. Xem migration nào hỏng
SELECT * FROM ordersvc.flyway_schema_history WHERE success = false;

-- 2. Dọn tay phần đã áp dụng dở (Flyway KHÔNG tự rollback với Postgres DDL
--    không transactional, dù Postgres hỗ trợ transactional DDL cho hầu hết lệnh)

-- 3. Xoá bản ghi hỏng
DELETE FROM ordersvc.flyway_schema_history WHERE success = false;

-- 4. Restart pod để chạy lại
```

💡 Hoặc dùng `flyway repair` nếu có Flyway CLI.

⚠️ **Checksum mismatch** là lỗi hay gặp thứ hai: sửa một file migration **đã chạy** ⇒ Flyway phát hiện checksum khác ⇒ fail. Đó là tính năng, không phải bug — nó bảo đảm schema ở mọi môi trường đi qua đúng cùng một chuỗi thay đổi. Quy tắc: **migration đã merge thì không bao giờ sửa, chỉ thêm migration mới.**

---

## 3. Mô hình dữ liệu chi tiết

### 3.1. `ordersvc.orders` — hồ sơ mọi lệnh

```sql
CREATE TABLE orders (
    id              BIGSERIAL PRIMARY KEY,
    order_id        VARCHAR(36)  NOT NULL UNIQUE,
    client_order_id VARCHAR(64)  NOT NULL,
    status          VARCHAR(10)  NOT NULL,
    reason          VARCHAR(200),
    symbol          VARCHAR(20)  NOT NULL,
    side            VARCHAR(4)   NOT NULL,
    price           NUMERIC(18,4) NOT NULL,
    quantity        BIGINT       NOT NULL,
    ref_price       NUMERIC(18,4),
    risk_level      VARCHAR(10),
    account_id      VARCHAR(32)  NOT NULL,
    created_at      TIMESTAMPTZ  NOT NULL,
    -- DEDUPE TẬP TRUNG cấp DB: chốt chặn cuối cùng chống duplicate lệnh
    CONSTRAINT uq_client_order UNIQUE (client_order_id)
);
-- V2
ALTER TABLE orders ADD COLUMN version        BIGINT        NOT NULL DEFAULT 0;
ALTER TABLE orders ADD COLUMN updated_at     TIMESTAMPTZ;
ALTER TABLE orders ADD COLUMN filled_qty     BIGINT;
ALTER TABLE orders ADD COLUMN avg_fill_price NUMERIC(18,4);
CREATE INDEX idx_orders_pending ON orders (created_at) WHERE status = 'ACCEPTED';
```

**Giải thích từng quyết định:**

| Cột / ràng buộc | Lý do |
|---|---|
| `id BIGSERIAL PRIMARY KEY` | Khoá kỹ thuật. Không dùng `order_id` làm PK vì `VARCHAR(36)` làm index lớn hơn và join chậm hơn `BIGINT` |
| `order_id VARCHAR(36) UNIQUE` | UUID do server sinh. 36 = độ dài UUID có dấu gạch |
| **`CONSTRAINT uq_client_order UNIQUE`** | 🔑 **Chốt chặn cuối cùng** chống lệnh trùng — Redis chết, đa pod, retry đều không vượt qua được |
| `price NUMERIC(18,4)` | 🔑 **Không** `DOUBLE PRECISION` — tiền tệ phải chính xác thập phân |
| `quantity BIGINT` | Khối lượng có thể rất lớn với phái sinh |
| `reason VARCHAR(200)` | Lưu cả lý do từ chối ⇒ trả lời được "tại sao lệnh này bị từ chối" |
| `version BIGINT NOT NULL DEFAULT 0` | Optimistic locking; `DEFAULT 0` để row cũ hợp lệ |
| `created_at` / `updated_at TIMESTAMPTZ` | 🔑 **`TIMESTAMPTZ` không phải `TIMESTAMP`** — xem mục 3.6 |

🔑 **Partial index `idx_orders_pending`:**

```sql
-- Truy vấn vận hành hay dùng: "lệnh nào đang treo chưa có phản hồi khớp?"
CREATE INDEX idx_orders_pending ON orders (created_at) WHERE status = 'ACCEPTED';
```

Bảng `orders` chỉ tăng (giữ cả lệnh REJECTED để audit), nhưng số lệnh đang `ACCEPTED` tại mọi thời điểm chỉ vài chục — lệnh vào rồi ra trong ~1 giây. Index này **chỉ chứa** các row đó, nên nó luôn nhỏ bất kể bảng có bao nhiêu triệu row.

Query vận hành quan trọng nhất của hệ chạy tức thì:

```sql
SELECT client_order_id, symbol, created_at,
       round(extract(epoch from (now() - created_at))) AS treo_giay
FROM ordersvc.orders
WHERE status = 'ACCEPTED' AND created_at < now() - interval '1 minute'
ORDER BY created_at;
```

⚠️ **Thiếu index cho các endpoint liệt kê.** `OrderController.list` lọc theo `accountId` và `status` với `ORDER BY createdAt DESC`, nhưng không có index nào phục vụ. Với bảng hàng triệu row, mỗi query là một seq scan + sort. Nên thêm:

```sql
CREATE INDEX idx_orders_account_created ON orders (account_id, created_at DESC);
CREATE INDEX idx_orders_status_created  ON orders (status, created_at DESC);
```

### 3.2. `ordersvc.outbox_event` / `matchsvc.outbox_event`

```sql
CREATE TABLE outbox_event (
    id             BIGSERIAL PRIMARY KEY,
    aggregate_type VARCHAR(30) NOT NULL,     -- chỉ có ở ordersvc
    aggregate_id   VARCHAR(64) NOT NULL,
    event_type     VARCHAR(30) NOT NULL,
    topic          VARCHAR(60) NOT NULL,
    msg_key        VARCHAR(64) NOT NULL,
    payload        TEXT        NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL,
    published_at   TIMESTAMPTZ
);
CREATE INDEX idx_outbox_unpublished ON outbox_event (id) WHERE published_at IS NULL;
```

Chi tiết ở [chương 05](./05-distributed-transactions-outbox-va-cdc.md). Hai điểm về mặt dữ liệu:

🔑 **`payload TEXT` chứ không `JSONB`.** JSONB cho phép truy vấn bên trong (`payload->>'symbol'`), index GIN, và validate cấu trúc JSON. Nhưng ở đây payload **không bao giờ được truy vấn** — relay chỉ đọc nguyên văn rồi gửi đi. `TEXT` đơn giản hơn, không tốn chi phí parse/normalize của JSONB khi ghi, và quan trọng nhất: nó **không ràng buộc payload phải là JSON** — nếu mai sau đổi sang Avro hoặc Protobuf (base64), cột này vẫn dùng được.

💡 Nhưng nếu cần debug nhiều (tìm event theo symbol chẳng hạn), JSONB sẽ tiện hơn. Đánh đổi hợp lý ở đây vì có `aggregate_id`, `topic`, `msg_key` làm cột riêng để tra cứu.

⚠️ **Cả hai bảng chỉ tăng, không có job dọn.** Row `published_at IS NOT NULL` nằm mãi. Cần:

```sql
DELETE FROM ordersvc.outbox_event
WHERE published_at IS NOT NULL AND published_at < now() - interval '7 days';
```

Hoặc tốt hơn — partition theo ngày rồi `DROP PARTITION` (rẻ hơn nhiều vì không sinh dead tuple cần VACUUM).

🔑 **Tuyệt đối không xoá row `published_at IS NULL`** — đó là event chưa ra Kafka, tức là lệnh chưa tới sàn.

### 3.3. `matchsvc.processed_event`

```sql
CREATE TABLE processed_event (
    client_order_id VARCHAR(64) PRIMARY KEY,   -- PK = cơ chế dedupe
    processed_at    TIMESTAMPTZ NOT NULL
);
```

🔑 **PRIMARY KEY *là* cơ chế dedupe** — không cần index riêng, không cần cột `id` thừa. `INSERT ... ON CONFLICT (client_order_id) DO NOTHING` dựa trực tiếp vào unique index của PK.

⚠️ **Gap #12** — hợp đồng ngầm với retention topic. Chi tiết ở [chương 06](./06-idempotency-va-consumer-deduplication.md) mục 2.7:

$$\text{thời gian giữ } \texttt{processed\_event} \;>\; \texttt{TOPIC\_RETENTION\_MS} \;(7\text{ ngày})$$

### 3.4. `matchsvc.execution`

```sql
CREATE TABLE execution (
    id              BIGSERIAL PRIMARY KEY,
    order_id        VARCHAR(36)   NOT NULL,
    client_order_id VARCHAR(64)   NOT NULL,
    symbol          VARCHAR(20)   NOT NULL,
    side            VARCHAR(4)    NOT NULL,
    fill_price      NUMERIC(18,4) NOT NULL,
    quantity        BIGINT        NOT NULL,
    exec_time       TIMESTAMPTZ   NOT NULL
);
CREATE INDEX idx_exec_client_order ON execution (client_order_id);
```

⚠️ **Gap #15 — `client_order_id` có INDEX nhưng KHÔNG có UNIQUE.**

```sql
CREATE INDEX idx_exec_client_order ON execution (client_order_id);   -- chỉ là index
```

So với `orders`:

```sql
CONSTRAINT uq_client_order UNIQUE (client_order_id)                  -- là constraint
```

🔴 Hệ quả: `processed_event` là lớp chặn khớp-trùng **duy nhất** ở matching-engine, không có lưới an toàn. Nếu bảng đó bị truncate hoặc dọn nhầm, hệ sẽ khớp lệnh hai lần và **không gì chặn lại**.

Sửa — migration `V3`:

```sql
-- matching-engine/src/main/resources/db/migration/V3__execution_unique.sql
-- Lưới an toàn thứ hai: processed_event không còn là lớp chặn duy nhất.
-- Sau migration này, job dọn processed_event chỉ còn là chuyện DUNG LƯỢNG,
-- không còn là chuyện ĐÚNG/SAI.
ALTER TABLE execution ADD CONSTRAINT uq_exec_client_order UNIQUE (client_order_id);
```

⚠️ Phải kiểm tra trước — migration sẽ **fail** nếu đã có row trùng:

```sql
SELECT client_order_id, count(*) FROM matchsvc.execution
GROUP BY client_order_id HAVING count(*) > 1;
```

💡 Và sau khi thêm, `UNIQUE` tự tạo index nên `idx_exec_client_order` trở thành **thừa** — nên xoá để tiết kiệm chi phí ghi.

💡 Lưu ý thêm: khi hỗ trợ partial fill (nhiều execution cho một lệnh), `UNIQUE(client_order_id)` sẽ **không còn đúng** — lúc đó khoá phải là `(client_order_id, exec_seq)` hoặc một `exec_id` riêng.

### 3.5. `public.market_snapshot`

```sql
CREATE TABLE market_snapshot (
    id          BIGSERIAL PRIMARY KEY,
    symbol      VARCHAR(20)    NOT NULL,
    last_price  NUMERIC(18,4),
    ema_price   NUMERIC(18,4),
    volatility  NUMERIC(10,6),
    var95       NUMERIC(10,6),
    risk_level  VARCHAR(10),
    snap_time   TIMESTAMPTZ    NOT NULL
);
CREATE INDEX idx_symbol_time ON market_snapshot (symbol, snap_time);
```

💡 **Precision khác nhau có chủ đích:** `NUMERIC(18,4)` cho giá (cần 4 chữ số thập phân cho tiền, khớp với Avro `scale=4`), `NUMERIC(10,6)` cho volatility/VaR (là tỉ lệ %, cần nhiều chữ số thập phân hơn nhưng phạm vi nhỏ hơn).

🔑 **Thứ tự cột trong index `(symbol, snap_time)` là đúng.** Query:

```sql
SELECT * FROM market_snapshot WHERE symbol = :symbol ORDER BY snap_time DESC LIMIT :limit
```

Postgres dùng cột đầu lọc `WHERE symbol = ?`, cột thứ hai phục vụ `ORDER BY snap_time DESC` mà **không cần sort thêm**. Đảo thành `(snap_time, symbol)` thì phải quét index theo thời gian rồi lọc — chậm hơn nhiều.

⚠️ **Ops #3 — bảng này phình nhanh nhất.** Chi tiết ở [chương 03](./03-pricing-realtime-risk-va-write-behind-cache.md) mục 3.4: `latest` không bao giờ bị xoá ⇒ mọi flush ghi lại toàn bộ mã ⇒ **864.000 row/ngày** với 50 mã, gần như toàn bộ là bản sao.

Ngoài việc sửa flush theo diff, bảng time-series này cần **partition theo ngày**:

```sql
-- Hướng nâng cấp (Postgres 11+ declarative partitioning)
CREATE TABLE market_snapshot (
    id          BIGSERIAL,
    symbol      VARCHAR(20) NOT NULL,
    ...
    snap_time   TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (id, snap_time)          -- PK phải chứa cột partition
) PARTITION BY RANGE (snap_time);

CREATE TABLE market_snapshot_2026_11 PARTITION OF market_snapshot
    FOR VALUES FROM ('2026-11-01') TO ('2026-12-01');

-- Dọn: DROP PARTITION thay vì DELETE -> tức thì, không sinh dead tuple
DROP TABLE market_snapshot_2026_08;
```

💡 Hoặc dùng TimescaleDB / `pg_partman` để tự động hoá việc tạo và dọn partition.

### 3.6. 🔑 `TIMESTAMPTZ` vs `TIMESTAMP`

**Mọi** cột thời gian trong repo đều là `TIMESTAMPTZ`. Đây không phải thói quen mà là một quyết định đúng:

| | `TIMESTAMP` (without time zone) | **`TIMESTAMPTZ`** |
|---|---|---|
| Lưu gì | Một chuỗi ngày-giờ, **không có ngữ cảnh** | Một **thời điểm tuyệt đối** (lưu dưới dạng UTC) |
| So sánh giữa hai máy khác múi giờ | 🔴 Vô nghĩa | ✅ Đúng |
| `now()` trả về | Giờ của session | Thời điểm tuyệt đối |
| Đổi múi giờ server | 🔴 Dữ liệu cũ đổi nghĩa | ✅ Không ảnh hưởng |
| Java mapping | `LocalDateTime` | **`Instant`** |

🔑 Và entity khai `Instant`, không phải `LocalDateTime`:

```java
// order-service/entity/OrderEntity.java
@Column(name = "created_at", nullable = false)
private Instant createdAt;
```

`Instant` là một thời điểm tuyệt đối không mang múi giờ — khớp chính xác ngữ nghĩa của `TIMESTAMPTZ`. Dùng `LocalDateTime` sẽ tạo ra sự mơ hồ: giờ của ai?

💡 Với hệ thống giao dịch, điều này đặc biệt quan trọng: thời điểm đặt lệnh phải so sánh được chính xác để xác định thứ tự ưu tiên, và báo cáo cho cơ quan quản lý phải nhất quán về múi giờ.

---

## 4. Quyết định JPA — mỗi cái tương ứng một sự cố

### 4.1. 🔑 `open-in-view: false`

```yaml
# Cả ba service có DB
spring:
  jpa:
    open-in-view: false          # QUAN TRỌNG: tắt OSIV, tránh giữ connection suốt request
```

**Open Session In View (OSIV)** là một tính năng Spring Boot **mặc định bật** — và là một trong những mặc định gây tranh cãi nhất của framework này.

| | OSIV bật (mặc định) | **OSIV tắt** |
|---|---|---|
| Vòng đời EntityManager | Từ đầu request tới khi render xong response | Chỉ trong `@Transactional` |
| Connection DB bị giữ | 🔴 **Suốt cả request**, kể cả lúc serialize JSON | Chỉ trong transaction |
| LAZY loading ở controller | Hoạt động (âm thầm) | Ném `LazyInitializationException` |
| N+1 query | 🔴 Xảy ra **âm thầm** ở tầng view | Lộ ra ngay lúc dev |
| Connection pool | 🔴 Cạn nhanh dưới tải | Dùng hiệu quả |

🔑 **Hậu quả cụ thể của OSIV bật với `maximum-pool-size: 20`:** nếu mỗi request giữ connection suốt 50ms (gồm cả thời gian serialize JSON và ghi response ra network), thì throughput trần là `20 / 0,05 = 400 req/s` — **bất kể** transaction thật chỉ mất 3ms. Phần lớn thời gian connection bị giữ là để... chờ Jackson và chờ TCP.

Tắt OSIV buộc phải viết đúng: mọi dữ liệu cần cho response phải được nạp **trong** transaction, và DTO phải được map **trong** service hoặc ngay sau khi đọc. Đó chính là lý do `OrderView` tồn tại.

### 4.2. DTO thay vì trả entity

```java
// order-service/src/main/java/com/trading/order/model/OrderView.java:8-29
/**
 * Bản chiếu CHỈ-ĐỌC của một lệnh, dùng cho các endpoint GET.
 *
 * Vì sao không trả thẳng OrderEntity:
 *
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
public record OrderView(
        String orderId, String clientOrderId, String status, String reason,
        String symbol, String side, BigDecimal price, Long quantity,
        BigDecimal refPrice, String riskLevel, String accountId,
        Long filledQty, BigDecimal avgFillPrice,
        Instant createdAt, Instant updatedAt
) {
    public static OrderView from(OrderEntity e) { ... }
}
```

🔑 **Lý do 2 là nghiêm trọng nhất về mặt bảo mật và tương thích.** Nếu trả thẳng `OrderEntity`, thì:
- `id` và `version` lọt ra API — client có thể bắt đầu phụ thuộc vào chúng.
- Thêm một cột nội bộ bất kỳ (ví dụ `internal_risk_score`, `commission_rate`) là **tự động** lộ ra API ở lần deploy kế tiếp, không ai review.

💡 Và `OrderView` **cố ý không có** `id` lẫn `version` — hai thứ thuần tuý kỹ thuật.

⚠️ **Không nhất quán:** `MarketDataController.history()` và `ExecutionController` **trả thẳng entity**:

```java
// pricing-risk-service/controller/MarketDataController.java:44-47
@GetMapping("/{symbol}/history")
public List<MarketSnapshotEntity> history(@PathVariable String symbol, ...) {
    return repository.findRecent(symbol, limit);
}
```
```java
// matching-engine/controller/ExecutionController.java:21-24
@GetMapping("/{clientOrderId}")
public List<ExecutionEntity> byClientOrderId(@PathVariable String clientOrderId) {
    return repository.findByClientOrderId(clientOrderId);
}
```

Cả hai entity này hiện không có quan hệ LAZY nên không nổ exception, nhưng vẫn vi phạm lý do 2 và 3. Nên có `SnapshotView` và `ExecutionView` tương ứng.

### 4.3. Phân trang bắt buộc

```java
// order-service/src/main/java/com/trading/order/repository/OrderRepository.java:13-24
/*
 * Phân trang là BẮT BUỘC cho endpoint liệt kê, không phải tuỳ chọn.
 * Bảng orders giữ MỌI lệnh kể cả REJECTED để phục vụ audit, nên nó chỉ có
 * tăng. Một `findAll()` không giới hạn sẽ kéo cả bảng vào heap 336Mi của pod
 * và giết nó bằng OOM - và sự cố đó chỉ xảy ra ở môi trường có dữ liệu thật,
 * không bao giờ lộ ra lúc dev.
 */
Page<OrderEntity> findAllByStatus(String status, Pageable pageable);
Page<OrderEntity> findAllByAccountId(String accountId, Pageable pageable);
Page<OrderEntity> findAllByAccountIdAndStatus(String accountId, String status, Pageable pageable);
```

🔑 *"sự cố đó chỉ xảy ra ở môi trường có dữ liệu thật, không bao giờ lộ ra lúc dev"* — đây là đặc điểm chung của cả một nhóm bug: OOM do query không giới hạn, N+1 query, index thiếu, bảng phình. Tất cả đều **không thể phát hiện bằng unit test** và chỉ xuất hiện khi đã có dữ liệu production.

Và trần cứng ở controller:

```java
// OrderController.java:26-27, 71
/** Tran cung cho size: khong de client tu dat 1_000_000 roi keo sap pod. */
private static final int MAX_PAGE_SIZE = 200;
...
int safeSize = Math.min(Math.max(size, 1), MAX_PAGE_SIZE);
```

⚠️ `MarketDataController.history()` **không có** trần tương ứng — `?limit=10000000` kéo hàng triệu row vào heap.

### 4.4. Native query khi JPQL không đủ

```java
// order-service/repository/OutboxEventRepository.java:16-23
@Query(value = """
        SELECT * FROM ordersvc.outbox_event
        WHERE published_at IS NULL
        ORDER BY id
        LIMIT :n
        FOR UPDATE SKIP LOCKED
        """, nativeQuery = true)
List<OutboxEventEntity> lockUnpublishedBatch(@Param("n") int n);
```

🔑 **JPQL không có `FOR UPDATE SKIP LOCKED`.** JPA có `@Lock(LockModeType.PESSIMISTIC_WRITE)` nhưng nó sinh `FOR UPDATE` thuần — **không có `SKIP LOCKED`** — và với 3 pod relay, đó là sự khác biệt giữa "song song thật" và "tuần tự hoá hoàn toàn" (xem [chương 05](./05-distributed-transactions-outbox-va-cdc.md) mục 2.4).

Tương tự với `ON CONFLICT DO NOTHING`:

```java
// matching-engine/repository/ProcessedEventRepository.java:14-23
@Modifying
@Query(value = """
        INSERT INTO matchsvc.processed_event (client_order_id, processed_at)
        VALUES (:id, now())
        ON CONFLICT (client_order_id) DO NOTHING
        """, nativeQuery = true)
int insertIfAbsent(@Param("id") String clientOrderId);
```

💡 **Nguyên tắc:** dùng JPA cho CRUD thông thường, rơi xuống native SQL khi cần tính năng đặc thù của database. Cố ép mọi thứ qua JPA sẽ dẫn tới code phức tạp hơn và chậm hơn — hoặc tệ hơn, dẫn tới cài đặt **sai** (như check-then-act thay vì atomic upsert).

⚠️ Cái giá: native query **khoá vào PostgreSQL**. Đổi sang MySQL thì cả hai query này phải viết lại (`INSERT IGNORE` / `ON DUPLICATE KEY UPDATE`, và MySQL 8 có `SKIP LOCKED`). Với một hệ thống đã chọn Postgres thì đánh đổi này hợp lý.

🔑 **Và `lockUnpublishedBatch` phải trả về MANAGED ENTITY**, không phải DTO projection — vì `OutboxRelay` dựa vào **dirty checking** của Hibernate để ghi `published_at`:

```java
// OutboxRelay.java:60
event.setPublishedAt(Instant.now());        // KHÔNG gọi save()
```

Nếu đổi sang projection, dòng này sẽ **không làm gì cả** và relay gửi lại vô hạn — một bug im lặng rất khó tìm.

### 4.5. Connection pool theo bản chất tải

```yaml
# pricing-risk-service
    hikari:
      maximum-pool-size: 10      # chỉ scheduler flush + history API dùng DB -> pool nhỏ đủ
```
```yaml
# order-service
    hikari:
      maximum-pool-size: 20      # mỗi lệnh 1 transaction ngắn -> pool vừa phải
```
```yaml
# matching-engine — không khai, dùng mặc định HikariCP (10)
```

| Service | Pool | Ai dùng DB |
|---|:--:|---|
| pricing | 10 | Scheduler flush mỗi 5s + API lịch sử. Hot path **không chạm DB** |
| order | 20 | **Mỗi lệnh** một transaction ngắn + relay + consumer execution report |
| matching | 10 (mặc định) | Mỗi message một transaction + relay |

🔑 **Công thức ước lượng pool size:**

$$\text{pool} \approx \text{số thread đồng thời cần DB} = \text{Tomcat threads dùng DB} + \text{Kafka listener threads} + \text{scheduler}$$

Với `order-service`: `setConcurrency(3)` × 2 listener = 6 thread Kafka + 1 relay scheduler + Tomcat (tới 200 thread nhưng mỗi request chỉ giữ connection ~3ms nhờ OSIV tắt). Pool 20 là hợp lý.

⚠️ Pool **quá lớn** cũng hại: mỗi connection Postgres là một **process** riêng (~10MB RAM), và quá nhiều connection đồng thời gây tranh chấp lock và context switch. Ba service × 20 = 60 connection, trong khi Postgres mặc định `max_connections = 100`. Nếu scale lên 3 pod mỗi service thì 180 connection ⇒ **vượt trần** ⇒ `FATAL: sorry, too many clients already`.

🔑 Đây là một ràng buộc dễ bị bỏ qua khi scale: **`max_connections` của Postgres phải ≥ tổng pool của mọi pod.** Giải pháp chuẩn: PgBouncer ở chế độ transaction pooling.

### 4.6. Hibernate batch insert

```yaml
# pricing-risk-service
    properties:
      hibernate:
        jdbc:
          batch_size: 100        # gom 100 INSERT/round-trip khi saveAll
        order_inserts: true
```

Không có `batch_size`, `saveAll(100 entities)` sinh **100 round-trip** tới Postgres. Với 100, nó sinh 1 round-trip chứa 100 câu INSERT. `order_inserts: true` sắp xếp INSERT theo bảng để batch không bị vỡ khi có nhiều loại entity.

⚠️ Batching JDBC **không hoạt động** với `GenerationType.IDENTITY` ở Hibernate 5 — Hibernate phải lấy generated key ngay sau mỗi insert. Hibernate 6 (Spring Boot 3.x) đã hỗ trợ qua `getGeneratedKeys`, nên cấu hình này có hiệu lực. Nhưng nếu nâng/hạ version cần kiểm chứng lại:

```yaml
spring.jpa.properties.hibernate.generate_statistics: true
# -> log sẽ in số JDBC batch thực tế
```

💡 Chỉ `pricing-risk-service` khai `batch_size` vì chỉ nó dùng `saveAll` với lô lớn. `order-service` và `matching-engine` ghi từng row một trong mỗi transaction — batching không giúp gì.

### 4.7. `GenerationType.IDENTITY`

```java
@Id
@GeneratedValue(strategy = GenerationType.IDENTITY)
private Long id;
```

| Strategy | Cơ chế | Ưu | Nhược |
|---|---|---|---|
| **`IDENTITY`** | `BIGSERIAL` của Postgres | Đơn giản, không cần round-trip thêm | Hibernate phải insert ngay để lấy ID ⇒ hạn chế batching (đã cải thiện ở Hibernate 6) |
| `SEQUENCE` | Sequence riêng, có `allocationSize` | 🔑 Batching tốt nhất — lấy 50 ID một lần | Phức tạp hơn; ID có khoảng trống |
| `TABLE` | Bảng đếm | Portable | Chậm, gây tranh chấp |
| `AUTO` | Hibernate tự chọn | — | Không tường minh |

💡 Với `pricing-risk-service` ghi batch lớn, `SEQUENCE` với `allocationSize = 100` sẽ tối ưu hơn `IDENTITY`. Nhưng `IDENTITY` đơn giản hơn và ở quy mô hiện tại khác biệt không đáng kể.

---

## 5. Vấn đề được giải quyết

| Nếu không có | Hậu quả |
|---|---|
| Flyway + `ddl-auto: validate` | Schema drift âm thầm; `update` tạo cột thừa; không reproduce được |
| Schema riêng mỗi service | Service đụng bảng của nhau; migration ràng buộc lẫn nhau |
| `UNIQUE(client_order_id)` | Redis chết ⇒ lệnh trùng |
| `PRIMARY KEY` trên `processed_event` | Không có atomic test-and-set ⇒ khớp lệnh hai lần |
| `UNIQUE` trên `execution` (⚠️ **thiếu**) | `processed_event` là lớp chặn duy nhất |
| Partial index | Index phình theo bảng, quét chậm |
| `TIMESTAMPTZ` + `Instant` | So sánh thời gian giữa service khác múi giờ ra kết quả sai |
| `NUMERIC` thay `DOUBLE` | Sai số tích luỹ thành tiền thật |
| `open-in-view: false` | Connection bị giữ suốt request ⇒ pool cạn ⇒ throughput trần thấp |
| DTO thay entity | `LazyInitializationException`; cột nội bộ lọt ra API |
| Phân trang | OOM ở môi trường có dữ liệu thật |
| Native query | Không có `SKIP LOCKED` / `ON CONFLICT` ⇒ cài đặt sai |
| Managed entity trong relay | Dirty checking không chạy ⇒ `published_at` không bao giờ được ghi |
| `batch_size` | 100 round-trip thay vì 1 |
| Pool size theo tải | Pool cạn, hoặc vượt `max_connections` |

---

## 6. Đánh đổi

### 6.1. Ưu

1. **Schema là mã nguồn** — version-control, review được, reproduce được.
2. **`validate` biến drift thành lỗi ồn ào** ngay lúc khởi động.
3. **Cô lập logic hoàn toàn** giữa ba service với chi phí một instance.
4. **Nâng cấp lên DB riêng không cần đổi mã.**
5. **Partial index** luôn nhỏ bất kể bảng lớn.
6. **Constraint ở tầng DB** là chốt chặn không thể vượt qua.
7. **`TIMESTAMPTZ` + `Instant`** đúng ngữ nghĩa.
8. **Tắt OSIV** dùng connection hiệu quả.
9. **DTO** bảo vệ hợp đồng API.
10. **Migration tương thích ngược** (expand-contract).

### 6.2. Nhược

1. **Không cô lập tài nguyên** — một service nặng làm chậm service kia.
2. ⚠️ **Thiếu `UNIQUE` trên `execution.client_order_id`** (Gap #15).
3. ⚠️ **Ba bảng chỉ tăng, không có job dọn:** `outbox_event` ×2, `processed_event`, `market_snapshot`.
4. ⚠️ **`market_snapshot` ghi lại cả mã không đổi** (Ops #3).
5. ⚠️ **Thiếu index cho endpoint liệt kê** `orders`.
6. ⚠️ **`MarketDataController.history` không chặn `limit`.**
7. ⚠️ **Hai controller trả thẳng entity**, không qua DTO.
8. **Native query khoá vào PostgreSQL.**
9. **Tổng pool có thể vượt `max_connections`** khi scale.
10. **Không có read replica** — mọi truy vấn vào primary.

---

## 7. Núm vặn cấu hình

| Tham số | Service | Mặc định | Ghi chú |
|---|---|---|---|
| `spring.flyway.schemas` | order / matching | `ordersvc` / `matchsvc` | Cô lập logic |
| `spring.jpa.hibernate.ddl-auto` | cả 3 | `validate` | 🔑 **Không bao giờ** đổi sang `update` |
| `spring.jpa.open-in-view` | cả 3 | `false` | 🔑 Bật lại = pool cạn |
| `hikari.maximum-pool-size` | pricing 10 / order 20 / matching mặc định | | Tổng × số pod phải ≤ `max_connections` |
| `hibernate.jdbc.batch_size` | pricing | 100 | Chỉ có ý nghĩa với `saveAll` lô lớn |
| `hibernate.order_inserts` | pricing | `true` | Giữ batch không bị vỡ |
| `MAX_PAGE_SIZE` | order (hardcode) | 200 | ⚠️ pricing không có tương ứng |
| `max_connections` | Postgres | 100 | ⚠️ Ràng buộc dễ quên khi scale |

---

## 8. Cách kiểm chứng

```bash
PSQL='docker compose exec postgres psql -U trading -d trading'

# === Cấu trúc ===
$PSQL -c "\dn"                         # liệt kê schema
$PSQL -c "\dt ordersvc.*"
$PSQL -c "\dt matchsvc.*"
$PSQL -c "\dt public.*"
$PSQL -c "\d ordersvc.orders"
$PSQL -c "\d matchsvc.execution"

# Constraint — chứng minh Gap #15
$PSQL -c "SELECT conname, contype, pg_get_constraintdef(oid)
          FROM pg_constraint WHERE conrelid = 'ordersvc.orders'::regclass;"
$PSQL -c "SELECT conname, contype, pg_get_constraintdef(oid)
          FROM pg_constraint WHERE conrelid = 'matchsvc.execution'::regclass;"
#  -> orders CÓ uq_client_order; execution KHÔNG có

# Index
$PSQL -c "SELECT schemaname, tablename, indexname, indexdef FROM pg_indexes
          WHERE schemaname IN ('ordersvc','matchsvc','public') ORDER BY tablename;"

# === Flyway ===
$PSQL -c "SELECT installed_rank, version, description, success, installed_on
          FROM ordersvc.flyway_schema_history ORDER BY installed_rank;"
$PSQL -c "SELECT version, description, success FROM matchsvc.flyway_schema_history;"
$PSQL -c "SELECT version, description, success FROM public.flyway_schema_history;"

# Migration nào thất bại?
$PSQL -c "SELECT * FROM ordersvc.flyway_schema_history WHERE success = false;"

# === Chứng minh ddl-auto: validate ===
# Thêm một field vào OrderEntity mà KHÔNG viết migration, rồi khởi động:
#   -> Schema-validation: missing column [...] in table [ordersvc.orders]
#   -> pod KHÔNG khởi động được (đúng như mong muốn)

# === Kích thước và tốc độ tăng ===
$PSQL -c "SELECT schemaname||'.'||relname AS bang,
                 n_live_tup AS row_song, n_dead_tup AS row_chet,
                 pg_size_pretty(pg_total_relation_size(relid)) AS kich_thuoc
          FROM pg_stat_user_tables
          WHERE schemaname IN ('ordersvc','matchsvc','public')
          ORDER BY pg_total_relation_size(relid) DESC;"

# Ba bảng chỉ tăng
$PSQL -c "SELECT count(*) FILTER (WHERE published_at IS NULL)  AS outbox_cho,
                 count(*) FILTER (WHERE published_at IS NOT NULL) AS outbox_da_gui
          FROM ordersvc.outbox_event;"
$PSQL -c "SELECT count(*), min(processed_at), max(processed_at) FROM matchsvc.processed_event;"
$PSQL -c "SELECT count(*), min(snap_time), max(snap_time) FROM market_snapshot;"

# Ops #3 — đếm row trùng lặp trong market_snapshot
$PSQL -c "SELECT symbol, last_price, ema_price, count(*) AS so_ban_sao
          FROM market_snapshot GROUP BY symbol, last_price, ema_price
          HAVING count(*) > 1 ORDER BY so_ban_sao DESC LIMIT 5;"

# === Index có được dùng không ===
$PSQL -c "EXPLAIN ANALYZE SELECT * FROM ordersvc.outbox_event
          WHERE published_at IS NULL ORDER BY id LIMIT 100 FOR UPDATE SKIP LOCKED;"
#  -> Index Scan using idx_outbox_unpublished

$PSQL -c "EXPLAIN ANALYZE SELECT * FROM ordersvc.orders
          WHERE status = 'ACCEPTED' ORDER BY created_at LIMIT 20;"
#  -> Index Scan using idx_orders_pending

$PSQL -c "EXPLAIN ANALYZE SELECT * FROM market_snapshot
          WHERE symbol = 'HPG' ORDER BY snap_time DESC LIMIT 50;"
#  -> Index Scan using idx_symbol_time

# ⚠️ Thiếu index cho endpoint liệt kê
$PSQL -c "EXPLAIN ANALYZE SELECT * FROM ordersvc.orders
          WHERE account_id = 'ACC001' ORDER BY created_at DESC LIMIT 20;"
#  -> Seq Scan + Sort (!!)

# Index không được dùng bao giờ
$PSQL -c "SELECT schemaname||'.'||relname AS bang, indexrelname, idx_scan
          FROM pg_stat_user_indexes
          WHERE schemaname IN ('ordersvc','matchsvc','public')
          ORDER BY idx_scan;"

# === Connection ===
$PSQL -c "SELECT count(*) AS dang_dung, (SELECT setting::int FROM pg_settings
          WHERE name='max_connections') AS tran FROM pg_stat_activity;"
$PSQL -c "SELECT application_name, state, count(*) FROM pg_stat_activity
          GROUP BY 1,2 ORDER BY 3 DESC;"
curl -s http://localhost:8083/actuator/metrics/hikaricp.connections.active | jq
curl -s http://localhost:8083/actuator/metrics/hikaricp.connections.pending | jq

# === Optimistic lock ===
$PSQL -c "SELECT client_order_id, status, version, created_at, updated_at
          FROM ordersvc.orders WHERE version > 0 ORDER BY version DESC LIMIT 5;"

# === Thời gian hoàn thành saga (dùng TIMESTAMPTZ) ===
$PSQL -c "SELECT
            percentile_cont(0.50) WITHIN GROUP (ORDER BY updated_at - created_at) AS p50,
            percentile_cont(0.95) WITHIN GROUP (ORDER BY updated_at - created_at) AS p95,
            percentile_cont(0.99) WITHIN GROUP (ORDER BY updated_at - created_at) AS p99
          FROM ordersvc.orders WHERE updated_at IS NOT NULL;"

# === Chuẩn bị cho migration V3 (Gap #15) ===
$PSQL -c "SELECT client_order_id, count(*) FROM matchsvc.execution
          GROUP BY client_order_id HAVING count(*) > 1;"
#  -> rỗng thì ALTER TABLE ... ADD CONSTRAINT UNIQUE sẽ chạy được

# === VACUUM / bloat ===
$PSQL -c "SELECT schemaname||'.'||relname AS bang, last_autovacuum, last_autoanalyze,
                 n_dead_tup
          FROM pg_stat_user_tables
          WHERE schemaname IN ('ordersvc','matchsvc','public') ORDER BY n_dead_tup DESC;"
```

---

## 9. Hạn chế đã biết

| # | Loại | Vị trí | Vấn đề | Hướng sửa |
|:--:|---|---|---|---|
| 15 | **Gap** | `matching/entity/ExecutionEntity.java:24-25` | `client_order_id` có INDEX nhưng **không** có UNIQUE ⇒ `processed_event` là lớp chặn duy nhất | Migration `V3` thêm `UNIQUE`; xoá index cũ (thành thừa) |
| 12 | **Gap** | `matchsvc.processed_event` | Phình vô hạn; dọn sớm hơn retention topic ⇒ replay khớp lệnh hai lần | Job dọn giữ > `TOPIC_RETENTION_MS` |
| 3 | **Ops** | `public.market_snapshot` | ~864.000 row/ngày với 50 mã, gần như toàn bộ là bản sao | Flush theo diff + partition theo ngày |
| — | **Ops** | `ordersvc.outbox_event`, `matchsvc.outbox_event` | Không có job dọn row đã publish | `DELETE ... published_at < now() - 7d`, hoặc partition |
| — | Gap | `ordersvc.orders` | Thiếu index cho `account_id`/`status` + `ORDER BY created_at DESC` | `CREATE INDEX idx_orders_account_created ON orders (account_id, created_at DESC)` |
| — | Gap | `MarketDataController.java:44` | `limit` không chặn trần | `Math.min(limit, 500)` |
| — | Gap | `MarketDataController`, `ExecutionController` | Trả thẳng entity, không qua DTO | Thêm `SnapshotView`, `ExecutionView` |
| — | Ops | Toàn hệ | Tổng pool × số pod có thể vượt `max_connections` (100) | PgBouncer transaction pooling |
| — | Ops | `market_snapshot` | Không có retention/partition | `pg_partman` hoặc TimescaleDB |

---

## 10. Câu hỏi phỏng vấn

**Q: Vì sao `ddl-auto: validate` chứ không `update`?**

A: `update` là mặc định nguy hiểm nhất của Hibernate — nguy hiểm hơn cả `create-drop`, vì `create-drop` thì ai cũng biết là chỉ dùng cho test. `update` chỉ **thêm**, không bao giờ xoá hay sửa: đổi tên cột thì nó tạo cột mới và để cột cũ nằm đó với toàn bộ dữ liệu, và không ai biết cái nào là thật. Nó không có version nên không reproduce được schema, không có rollback, và tệ nhất là **kết quả phụ thuộc thứ tự deploy** — trong rolling update, pod cũ và pod mới cùng chạy, schema cuối cùng tuỳ thuộc pod nào khởi động sau. `validate` thì ngược lại: nó biến schema drift thành **lỗi khởi động** — ồn ào, ngay lập tức, không thể bỏ qua. Thêm field vào entity mà quên migration thì pod không khởi động được, và đó chính xác là điều ta muốn.

**Q: Ba service dùng chung một Postgres. Đó chẳng phải vi phạm "database per service" sao?**

A: Vi phạm *cô lập tài nguyên*, không vi phạm *cô lập logic*. Mỗi service có schema riêng, không đụng bảng của nhau, và có `flyway_schema_history` riêng nên migration hoàn toàn độc lập. Bằng chứng: có hai bảng cùng tên `outbox_event` ở hai schema với **lược đồ khác nhau** — `ordersvc` có `aggregate_type`, `matchsvc` không có — và chúng tiến hoá độc lập. Cái thiếu là cô lập tài nguyên: chung connection, chung disk, chung WAL, chung autovacuum, nên một service quét bảng lớn sẽ làm chậm service kia. Điểm quan trọng khiến điểm dừng này hợp lý: nâng lên database riêng **không cần đổi một dòng mã** — mỗi service đã đọc `DB_HOST` riêng từ biến môi trường. Và prod thì chuyển hẳn sang RDS.

**Q: Giải thích partial index và vì sao nó quan trọng ở đây.**

A: `CREATE INDEX ... WHERE published_at IS NULL` chỉ index các row thoả điều kiện. Ý nghĩa với bảng này: `outbox_event` chỉ tăng, sau một tháng có thể hàng triệu row, nhưng số row chưa publish tại mọi thời điểm chỉ vài chục — relay dọn mỗi 500ms. Index thường sẽ bao cả triệu row đã publish: tốn RAM trong shared_buffers, chậm khi insert, và vô dụng cho query của relay. Partial index luôn nhỏ bất kể bảng lớn tới đâu, và khi một row được mark published nó **tự động rời khỏi index**. Repo dùng kỹ thuật này ở hai chỗ nữa: `idx_orders_pending WHERE status = 'ACCEPTED'` làm query vận hành quan trọng nhất — "lệnh nào đang treo?" — chạy tức thì, và `idx_match_outbox_unpublished` tương tự cho matching-engine.

**Q: `open-in-view: false` giải quyết vấn đề gì?**

A: OSIV là tính năng Spring Boot mặc định **bật**, giữ EntityManager — và do đó giữ connection database — suốt cả request, kể cả lúc Jackson serialize JSON và lúc ghi response ra network. Hậu quả cụ thể với `maximum-pool-size: 20`: nếu mỗi request giữ connection 50ms thì throughput trần là 400 req/s, **bất kể** transaction thật chỉ mất 3ms. Phần lớn thời gian connection bị giữ là để chờ Jackson và chờ TCP. Tắt OSIV buộc phải viết đúng: nạp hết dữ liệu trong transaction, map sang DTO ngay. Nó cũng làm N+1 query **lộ ra** thành `LazyInitializationException` lúc dev thay vì âm thầm chạy ở tầng view. Đây chính là lý do `OrderView` tồn tại.

**Q: Vì sao dùng native query cho `lockUnpublishedBatch` thay vì JPA `@Lock`?**

A: Vì JPQL không có `SKIP LOCKED`. JPA có `@Lock(LockModeType.PESSIMISTIC_WRITE)` nhưng nó sinh `FOR UPDATE` thuần — và với 3 pod relay, đó là khác biệt giữa "song song thật" và "tuần tự hoá hoàn toàn": pod 2 và 3 sẽ nằm chờ pod 1 nhả khoá, throughput bằng đúng một pod. `SKIP LOCKED` làm pod 2 bỏ qua row bị khoá và lấy lô tiếp theo. Có một chi tiết đi kèm rất dễ sai: query này **phải** trả về managed entity, không phải DTO projection, vì `OutboxRelay` dựa vào dirty checking của Hibernate để ghi `published_at` — nó gọi `setPublishedAt()` mà không gọi `save()`. Đổi sang projection thì dòng đó không làm gì cả và relay gửi lại vô hạn, một bug im lặng rất khó tìm.

**Q: Bảng nào phình nhanh nhất và bạn xử lý thế nào?**

A: `market_snapshot`, và không phải vì lưu lượng mà vì một bug: `SnapshotStore.latest` không bao giờ bị xoá, nên mọi lần flush ghi lại **toàn bộ** symbol kể cả khi dữ liệu y hệt — 17.280 lần flush mỗi ngày × 50 mã = 864.000 row/ngày, gần như toàn bộ là bản sao chỉ khác `snap_time`. Sửa hai tầng: tầng ứng dụng flush theo diff (so sánh reference là đủ, vì `RiskCalculator` tạo object mới mỗi tick), tầng dữ liệu thì partition theo ngày để dọn bằng `DROP PARTITION` thay vì `DELETE` — `DELETE` hàng triệu row sinh dead tuple cần VACUUM và gây bloat. Hai bảng `outbox_event` cũng chỉ tăng và cũng chưa có job dọn, nhưng ở đó có một cạm bẫy: tuyệt đối không xoá row `published_at IS NULL`, đó là lệnh chưa ra Kafka. Và `processed_event` thì dọn là chuyện **tính đúng đắn** chứ không chỉ dung lượng — phải giữ lâu hơn retention của topic.

**Q: Thêm một cột vào `orders` giữa phiên giao dịch. Làm thế nào để không downtime?**

A: Theo nguyên tắc expand-contract, và `V2__order_state_machine.sql` của repo là ví dụ đúng. **Expand:** `ALTER TABLE orders ADD COLUMN x ... NULL` hoặc có `DEFAULT` — thao tác này trên Postgres 11+ là tức thì vì default được lưu ở metadata chứ không rewrite bảng. Mã cũ bỏ qua cột mới nên nó vẫn chạy bình thường với schema mới. **Migrate:** deploy mã mới đọc/ghi cột đó; backfill dữ liệu theo lô nhỏ nếu cần. **Contract:** xoá cột cũ ở một migration sau, khi không còn mã nào dùng. Những thao tác phải tránh trong rolling update: đổi tên cột, đổi kiểu, thêm `NOT NULL` không default, xoá cột, thêm constraint mà dữ liệu hiện có vi phạm. Và một lý do nữa khiến expand-contract không phải tuỳ chọn: `helm rollback` **không** undo Flyway migration, nên mã cũ sẽ chạy với schema mới — điều đó chỉ an toàn nếu mọi migration đều tương thích ngược.

---

## 11. Bảng tổng kết

| Khía cạnh | Lựa chọn | Lý do | Cái giá |
|---|---|---|---|
| Cô lập | Schema riêng, 1 instance | Cô lập logic đủ; nâng cấp không cần đổi mã | Không cô lập tài nguyên |
| Schema | **Flyway**, version-control | Reproduce được, review được, có lịch sử | Migration đã merge không sửa được |
| `ddl-auto` | **`validate`** | 🔑 Drift thành lỗi khởi động ồn ào | Phải viết migration tay |
| Thứ tự khởi động | Flyway → Hibernate validate | Bảng phải tồn tại trước khi validate | startupProbe phải rộng tay |
| Migration | Expand-contract | Rolling update không downtime; rollback an toàn | Ba bước thay vì một |
| Dedupe lệnh | `UNIQUE(client_order_id)` | Chốt chặn tuyệt đối | ~3ms |
| Dedupe message | `PRIMARY KEY` + `ON CONFLICT DO NOTHING` | Atomic test-and-set | ⚠️ Thiếu lưới an toàn trên `execution` |
| Index outbox | Partial `WHERE published_at IS NULL` | Luôn nhỏ bất kể bảng lớn | Chi phí cập nhật khi mark |
| Index lệnh treo | Partial `WHERE status='ACCEPTED'` | Query vận hành quan trọng nhất chạy tức thì | — |
| Index lịch sử | `(symbol, snap_time)` | Phục vụ cả `WHERE` lẫn `ORDER BY` | — |
| Index liệt kê | **Chưa có** | — | ⚠️ Seq scan + sort |
| Kiểu tiền | `NUMERIC(18,4)` / `BigDecimal` | Không sai số floating-point | Chậm hơn `double` |
| Kiểu thời gian | `TIMESTAMPTZ` / `Instant` | Thời điểm tuyệt đối, so sánh đúng | — |
| Payload outbox | `TEXT` | Không truy vấn bên trong; không khoá vào JSON | Khó tra cứu nội dung |
| OSIV | **Tắt** | Connection không bị giữ suốt request | Phải map DTO trong transaction |
| API response | DTO (`OrderView`) | Không lộ cột nội bộ; compiler bắt drift | ⚠️ 2 controller chưa làm |
| Liệt kê | Phân trang bắt buộc, trần 200 | OOM chỉ xảy ra khi có dữ liệu thật | ⚠️ pricing không có trần |
| Query đặc thù | Native SQL | JPQL không có `SKIP LOCKED`/`ON CONFLICT` | Khoá vào PostgreSQL |
| Pool | 10 / 20 / mặc định theo bản chất tải | Khớp số thread thật sự cần DB | ⚠️ Tổng × pod ≤ `max_connections` |
| Batch insert | `batch_size: 100` ở pricing | 1 round-trip thay vì 100 | Chỉ hiệu quả với `saveAll` |
| Sinh ID | `IDENTITY` | Đơn giản | `SEQUENCE` tốt hơn cho batch lớn |
| Dọn dẹp | **Chưa có** | — | ⚠️ 4 bảng chỉ tăng |

> **Đọc tiếp:** [Chương 14 — Hạn chế đã biết & Lộ trình](./14-han-che-da-biet-va-lo-trinh.md).
