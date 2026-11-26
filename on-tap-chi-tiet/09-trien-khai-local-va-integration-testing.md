# Chương 09 — Triển Khai Local & Kiểm Thử Tích Hợp

> **Câu hỏi trung tâm:** làm sao chạy được 11 container trên một máy cá nhân, debug được một service trong IDE trong khi 10 cái kia vẫn chạy, và kiểm chứng tính đúng đắn phân tán bằng test tự động?
>
> **Tài liệu đối chiếu:** `DEPLOY-LOCAL.md`, `GUIDE.md` mục 2 & 6-8, `.github/workflows/ci.yml`.

---

## 1. Nó là gì?

### 1.1. Mười một container

```yaml
# docker-compose.yml
# Chạy local: docker compose up -d
# Kafka (KRaft - không cần Zookeeper), Postgres, Prometheus, Grafana, 3 services
```

| # | Container | Cổng host | Vai trò | Build từ source? |
|:--:|---|:--:|---|:--:|
| 1 | `kafka` | 9092 | Broker KRaft | Không |
| 2 | `postgres` | 5432 | 3 schema: `public`, `ordersvc`, `matchsvc` | Không |
| 3 | `schema-registry` | **8085**→8081 | Cưỡng chế tương thích Avro | Không |
| 4 | `redis` | 6379 | Cache idempotency | Không |
| 5 | `zipkin` | 9411 | Tracing | Không |
| 6 | `prometheus` | 9090 | Metrics | Không |
| 7 | `grafana` | 3000 | Dashboard (admin/admin) | Không |
| 8 | `market-gateway` | 8081 | | ✅ |
| 9 | `pricing-risk-service` | 8082 | | ✅ |
| 10 | `order-service` | 8083 | | ✅ |
| 11 | `matching-engine` | 8084 | | ✅ |

💡 **Schema Registry ánh xạ `8085:8081`** — vì `8081` trên host đã dành cho `market-gateway`. Đây là lý do `SCHEMA_REGISTRY_URL` có hai giá trị tuỳ ngữ cảnh: `http://schema-registry:8081` (trong mạng Docker) và `http://localhost:8085` (từ host/IDE).

### 1.2. Multi-stage Dockerfile

```dockerfile
# Multi-stage build: image cuối chỉ ~200MB (JRE + jar), không chứa Maven/source
FROM maven:3.9-eclipse-temurin-17 AS build
ARG MODULE
WORKDIR /build
COPY pom.xml .
COPY common-contracts/pom.xml common-contracts/
COPY outbox-support/pom.xml outbox-support/
COPY market-gateway/pom.xml market-gateway/
COPY pricing-risk-service/pom.xml pricing-risk-service/
COPY order-service/pom.xml order-service/
COPY matching-engine/pom.xml matching-engine/
RUN mvn -q dependency:go-offline -pl ${MODULE} -am || true
COPY . .
RUN mvn -q clean package -pl ${MODULE} -am -DskipTests

FROM eclipse-temurin:17-jre
ARG MODULE
COPY --from=build /build/${MODULE}/target/*.jar /app/app.jar
# Container-aware JVM: giới hạn heap theo cgroup limit của K8s
ENV JAVA_OPTS="-XX:MaxRAMPercentage=75 -XX:+UseG1GC -XX:MaxGCPauseMillis=50"
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar /app/app.jar"]
```

**Bốn kỹ thuật đáng học:**

🔑 **(a) Copy `pom.xml` trước, source sau.** Docker cache theo layer; một layer được tái sử dụng nếu mọi thứ phía trên nó không đổi. Vì `pom.xml` ít khi đổi còn source đổi liên tục, thứ tự này làm cho `dependency:go-offline` (bước chậm nhất, tải hàng trăm MB) được cache qua mọi lần build. Đảo thứ tự lại ⇒ mỗi lần sửa một dòng Java là tải lại toàn bộ dependency.

💡 **(b) `|| true` sau `go-offline`.** Lệnh này có thể fail vì nhiều lý do vô hại (plugin chưa resolve được khi chưa có source). `|| true` cho phép build tiếp — dependency còn thiếu sẽ được tải ở bước `package`. Đây là tối ưu best-effort: thành công thì nhanh, thất bại thì vẫn chạy.

🔑 **(c) `-pl ${MODULE} -am`** — `--projects` + `--also-make`: chỉ build module đích **và các module nó phụ thuộc**. Build `order-service` sẽ build `common-contracts` + `outbox-support` + `order-service`, **không** build `pricing-risk-service` hay `matching-engine`. Tiết kiệm đáng kể trong CI matrix.

🔑 **(d) `-XX:MaxRAMPercentage=75`** thay vì `-Xmx512m`. JVM từ Java 10+ **đọc được cgroup limit** của container. Với `-Xmx` cố định, hạ `limits.memory` trong K8s mà quên sửa Dockerfile ⇒ JVM cố cấp phát quá giới hạn ⇒ **OOMKilled**. Với `MaxRAMPercentage`, heap tự động co giãn theo limit.

Vì sao 75% chứ không 100%: JVM còn cần bộ nhớ ngoài heap — metaspace, thread stack, code cache, direct buffer của Netty/Kafka client, GC structures. 25% còn lại là cho những thứ đó.

💡 `-XX:+UseG1GC -XX:MaxGCPauseMillis=50` — G1 là GC mặc định từ Java 9, nhưng khai tường minh cùng mục tiêu pause 50ms là một tuyên bố: **hệ giao dịch quan tâm tới p99, và một GC pause 500ms sẽ xuất hiện trong p99 đó.**

---

## 2. Quy trình khởi động và thứ tự phụ thuộc

### 2.1. Hai bước, không phải một

```bash
# Bước 1: bật hạ tầng trước — không cần build, lên trong ~20 giây
docker compose up -d kafka postgres schema-registry redis zipkin prometheus grafana

# Bước 2: build và bật 4 service Java (lần đầu ~3-5 phút)
docker compose up -d --build market-gateway pricing-risk-service order-service matching-engine
```

**Vì sao tách hai bước** dù `depends_on` đã khai báo:

```yaml
order-service:
  depends_on: [kafka, postgres, redis, zipkin]
```

⚠️ **`depends_on` chỉ đợi container *khởi động*, không đợi dịch vụ *sẵn sàng*.** Postgres container "started" ở giây thứ 1 nhưng chỉ nhận kết nối ở giây thứ 8. `order-service` khởi động ở giây thứ 2, Flyway **fail cứng** vì không kết nối được DB, và pod vào `CrashLoopBackOff`.

Ba cách xử lý:

| Cách | Cài đặt | Nhận xét |
|---|---|---|
| Tách hai bước thủ công | `DEPLOY-LOCAL.md` | Đơn giản nhất cho local |
| `depends_on: condition: service_healthy` | Thêm `healthcheck` cho từng dịch vụ hạ tầng | Sạch hơn, nhưng làm `docker-compose.yml` dài gấp đôi |
| **initContainer `wait-for-db`** | 🔑 Có trong K8s manifest | Cách đúng cho cluster |

```yaml
# k8s/base/apps/order-service.yaml
initContainers:
  # Flyway fail cứng khi DB chưa sẵn sàng -> chặn ở đây thay vì đốt
  # vòng CrashLoopBackOff (mỗi vòng lại chờ lâu hơn, rollout treo).
  - name: wait-for-db
    image: postgres:16.4-alpine
    command:
      - sh
      - -c
      - |
        until pg_isready -h "$DB_HOST" -p "$DB_PORT" -U "$PGUSER" -t 3; do
          echo "cho postgres tai $DB_HOST:$DB_PORT"; sleep 2
        done
        echo "postgres da nhan ket noi"
```

💡 Lý do nêu trong comment rất đáng chú ý: không chỉ là "chờ cho đẹp", mà là tránh `CrashLoopBackOff` — Kubernetes tăng dần thời gian chờ giữa các lần restart (10s, 20s, 40s, 80s, …, tối đa 5 phút). Sau vài vòng, pod nằm chờ 5 phút dù DB đã sẵn sàng từ lâu, và `progressDeadlineSeconds` hết hạn làm rollout thất bại.

### 2.2. Kiểm tra sức khoẻ

```bash
# Cả 4 phải UP
for p in 8081 8082 8083 8084; do
  echo -n "$p: "; curl -s http://localhost:$p/actuator/health | jq -r .status
done
```

### 2.3. Thứ tự bắt buộc khi kiểm chứng

⚠️ **Dữ liệu giá phải có trước khi đặt lệnh.** Lệnh đầu tiên sau khi khởi động thường nhận `422 NO_FRESH_MARKET_DATA` vì cache của `order-service` còn rỗng.

```bash
# 1. Chờ feed bơm tick và pipeline chạy (~5 giây)
sleep 5
curl -s http://localhost:8082/api/v1/marketdata/HPG | jq
#    -> phải có lastPrice, emaPrice, volatility, var95, riskLevel, calcTime mới

# 2. Nếu riskLevel = HIGH thì lệnh sẽ bị từ chối. Mẹo của DEPLOY-LOCAL:
#    bơm 6 tick GIÁ Y HỆT NHAU cho một mã mới -> volatility = 0 -> riskLevel = LOW
for i in 1 2 3 4 5 6; do
  curl -s -X POST http://localhost:8081/api/v1/ticks \
    -H 'Content-Type: application/json' \
    -d "{\"symbol\":\"LOCAL\",\"price\":28500,\"volume\":100,\"eventTime\":$(date +%s%3N)}"
done
sleep 2
curl -s http://localhost:8082/api/v1/marketdata/LOCAL | jq '{volatility, riskLevel}'
#    -> volatility ~0, riskLevel LOW

# 3. Giờ đặt lệnh mới được chấp nhận
curl -s -X POST http://localhost:8083/api/v1/orders \
  -H 'Content-Type: application/json' -H 'X-API-KEY: demo-key' \
  -d '{"clientOrderId":"local-1","symbol":"LOCAL","side":"BUY","price":28500,"quantity":100,"accountId":"ACC001"}'
```

💡 **Mẹo "6 tick giá y hệt nhau"** là một hiểu biết sâu về thuật toán: `RiskCalculator` cần `n >= 2` log-return để tính variance (`:67`), và nếu mọi giá bằng nhau thì mọi `ln(px/lastPrice) = 0` ⇒ variance = 0 ⇒ volatility = 0 ⇒ `riskLevel = LOW` chắc chắn. Mã mới (`LOCAL`) không bị ảnh hưởng bởi random walk của `SimulatedFeed`.

⚠️ Và nhớ: dữ liệu chỉ "tươi" trong **10 giây** (`order.max-data-age-ms`). Nếu dừng lại suy nghĩ quá lâu giữa bước 2 và 3, phải bơm lại tick.

---

## 3. Chế độ Hybrid — debug trong IDE

🔑 Đây là kỹ thuật thực dụng nhất của chương: **chạy hạ tầng + 3 service trong Docker, service thứ 4 trong IDE với debugger.**

```bash
# 1. Hạ tầng + 3 service, KHÔNG có service cần debug
docker compose up -d kafka postgres schema-registry redis zipkin prometheus grafana
docker compose up -d --build market-gateway pricing-risk-service matching-engine

# 2. Chạy order-service từ IDE (IntelliJ: Run/Debug Configuration)
#    Toàn bộ mặc định trong application.yml đã trỏ về localhost — không cần sửa gì:
#      KAFKA_BOOTSTRAP      -> localhost:9092   (listener EXTERNAL)
#      DB_HOST              -> localhost:5432
#      REDIS_HOST           -> localhost:6379
#      ZIPKIN_ENDPOINT      -> http://localhost:9411/api/v2/spans
#      SCHEMA_REGISTRY_URL  -> http://localhost:8085
mvn -pl order-service spring-boot:run
```

**Vì sao chạy được không cần sửa cấu hình:**

```yaml
# order-service/src/main/resources/application.yml
spring:
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP:localhost:9092}
  datasource:
    url: jdbc:postgresql://${DB_HOST:localhost}:${DB_PORT:5432}/${DB_NAME:trading}
  data:
    redis:
      host: ${REDIS_HOST:localhost}
```

🔑 **Mọi biến môi trường đều có giá trị mặc định là `localhost`.** Docker Compose ghi đè bằng tên service (`kafka:19092`, `postgres`, `redis`); chạy từ IDE thì mặc định có tác dụng. Đây là một quyết định thiết kế nhỏ nhưng làm cho chế độ hybrid trở nên miễn phí.

Và nó hoạt động được nhờ hai listener của Kafka:

```yaml
KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:19092,CONTROLLER://0.0.0.0:9093,EXTERNAL://0.0.0.0:9092
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:19092,EXTERNAL://localhost:9092
```

| Ai kết nối | Dùng listener | Advertised address |
|---|---|---|
| Container khác trong mạng Docker | `PLAINTEXT` | `kafka:19092` |
| Process trên host (IDE) | `EXTERNAL` | `localhost:9092` |

⚠️ Nếu chỉ khai một listener, client sẽ nhận `advertised.listeners` sai cho ngữ cảnh của nó và lỗi `Connection refused` — một trong những lỗi Kafka khó hiểu nhất với người mới.

💡 **Lưu ý khi debug:** đặt breakpoint trong `@KafkaListener` sẽ giữ consumer quá `max.poll.interval.ms` (mặc định 5 phút) ⇒ broker coi consumer đã chết ⇒ rebalance. Với `order-service` thì không nghiêm trọng; với `pricing-risk-service` thì hãy tăng `max.poll.interval.ms` khi debug, hoặc đặt breakpoint ở lớp service bên dưới thay vì ở listener.

---

## 4. Giao diện UI local và bài học CORS

```javascript
// ui/serve.js
// Static server cho ui/ + proxy same-origin toi backend (tranh CORS).
// Chay: node ui/serve.js  -> http://localhost:8080
const PORT = 8080;
const PORTS = ['8081', '8082', '8083', '8084'];

const server = http.createServer((req, res) => {
  const m = req.url.match(/^\/p\/(\d{4})(\/.*)$/);
  if (m && PORTS.includes(m[1])) {
    const pr = http.request(
      { host: '127.0.0.1', port: +m[1], path: m[2], method: req.method,
        headers: { ...req.headers, host: `127.0.0.1:${m[1]}` } },
      (pres) => { res.writeHead(pres.statusCode, pres.headers); pres.pipe(res); }
    );
    pr.on('error', (e) => { res.writeHead(502); res.end('proxy error: ' + e.message); });
    req.pipe(pr);
    return;
  }

  let file = req.url.split('?')[0];
  if (file === '/' || file === '') file = '/index.html';
  const full = path.join(UI_DIR, path.normalize(file).replace(/^[\/]+/, ''));
  fs.readFile(full, (err, buf) => {
    if (err) { res.writeHead(404); res.end('Not found'); return; }
    if (full.endsWith('.html')) {
      let html = buf.toString('utf8');
      for (const p of PORTS) html = html.split(`http://localhost:${p}/api/v1`).join(`/p/${p}/api/v1`);
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end(html);
    } else { res.writeHead(200); res.end(buf); }
  });
});
```

**Vấn đề nó giải quyết:** mở `ui/index.html` bằng `file://` thì origin là `null`, và mọi `fetch('http://localhost:8082/...')` bị trình duyệt chặn vì thiếu header `Access-Control-Allow-Origin`. Request **vẫn tới server** và server **vẫn trả dữ liệu** — nhưng trình duyệt không cho JavaScript đọc. Đây là nguồn nhầm lẫn kinh điển: `curl` chạy tốt, trình duyệt báo lỗi.

**Cách giải:** phục vụ UI và proxy API **cùng một origin** (`localhost:8080`). Script tự động viết lại mọi URL `http://localhost:808x/api/v1` trong HTML thành đường dẫn tương đối `/p/808x/api/v1` ⇒ trình duyệt thấy mọi request là same-origin ⇒ không có CORS.

💡 Hai chi tiết an toàn trong 40 dòng này:
- **`PORTS.includes(m[1])`** — allowlist cổng. Không có nó, `/p/5432/...` sẽ biến server thành một SSRF proxy tới bất kỳ cổng nào trên máy.
- **`path.normalize(file).replace(/^[\/]+/, '')`** — chặn path traversal (`../../etc/passwd`). Tuy nhiên `normalize` trên Windows không chặn được mọi biến thể; với một công cụ dev local thì chấp nhận được, nhưng đừng bao giờ dùng mẫu này cho server công khai.

Phương án thay thế (sạch hơn về lâu dài) là bật CORS ở backend — và repo **đã** có `@CrossOrigin("*")` trên các controller:

```java
// market-gateway/controller/TickController.java:16
// pricing-risk-service/controller/MarketDataController.java:19
// order-service/controller/OrderController.java:22
// matching-engine/controller/ExecutionController.java:11
@CrossOrigin("*")
```

⚠️ `@CrossOrigin("*")` **không phù hợp production** — nó cho phép bất kỳ website nào gọi API bằng trình duyệt của người dùng. Với `order-service` có API key trong header, rủi ro thấp hơn (`*` không cho phép gửi credential), nhưng vẫn nên giới hạn origin cụ thể. Và lưu ý `@CrossOrigin("*")` **không** giải quyết được trường hợp `file://` (origin `null`), nên proxy vẫn cần cho UI local.

---

## 5. Integration Testing với Testcontainers

### 5.1. Triết lý: dùng hàng thật

```java
// pricing-risk-service/src/test/java/com/trading/pricing/TickToMarketDataIT.java:30-39
/**
 * INTEGRATION TEST với Testcontainers: Kafka THẬT + Postgres THẬT trong Docker,
 * không mock. Verify luồng: produce tick Avro -> consumer tính risk -> REST trả enriched.
 *
 * Schema Registry: dùng MockSchemaRegistry (url "mock://scope") - producer test
 * và consumer app cùng JVM chia sẻ cùng registry in-memory, không cần container SR.
 *
 * Yêu cầu: máy chạy test phải có Docker (local dev, CI runner đều có).
 */
```

🔑 **Vì sao không mock.** Những tính chất đang được kiểm chứng là tính chất **của hạ tầng**, không phải của code:

| Tính chất | Thuộc về | Mock được không |
|---|---|---|
| `INSERT ... ON CONFLICT DO NOTHING` nguyên tử | PostgreSQL | ❌ Mock chỉ chứng minh "ta gọi đúng method" |
| `FOR UPDATE SKIP LOCKED` không chặn nhau | PostgreSQL | ❌ |
| Kafka giữ thứ tự theo key | Kafka | ❌ |
| Consumer group rebalance | Kafka | ❌ |
| Flyway migration chạy đúng thứ tự | PostgreSQL | ❌ |
| Avro decimal scale đúng | Avro + Registry | ❌ |

Mock `ProcessedEventRepository` sẽ cho một test **pass trong khi hệ thống sai**.

### 5.2. Cấu hình container động

```java
// TickToMarketDataIT.java:41-66
@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class TickToMarketDataIT {

    @Container
    static KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("trading").withUsername("trading").withPassword("trading123");

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("schema.registry.url", () -> "mock://pricing-it");
    }

    @LocalServerPort
    int port;
```

**Ba kỹ thuật:**

🔑 **(a) `@DynamicPropertySource` với method reference (`kafka::getBootstrapServers`)** — Testcontainers map cổng container sang **một cổng ngẫu nhiên trên host** để nhiều test chạy song song không xung đột. Cổng đó chỉ biết được *sau khi* container khởi động. `@DynamicPropertySource` nhận `Supplier`, được gọi *lazy* khi Spring context cần giá trị — tức là sau khi container đã sẵn sàng. Dùng `kafka.getBootstrapServers()` (gọi ngay) thay vì method reference sẽ ném exception vì container chưa chạy.

🔑 **(b) `static` container** — JUnit 5 với `@Container` `static` khởi động container **một lần cho cả class**, không phải mỗi `@Test`. Khởi động Kafka mất ~10 giây; với 5 test method thì đó là 50 giây tiết kiệm được.

🔑 **(c) `mock://pricing-it`** — scheme `mock://` kích hoạt `MockSchemaRegistryClient`, một registry in-memory theo **scope** (phần sau `mock://`). Producer của test và consumer của ứng dụng chạy cùng JVM nên chia sẻ cùng instance. Tiết kiệm ~10 giây khởi động container Schema Registry.

⚠️ Đánh đổi: test không kiểm chứng hành vi **thật** của Confluent Schema Registry — kiểm tra tương thích BACKWARD, HTTP retry, cache eviction. Nếu schema evolution là rủi ro chính của dự án, cần ít nhất một test dùng container thật.

### 5.3. Ba test và điều mỗi test chứng minh

#### Test 1 — `TickToMarketDataIT`: pipeline tính toán hoạt động

```java
// TickToMarketDataIT.java:71-99
@Test
void tickStreamProducesEnrichedMarketData() {
    // Bơm 50 tick random-walk cho mã TEST qua Kafka thật
    try (KafkaProducer<String, TickEventAvro> producer = buildAvroProducer()) {
        BigDecimal price = new BigDecimal("100.0000");
        for (int i = 0; i < 50; i++) {
            double drift = 1 + (ThreadLocalRandom.current().nextDouble() - 0.5) * 0.01;
            price = price.multiply(BigDecimal.valueOf(drift)).setScale(4, RoundingMode.HALF_UP);
            TickEventAvro tick = TickEventAvro.newBuilder()
                    .setSymbol("TEST").setPrice(price)
                    .setVolume(10).setEventTime(System.currentTimeMillis()).build();
            producer.send(new ProducerRecord<>("stock.ticks", "TEST", tick));
        }
        producer.flush();
    }

    // Chờ pipeline xử lý xong rồi assert qua REST API thật
    RestTemplate rest = new RestTemplate();
    await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(500))
            .untilAsserted(() -> {
                ResponseEntity<Map> resp = rest.getForEntity(
                        "http://localhost:" + port + "/api/v1/marketdata/TEST", Map.class);
                assertThat(resp.getStatusCode().is2xxSuccessful()).isTrue();
                Map<?, ?> body = resp.getBody();
                assertThat(body).isNotNull();
                assertThat(body.get("symbol")).isEqualTo("TEST");
                assertThat(body.get("riskLevel")).isIn("LOW", "MEDIUM", "HIGH");
                // volatility phải được tính ra (>= 0) sau 50 tick
                assertThat(new BigDecimal(body.get("volatility").toString()))
                        .isGreaterThanOrEqualTo(BigDecimal.ZERO);
            });
}
```

💡 Test này assert qua **REST API thật** (`RestTemplate` tới `localhost:{randomPort}`), không gọi thẳng `SnapshotStore`. Nhờ vậy nó kiểm chứng cả chuỗi: Kafka → deserialize Avro → `RiskCalculator` → `SnapshotStore` → controller → Jackson serialize. Một lỗi ở bất kỳ mắt xích nào đều bị bắt.

💡 Assertion `isIn("LOW","MEDIUM","HIGH")` thay vì so chính xác — vì random walk nên volatility không tiền đoán được. Test khẳng định *tính chất* (riskLevel là một trong ba giá trị hợp lệ, volatility ≥ 0) chứ không khẳng định *giá trị*. Đây là cách đúng để test code có yếu tố ngẫu nhiên.

#### Test 2 — `IdempotentConsumerIT`: effectively-once

```java
// matching-engine/src/test/java/com/trading/matching/IdempotentConsumerIT.java:27-31
/**
 * TEST QUAN TRỌNG NHẤT của hệ thống: chứng minh EFFECTIVELY-ONCE.
 * Gửi cùng 1 order 3 lần (giả lập outbox relay re-send / Kafka redelivery)
 * -> bảng execution phải có ĐÚNG 1 dòng.
 */
```

```java
// IdempotentConsumerIT.java:73-83
try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
    for (int i = 0; i < 3; i++) {   // GỬI TRÙNG 3 LẦN
        producer.send(new ProducerRecord<>("stock.orders", "HPG", orderJson));
    }
    producer.flush();
}

// Chờ consumer xử lý cả 3, rồi khẳng định chỉ có 1 execution
await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(500))
        .untilAsserted(() ->
                assertThat(executionRepository.findByClientOrderId("dup-test-1")).hasSize(1));

// Giữ nguyên khẳng định thêm 3s nữa (đề phòng message trễ tạo thêm dòng)
await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(10))
        .untilAsserted(() ->
                assertThat(executionRepository.findByClientOrderId("dup-test-1")).hasSize(1));
```

🔑 **Hai lần `await()` — kỹ thuật quan trọng nhất của test bất đồng bộ.**

| | `atMost(30s).untilAsserted(…)` | `during(3s).atMost(10s).untilAsserted(…)` |
|---|---|---|
| Ngữ nghĩa | Chờ tới khi điều kiện **đúng lần đầu** | Khẳng định điều kiện **đúng liên tục suốt 3 giây** |
| Chứng minh | "Đã xử lý xong" | **"Không có execution thứ 2 xuất hiện muộn"** |

Thiếu lần hai thì test **pass sai**: nó chỉ nói rằng tại một thời điểm nào đó có 1 row, không nói rằng message thứ 2 và 3 không tạo thêm row *sau đó*. Đây là mẫu chung cho mọi assertion về **sự vắng mặt** trong hệ bất đồng bộ — không thể chứng minh một thứ không xảy ra bằng cách kiểm tra một lần.

#### Test 3 — `SagaRejectionIT`: saga đóng được ở nhánh thất bại

```java
// matching-engine/src/test/java/com/trading/matching/SagaRejectionIT.java:76-85
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
```

💡 Hai khẳng định trong cùng một `untilAsserted`: report REJECTED **có** trong outbox, **và** không có execution nào. Điều thứ hai quan trọng không kém — nó chứng minh nhánh từ chối thực sự `return` *trước khi* ghi execution.

💡 Test assert trên **bảng outbox**, không phải trên Kafka. Đó là ranh giới đúng: `MatchingService` chịu trách nhiệm ghi outbox; việc nó lên Kafka là trách nhiệm của `OutboxRelay` và được kiểm riêng.

### 5.4. Vùng phủ hiện tại và lỗ hổng

| Đã test | Chưa test |
|---|---|
| ✅ Pipeline tick → enriched → REST | ❌ Pre-trade check ở `order-service` (3 luật) |
| ✅ Idempotency ở matching-engine (effectively-once) | ❌ Idempotency ở order-service (Redis + UNIQUE) |
| ✅ Saga chiều về nhánh REJECTED | ❌ Saga đầy đủ **xuyên hai service** |
| | ❌ `OutboxRelay` publish + mark + rollback batch |
| | ❌ `FOR UPDATE SKIP LOCKED` với nhiều pod |
| | ❌ Optimistic lock khi hai report song song |
| | ❌ Circuit breaker mở/đóng |
| | ❌ DLT nhận được message khi tick hỏng |
| | ❌ Avro schema evolution (Registry thật) |

⚠️ **Gap #22 — vùng phủ của môi trường dev.** Ngoài lỗ hổng test, còn một lỗ hổng *môi trường*: dev có 3 partition / 1 pod / `concurrency = 3` ⇒ mỗi consumer thread ôm **đúng 1 partition**. Mọi bug phụ thuộc việc một thread xử lý **xen kẽ hai partition** — ví dụ batch 1000 bản ghi của `TickConsumer` trộn hai nhóm symbol — **không bao giờ lộ ra ở dev**.

Cách sửa **không** phải tăng partition dev (sẽ thừa partition không consumer). Hai hướng đúng: dựa vào staging lúc ở `minReplicaCount: 1` (6 partition / 3 = 2 partition mỗi consumer), và thêm một integration test Testcontainers tạo topic với 2 partition trên 1 consumer.

---

## 6. Load Testing với k6

```javascript
// loadtest/order-load.js
// k6 load test: kiểm chứng con số latency thay vì tin lời hứa trong tài liệu.
//   k6 run loadtest/order-load.js
export const options = {
  stages: [
    { duration: '30s', target: 50 },   // tăng dần
    { duration: '1m',  target: 200 },  // giữ tải - vượt RateLimiter 200/s để xem 429
    { duration: '30s', target: 0 },
  ],
  thresholds: {
    // Ngưỡng FAIL build nếu vi phạm - biến hiệu năng thành hợp đồng, không phải kỳ vọng
    'http_req_duration{expected_response:true}': ['p(95)<200', 'p(99)<500'],
    'http_req_failed': ['rate<0.05'],
  },
};

const SYMBOLS = ['VN30F2607', 'HPG', 'VCB', 'FPT'];

export default function () {
  const symbol = SYMBOLS[Math.floor(Math.random() * SYMBOLS.length)];
  // clientOrderId duy nhất mỗi request: đo đường đặt lệnh thật,
  // không phải đo tốc độ Redis trả cache.
  const clientOrderId = `k6-${__VU}-${__ITER}-${Date.now()}`;

  const res = http.post(
    'http://localhost:8083/api/v1/orders',
    JSON.stringify({ clientOrderId, symbol, side: Math.random() > 0.5 ? 'BUY' : 'SELL',
                     price: 28500, quantity: 100, accountId: `ACC${__VU}` }),
    { headers: { 'Content-Type': 'application/json', 'X-API-KEY': 'demo-key' } },
  );

  if (res.status === 429) rateLimited.add(1);
  if (res.status === 422) rejected.add(1);

  check(res, { 'khong loi 5xx': (r) => r.status < 500 });
}
```

**Bốn quyết định thiết kế trong 40 dòng:**

🔑 **(a) `thresholds` làm k6 trả exit code ≠ 0** khi vi phạm. Nghĩa là đặt vào CI pipeline thì **hiệu năng trở thành hợp đồng, không phải kỳ vọng**. "p95 < 200ms" trong tài liệu là một lời hứa; cùng câu đó trong `thresholds` là một ràng buộc chặn merge.

🔑 **(b) `clientOrderId` duy nhất mỗi request.** Nếu dùng id cố định, từ request thứ hai trở đi Redis sẽ hit và trả về ngay — ta sẽ đo **tốc độ của Redis**, không đo đường đặt lệnh thật. Đây là cạm bẫy phổ biến nhất khi load-test một API có idempotency.

🔑 **(c) `{expected_response:true}` trong threshold.** Chỉ tính latency của response thành công. Nếu không lọc, các response `429` (trả về tức thì, rất nhanh) sẽ **kéo p95 xuống** và che mất vấn đề hiệu năng thật.

🔑 **(d) `stages` vượt ngưỡng rate limiter có chủ đích.** Giai đoạn giữa đẩy lên 200 VU để **quan sát hành vi khi vượt giới hạn** — mục tiêu không phải "không có 429" mà là "429 được trả về đúng cách, nhanh, và không có 5xx". Đó là lý do `check` chỉ khẳng định `status < 500`.

💡 Counter tuỳ chỉnh `orders_rejected` và `orders_rate_limited` tách hai loại "thất bại" rất khác nhau: `422` là nghiệp vụ từ chối (bình thường khi dữ liệu stale), `429` là bảo vệ hệ thống (bình thường khi vượt tải).

```bash
k6 run loadtest/order-load.js

# Kết quả mong đợi:
#   http_req_duration{expected_response:true}..: p(95)=... p(99)=...
#   ✓ http_req_duration{expected_response:true}
#   ✓ http_req_failed
#   ✓ khong loi 5xx
```

---

## 7. CI Pipeline

```yaml
# .github/workflows/ci.yml
# CI/CD: build + integration test (Testcontainers chạy được vì GitHub runner có Docker)
# + build Docker image cho 4 service khi merge vào main.
name: CI

on:
  push:
    branches: [main]
  pull_request:

jobs:
  build-and-test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-java@v4
        with:
          distribution: temurin
          java-version: '17'
          cache: maven
      # verify = compile + unit test + INTEGRATION TEST (Testcontainers spin Kafka/Postgres thật)
      - name: Build & Test
        run: mvn -B clean verify

  docker-images:
    needs: build-and-test
    if: github.ref == 'refs/heads/main'
    runs-on: ubuntu-latest
    strategy:
      matrix:
        module: [market-gateway, pricing-risk-service, order-service, matching-engine]
    steps:
      - uses: actions/checkout@v4
      - name: Build image
        run: docker build --build-arg MODULE=${{ matrix.module }} -t ghcr.io/${{ github.repository }}/${{ matrix.module }}:${{ github.sha }} .
```

💡 **`mvn verify` chứ không `mvn test`.** Trong vòng đời Maven, `test` chỉ chạy unit test (`*Test.java`); `verify` chạy thêm `failsafe:integration-test` cho `*IT.java`. Vì toàn bộ integration test ở đây đặt tên `*IT`, dùng `mvn test` sẽ **bỏ qua hoàn toàn** chúng — và CI sẽ xanh trong khi không kiểm chứng gì về hệ phân tán.

💡 **GitHub Actions runner có Docker sẵn**, nên Testcontainers chạy được không cần cấu hình gì thêm. Đây là lý do Testcontainers trở nên thực tế so với 5 năm trước.

💡 **`cache: maven`** — cache `~/.m2` giữa các lần chạy, tiết kiệm vài phút tải dependency.

💡 **Matrix build 4 image song song** — mỗi image chỉ build module của nó nhờ `-pl ${MODULE} -am`.

⚠️ **Push lên registry bị comment:**

```yaml
# Push lên registry: bỏ comment và cấu hình quyền packages:write
# - name: Login GHCR
#   run: echo "${{ secrets.GITHUB_TOKEN }}" | docker login ghcr.io -u ${{ github.actor }} --password-stdin
# - name: Push
#   run: docker push ghcr.io/${{ github.repository }}/${{ matrix.module }}:${{ github.sha }}
```

Nghĩa là pipeline hiện **build image rồi vứt đi** — không có artifact nào để deploy. Và quan trọng hơn: overlay `prod` yêu cầu ghim image theo **digest** (`k8s-validate.yml` chặn merge nếu dùng tag), mà digest chỉ có sau khi push. Vậy nên chuỗi CI→CD hiện chưa khép kín.

Pipeline thứ hai (`k8s-validate.yml`) được phân tích ở [chương 10](./10-trien-khai-kubernetes-kustomize-va-keda.md).

---

## 8. Đánh đổi

### 8.1. Ưu

1. **Một lệnh dựng cả hệ** — `docker compose up -d`.
2. **Hybrid mode miễn phí** nhờ mặc định `localhost` + hai listener Kafka.
3. **Integration test dùng hạ tầng thật** — kiểm chứng được tính chất của DB/Kafka.
4. **Test chứng minh effectively-once**, không chỉ mô tả nó.
5. **k6 threshold biến hiệu năng thành hợp đồng.**
6. **Image ~200MB** nhờ multi-stage.
7. **JVM container-aware** — không OOMKilled khi đổi limit.
8. **Docker layer cache** tối ưu đúng chỗ.

### 8.2. Nhược

1. **Nặng máy:** 11 container ≈ 6-8GB RAM, build lần đầu 3-5 phút.
2. ⚠️ **`depends_on` không đợi dịch vụ sẵn sàng** — phải khởi động hai bước thủ công.
3. ⚠️ **Phải bơm tick trước khi đặt lệnh** — bước này dễ quên và gây hiểu nhầm "hệ hỏng".
4. ⚠️ **Vùng phủ test còn hẹp** — 3 test cho 2.700 dòng; thiếu test cho outbox relay, order-service, optimistic lock, DLT.
5. ⚠️ **Gap #22:** dev không bao giờ có 2 partition trên 1 consumer thread.
6. ⚠️ **CI không push image** ⇒ chuỗi CI→CD chưa khép kín.
7. ⚠️ **`@CrossOrigin("*")`** không phù hợp production.
8. ⚠️ **Không có dashboard Grafana** — thư mục `monitoring/dashboards/` không tồn tại.
9. **Testcontainers cần Docker** — không chạy được trên runner không có Docker daemon.
10. **MockSchemaRegistry không kiểm chứng tương thích thật.**

---

## 9. Phương án thay thế

| Phương án | Khi nào hợp | Vì sao repo không chọn |
|---|---|---|
| **Embedded Kafka** (`spring-kafka-test`) | Test nhanh, không cần Docker | Không phải Kafka thật — hành vi rebalance, ISR, log compaction khác |
| **H2 / HSQLDB in-memory** | Unit test repository | 🔑 **Không có** `ON CONFLICT DO NOTHING`, `FOR UPDATE SKIP LOCKED`, partial index — đúng những thứ cần test |
| **Mock mọi thứ** | Unit test logic thuần | Tính chất đang test thuộc về hạ tầng, không phải code |
| **Minikube/kind cho local** | Muốn giống production | Nặng hơn Compose nhiều; repo **có** overlay `dev` cho việc này |
| **Tilt / Skaffold** | Vòng lặp dev nhanh trên K8s | Thêm một công cụ; hybrid mode đã giải quyết nhu cầu debug |
| **Gatling / JMeter** thay k6 | Đội quen Java/Scala | k6 viết bằng JS, nhẹ, `thresholds` tích hợp CI tự nhiên |
| **Compose `healthcheck` + `service_healthy`** | Muốn `up -d` một lệnh | Làm file dài gấp đôi; hai bước thủ công đơn giản hơn cho local |

---

## 10. Cách kiểm chứng

```bash
# === Dựng hệ ===
docker compose up -d kafka postgres schema-registry redis zipkin prometheus grafana
docker compose up -d --build market-gateway pricing-risk-service order-service matching-engine
docker compose ps

# Health
for p in 8081 8082 8083 8084; do echo -n "$p: "; curl -s http://localhost:$p/actuator/health | jq -r .status; done

# === Kiểm chứng luồng (theo đúng thứ tự!) ===
sleep 5
curl -s http://localhost:8082/api/v1/marketdata/HPG | jq

# Bơm 6 tick giống nhau -> volatility 0 -> LOW
for i in 1 2 3 4 5 6; do
  curl -s -X POST http://localhost:8081/api/v1/ticks -H 'Content-Type: application/json' \
    -d "{\"symbol\":\"LOCAL\",\"price\":28500,\"volume\":100,\"eventTime\":$(date +%s%3N)}"
done
sleep 2
curl -s http://localhost:8082/api/v1/marketdata/LOCAL | jq '{volatility, riskLevel}'

# Đặt lệnh
curl -s -X POST http://localhost:8083/api/v1/orders \
  -H 'Content-Type: application/json' -H 'X-API-KEY: demo-key' \
  -d '{"clientOrderId":"local-1","symbol":"LOCAL","side":"BUY","price":28500,"quantity":100,"accountId":"ACC001"}' | jq
sleep 2
curl -s http://localhost:8083/api/v1/orders/local-1 -H 'X-API-KEY: demo-key' | jq '{status, filledQty}'
curl -s http://localhost:8084/api/v1/executions/local-1 | jq 'length'

# === Hybrid mode ===
docker compose stop order-service
mvn -pl order-service spring-boot:run        # IDE: Run/Debug OrderServiceApplication
curl -s http://localhost:8083/actuator/health | jq -r .status

# === UI ===
node ui/serve.js                              # -> http://localhost:8080
curl -s http://localhost:8080/p/8082/api/v1/marketdata/HPG | jq

# === Test ===
mvn clean verify                              # tất cả, cần Docker
mvn -pl pricing-risk-service verify -Dtest=TickToMarketDataIT
mvn -pl matching-engine verify -Dtest=IdempotentConsumerIT
mvn -pl matching-engine verify -Dtest=SagaRejectionIT

# Xem container Testcontainers tạo ra trong lúc test chạy
docker ps --filter "label=org.testcontainers=true"

# === Load test ===
k6 run loadtest/order-load.js

# === Gỡ lỗi ===
docker compose logs -f order-service
docker compose logs -f matching-engine | grep "KHOP LENH"
docker compose exec postgres psql -U trading -d trading -c "\dn"      # liệt kê schema
docker compose exec postgres psql -U trading -d trading -c "\dt ordersvc.*"
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:19092 --describe --all-groups

# Xem Flyway đã chạy migration nào
docker compose exec postgres psql -U trading -d trading -c \
  "SELECT installed_rank, version, description, success FROM ordersvc.flyway_schema_history;"

# === Dọn ===
docker compose down          # giữ volume
docker compose down -v       # XOÁ CẢ DỮ LIỆU
```

---

## 11. Hạn chế đã biết

| # | Loại | Vị trí | Vấn đề | Hướng sửa |
|:--:|---|---|---|---|
| 22 | **Gap** | Môi trường dev | 3 partition / 1 pod / concurrency 3 ⇒ một consumer không bao giờ ôm quá 1 partition; bug phụ thuộc batch trộn nhiều partition không lộ ra | Không tăng partition dev; dựa vào staging ở `minReplicaCount: 1`, và thêm IT với 2 partition trên 1 consumer |
| — | Ops | `.github/workflows/ci.yml:37-41` | Push registry bị comment ⇒ không có artifact để deploy; overlay prod cần digest mà digest chỉ có sau push | Bỏ comment + `permissions: packages: write` |
| — | Ops | Vùng phủ test | Thiếu test cho `OutboxRelay`, pre-trade check, optimistic lock, DLT, `SKIP LOCKED` đa pod | Bổ sung IT |
| — | Ops | `docker-compose.yml` | `depends_on` không đợi dịch vụ sẵn sàng | `healthcheck` + `condition: service_healthy` |
| — | Ops | 4 controller | `@CrossOrigin("*")` | Giới hạn origin cụ thể ở production |
| — | Ops | `monitoring/grafana-dashboards.yml` | Thư mục `monitoring/dashboards/` không tồn tại | Thêm dashboard JSON |
| — | Gap | Test | MockSchemaRegistry không kiểm chứng tương thích BACKWARD thật | Một IT dùng container Schema Registry |

---

## 12. Câu hỏi phỏng vấn

**Q: Vì sao dùng Testcontainers thay vì H2 và Embedded Kafka?**

A: Vì những tính chất đang được kiểm chứng là tính chất **của hạ tầng**, không phải của code. `INSERT ... ON CONFLICT DO NOTHING` có thực sự nguyên tử khi hai transaction chạy song song không? `FOR UPDATE SKIP LOCKED` có thực sự không chặn nhau không? H2 **không có** cả hai cú pháp đó — nó sẽ báo lỗi hoặc, tệ hơn, hiểu khác. Embedded Kafka có hành vi rebalance và ISR khác Kafka thật. Nếu mock `ProcessedEventRepository`, tôi chỉ chứng minh được rằng mình gọi đúng method — không chứng minh được rằng hệ thống không khớp lệnh hai lần. Cái giá là test chậm hơn (~10 giây khởi động container) và cần Docker; cả hai đều chấp nhận được, nhất là khi GitHub runner có Docker sẵn.

**Q: Trong `IdempotentConsumerIT` có hai lần `await()`. Bỏ lần thứ hai được không?**

A: Không — bỏ nó thì test **pass sai**. Lần đầu chờ tới khi có đúng 1 execution, tức chứng minh "đã xử lý xong". Nhưng nó không loại trừ việc message thứ 2 và 3 tạo thêm row *sau* thời điểm kiểm tra. Lần hai `await().during(3s)` khẳng định điều kiện đúng **liên tục** suốt 3 giây, tức chứng minh "không có execution thứ hai xuất hiện muộn". Đây là mẫu chung cho mọi assertion về **sự vắng mặt** trong hệ bất đồng bộ: không thể chứng minh một thứ không xảy ra bằng cách kiểm tra một lần.

**Q: Giải thích chế độ hybrid và vì sao nó hoạt động.**

A: Chạy hạ tầng và 3 service trong Docker, service cần debug thì chạy từ IDE với debugger gắn vào. Nó hoạt động nhờ hai thứ. Thứ nhất, mọi biến môi trường trong `application.yml` đều có **mặc định là `localhost`** — Docker Compose ghi đè bằng tên service, chạy từ IDE thì mặc định có tác dụng, không phải sửa gì. Thứ hai, Kafka khai **hai listener**: `PLAINTEXT` advertised là `kafka:19092` cho container, `EXTERNAL` advertised là `localhost:9092` cho process trên host. Nếu chỉ có một listener, client sẽ nhận địa chỉ sai cho ngữ cảnh của nó và lỗi `Connection refused`. Một lưu ý khi debug: breakpoint trong `@KafkaListener` sẽ giữ consumer quá `max.poll.interval.ms` và gây rebalance — nên đặt breakpoint ở lớp service bên dưới.

**Q: `mvn verify` hay `mvn test` trong CI?**

A: `verify`, và sự khác biệt ở đây không nhỏ. `test` chỉ chạy Surefire với `*Test.java`; `verify` chạy thêm Failsafe với `*IT.java`. Toàn bộ integration test của repo này đặt tên `*IT`, nên `mvn test` sẽ **bỏ qua hoàn toàn** chúng — CI xanh trong khi không kiểm chứng một dòng nào về tính đúng đắn phân tán. Đây là lỗi âm thầm: không có cảnh báo nào nói rằng bạn vừa bỏ qua 3 test.

**Q: Vì sao k6 dùng `clientOrderId` khác nhau mỗi request?**

A: Vì nếu dùng id cố định, từ request thứ hai trở đi Redis sẽ hit ở lớp 1 của `OrderService` và trả kết quả cũ ngay — tôi sẽ đo **tốc độ Redis trả cache**, không đo đường đặt lệnh thật (pre-trade check + 2 INSERT trong transaction). Con số sẽ đẹp một cách vô nghĩa. Đây là cạm bẫy phổ biến nhất khi load-test một API có idempotency. Một chi tiết liên quan trong cùng file: threshold lọc `{expected_response:true}` để các response `429` — vốn trả về cực nhanh — không kéo p95 xuống và che mất vấn đề hiệu năng thật.

**Q: Vùng phủ test hiện tại đủ chưa? Nếu được thêm một test, bạn thêm gì?**

A: Chưa đủ — 3 integration test cho 2.700 dòng. Nếu chỉ được thêm một, tôi thêm test cho `OutboxRelay`: nó là thành phần mà **mọi** tính đúng đắn của hệ dựa vào, chạy ở cả hai service, và hiện **không có test nào**. Cụ thể tôi muốn kiểm chứng ba hành vi: (1) publish thành công thì `published_at` được ghi; (2) publish thất bại thì **cả batch** rollback và không event nào bị mark; (3) hai pod chạy `FOR UPDATE SKIP LOCKED` song song thì lấy được các lô khác nhau mà không chờ nhau. Hành vi thứ ba đặc biệt quan trọng vì nó là tính chất của PostgreSQL mà chỉ Testcontainers mới kiểm được.

---

## 13. Bảng tổng kết

| Khía cạnh | Lựa chọn | Lý do | Cái giá |
|---|---|---|---|
| Môi trường local | Docker Compose, 11 container | Một lệnh dựng cả hệ | 6-8GB RAM |
| Image | Multi-stage, `-pl -am`, cache pom trước | ~200MB, build nhanh nhờ layer cache | — |
| JVM | `MaxRAMPercentage=75` + G1 50ms | Container-aware, không OOMKilled khi đổi limit | — |
| Thứ tự khởi động | Hai bước thủ công | `depends_on` không đợi dịch vụ sẵn sàng | Phải nhớ; K8s dùng initContainer |
| Debug | **Hybrid mode** | Miễn phí nhờ mặc định `localhost` + 2 listener Kafka | Breakpoint ở listener gây rebalance |
| UI | Static server + proxy same-origin | Tránh CORS với `file://` | Thêm một script Node |
| Integration test | **Testcontainers** — Kafka + Postgres thật | Kiểm chứng tính chất của hạ tầng | +10s khởi động, cần Docker |
| Schema Registry trong test | `mock://` | Cùng JVM, tiết kiệm một container | Không kiểm chứng tương thích thật |
| Assertion bất đồng bộ | Awaitility, **hai lần** `await` | Chứng minh cả "đã xảy ra" và "không xảy ra thêm" | — |
| Load test | k6 với `thresholds` | Hiệu năng thành hợp đồng chặn merge | Phải cài k6 |
| CI | `mvn verify` + matrix 4 image | Chạy cả integration test; build song song | ⚠️ Chưa push registry |
| Vùng phủ | 3 IT | Phủ đúng ba tính chất khó nhất | ⚠️ Thiếu relay, order-service, DLT |

> **Đọc tiếp:** [Chương 10 — Kubernetes: Kustomize & KEDA](./10-trien-khai-kubernetes-kustomize-va-keda.md).
