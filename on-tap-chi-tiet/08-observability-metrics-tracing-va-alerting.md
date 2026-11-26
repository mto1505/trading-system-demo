# Chương 08 — Observability: Metrics, Tracing & Alerting

> **Câu hỏi trung tâm:** một giao dịch đi qua 4 service, 4 topic, 3 schema database. Khi nó hỏng, làm sao *biết*, và làm sao *tìm ra* hỏng ở đâu?
>
> **Nguyên tắc nền của chương này:** mỗi cơ chế "degrade gracefully" phải đi kèm một cơ chế phát hiện. **Lỗi im lặng nguy hiểm hơn lỗi ồn ào.**
>
> **Tài liệu đối chiếu:** `DESIGN-NOTES.md` mục 13-14, `GUIDE.md` mục 6, `DEEP-DIVE.md` mục 5.

---

## 1. Nó là gì?

### 1.1. Ba trụ cột và vai trò khác nhau của từng cái

| Trụ cột | Công cụ | Trả lời câu hỏi | Chi phí |
|---|---|---|---|
| **Metrics** | Micrometer → Prometheus → Grafana | *"Có đang hỏng không? Hỏng bao nhiêu?"* | Thấp — số liệu tổng hợp |
| **Tracing** | Micrometer Tracing (Brave) → Zipkin | *"Một request cụ thể đi đâu, chậm ở chặng nào?"* | Cao — phải sampling |
| **Logs** | SLF4J + pattern có traceId | *"Chuyện gì đã xảy ra, chi tiết?"* | Trung bình |

🔑 **Ba thứ này được nối với nhau bằng `traceId`** — đó là điểm làm cho bộ công cụ này dùng được thay vì chỉ tồn tại:

```yaml
# Có ở CẢ BỐN application.yml
logging:
  pattern:
    level: "%5p [${spring.application.name:},%X{traceId:-},%X{spanId:-}]"
```

Log ra như sau:

```
 INFO [matching-engine,4f3a2b1c9d8e7f60,a1b2c3d4e5f60718] KHOP LENH DEMO-0001 BUY HPG x100 @ 28512.0000
```

Copy `4f3a2b1c9d8e7f60` dán vào Zipkin ⇒ thấy toàn bộ hành trình xuyên 4 service. **Không có dòng pattern này, distributed tracing chỉ dùng được một nửa:** thấy được biểu đồ nhưng không nối được từ một dòng log cụ thể tới biểu đồ đó.

### 1.2. Hạ tầng

```yaml
# docker-compose.yml
prometheus:
  image: prom/prometheus:v2.52.0
  ports: ["9090:9090"]
  volumes: ["./monitoring/prometheus.yml:/etc/prometheus/prometheus.yml"]

grafana:
  image: grafana/grafana:11.0.0
  ports: ["3000:3000"]
  environment:
    GF_SECURITY_ADMIN_PASSWORD: admin
  volumes:
    - "./monitoring/grafana-datasource.yml:/etc/grafana/provisioning/datasources/ds.yml"
    - "./monitoring/grafana-dashboards.yml:/etc/grafana/provisioning/dashboards/provider.yml"
    - "./monitoring/dashboards:/var/lib/grafana/dashboards"

zipkin:
  image: openzipkin/zipkin:3
  ports: ["9411:9411"]
```

```xml
<!-- pom.xml - dùng chung cho cả 4 service -->
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-actuator</artifactId>
</dependency>
<dependency>
    <groupId>io.micrometer</groupId>
    <artifactId>micrometer-registry-prometheus</artifactId>
</dependency>
<!-- Distributed tracing: traceId xuyên suốt REST + Kafka, export sang Zipkin -->
<dependency>
    <groupId>io.micrometer</groupId>
    <artifactId>micrometer-tracing-bridge-brave</artifactId>
</dependency>
<dependency>
    <groupId>io.zipkin.reporter2</groupId>
    <artifactId>zipkin-reporter-brave</artifactId>
</dependency>
```

---

## 2. Metrics

### 2.1. Pull model — vì sao Prometheus kéo chứ không nhận đẩy

```yaml
# monitoring/prometheus.yml
global:
  scrape_interval: 10s      # trading system nên scrape dày (mặc định 1m quá thưa)

scrape_configs:
  - job_name: 'trading-services'
    metrics_path: /actuator/prometheus
    static_configs:
      - targets:
          - 'market-gateway:8081'
          - 'pricing-risk-service:8082'
          - 'order-service:8083'
          - 'matching-engine:8084'
```

| | **Pull (Prometheus)** | Push (StatsD, Graphite) |
|---|---|---|
| Ai khởi xướng | Prometheus gọi `/actuator/prometheus` | Ứng dụng gửi lên collector |
| Biết service chết | ✅ Scrape fail ⇒ `up = 0` ⇒ alert | ❌ Không gửi ≡ không có dữ liệu ≡ **không phân biệt được với "đang khoẻ nhưng rảnh"** |
| Service phải biết về monitoring | Không | Có (địa chỉ collector) |
| Cấu hình tập trung | ✅ Một file | Phân tán trong từng service |
| Debug | ✅ `curl /actuator/prometheus` thấy ngay | Phải vào collector |
| Hợp với | Service dài hạn | **Job ngắn hạn** (dùng Pushgateway) |

🔑 **`up == 0` là alert mà push model không thể có.** Với push, một service chết đơn giản là ngừng gửi — và Prometheus không phân biệt được với một service đang khoẻ mà không có sự kiện nào.

```yaml
# monitoring/alert-rules.yml:17-22
- alert: ServiceDown
  expr: up == 0
  for: 30s
  labels: { severity: critical }
  annotations:
    summary: "Service {{ $labels.instance }} khong phan hoi"
```

💡 `scrape_interval: 10s` thay vì mặc định 60s. Với hệ giao dịch, 60s nghĩa là phát hiện sự cố chậm tới một phút — trong một phút của phiên ATC có thể mất hàng nghìn lệnh. Cái giá: lưu trữ nhiều điểm dữ liệu hơn 6 lần.

### 2.2. Exposure tối thiểu

```yaml
# Có ở cả 4 application.yml
management:
  endpoints:
    web:
      exposure:
        include: health,info,prometheus,metrics   # /actuator/prometheus cho Prometheus scrape
  endpoint:
    health:
      probes:
        enabled: true    # bật /actuator/health/liveness & /readiness cho Kubernetes probe
  metrics:
    tags:
      application: ${spring.application.name}
    distribution:
      percentiles-histogram:
        http.server.requests: true   # histogram cho latency REST API
```

🔑 **`include` liệt kê tường minh, không dùng `*`.** Actuator có các endpoint nguy hiểm: `/actuator/env` lộ toàn bộ biến môi trường kể cả mật khẩu, `/actuator/heapdump` tải về toàn bộ heap, `/actuator/shutdown` tắt ứng dụng. Mở `*` là một lỗ hổng bảo mật thật — và nhớ rằng `ApiKeyFilter` chỉ bảo vệ `/api/**`, **không** bảo vệ `/actuator/**`.

🔑 **`metrics.tags.application`** gắn tên service vào **mọi** metric. Không có nó, `orders_accepted_total` từ 3 pod bị trộn lẫn và không biết pod nào.

🔑 **`percentiles-histogram: true`** sinh ra các bucket `http_server_requests_seconds_bucket{le="..."}`, cho phép tính percentile **ở phía Prometheus** bằng `histogram_quantile()`. Nếu chỉ publish percentile pre-computed từ ứng dụng, ta **không thể tổng hợp** chúng qua nhiều pod — trung bình của các p99 không phải là p99 của toàn hệ.

### 2.3. Custom metrics nghiệp vụ

Metrics có sẵn (`jvm_*`, `http_server_requests_*`, `kafka_consumer_*`, `hikaricp_*`) trả lời được *"JVM có khoẻ không"*. Chúng **không** trả lời được *"có lệnh nào bị kẹt không"*. Đó là việc của custom metrics.

#### Trong `pricing-risk-service`

```java
// pricing-risk-service/src/main/java/com/trading/pricing/consumer/TickConsumer.java:33-43
// Metric quan trọng nhất của hệ thống trading:
this.processingTimer = Timer.builder("pricing.batch.process")
        .description("Thoi gian xu ly 1 batch tick")
        .publishPercentiles(0.5, 0.95, 0.99)   // p50/p95/p99 hien tren Grafana
        .register(registry);
this.e2eLatencyTimer = Timer.builder("pricing.e2e.latency")
        .description("Latency tu nguon toi khi tinh xong")
        .publishPercentiles(0.5, 0.95, 0.99)
        .register(registry);
```

🔑 **Hai Timer đo hai thứ khác hẳn nhau — và phân biệt chúng là chìa khoá chẩn đoán:**

| Metric | Đo | Vọt lên nghĩa là | Hành động |
|---|---|---|---|
| `pricing_batch_process` | Thời gian CPU xử lý một lô | Thuật toán chậm, GC, CPU throttle | Xem heap, xem có ai thêm I/O vào hot path |
| `pricing_e2e_latency` | `now − tick.eventTime()` | **Lag Kafka** — consumer không theo kịp | Scale (KEDA tự làm), hoặc tăng partition |

Cả hai cùng vọt ⇒ nghẽn CPU/GC. Chỉ `e2e` vọt ⇒ nghẽn vận chuyển. Chỉ có một metric thì không phân biệt được.

#### Danh mục đầy đủ custom metrics

| Metric | Loại | Khai ở | Ý nghĩa |
|---|---|---|---|
| `gateway_ticks_sent_total` | Counter | `TickProducer.java:24` | Tick produce thành công |
| `gateway_ticks_error_total` | Counter | `TickProducer.java:25` | Tick produce thất bại |
| `pricing_batch_process` | Timer | `TickConsumer.java:34` | Thời gian xử lý một lô |
| `pricing_e2e_latency` | Timer | `TickConsumer.java:39` | 🔑 Độ trễ từ nguồn tới khi tính xong |
| `orders_accepted_total` | Counter | `OrderService.java:65` | Lệnh vào saga |
| `orders_rejected_total` | Counter | `OrderService.java:66` | Lệnh bị pre-trade từ chối |
| `orders_duplicate_total` | Counter | `OrderService.java:67` | Phát hiện trùng (cả 3 lớp) |
| `orders_filled_total` | Counter | `OrderStatusService.java:25` | 🔑 Saga đóng với FILLED |
| `orders_rejected_by_matching_total` | Counter | `OrderStatusService.java:26` | 🔑 Saga đóng với REJ_MATCH |
| `orders_report_ignored_total` | Counter | `OrderStatusService.java:27` | Report bị bỏ qua (trùng **hoặc** lệnh không tồn tại) |
| `matching_executed_total` | Counter | `MatchingService.java:45` | Lệnh được khớp |
| `matching_rejected_total` | Counter | `MatchingService.java:46` | Bị luật sàn từ chối |
| `matching_duplicate_skipped_total` | Counter | `MatchingService.java:47` | Message trùng bị bỏ qua |
| `outbox_published_total` | Counter | `OutboxRelay.java:46` | Event đã publish |
| `outbox_publish_failed_total` | Counter | `OutboxRelay.java:47` | 🔑 Relay thất bại |

🔑 **Ba counter in đậm là nền của alert quan trọng nhất** — xem mục 4.2.

⚠️ `orders_report_ignored_total` gộp hai tình huống rất khác nhau: *report trùng* (hoàn toàn bình thường) và *lệnh không tồn tại* (bất thường, nghĩa là ai đó publish thẳng vào Kafka). Nên tách thành hai counter, nếu không alert không phân biệt được.

#### Metrics còn thiếu

| Nên có | Vì sao |
|---|---|
| `snapshot_flush_failed_total` | ⚠️ `SnapshotStore.flushToDatabase()` nuốt lỗi mà **không có counter** — DB chết 3 ngày không ai biết |
| `clock_skew_detected_total` | ⚠️ Lệch đồng hồ vô hiệu hoá staleness check âm thầm (Gap #6) |
| `dlt_published_total` | ⚠️ Alert DLT hiện tại không bắn (mục 4.3) |
| `orders_pending_gauge` | Đo trực tiếp lệnh treo thay vì trừ counter (mục 4.2) |

### 2.4. 🔑 p99 vs trung bình — vì sao trung bình nói dối

Ví dụ cụ thể: 100 request trong một giây.

```
99 request:  10ms
 1 request: 5000ms   (bị GC pause hoặc chờ khoá DB)
```

| Chỉ số | Giá trị | Diễn giải |
|---|---|---|
| **Trung bình** | `(99×10 + 5000)/100 = 59,9ms` | "Hệ thống khoẻ" — hoàn toàn sai |
| **p50** | 10ms | "Rất nhanh" — cũng không phản ánh vấn đề |
| **p99** | ~5000ms | 🔑 **"Có một request bị treo 5 giây"** |

Với hệ giao dịch, 1% request bị treo 5 giây nghĩa là **cứ 100 khách thì có 1 người không đặt được lệnh đúng lúc cần**. Đó chính xác là kiểu sự cố gây khiếu nại.

```java
.publishPercentiles(0.5, 0.95, 0.99)
```

💡 Và đây là lý do `percentiles-histogram: true` quan trọng: nó cho phép tính p99 **toàn hệ** bằng `histogram_quantile(0.99, sum by (le) (rate(..._bucket[1m])))`. Nếu mỗi pod chỉ publish p99 của riêng nó, ta không có cách nào ghép lại — **trung bình của các p99 không phải p99**.

### 2.5. Thu thập trong Kubernetes — hai cơ chế

#### (a) ServiceMonitor (có Prometheus Operator)

```yaml
# k8s/components/monitoring/servicemonitor.yaml
# Một ServiceMonitor phủ cả 4 service: chọn theo label chứ không liệt kê tên,
# nên thêm service mới là tự động được scrape.
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: trading-system
  namespace: trading
spec:
  selector:
    matchExpressions:
      - key: app.kubernetes.io/component
        operator: In
        values: [gateway, pricing, order, matching]
  endpoints:
    - port: http
      path: /actuator/prometheus
      interval: 15s
      scrapeTimeout: 10s
      relabelings:
        # Giữ tên pod để truy ngược một pod/node lỗi từ biểu đồ.
        - sourceLabels: [__meta_kubernetes_pod_name]
          targetLabel: pod
        - sourceLabels: [__meta_kubernetes_pod_label_app_kubernetes_io_name]
          targetLabel: application
```

💡 **Chọn theo label thay vì liệt kê tên** — thêm service mới cùng `component` là tự động được scrape, không phải sửa file này.

⚠️ **Cạm bẫy lớn nhất khi dùng Prometheus Operator**, ghi rõ trong `helm/trading-system/values.yaml`:

```yaml
monitoring:
  # kube-prometheus-stack MAC DINH chi nhan PodMonitor/ServiceMonitor mang label
  # `release: <ten-release-cua-stack>` (podMonitorSelector). Thieu label nay thi
  # object van duoc tao ra nhung Prometheus KHONG BAO GIO doc toi.
  # Kiem tra: kubectl -n monitoring get prometheus -o yaml | grep -A5 podMonitorSelector
  extraLabels: {}
    # release: kube-prometheus-stack
```

🔑 Đây là loại lỗi tệ nhất: **mọi thứ trông đúng** — `kubectl get servicemonitor` thấy object, không có lỗi nào — nhưng Prometheus im lặng bỏ qua nó vì thiếu một label. Triệu chứng: dashboard trống, không ai biết tại sao.

#### (b) Prometheus tối giản (chưa có Operator)

```yaml
# k8s/components/in-cluster-prometheus/files/prometheus.yml
# Prometheus tối giản cho dev/staging khi cluster CHƯA có kube-prometheus-stack.
scrape_configs:
  - job_name: trading-pods
    kubernetes_sd_configs:
      - role: pod
        namespaces:
          own_namespace: true     # chỉ namespace của chính nó -> Role là đủ, không cần ClusterRole
    relabel_configs:
      - source_labels: [__meta_kubernetes_pod_annotation_prometheus_io_scrape]
        action: keep
        regex: "true"
      - source_labels: [__meta_kubernetes_pod_annotation_prometheus_io_path]
        action: replace
        target_label: __metrics_path__
      - source_labels: [__address__, __meta_kubernetes_pod_annotation_prometheus_io_port]
        action: replace
        regex: ([^:]+)(?::\d+)?;(\d+)
        replacement: $1:$2
        target_label: __address__
```

Khớp với annotation trên pod:

```yaml
# k8s/base/apps/order-service.yaml
annotations:
  prometheus.io/scrape: "true"
  prometheus.io/port: "8083"
  prometheus.io/path: "/actuator/prometheus"
```

💡 **`own_namespace: true` là một quyết định bảo mật tốt.** Nó cho phép dùng `Role` thay vì `ClusterRole` — Prometheus chỉ đọc được pod trong namespace của chính nó. Với `ClusterRole`, một Prometheus bị chiếm quyền đọc được metadata pod của **toàn cluster**.

Và NetworkPolicy phải mở đường cho cả hai nguồn:

```yaml
# k8s/base/apps/apps-networkpolicy.yaml
# Prometheus/Grafana scrape /actuator/prometheus. Hai nguồn: stack ngoài
# (namespace monitoring) và Prometheus tối giản trong namespace (component
# in-cluster-prometheus, dùng ở dev).
- from:
    - namespaceSelector:
        matchLabels: { kubernetes.io/metadata.name: monitoring }
    - podSelector:
        matchLabels: { app.kubernetes.io/name: prometheus }
```

💡 Và một ghi chú quan trọng trong cùng file:

```yaml
# LƯU Ý: probe của kubelet xuất phát từ NODE, không đi qua NetworkPolicy, nên
# liveness/readiness vẫn chạy dù không có rule ingress nào cho chúng.
```

Đây là chi tiết khiến nhiều người lúng túng khi mới dùng NetworkPolicy: tại sao probe vẫn hoạt động trong khi default-deny đang bật.

---

## 3. Distributed Tracing

### 3.1. Cấu hình và sampling

```yaml
# Có ở cả 4 application.yml
# === DISTRIBUTED TRACING ===
# Sample 10%: trace 100% luong tick se ton CPU/băng thông đáng kể trên hot path.
# 10% đủ để thấy hình dạng latency; khi điều tra sự cố có thể tạm nâng lên 1.0.
management:
  tracing:
    sampling:
      probability: ${TRACING_PROBABILITY:0.1}
  zipkin:
    tracing:
      endpoint: ${ZIPKIN_ENDPOINT:http://localhost:9411/api/v2/spans}
```

| Môi trường | `TRACING_PROBABILITY` | Lý do |
|---|:--:|---|
| dev | **1.0** | Trace mọi request để soi luồng |
| staging | 0.3 | Đủ để thấy vấn đề, không quá tốn |
| production | **0.05** | Hot path 100.000 tick/s — trace 100% sẽ tốn CPU và băng thông nhiều hơn chính việc tính toán |

🔑 **Vì sao sampling là bắt buộc trên hot path.** Mỗi span cần: cấp phát object, tạo context, propagate qua header, serialize, gửi HTTP tới Zipkin. Ở 100.000 tick/giây, 100% sampling nghĩa là 100.000 span/giây — vượt xa khả năng của Zipkin và tạo áp lực GC lớn hơn chính `RiskCalculator`.

💡 **Sampling là quyết định đầu-luồng (head-based):** quyết định trace hay không được lấy ở span đầu tiên và propagate xuyên suốt. Nghĩa là một trace đã được chọn thì **đầy đủ cả 4 service**, không bị cắt khúc. Nhược: 95% request không được trace, nên request chậm cụ thể mà khách phàn nàn có thể không có trace. Giải pháp nâng cao là **tail-based sampling** (OpenTelemetry Collector) — giữ lại trace *sau khi* biết nó chậm hoặc lỗi.

### 3.2. Propagate xuyên REST và Kafka

REST được Spring Boot tự động instrument. Kafka thì phải bật tường minh **ở cả hai đầu**:

```java
// market-gateway/config/KafkaProducerConfig.java:62-66
@Bean
public KafkaTemplate<String, TickEventAvro> tickKafkaTemplate() {
    KafkaTemplate<String, TickEventAvro> template = new KafkaTemplate<>(tickProducerFactory());
    template.setObservationEnabled(true);
    return template;
}
```

```java
// pricing-risk-service/config/KafkaConfig.java:82-83
// Propagate traceId từ Kafka header vào span của listener
factory.getContainerProperties().setObservationEnabled(true);
```

```java
// pricing-risk-service/config/KafkaConfig.java:112-113
var template = new KafkaTemplate<>(enrichedProducerFactory());
template.setObservationEnabled(true);   // nối tiếp trace sang order-service
```

```java
// order-service/config/KafkaConfig.java:72, matching-engine/config/KafkaConfig.java:69
template.setObservationEnabled(true);
```

🔑 **Phải bật ở `KafkaTemplate` (producer) và `ContainerProperties` (consumer).** Bật một bên thì trace đứt ngay tại ranh giới Kafka — bạn thấy span ở producer rồi mất dấu. Repo bật đủ ở cả 6 chỗ (4 producer + 4 consumer factory).

Header được Brave gắn vào message (định dạng B3):

| Header | Nội dung |
|---|---|
| `b3` hoặc `X-B3-TraceId` | ID của toàn bộ trace |
| `X-B3-SpanId` | ID của span hiện tại |
| `X-B3-ParentSpanId` | Span cha |
| `X-B3-Sampled` | `1` = trace này được giữ |

### 3.3. Một trace đầy đủ trông như thế nào

```
Trace 4f3a2b1c9d8e7f60                                       tổng: 1.047ms
│
├─ market-gateway    send stock.ticks                   2ms  ├
│                                                             │
├─ pricing-risk      poll stock.ticks                   8ms   ├──
│  └─ onTicks (batch 340 records)                       4ms   │ ├
│     └─ send stock.enriched                            3ms   │ └─
│                                                             │
├─ order-service     poll stock.enriched                5ms   ├──
│                                                             │
├─ order-service     POST /api/v1/orders               12ms   ├────
│  ├─ redis GET idem:DEMO-0001                          1ms   │ ├
│  ├─ saveWithOutbox (2 INSERT)                         7ms   │ ├──
│  └─ redis SET                                         1ms   │ └
│                                                             │
├─ order-service     OutboxRelay.relay                482ms   ├──────────────
│  └─ send stock.orders (acks=all)                      6ms   │             └─
│                                                             │
├─ matching-engine   poll stock.orders                  4ms   ├──
│  └─ MatchingService.process                          11ms   │ ├───
│     ├─ insertIfAbsent processed_event                 2ms   │ │├
│     ├─ TradingRules.validate                         0.1ms  │ ││
│     ├─ save execution                                 4ms   │ │├─
│     └─ save outbox_event                              3ms   │ │└─
│                                                             │
├─ matching-engine   OutboxRelay.relay                501ms   ├──────────────
│  └─ send stock.executions                             5ms   │             └─
│                                                             │
└─ order-service     poll stock.executions              4ms   ├──
   └─ OrderStatusService.apply (optimistic UPDATE)      6ms   │ ├──
```

🔑 **Biểu đồ này cho thấy ngay thủ phạm:** hai span `OutboxRelay.relay` chiếm **983ms / 1047ms = 94%** tổng thời gian. Và chúng không phải "xử lý chậm" — chúng là thời gian **nằm chờ** chu kỳ `@Scheduled(fixedDelay = 500)`. Đây chính xác là loại kết luận mà metrics không cho được: metrics nói "saga mất 1 giây", tracing nói "1 giây đó nằm ở đâu".

💡 Hành động rút ra từ trace này: hạ `outbox.relay.interval-ms` xuống 100ms (cắt ~80% độ trễ, đổi lấy 10 query/giây/pod), hoặc chuyển sang Debezium CDC (cắt gần hết, đổi lấy phải vận hành Kafka Connect + canh replication slot).

---

## 4. Alerting

### 4.1. Hai nơi khai báo

| Nơi | Dùng cho | File |
|---|---|---|
| File rule của Prometheus | Docker Compose local | `monitoring/alert-rules.yml` |
| CRD `PrometheusRule` | Kubernetes có Operator | `k8s/components/monitoring/prometheusrule.yaml` |

Nội dung gần như giống nhau; bản CRD có thêm `DeadLetterTopicGrowing` và nhóm rule theo chủ đề.

### 4.2. 🔑 `SagaStuckOrders` — alert quan trọng nhất

```yaml
# k8s/components/monitoring/prometheusrule.yaml
- name: trading-correctness
  rules:
    # Cảnh báo NGHIÊM TRỌNG NHẤT của hệ: lệnh đã ACCEPTED nhưng không bao
    # giờ nhận được execution report -> khách bị giữ tiền cho lệnh treo.
    - alert: SagaStuckOrders
      expr: (orders_accepted_total - (orders_filled_total + orders_rejected_by_matching_total)) > 10
      for: 2m
      labels: { severity: critical }
      annotations:
        summary: "Co lenh ACCEPTED khong nhan duoc execution report"
        description: "Kiem tra outbox_publish_failed_total, matching-engine con song khong, lag topic stock.orders/stock.executions."
```

**Vì sao đây là alert quan trọng nhất:** nó đo một thứ **về nghiệp vụ**, không về hạ tầng. Mọi pod có thể `UP`, CPU bình thường, không có exception nào — mà vẫn có 500 lệnh treo vì relay đang thất bại im lặng. Không alert nào về CPU/memory/HTTP bắt được điều đó.

**Hiệu số = số lệnh đang trong saga.** Khoẻ mạnh thì dao động quanh 0 (vào rồi ra trong ~1 giây). > 10 kéo dài 2 phút = saga đang đứt.

⚠️ **Nhược điểm thật của công thức:** Micrometer counter **reset về 0 khi pod restart**. `rate()` và `increase()` của Prometheus xử lý được counter reset, nhưng **phép trừ hai counter thô thì không**. Sau một rolling update, ba counter reset ở những thời điểm hơi khác nhau và hiệu số có thể âm hoặc dương giả — alert kêu oan hoặc im lặng sai.

Công thức bền hơn:

```promql
sum(increase(orders_accepted_total[10m]))
  - sum(increase(orders_filled_total[10m]))
  - sum(increase(orders_rejected_by_matching_total[10m])) > 10
```

Tốt nhất: đo **trực tiếp** bằng một gauge đếm lệnh `ACCEPTED` quá hạn, truy vấn từ chính partial index `idx_orders_pending`:

```java
Gauge.builder("orders.pending.overdue",
        () -> orderRepository.countStuckOlderThan(Duration.ofMinutes(1)))
     .register(registry);
```

### 4.3. Toàn bộ 6 alert rule và ý nghĩa thật

```yaml
# monitoring/alert-rules.yml (toàn văn)
groups:
  - name: trading-alerts
    rules:
      # Latency end-to-end p99 > 500ms trong 2 phút -> cảnh báo
      - alert: HighE2ELatency
        expr: histogram_quantile(0.99, rate(pricing_e2e_latency_seconds_bucket[1m])) > 0.5
        for: 2m
        labels: { severity: critical }
        annotations:
          summary: "P99 latency tick->calc vuot 500ms"

      # Consumer lag tang (can kafka_exporter trong production)
      - alert: OrderRejectSpike
        expr: rate(orders_rejected_total[5m]) > 10
        for: 1m
        labels: { severity: warning }
        annotations:
          summary: "Ty le tu choi lenh tang bat thuong"

      - alert: ServiceDown
        expr: up == 0
        for: 30s
        labels: { severity: critical }
        annotations:
          summary: "Service {{ $labels.instance }} khong phan hoi"

      - alert: SagaStuckOrders
        expr: (orders_accepted_total - (orders_filled_total + orders_rejected_by_matching_total)) > 10
        for: 2m
        labels: { severity: critical }
        ...

      # Outbox tắc: event ghi vào nhưng không ra được Kafka
      - alert: OutboxPublishFailing
        expr: rate(outbox_publish_failed_total[5m]) > 0
        for: 1m
        labels: { severity: critical }
        annotations:
          summary: "OutboxRelay dang that bai khi publish"
```

| Alert | Severity | Đo gì | Ý nghĩa nghiệp vụ |
|---|:--:|---|---|
| `SagaStuckOrders` | critical | accepted − (filled + rejected) | 🔑 **Khách bị giữ tiền cho lệnh không kết thúc** |
| `OutboxPublishFailing` | critical | `rate(outbox_publish_failed_total[5m]) > 0` | 🔑 Điểm đứt của Outbox — im lặng thì mất event |
| `HighE2ELatency` | critical | p99 `pricing_e2e_latency` > 500ms | Consumer không theo kịp, hoặc GC nặng |
| `ServiceDown` | critical | `up == 0` | Scrape fail quá 30s |
| `OrderRejectSpike` | warning | `rate(orders_rejected_total[5m]) > 10` | Dữ liệu stale / thị trường biến động / client bug |
| `DeadLetterTopicGrowing` | warning | message vào DLT | Đã retry hết mà vẫn hỏng |

💡 **`for:` là thứ chống alert nhiễu.** `ServiceDown` có `for: 30s` — một lần scrape fail do mạng chập không kêu. `SagaStuckOrders` có `for: 2m` — một đợt lệnh dồn tạm thời không kêu.

💡 **`OrderRejectSpike` đo `rate` chứ không đo tổng.** Tổng số lệnh bị từ chối luôn tăng (nó là counter); chỉ **tốc độ tăng** mới là tín hiệu. Đây là lỗi hay gặp khi mới viết alert: đặt ngưỡng trên counter thô.

⚠️ **Alert DLT hiện tại không bao giờ bắn:**

```yaml
# k8s/components/monitoring/prometheusrule.yaml
- alert: DeadLetterTopicGrowing
  expr: sum(rate(kafka_consumer_fetch_manager_records_consumed_total{topic=~"stock\..*\.DLT"}[5m])) > 0
  for: 5m
```

Metric `kafka_consumer_fetch_manager_records_consumed_total` chỉ tồn tại khi **có consumer đang đọc** topic đó. Hiện không service nào consume DLT ⇒ metric luôn bằng 0 ⇒ alert không bao giờ bắn. Đúng ra phải dùng `kafka_exporter` (đo số message trong topic từ phía broker) hoặc thêm một counter ở phía `DeadLetterPublishingRecoverer`.

🔑 **Và đây đúng là điều `DEEP-DIVE.md` cảnh báo: "DLQ không alert thì tương đương skip".** Alert tồn tại nhưng không chạy còn nguy hiểm hơn không có alert, vì nó tạo cảm giác an toàn giả.

### 4.4. Những gì còn thiếu

| Thiếu | Hậu quả |
|---|---|
| **Consumer lag** | Comment trong `alert-rules.yml:14` thừa nhận "can kafka_exporter trong production". Không có nó thì chỉ thấy triệu chứng (`e2e_latency` cao), không thấy nguyên nhân (lag). |
| **Alertmanager** | Rule được đánh giá nhưng **không ai được thông báo**. Không có routing, không có dedupe, không có silence, không có escalation. Prometheus chỉ hiển thị alert ở UI của nó. |
| **Dashboard Grafana** | `monitoring/grafana-dashboards.yml` khai provider trỏ vào `/var/lib/grafana/dashboards`, nhưng **thư mục `monitoring/dashboards/` không có trong repo** ⇒ không có dashboard nào được nạp. |
| `snapshot_flush_failed_total` | DB chết, write-behind thất bại im lặng |
| `clock_skew_detected_total` | Staleness check bị vô hiệu hoá âm thầm |
| **SLO / error budget** | Có threshold nhưng không có mục tiêu dịch vụ chính thức |

---

## 5. Health check — và một quyết định quan trọng

```yaml
# Cả 4 service
management:
  endpoint:
    health:
      probes:
        enabled: true
```

Bật `probes` tạo ra hai endpoint riêng:

| Endpoint | Gồm | K8s dùng cho |
|---|---|---|
| `/actuator/health/liveness` | `livenessState` (chỉ: JVM còn sống không) | `livenessProbe` — fail ⇒ **restart pod** |
| `/actuator/health/readiness` | `readinessState` + các health indicator của dependency | `readinessProbe` — fail ⇒ **gỡ pod khỏi Service** |

```yaml
# k8s/base/apps/order-service.yaml
startupProbe:
  # Rộng tay: JVM nguội + Flyway migration có thể mất vài chục giây.
  httpGet: { path: /actuator/health/liveness, port: http }
  periodSeconds: 5
  failureThreshold: 36            # 36 × 5s = 180 giây
readinessProbe:
  httpGet: { path: /actuator/health/readiness, port: http }
  periodSeconds: 5
  timeoutSeconds: 3
  failureThreshold: 3
livenessProbe:
  # Lỏng hơn readiness có chủ đích: DB ốm thì gỡ pod khỏi Service,
  # KHÔNG restart hàng loạt pod đang khoẻ.
  httpGet: { path: /actuator/health/liveness, port: http }
  periodSeconds: 10
  timeoutSeconds: 3
  failureThreshold: 6             # 6 × 10s = 60 giây
```

🔑 **Liveness lỏng hơn readiness là một quyết định có chủ đích, không phải cẩu thả.**

Kịch bản: database ốm.
- **Readiness fail** (3 × 5s = 15s) ⇒ pod bị gỡ khỏi Service ⇒ không nhận request mới. Đúng — pod không phục vụ được.
- **Liveness không fail**, vì nó chỉ kiểm tra JVM. Đúng — restart pod **không làm database khoẻ lại**, mà còn: mất request đang xử lý, gây rebalance consumer group, và nếu mọi pod cùng restart thì khi DB phục hồi sẽ có một cơn bão khởi động đồng loạt.

🔑 **Quy tắc:** *liveness chỉ nên fail khi restart thực sự sửa được vấn đề* (deadlock, heap cạn không phục hồi). Dependency bên ngoài ốm thuộc về readiness.

Và quyết định quan trọng nhất về health check trong repo này:

```yaml
# order-service/application.yml
management:
  health:
    redis:
      enabled: false   # Redis là cache phụ - nó chết KHÔNG được làm readiness fail
                       # (nếu không K8s sẽ rút pod khỏi Service dù pod vẫn phục vụ tốt)
```

Mặc định Spring Boot đưa Redis health indicator vào `readinessState`. Redis chết ⇒ readiness fail trên **mọi** pod ⇒ Kubernetes rút toàn bộ khỏi Service ⇒ **không ai đặt được lệnh** — dù mọi pod hoàn toàn có khả năng phục vụ qua DB constraint.

🔑 **Nguyên tắc:** health check phải phản ánh **khả năng phục vụ**, không phản ánh **sự hiện diện của mọi dependency**. Dependency nào mà thiếu nó vẫn phục vụ được thì không được vào readiness.

💡 `startupProbe` với `failureThreshold: 36` (180 giây) tách biệt "khởi động chậm" khỏi "đã chết". Không có nó, `livenessProbe` sẽ giết pod đang chạy Flyway migration — và mỗi vòng `CrashLoopBackOff` lại chờ lâu hơn, làm rollout treo vĩnh viễn.

---

## 6. Đánh đổi

### 6.1. Ưu

1. **Pull model** biết được service chết (`up == 0`).
2. **Custom metrics nghiệp vụ** bắt được lỗi mà metrics hạ tầng không thấy.
3. **p99 thay vì trung bình** — nhìn thấy đuôi, nơi sự cố thật nằm.
4. **TraceId trong log** nối ba trụ cột lại với nhau.
5. **Sampling theo môi trường** — không giết hot path ở production.
6. **Probe phân tầng** — liveness/readiness/startup có vai trò khác nhau.
7. **Alert về nghiệp vụ**, không chỉ về hạ tầng.
8. **ServiceMonitor chọn theo label** — thêm service là tự động được scrape.

### 6.2. Nhược

1. ⚠️ **Không có Alertmanager** — alert được đánh giá nhưng **không ai được báo**.
2. ⚠️ **Không có dashboard** — thư mục `monitoring/dashboards/` không tồn tại.
3. ⚠️ **Không có consumer lag metric** — thiếu `kafka_exporter`.
4. ⚠️ **Alert DLT không bao giờ bắn.**
5. ⚠️ **`SagaStuckOrders` dùng phép trừ counter thô** — không bền khi pod restart.
6. ⚠️ **Thiếu counter cho flush thất bại và lệch đồng hồ.**
7. **95% request không được trace** ở production — request chậm cụ thể có thể không có trace.
8. **Zipkin không bền** — `openzipkin/zipkin:3` mặc định lưu in-memory, mất khi restart.
9. **`/actuator/**` không được `ApiKeyFilter` bảo vệ** — dựa hoàn toàn vào NetworkPolicy.
10. **Không có SLO/error budget** chính thức.

---

## 7. Núm vặn cấu hình

| Tham số | Nơi khai | Mặc định | Ghi chú |
|---|---|---|---|
| `scrape_interval` | `monitoring/prometheus.yml` | 10s | Dày hơn mặc định 60s; ServiceMonitor dùng 15s |
| `TRACING_PROBABILITY` | env / overlay | 0,1 (dev 1.0 / prod 0.05) | 🔑 100% trên hot path tốn hơn chính việc tính toán |
| `ZIPKIN_ENDPOINT` | env | `http://zipkin:9411/api/v2/spans` | — |
| `management.endpoints.web.exposure.include` | mỗi service | `health,info,prometheus,metrics` | 🔑 **Không** dùng `*` |
| `percentiles-histogram` | mỗi service | `true` cho `http.server.requests` | Cần để tính p99 toàn hệ |
| `publishPercentiles` | `TickConsumer.java:36,41` | 0.5, 0.95, 0.99 | — |
| `management.health.redis.enabled` | `order-service` | `false` | 🔑 Cache phụ không được kéo sập readiness |
| `startupProbe.failureThreshold` | manifest | 36 (= 180s) | Đủ cho JVM nguội + Flyway |
| `livenessProbe.failureThreshold` | manifest | 6 (= 60s) | Lỏng hơn readiness có chủ đích |
| `monitoring.extraLabels` | `helm/values.yaml` | `{}` | ⚠️ Thiếu `release:` ⇒ Prometheus Operator bỏ qua |

---

## 8. Cách kiểm chứng

```bash
# 1. Prometheus có scrape được cả 4 service không?
curl -s http://localhost:9090/api/v1/targets | jq '.data.activeTargets[] | {instance:.labels.instance, health, lastError}'

# 2. Metric thô của một service
curl -s http://localhost:8083/actuator/prometheus | grep -E '^orders_'

# 3. Ba counter của SagaStuckOrders
curl -s http://localhost:8083/actuator/prometheus | \
  grep -E '^orders_(accepted|filled|rejected_by_matching)_total'

# 4. Tính thử công thức alert
curl -sG http://localhost:9090/api/v1/query \
  --data-urlencode 'query=orders_accepted_total - (orders_filled_total + orders_rejected_by_matching_total)' | jq

# 5. p99 latency pricing
curl -sG http://localhost:9090/api/v1/query \
  --data-urlencode 'query=histogram_quantile(0.99, rate(pricing_e2e_latency_seconds_bucket[1m]))' | jq

# 6. So sánh p50 / p99 / trung bình của REST API (chứng minh trung bình nói dối)
curl -sG http://localhost:9090/api/v1/query --data-urlencode \
  'query=histogram_quantile(0.50, rate(http_server_requests_seconds_bucket{uri="/api/v1/orders"}[5m]))' | jq -r '.data.result[0].value[1]'
curl -sG http://localhost:9090/api/v1/query --data-urlencode \
  'query=histogram_quantile(0.99, rate(http_server_requests_seconds_bucket{uri="/api/v1/orders"}[5m]))' | jq -r '.data.result[0].value[1]'
curl -sG http://localhost:9090/api/v1/query --data-urlencode \
  'query=rate(http_server_requests_seconds_sum{uri="/api/v1/orders"}[5m]) / rate(http_server_requests_seconds_count{uri="/api/v1/orders"}[5m])' | jq -r '.data.result[0].value[1]'

# 7. Alert đang kích hoạt
curl -s http://localhost:9090/api/v1/alerts | jq '.data.alerts[] | {alertname:.labels.alertname, state, activeAt}'

# 8. Lấy traceId từ log rồi tra Zipkin
docker compose logs --tail=50 order-service | grep -oE '\[order-service,[a-f0-9]{16,32},' | head -3
# -> mở http://localhost:9411 và dán traceId vào ô tìm kiếm

# 9. Trace một lệnh cụ thể từ đầu đến cuối
#    (tạm nâng sampling lên 100% để chắc chắn request được trace)
docker compose stop order-service
docker compose run -d -e TRACING_PROBABILITY=1.0 --name order-trace order-service
sleep 20
curl -s -X POST http://localhost:8083/api/v1/orders \
  -H 'Content-Type: application/json' -H 'X-API-KEY: demo-key' \
  -d '{"clientOrderId":"TRACE-01","symbol":"HPG","side":"BUY","price":28500,"quantity":100,"accountId":"ACC001"}'
#    -> Zipkin: tìm service=order-service, xem span OutboxRelay chiếm bao nhiêu %

# 10. Health check phân tầng
curl -s http://localhost:8083/actuator/health | jq
curl -s http://localhost:8083/actuator/health/liveness | jq
curl -s http://localhost:8083/actuator/health/readiness | jq

# 11. Chứng minh Redis không nằm trong readiness
docker compose stop redis
curl -s http://localhost:8083/actuator/health/readiness | jq -r .status    # -> UP
docker compose start redis

# 12. Consumer lag (cho tới khi có kafka_exporter)
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:19092 --describe --all-groups
```

---

## 9. Hạn chế đã biết

| # | Loại | Vị trí | Vấn đề | Hướng sửa |
|:--:|---|---|---|---|
| 13 | **Ops** | `stock.*.DLT` | Alert `DeadLetterTopicGrowing` đo `records_consumed` ⇒ không bao giờ bắn khi không ai consume DLT | `kafka_exporter`, hoặc counter ở `DeadLetterPublishingRecoverer` |
| — | **Ops** | Toàn hệ | **Không có Alertmanager** ⇒ rule được đánh giá nhưng không ai được báo | Thêm Alertmanager + routing + Slack/PagerDuty |
| — | Ops | `monitoring/grafana-dashboards.yml` | Provider trỏ vào `monitoring/dashboards/` nhưng thư mục **không tồn tại** | Thêm dashboard JSON |
| — | Ops | `monitoring/alert-rules.yml:14` | Không có consumer lag metric (comment tự thừa nhận) | `kafka_exporter` hoặc JMX exporter |
| — | Gap | `prometheusrule.yaml` | `SagaStuckOrders` dùng phép trừ counter thô, không bền khi pod restart | `increase(...[10m])` hoặc gauge đếm lệnh treo |
| — | Gap | `SnapshotStore.java:65` | Flush thất bại không có counter | Thêm `snapshot_flush_failed_total` + alert |
| — | Gap | `OrderService.java:112` | Lệch đồng hồ không có counter | Thêm `clock_skew_detected_total` |
| — | Ops | `OrderStatusService.java:44,61` | Một counter `ignored` cho hai tình huống khác hẳn nhau | Tách hai counter |
| — | Ops | `docker-compose.yml` | Zipkin lưu in-memory, mất trace khi restart | Zipkin + Elasticsearch/Cassandra, hoặc Tempo/Jaeger |
| — | Ops | `helm/values.yaml` | `monitoring.extraLabels` rỗng ⇒ Prometheus Operator bỏ qua ServiceMonitor | Đặt `release: <tên-release-của-stack>` |

---

## 10. Câu hỏi phỏng vấn

**Q: Vì sao Prometheus dùng pull model?**

A: Lợi ích lớn nhất là **phát hiện được service chết**: scrape fail ⇒ `up = 0` ⇒ alert `ServiceDown`. Với push model, một service chết đơn giản là ngừng gửi, và collector không phân biệt được "chết" với "đang khoẻ nhưng không có sự kiện nào". Ngoài ra pull giữ cấu hình tập trung ở một file, service không cần biết gì về hệ monitoring, và debug cực dễ — chỉ cần `curl /actuator/prometheus` là thấy đúng thứ Prometheus thấy. Pull không hợp với job ngắn hạn (chết trước khi được scrape) — đó là lý do tồn tại Pushgateway.

**Q: p99 hay trung bình?**

A: p99, và đây không phải sở thích mà là toán học. Nếu 99 request mất 10ms và 1 request mất 5000ms, trung bình là 59,9ms — một con số nói rằng "hệ khoẻ" trong khi có một khách hàng không đặt được lệnh suốt 5 giây. Trung bình bị kéo bởi outlier nhưng vẫn che được outlier; p99 chỉ thẳng vào nó. Chi tiết kỹ thuật đi kèm: muốn tính p99 **toàn hệ** thì phải publish histogram bucket (`percentiles-histogram: true`) để Prometheus tính bằng `histogram_quantile()`. Nếu mỗi pod chỉ publish p99 của riêng nó, ta không ghép lại được — trung bình của các p99 không phải p99.

**Q: Alert nào bạn coi là quan trọng nhất trong hệ này, và vì sao?**

A: `SagaStuckOrders`. Nó đo `orders_accepted − (orders_filled + orders_rejected_by_matching)`, tức số lệnh đang trong saga. Quan trọng nhất vì nó là alert duy nhất đo một thứ **về nghiệp vụ**: mọi pod có thể `UP`, CPU bình thường, không exception nào, mà vẫn có 500 lệnh treo vì relay thất bại im lặng — và khách đang bị giữ tiền cho những lệnh không bao giờ kết thúc. Không alert nào về CPU, memory hay HTTP bắt được điều đó. Tôi cũng phải nói rõ một nhược điểm của công thức hiện tại: nó trừ hai counter thô, mà counter reset khi pod restart, nên sau rolling update hiệu số có thể sai. Tôi sẽ đổi sang `increase(...[10m])` hoặc tốt hơn là một gauge đếm trực tiếp lệnh `ACCEPTED` quá hạn từ bảng.

**Q: Vì sao sampling tracing chỉ 10%, không phải 100%?**

A: Vì chi phí mỗi span là thật: cấp phát object, tạo context, propagate header, serialize, gửi HTTP tới Zipkin. Trên hot path 100.000 tick/giây, 100% sampling nghĩa là 100.000 span/giây — vượt khả năng Zipkin và tạo áp lực GC lớn hơn chính `RiskCalculator`. 10% đủ để thấy *hình dạng* latency và tìm ra chặng nào chậm. Điểm quan trọng: sampling ở đây là head-based, quyết định ở span đầu tiên rồi propagate, nên một trace đã chọn thì **đầy đủ cả 4 service** chứ không bị cắt khúc. Nhược là 95% request không có trace, nên một request chậm mà khách phàn nàn có thể không tìm thấy — giải pháp là tail-based sampling qua OpenTelemetry Collector, giữ lại trace *sau khi* biết nó chậm hoặc lỗi. Khi điều tra sự cố thì tạm nâng lên 1.0.

**Q: Vì sao liveness probe lỏng hơn readiness?**

A: Vì chúng gây ra hai hậu quả hoàn toàn khác nhau: readiness fail chỉ gỡ pod khỏi Service, liveness fail **restart pod**. Khi database ốm, điều đúng là gỡ pod khỏi Service (nó không phục vụ được) nhưng **không** restart — restart không làm DB khoẻ lại, mà còn mất request đang xử lý, gây rebalance consumer group, và nếu mọi pod cùng restart thì khi DB phục hồi sẽ có một cơn bão khởi động đồng loạt. Quy tắc tôi dùng: liveness chỉ nên fail khi restart **thực sự sửa được** vấn đề — deadlock, heap cạn không phục hồi. Dependency bên ngoài ốm thuộc về readiness.

**Q: Bạn thấy thiếu gì trong hệ observability này?**

A: Ba thứ nghiêm trọng. Thứ nhất, **không có Alertmanager** — rule được đánh giá và hiển thị ở UI Prometheus, nhưng không ai được thông báo; về thực chất là có alert mà không có cảnh báo. Thứ hai, **không có consumer lag metric** — comment trong chính file alert thừa nhận "cần kafka_exporter trong production"; hiện ta chỉ thấy triệu chứng (`e2e_latency` cao) chứ không thấy nguyên nhân (lag theo partition), nên không biết là thiếu pod hay là hot partition. Thứ ba, **alert DLT không bao giờ bắn** vì nó đo `records_consumed` mà không ai consume DLT — và alert tồn tại nhưng không chạy còn nguy hiểm hơn không có alert, vì nó tạo cảm giác an toàn giả. Ngoài ra còn thiếu counter cho hai cơ chế nuốt lỗi: flush snapshot thất bại và lệch đồng hồ.

---

## 11. Bảng tổng kết

| Khía cạnh | Lựa chọn | Lý do | Cái giá |
|---|---|---|---|
| Mô hình thu thập | **Pull** (Prometheus) | Biết được service chết (`up == 0`) | Không hợp job ngắn hạn |
| Scrape interval | 10s (local) / 15s (K8s) | Hệ giao dịch cần phát hiện nhanh | Nhiều điểm dữ liệu hơn 4-6× |
| Endpoint actuator | Liệt kê tường minh | 🔑 `*` lộ `/env`, `/heapdump`, `/shutdown` | Phải nhớ thêm khi cần endpoint mới |
| Percentile | p50/p95/p99 + histogram bucket | Trung bình che outlier; bucket cho phép tổng hợp đa pod | Nhiều time series hơn |
| Metrics nghiệp vụ | 15 custom metric | Bắt lỗi mà metrics hạ tầng không thấy | Phải tự viết và tự bảo trì |
| Tracing | Brave → Zipkin, sampling theo môi trường | Thấy latency nằm ở chặng nào | 95% request không có trace ở prod |
| Kafka propagation | `setObservationEnabled(true)` ở cả 8 chỗ | Trace không đứt tại ranh giới Kafka | Phải nhớ bật đủ hai đầu |
| Nối ba trụ cột | traceId trong pattern log | Từ một dòng log tới toàn bộ trace | — |
| Thu thập trong K8s | ServiceMonitor chọn theo label | Thêm service là tự động scrape | ⚠️ Thiếu label `release:` ⇒ bị bỏ qua |
| Alert | 6 rule, có cả alert nghiệp vụ | Đo thứ quan trọng, không chỉ thứ dễ đo | ⚠️ Không có Alertmanager; 1 rule không bắn |
| Health check | liveness/readiness/startup phân tầng | Mỗi probe một vai trò, hậu quả khác nhau | — |
| Redis trong readiness | **Loại ra** | Cache phụ chết không được kéo sập pod khoẻ | Mất tín hiệu Redis trong `/health` |

> **Đọc tiếp:** [Chương 09 — Triển khai Local & Kiểm thử tích hợp](./09-trien-khai-local-va-integration-testing.md).
