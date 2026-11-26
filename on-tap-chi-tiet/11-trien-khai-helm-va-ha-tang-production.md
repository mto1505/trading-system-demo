# Chương 11 — Helm & Hạ Tầng Production

> **Câu hỏi trung tâm:** Kustomize đã làm được một manifest ba môi trường. Vậy Helm thêm gì? Và hạ tầng production khác staging ở đâu — không phải "to hơn", mà **khác bản chất**?
>
> **Tài liệu đối chiếu:** `HELM-DEPLOY.md`, `helm/trading-system/values*.yaml`, `scripts/helm/`.

---

## 1. Nó là gì?

### 1.1. Cấu trúc chart

```
helm/trading-system/
├── Chart.yaml
├── values.yaml                  ← mặc định (≈ k8s/base + trading.env)
├── values-dev.yaml              ← lớp dev
├── values-staging.yaml          ← lớp staging
├── values-staging-local.yaml    ← 🔑 PHỦ LÊN staging (minikube nhỏ)
├── values-prod.yaml             ← lớp prod
└── templates/
    ├── _helpers.tpl             ← 🔑 named template dùng chung
    ├── namespace.yaml
    ├── configmap.yaml  secret.yaml  externalsecret.yaml
    ├── limitrange.yaml  resourcequota.yaml  priorityclass.yaml
    ├── ingress.yaml  issuer.yaml
    ├── networkpolicy-default-deny.yaml
    ├── networkpolicy-apps.yaml  networkpolicy-infra.yaml
    ├── networkpolicy-external.yaml
    ├── apps/                    4 service
    ├── infra/                   postgres, kafka, kafka-topics-job,
    │                            redis, schema-registry, zipkin, prometheus
    ├── keda/scaled-objects.yaml
    └── monitoring/              podmonitor.yaml, service-monitor.yaml
```

```yaml
# helm/trading-system/Chart.yaml
apiVersion: v2
name: trading-system
description: |
  Helm chart for trading-system containing 4 microservices (market-gateway,
  pricing-risk-service, order-service, matching-engine) and supporting infrastructure
  (Kafka, Postgres, Redis, Schema Registry, Zipkin, Monitoring, KEDA).
type: application
version: 0.1.0
appVersion: "1.0.0"
```

### 1.2. Helm thêm gì so với Kustomize

| | Kustomize | **Helm** |
|---|---|---|
| Mô hình | Patch YAML sẵn có | **Template + values** |
| Logic điều kiện | Component bật/tắt | `{{- if .Values.x.enabled }}` |
| Vòng lặp | Không có | `range`, named template |
| Hàm | Không có | `default`, `trunc`, `printf`, `toYaml`, … |
| Theo dõi phiên bản phát hành | Không (Git là nguồn sự thật) | 🔑 **`helm history` + `helm rollback`** |
| Gỡ cài đặt | `kubectl delete -k` (thủ công) | `helm uninstall` (theo dõi đủ resource) |
| Phân phối | Git | **Chart repo / OCI registry** |
| Đọc kết quả | YAML thuần, dễ đọc | Template có thể khó đọc |
| Lồng nhiều lớp values | 1 overlay | 🔑 **Nhiều `-f` chồng lên nhau** |

🔑 **Hai thứ Helm có mà Kustomize không có**, và cả hai đều quan trọng ở production:

1. **Lịch sử phát hành trong cluster.** Helm lưu manifest của từng revision vào Secret trong namespace. `helm rollback trading 5` khôi phục **chính xác** manifest của revision 5 — không cần Git, không cần ai nhớ commit nào.
2. **Values chồng lớp.** `-f values-staging.yaml -f values-staging-local.yaml` — file sau đè file trước, cho phép tách "hình dạng môi trường" khỏi "điều chỉnh cho máy cụ thể". Xem mục 4.

---

## 2. Named template — tránh lặp lại 4 lần

```go
{{/* helm/trading-system/templates/_helpers.tpl */}}

{{/*
Common labels — Tương đương labels trong base kustomization (includeSelectors: false).
KHÔNG bao giờ đưa vào selector của Deployment để tránh lỗi immutable selector.
*/}}
{{- define "trading.commonLabels" -}}
app.kubernetes.io/part-of: {{ .Values.global.partOf }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end -}}

{{/*
Selector labels — Chỉ chứa tên service để bảo đảm tính bất biến (immutable).
*/}}
{{- define "trading.selectorLabels" -}}
app.kubernetes.io/name: {{ .name }}
{{- end -}}
```

🔑 **Tách `commonLabels` khỏi `selectorLabels` là chi tiết quan trọng nhất của file này.**

`spec.selector` của Deployment là **immutable** — không sửa được sau khi tạo. Nếu `helm.sh/chart: trading-system-0.1.0` lọt vào selector, thì bump `Chart.version` lên `0.1.1` sẽ làm selector đổi ⇒ `helm upgrade` **thất bại** với lỗi:

```
field is immutable: spec.selector
```

Và cách duy nhất để thoát là `helm uninstall` rồi `install` lại — tức là **downtime**. Đây là lỗi kinh điển với Helm chart tự viết. Kustomize giải cùng vấn đề này bằng `includeSelectors: false`.

```go
{{/*
Image string: nếu digest có giá trị thì dùng @digest (prod), ngược lại dùng :tag (dev/staging).
*/}}
{{- define "trading.image" -}}
{{- if .digest -}}
{{ .repository }}@{{ .digest }}
{{- else -}}
{{ .repository }}:{{ .tag | default "latest" }}
{{- end -}}
{{- end -}}
```

💡 Một template, hai hành vi: prod truyền `digest` ⇒ ra `repo@sha256:...`; dev/staging để `digest: ""` ⇒ ra `repo:tag`. Logic "chọn digest hay tag" nằm **một chỗ** thay vì lặp ở 4 file service.

```go
{{- define "trading.appPodSecurityContext" -}}
runAsNonRoot: true
runAsUser: 10001
runAsGroup: 10001
fsGroup: 10001
seccompProfile:
  type: RuntimeDefault
{{- end -}}
```

💡 Hardening được định nghĩa **một lần**, áp cho cả 4 service. Với Kustomize, cùng 6 dòng này lặp lại trong 4 file manifest — và nếu sửa một chỗ quên ba chỗ kia thì không gì báo.

---

## 3. Values phân tầng

### 3.1. `values.yaml` — mặc định an toàn

```yaml
# helm/trading-system/values.yaml
# =============================================================================
# values.yaml — Giá trị mặc định (Base)
# Tương đương k8s/base + config/trading.env trong cấu trúc Kustomize.
# Overlay-specific values nằm ở values-{dev,staging,prod}.yaml.
# =============================================================================

postgres:
  enabled: true          # false ở prod → dùng RDS
kafka:
  enabled: true          # false ở prod → dùng managed Kafka
...
monitoring:
  enabled: false         # staging/prod bật
keda:
  enabled: false         # staging/prod bật
externalSecrets:
  enabled: false
priorityClass:
  enabled: false
prometheus:
  inCluster:
    enabled: false       # dev bật, staging/prod dùng kube-prometheus-stack
```

🔑 **Mặc định là "môi trường nhỏ nhất, an toàn nhất"**: hạ tầng in-cluster bật (để `helm install` trần cũng chạy được), còn mọi tính năng cần operator bên ngoài (KEDA, Prometheus Operator, ESO, cert-manager) đều **tắt**. Nguyên tắc: `helm install` không có `-f` nào cũng phải ra một hệ chạy được, không phải một đống `CrashLoopBackOff` vì thiếu CRD.

### 3.2. Ba cảnh báo về heap — bài học vận hành đắt giá

```yaml
kafka:
  # apache/kafka, KHONG phai bitnami: kafka-topics-job.yaml goi /opt/kafka/bin/*.sh
  # (layout cua apache). Bitnami dat script o /opt/bitnami/kafka/bin va tu 2025
  # da go image public tren Docker Hub -> ErrImagePull "not found".
  # Dong bo voi k8s/base/infra/kafka.yaml.
  image: apache/kafka:3.7.0
  # Heap cua broker. PHAI thap hon resources.limits.memory it nhat ~250Mi.
  # Ha limit ma quen ha dong nay = OOMKilled.
  heapOpts: "-Xms512m -Xmx1g"
  resources:
    requests: { cpu: 200m, memory: 512Mi }
    limits: { memory: 1Gi }
```

```yaml
schemaRegistry:
  image: confluentinc/cp-schema-registry:7.7.0
  # Mac dinh cua image la -Xmx512M. Ha limits.memory xuong duoi do ma quen
  # dong nay = OOMKilled luc khoi dong.
  heapOpts: "-Xmx512m"
```

🔑 **Đây là ba loại thành phần với ba hành vi heap khác nhau** — và `values-staging-local.yaml` tóm tắt rất rõ:

```yaml
# -----------------------------------------------------------------------------
# QUY TẮC VÀNG KHI HẠ limits.memory
# -----------------------------------------------------------------------------
# Mọi service ở đây đều là JVM. Hạ limit mà quên hạ heap = OOMKilled, và
# Kubernetes chỉ báo `Reason: OOMKilled` chứ không nói heap là thủ phạm.
#
#   4 service Java : Dockerfile đặt -XX:MaxRAMPercentage=75
#                    -> heap TỰ bám theo limit. Chỉ cần sửa limit.
#   kafka          : KAFKA_HEAP_OPTS — PHẢI sửa tay cùng lúc (kafka.heapOpts).
#   schema-registry: mặc định image là -Xmx512M — PHẢI sửa tay (schemaRegistry.heapOpts).
```

| Thành phần | Heap theo | Hạ `limits.memory` thì |
|---|---|---|
| 4 service Java | `-XX:MaxRAMPercentage=75` (Dockerfile) | ✅ Heap tự co theo |
| Kafka | `KAFKA_HEAP_OPTS` cố định | ⚠️ **Phải sửa `kafka.heapOpts` cùng lúc** |
| Schema Registry | `-Xmx512M` mặc định của image | ⚠️ **Phải sửa `schemaRegistry.heapOpts`** |

🔑 Và điểm đau nhất: *"Kubernetes chỉ báo `Reason: OOMKilled` chứ không nói heap là thủ phạm."* Bạn thấy pod chết, `kubectl describe` nói OOMKilled, và không có manh mối nào chỉ rằng nguyên nhân là một dòng `-Xmx` quên sửa.

💡 Ghi chú về **image Kafka** cũng là một bài học thật: `bitnami/kafka` đặt script ở `/opt/bitnami/kafka/bin` còn `apache/kafka` ở `/opt/kafka/bin`. Job `kafka-topics-init` gọi `/opt/kafka/bin/kafka-topics.sh`, nên đổi image sang bitnami sẽ làm Job fail. Và từ 2025 Bitnami đã gỡ image public trên Docker Hub ⇒ `ErrImagePull "not found"`.

### 3.3. Cạm bẫy template: `nil pointer` do khoá không tồn tại

```yaml
# helm/trading-system/values.yaml
    # cert-manager Issuer tự ký, dùng cho môi trường KHÔNG ra được Internet
    # (local/staging nội bộ) — nơi ACME/Let's Encrypt không chạy được.
    #
    # PHẢI khai báo ở đây dù mặc định là false: templates/ingress.yaml đọc
    # .selfSignedIssuer.enabled, và nếu khoá này không tồn tại thì render CHẾT
    # ngay khi tls.enabled=true:
    #     nil pointer evaluating interface {}.enabled
    # (dev và local thoát được chỉ vì tls.enabled=false làm `and` short-circuit
    #  — một sự may mắn, không phải thiết kế.)
    selfSignedIssuer:
      enabled: false
```

🔑 **Đây là lớp cạm bẫy đặc trưng của Helm mà Kustomize không có.** Go template truy cập một khoá không tồn tại trên map lồng nhau sẽ ném `nil pointer evaluating interface {}.enabled` — và lỗi chỉ xuất hiện khi **một nhánh điều kiện khác** được bật. Ở đây: `tls.enabled=false` làm `and` short-circuit nên không ai chạm tới `selfSignedIssuer.enabled`; bật TLS lên là render chết.

💡 Comment thừa nhận thẳng: *"một sự may mắn, không phải thiết kế"*. Cách phòng đúng:
- Khai báo **mọi** khoá mà template đọc trong `values.yaml`, kể cả khi mặc định là `false`.
- Hoặc dùng `(.Values.x | default dict).enabled` / `dig` để an toàn với khoá thiếu.
- Và quan trọng nhất: `helm template` với **mọi tổ hợp values** trong CI.

### 3.4. Ingress route sinh ra hai thứ

```yaml
# helm/trading-system/values.yaml
  # ---------------------------------------------------------------------------
  # Route: moi muc sinh RA HAI thu — mot path trong Ingress VA mot NetworkPolicy
  # cho phep nginx toi dung service do. Them route o day la du, khong phai sua
  # cho nao khac.
  #
  # `service` phai la mot trong 4 ten co trong trading.servicePorts
  # (_helpers.tpl), neu khong render se chet voi loi index nil.
  #
  # pathType Prefix: /api/v1/orders cung bat /api/v1/orders/{clientOrderId}.
  # ---------------------------------------------------------------------------
  routes:
    - path: /api/v1/orders
      service: order-service
    - path: /api/v1/marketdata
      service: pricing-risk-service
```

🔑 **Một mục trong `routes` sinh ra hai resource khác nhau**: một path trong Ingress **và** một rule NetworkPolicy cho phép ingress-nginx tới đúng service đó. Đây là năng lực mà Kustomize không có — nó đòi hỏi vòng lặp qua một danh sách và sinh resource ở hai nơi.

Giá trị thực tế: **thêm một API mới chỉ cần sửa một chỗ.** Với Kustomize, phải nhớ sửa cả `ingress.yaml` lẫn `apps-networkpolicy.yaml` — và quên cái thứ hai sẽ gây lỗi `502 Bad Gateway` rất khó chẩn đoán (Ingress route đúng, pod khoẻ, nhưng NetworkPolicy chặn).

💡 Lưu ý về `pathType: Prefix` — `/api/v1/orders` cũng bắt `/api/v1/orders/{clientOrderId}`. Nếu dùng `Exact` thì endpoint tra cứu sẽ không route được.

⚠️ So với `k8s/base/apps/ingress.yaml`, bản Helm mặc định **thiếu** hai route: `/api/v1/executions` (matching-engine) và `/api/v1/ticks` (market-gateway). Với `/api/v1/ticks` thì đó là đúng ở production (nó là công cụ dev — overlay Kustomize prod cũng gỡ nó đi), nhưng `/api/v1/executions` bị thiếu là một khác biệt không chủ đích giữa hai bộ manifest.

---

## 4. 🔑 Values chồng lớp — `values-staging-local.yaml`

Đây là file đáng học nhất trong toàn bộ thư mục `helm/`, và là thứ Kustomize làm khó hơn hẳn.

```yaml
# =============================================================================
# values-staging-local.yaml — LỚP PHỦ LÊN values-staging.yaml, KHÔNG thay thế nó.
#
# Mục tiêu: chạy ĐÚNG hình dạng staging (KEDA + monitoring + NetworkPolicy) trên
# một node minikube nhỏ, để THỬ NGHIỆM — không phải để đo hiệu năng.
#
# Thứ tự -f có ý nghĩa, file sau đè file trước:
#   helm upgrade --install trading ./helm/trading-system \
#     -f ./helm/trading-system/values-staging.yaml \
#     -f ./helm/trading-system/values-staging-local.yaml \
#     -n trading-staging
```

🔑 **Ý tưởng:** tách **hình dạng môi trường** (`values-staging.yaml`: KEDA bật, monitoring bật, NetworkPolicy bật) khỏi **điều chỉnh cho một máy cụ thể** (`values-staging-local.yaml`: hạ resource cho vừa minikube). Nhờ vậy hình dạng không bị sao chép hai lần và không thể lệch nhau.

### 4.1. Bài học về ngân sách RAM

```yaml
# -----------------------------------------------------------------------------
# NGÂN SÁCH RAM — đọc trước khi sửa bất kỳ con số nào
# -----------------------------------------------------------------------------
# Node minikube của máy này bị cgroup chặn ở 6144 Mi. CẢNH BÁO: `kubectl describe
# node` KHÔNG nói thật — kubelet đọc /proc/meminfo nên báo RAM của HOST (15.6 GiB)
# cho scheduler, trong khi cgroup giết ở 6 GiB. Scheduler sẽ vui vẻ xếp pod cho
# đủ 16 GiB vào cái hộp 6 GiB rồi cả node thrashing. Con số THẬT lấy bằng:
#
#     docker stats minikube --no-stream
#
# Chi phí cố định đo được trên cluster này:
#     kube-system (gồm calico, metrics-server)   ~1100 Mi
#     kube-prometheus-stack                       ~930 Mi
#     ingress-nginx + KEDA + dashboard            ~240 Mi
#     -------------------------------------------------
#     còn lại cho ứng dụng                       ~3800 Mi
#
# Cấu hình dưới đây đặt tổng requests ~1.8 GiB, tổng limits ~3.3 GiB → vừa khít.
#
# LƯU Ý: node này KHÔNG đủ chỗ cho hai stack cùng lúc. Namespace trading-dev
# (Kustomize, `scripts\deploy-dev.ps1`) ngốn ~3000 Mi — dựng lại dev trong khi
# staging đang chạy là cả node thrashing. Chạy một trong hai, không chạy cả hai.
```

🔑 **"`kubectl describe node` KHÔNG nói thật"** là một phát hiện vận hành rất giá trị, và nó đúng cho mọi minikube chạy trên Docker Desktop:

- kubelet đọc `/proc/meminfo` ⇒ thấy RAM của **host** (15,6 GiB).
- Nhưng container `minikube` bị **cgroup** chặn ở 6 GiB.
- Scheduler tin kubelet ⇒ xếp pod cho đủ 16 GiB vào cái hộp 6 GiB ⇒ cả node thrashing, pod bị OOMKilled ngẫu nhiên, và `describe node` vẫn báo "đủ chỗ".

Con số thật chỉ lấy được từ phía Docker: `docker stats minikube --no-stream`.

💡 Cách tiếp cận trong file này — **liệt kê chi phí cố định rồi tính phần còn lại** — là cách đúng để lập kế hoạch tài nguyên. Không phải "đặt đại rồi xem có chạy không", mà là một ngân sách có số liệu đo được.

---

## 5. Hạ tầng Production — khác bản chất, không chỉ khác quy mô

### 5.1. Bảng so sánh ba môi trường

| | dev | staging | **prod** |
|---|---|---|---|
| PostgreSQL | StatefulSet in-cluster | StatefulSet in-cluster | **AWS RDS** (`postgres.enabled: false`) |
| Kafka | StatefulSet in-cluster | StatefulSet in-cluster | **Managed (MSK/Confluent)** (`kafka.enabled: false`) |
| `kafkaTopicsJob` | bật | bật | **tắt** (managed tự tạo topic) |
| Redis | in-cluster | in-cluster | in-cluster (cache, mất được) |
| Schema Registry | in-cluster | in-cluster | in-cluster |
| Prometheus | tối giản in-cluster | kube-prometheus-stack | kube-prometheus-stack |
| KEDA | tắt | **bật** | **bật** |
| Secret | `secrets.create: true` | `secrets.create: true` | **ESO** (`secrets.create: false`) |
| Image | `:dev` | `:main` | **`@sha256:`** |
| TLS | tắt | tự ký (cert-manager) | **Let's Encrypt** |
| PriorityClass | tắt | tắt | **`trading-critical`** |
| `KAFKA_PARTITIONS` | 3 | 6 | **12** |
| `KAFKA_TOPIC_REPLICAS` | 1 | 1 | **3** |
| `TRACING_PROBABILITY` | 1.0 | 0.3 | **0.05** |
| `SIMULATED_FEED` | true | true | **false** |
| `topologySpread` | ScheduleAnyway | ScheduleAnyway | **DoNotSchedule** |

### 5.2. 🔑 Vì sao prod không chạy Postgres/Kafka trong cluster

```yaml
# k8s/components/external-postgres/kustomization.yaml (cùng lý lẽ áp cho Helm)
# Prod thật chạy database NGOÀI cluster (RDS / Cloud SQL / CloudNativePG ở
# namespace riêng): backup, PITR, failover, patch version là việc của managed
# service chứ không phải của một StatefulSet 1 replica.
```

| Năng lực | StatefulSet 1 replica | Managed (RDS/MSK) |
|---|---|---|
| Backup tự động | ❌ Phải tự viết CronJob | ✅ |
| Point-in-time recovery | ❌ | ✅ |
| Failover tự động | ❌ **Pod chết = DB chết** | ✅ Multi-AZ |
| Vá bản vá bảo mật | ❌ Phải tự nâng image + test | ✅ Cửa sổ bảo trì |
| Giám sát chuyên sâu | ❌ | ✅ Performance Insights / enhanced monitoring |
| Chi phí | Rẻ | Đắt |
| Trách nhiệm khi mất dữ liệu | **Của bạn** | Nhà cung cấp (trong SLA) |

🔑 Lập luận quyết định không phải về hiệu năng mà về **trách nhiệm**: một StatefulSet 1 replica không có backup, không có PITR, không có failover. Mất dữ liệu giao dịch là mất không lấy lại được, và trong tài chính đó là vấn đề pháp lý chứ không chỉ kỹ thuật.

💡 Redis thì **vẫn in-cluster ở prod** — nhất quán với triết lý xuyên suốt: Redis là cache tăng tốc, mất dữ liệu không ảnh hưởng tính đúng đắn (xem [chương 06](./06-idempotency-va-consumer-deduplication.md)). Không cần trả tiền cho ElastiCache để lưu một cache 10 phút.

### 5.3. External Secrets Operator

```yaml
# helm/trading-system/templates/externalsecret.yaml
{{- if .Values.externalSecrets.enabled }}
apiVersion: external-secrets.io/v1beta1
kind: ExternalSecret
metadata:
  name: trading-secrets
spec:
  refreshInterval: {{ .Values.externalSecrets.refreshInterval }}
  secretStoreRef:
    name: {{ .Values.externalSecrets.secretStoreRef.name }}
    kind: {{ .Values.externalSecrets.secretStoreRef.kind }}
  target:
    name: trading-secrets
    creationPolicy: Owner
  data:
    - secretKey: DB_USER
      remoteRef:
        key: {{ .Values.externalSecrets.data.dbKey }}
        property: username
    - secretKey: DB_PASSWORD
      remoteRef:
        key: {{ .Values.externalSecrets.data.dbKey }}
        property: password
    - secretKey: ORDER_API_KEY
      remoteRef:
        key: {{ .Values.externalSecrets.data.apiKey }}
        property: order-api-key
{{- end }}
```

```yaml
# values-prod.yaml
externalSecrets:
  enabled: true
  refreshInterval: 1h
  secretStoreRef:
    name: aws-secrets-manager
    kind: ClusterSecretStore
  data:
    dbKey: trading/prod/db
    apiKey: trading/prod/api

secrets:
  create: false     # 🔑 tắt Secret tĩnh, nhường cho ESO
```

**Cách hoạt động:** ESO đọc secret từ AWS Secrets Manager (hoặc Vault, GCP Secret Manager, Azure Key Vault) và **dựng ra một Kubernetes Secret** trong cluster. Ứng dụng không biết gì về ESO — nó vẫn đọc `secretKeyRef` như bình thường.

Lợi ích:
- **Mật khẩu không bao giờ nằm trong Git**, kể cả dạng mã hoá.
- **Xoay mật khẩu ở một nơi** (AWS), ESO tự đồng bộ trong `refreshInterval`.
- **Audit trail** của AWS: ai đọc secret nào, lúc nào.
- **IAM** kiểm soát quyền truy cập.

⚠️ **Gap #19 — ESO cập nhật secret nhưng không pod nào restart.** Secret do ESO dựng **không có hash** (nó không qua `secretGenerator`/`configmap` template có checksum). ESO cập nhật xong thì Secret trong cluster đổi, nhưng `env.valueFrom.secretKeyRef` **chỉ đọc một lần lúc container start** ⇒ ứng dụng vẫn dùng mật khẩu cũ tới lần restart ngẫu nhiên sau đó.

Ba cách xử lý, theo mức độ triệt để:

| Cách | Cài đặt | Nhận xét |
|---|---|---|
| `kubectl rollout restart` thủ công | Đã vào runbook `k8s/README.md` | Phải nhớ |
| **Reloader (stakater)** | Annotation `reloader.stakater.com/auto: "true"` | Watch Secret/ConfigMap và tự restart |
| **Mount Secret thành volume** | `volumeMounts` thay vì `env` | kubelet tự cập nhật file (độ trễ tới ~1 phút); ứng dụng phải đọc lại file |

💡 Cách thứ ba là đúng nhất về nguyên lý nhưng đòi hỏi ứng dụng hỗ trợ đọc lại cấu hình — Spring Cloud Kubernetes có `reload` cho việc này.

### 5.4. TLS — hai nguồn chứng chỉ cho hai hoàn cảnh

```yaml
# values-prod.yaml
ingress:
  host: trading.example.com
  tls:
    enabled: true
    secretName: trading-tls
  annotations:
    cert-manager.io/cluster-issuer: letsencrypt-prod
```

```yaml
# values.yaml — cho môi trường KHÔNG ra được Internet
    selfSignedIssuer:
      enabled: false
      name: selfsigned-issuer
      # true  -> tạo CA gốc tự ký rồi dùng CA đó ký cert cho Ingress
      #          (một CA duy nhất để tin tưởng, giống CA nội bộ thật)
      # false -> Issuer tự ký thẳng cert cho Ingress (mỗi cert là một gốc riêng)
      useCA: true
      # CA TỰ MANG: tên Secret kubernetes.io/tls đã tạo sẵn (cùng namespace).
      # Đặt giá trị này thì chart BỎ QUA useCA và không sinh CA nữa — nó chỉ tạo
      # Issuer kind: ca trỏ vào Secret đó.
      #
      # Vì sao nên dùng: CA do cert-manager sinh gắn với vòng đời release; gỡ
      # chart rồi cài lại là CA mới, thumbprint mới, và cái đã cài vào trust
      # store của máy thành vô dụng. CA tự mang sống độc lập với Helm.
      # Tạo bằng: scripts\helm\New-LocalCA.ps1
      existingCaSecret: ""
      caCommonName: trading-local-ca
      caDuration: 8760h    # 1 năm
      certDuration: 2160h  # 90 ngày — giống Let's Encrypt để lộ sớm lỗi gia hạn
      certRenewBefore: 360h
```

🔑 **Ba chi tiết rất đáng học trong khối này:**

**(a) `useCA: true` thay vì tự ký thẳng.** Với `false`, mỗi chứng chỉ là một gốc tin cậy riêng — mỗi lần gia hạn là phải cài lại vào trust store. Với `true`, cert-manager tạo **một CA gốc** rồi dùng nó ký cert cho Ingress ⇒ chỉ cần tin một CA, giống mô hình CA nội bộ thật của doanh nghiệp.

**(b) `existingCaSecret` — CA tự mang.** Lý lẽ trong comment rất sắc: *"CA do cert-manager sinh gắn với vòng đời release; gỡ chart rồi cài lại là CA mới, thumbprint mới, và cái đã cài vào trust store của máy thành vô dụng."* Một CA tự tạo bên ngoài (bằng `scripts/helm/New-LocalCA.ps1`) sống độc lập với Helm — `helm uninstall` không giết nó.

**(c) `certDuration: 2160h` (90 ngày) — "giống Let's Encrypt để lộ sớm lỗi gia hạn".** 🔑 Đây là một ý tưởng xuất sắc: nếu đặt chứng chỉ nội bộ sống 10 năm thì cơ chế **gia hạn tự động không bao giờ được kiểm chứng** — và khi lên production với Let's Encrypt (90 ngày), lỗi gia hạn sẽ xuất hiện lần đầu vào ngày thứ 60 của production. Đặt 90 ngày ở local nghĩa là cơ chế gia hạn được thực thi thật, sớm, ở nơi hỏng không ai chết.

Cùng tinh thần đó là nguyên tắc chung: **làm cho môi trường thấp cũng phải chạy qua đúng những cơ chế mà production sẽ dựa vào.**

### 5.5. NetworkPolicy cho hạ tầng ngoài cluster

```yaml
# values-prod.yaml
networkPolicy:
  enabled: true
  externalDb:
    enabled: true
    cidr: 10.100.0.0/16
  externalKafka:
    enabled: true
    cidr: 10.200.0.0/16
  externalKafkaPlatform:
    enabled: true
    cidr: 10.200.0.0/16
```

Khi Postgres/Kafka nằm ngoài cluster, `podSelector` không dùng được nữa — phải mở egress theo **CIDR**.

⚠️ `10.100.0.0/16` là 65.536 địa chỉ. Chặt hơn thì nên dùng CIDR của đúng subnet RDS, hoặc tốt nhất là `/32` của endpoint. Lưu ý thêm: NetworkPolicy **không hỗ trợ tên DNS** — và endpoint RDS là một CNAME có thể đổi IP khi failover. Đó là lý do phải dùng CIDR của subnet thay vì IP cụ thể, và là một hạn chế thật của NetworkPolicy (Cilium `CiliumNetworkPolicy` có `toFQDNs` giải quyết được).

💡 `externalKafkaPlatform` là một rule riêng — nhiều managed Kafka (Confluent Cloud, MSK với IAM) cần thêm kết nối tới endpoint quản trị/xác thực ngoài cổng broker.

### 5.6. PriorityClass

```yaml
# k8s/overlays/prod/priorityclass.yaml (Helm: priorityClass.enabled)
# Khi node thiếu chỗ, kube-scheduler phải biết cái gì được hy sinh trước.
# Luồng lệnh của sàn không bao giờ là thứ bị preempt để nhường cho một job batch.
apiVersion: scheduling.k8s.io/v1
kind: PriorityClass
metadata:
  name: trading-critical
value: 1000000
globalDefault: false
preemptionPolicy: PreemptLowerPriority
description: "Luong giao dich - uu tien cao nhat trong cluster"
```

💡 `globalDefault: false` — chỉ pod khai tường minh `priorityClassName: trading-critical` mới được ưu tiên. Đặt `true` sẽ làm **mọi** pod trong cluster thành critical, tức là không pod nào critical.

---

## 6. Vòng đời phát hành

### 6.1. Lệnh

```bash
# Kiểm tra trước (không apply)
helm lint ./helm/trading-system -f ./helm/trading-system/values-prod.yaml
helm template trading ./helm/trading-system -f ./helm/trading-system/values-prod.yaml > /tmp/prod.yaml

# Dry-run với API server thật (bắt lỗi mà template không bắt được: CRD thiếu, quota, …)
helm upgrade --install trading ./helm/trading-system \
  -f ./helm/trading-system/values-prod.yaml \
  -n trading-prod --create-namespace --dry-run=server

# Deploy thật — CI ghi đè digest
helm upgrade --install trading ./helm/trading-system \
  -f ./helm/trading-system/values-prod.yaml \
  -n trading-prod --create-namespace \
  --set marketGateway.image.digest=sha256:abc... \
  --set pricingRiskService.image.digest=sha256:def... \
  --set orderService.image.digest=sha256:123... \
  --set matchingEngine.image.digest=sha256:456... \
  --atomic --timeout 10m

# Staging local: hai lớp values chồng lên nhau
helm upgrade --install trading ./helm/trading-system \
  -f ./helm/trading-system/values-staging.yaml \
  -f ./helm/trading-system/values-staging-local.yaml \
  -n trading-staging --create-namespace
```

🔑 **`--atomic` là cờ quan trọng nhất cho production.** Nếu `helm upgrade` thất bại hoặc quá `--timeout`, Helm **tự động rollback** về revision trước. Không có nó, một upgrade hỏng để lại cluster ở trạng thái lai — một nửa resource mới, một nửa cũ — và phải dọn tay.

💡 `--dry-run=server` khác `--dry-run` (client): nó gửi manifest tới API server để validate thật, nên bắt được CRD thiếu, vi phạm ResourceQuota, admission webhook từ chối — những thứ `helm template` không thấy.

### 6.2. 🔑 Rollback tức thì

```bash
helm history trading -n trading-prod
# REVISION  UPDATED       STATUS      CHART                 APP VERSION  DESCRIPTION
# 1         2026-11-20..  superseded  trading-system-0.1.0  1.0.0        Install complete
# 2         2026-11-22..  superseded  trading-system-0.1.0  1.0.0        Upgrade complete
# 3         2026-11-25..  deployed    trading-system-0.1.0  1.0.0        Upgrade complete

helm rollback trading 2 -n trading-prod        # về revision 2
helm rollback trading -n trading-prod          # về revision ngay trước
```

**Helm lưu manifest đầy đủ của từng revision vào một Secret trong namespace** (`sh.helm.release.v1.trading.v3`). Nghĩa là rollback khôi phục **chính xác** trạng thái đã deploy — không cần Git, không cần ai nhớ commit nào, không cần build lại image.

| | Kustomize | **Helm** |
|---|---|---|
| Rollback | `git revert` + rebuild + `kubectl apply` | **`helm rollback trading 2`** |
| Nguồn sự thật | Git | Secret trong cluster |
| Cần Git access? | Có | Không |
| Khôi phục đúng trạng thái đã chạy? | Chỉ nếu Git khớp với thực tế | ✅ Luôn luôn |

💡 Điểm đánh đổi: Helm rollback khôi phục trạng thái *đã deploy*, có thể **khác** với Git nếu ai đó đã `kubectl edit` thủ công. Với GitOps thuần (Argo CD + Kustomize), Git luôn là nguồn sự thật và drift được phát hiện tự động. Hai triết lý khác nhau, và nhiều đội dùng cả hai: Helm để đóng gói, Argo CD để đồng bộ.

⚠️ `helm rollback` **không** rollback được những thứ nằm ngoài quản lý của Helm: dữ liệu đã migrate (Flyway đã chạy `V3` thì rollback chart không undo schema), message đã publish lên Kafka, row đã ghi vào DB. **Rollback chart ≠ rollback hệ thống.**

### 6.3. Script giám sát

```bash
# scripts/helm/monitor-trading.sh
```

Repo có sẵn script này để theo dõi trạng thái sau deploy. Cùng thư mục còn có `New-LocalCA.ps1` (tạo CA tự mang, xem mục 5.4) và `deploy-dev.ps1`.

---

## 7. Helm hay Kustomize — chọn thế nào

🔑 **Repo này có CẢ HAI, và đó là một quyết định cần giải thích.**

| | Kustomize (`k8s/`) | Helm (`helm/`) |
|---|---|---|
| Phù hợp nhất với | GitOps (Argo CD, Flux) | Đóng gói & phân phối, rollback nhanh |
| Đọc kết quả | YAML thuần, `diff` được | Template, phải `helm template` mới thấy |
| Logic | Component bật/tắt | `if`, `range`, hàm |
| Lồng nhiều lớp | 1 overlay | 🔑 Nhiều `-f` chồng nhau |
| Lịch sử trong cluster | Không | 🔑 `helm history` |
| Phân phối cho bên thứ ba | Khó | 🔑 Chart repo / OCI |
| Cạm bẫy | Strategic merge vs JSON patch | 🔑 `nil pointer`, selector immutable |

**Khi nào chọn Kustomize:** đội dùng GitOps, muốn `kubectl diff -k` thấy chính xác thứ sắp đổi, không cần phân phối chart ra ngoài.

**Khi nào chọn Helm:** cần rollback tức thì không qua Git, cần phân phối chart cho đội khác hoặc khách hàng, cần logic điều kiện phức tạp (như `routes` sinh hai resource), cần values chồng nhiều lớp.

💡 **Dùng cả hai cũng hợp lệ** — và khá phổ biến: Helm để render, Kustomize để patch output của Helm (Argo CD hỗ trợ `kustomize.buildOptions` với `--enable-helm`). Nhưng với repo này, **duy trì song song hai bộ manifest cho cùng một hệ thống là một nợ kỹ thuật thật**: mọi thay đổi phải làm hai lần, và chúng *đã* lệch nhau (ví dụ Helm thiếu route `/api/v1/executions`, mục 3.4).

---

## 8. Đánh đổi

### 8.1. Ưu

1. **Named template** loại bỏ lặp lại giữa 4 service.
2. **Values chồng lớp** tách hình dạng môi trường khỏi điều chỉnh máy cụ thể.
3. **`helm rollback`** khôi phục chính xác trạng thái đã deploy, không cần Git.
4. **`--atomic`** tự rollback khi upgrade hỏng.
5. **Một mục `routes` sinh hai resource** — thêm API chỉ sửa một chỗ.
6. **Logic điều kiện** cho bật/tắt từng thành phần hạ tầng.
7. **Tài liệu nội tuyến rất chất lượng** — ba cảnh báo heap, ngân sách RAM, cạm bẫy `nil pointer`.
8. **Mặc định an toàn** — `helm install` trần cũng chạy được.

### 8.2. Nhược

1. ⚠️ **Duy trì song song với Kustomize** — mọi thay đổi làm hai lần, và **đã lệch nhau**.
2. ⚠️ **Cạm bẫy `nil pointer`** khi khoá values không tồn tại; chỉ lộ ra khi bật nhánh điều kiện khác.
3. ⚠️ **Selector immutable** — label lọt vào selector là `helm upgrade` fail vĩnh viễn.
4. ⚠️ **Gap #19:** ESO không trigger restart.
5. ⚠️ **Helm thiếu route `/api/v1/executions`** so với Kustomize.
6. **Template khó đọc hơn YAML thuần** — `helm template` mới biết chính xác ra gì.
7. **Rollback chart ≠ rollback hệ thống** — không undo được Flyway migration, message Kafka, dữ liệu DB.
8. **CIDR trong NetworkPolicy quá rộng** (`/16`) và không hỗ trợ tên DNS.
9. **Digest placeholder `sha256:0000...`** trong values — chạy thật sẽ `ErrImagePull` nếu CI không ghi đè.

---

## 9. Cách kiểm chứng

```bash
CHART=./helm/trading-system

# === Kiểm tra tĩnh ===
helm lint $CHART -f $CHART/values-dev.yaml
helm lint $CHART -f $CHART/values-staging.yaml
helm lint $CHART -f $CHART/values-prod.yaml

# Render từng môi trường
helm template trading $CHART -f $CHART/values-dev.yaml     > /tmp/helm-dev.yaml
helm template trading $CHART -f $CHART/values-prod.yaml    > /tmp/helm-prod.yaml
helm template trading $CHART \
  -f $CHART/values-staging.yaml -f $CHART/values-staging-local.yaml > /tmp/helm-sl.yaml

# Prod có dùng digest không?
grep -E 'image: ghcr\.io/' /tmp/helm-prod.yaml

# Prod có GỠ Postgres/Kafka in-cluster không?
grep -E 'kind: StatefulSet' /tmp/helm-prod.yaml      # -> rỗng

# Prod có ExternalSecret và PriorityClass không?
grep -E 'kind: (ExternalSecret|PriorityClass)' /tmp/helm-prod.yaml

# Prod có Secret tĩnh không? (phải KHÔNG có)
grep -A3 'kind: Secret' /tmp/helm-prod.yaml

# Đếm resource theo loại
grep '^kind:' /tmp/helm-prod.yaml | sort | uniq -c | sort -rn

# === So sánh Helm vs Kustomize — tìm chỗ đã lệch ===
kustomize build k8s/overlays/prod > /tmp/kust-prod.yaml
diff <(grep '^kind:' /tmp/helm-prod.yaml | sort | uniq -c) \
     <(grep '^kind:' /tmp/kust-prod.yaml | sort | uniq -c)

# Cụ thể: Helm có thiếu route executions không?
grep -A2 'path: /api/v1' /tmp/helm-prod.yaml
grep -A2 'path: /api/v1' /tmp/kust-prod.yaml

# === Kiểm chứng cạm bẫy nil pointer ===
helm template trading $CHART --set ingress.tls.enabled=true \
  --set-json 'ingress.tls.selfSignedIssuer=null' 2>&1 | head -5
#  -> nil pointer evaluating interface {}.enabled

# === Dry-run với API server ===
helm upgrade --install trading $CHART -f $CHART/values-dev.yaml \
  -n trading-dev --create-namespace --dry-run=server | head -40

# === Deploy staging-local lên minikube ===
# Kiểm tra RAM THẬT của node (kubectl describe node nói dối!)
docker stats minikube --no-stream

helm upgrade --install trading $CHART \
  -f $CHART/values-staging.yaml -f $CHART/values-staging-local.yaml \
  -n trading-staging --create-namespace --atomic --timeout 10m

kubectl -n trading-staging get pods -w
kubectl -n trading-staging top pods           # cần metrics-server

# Tổng requests/limits thực tế của namespace
kubectl -n trading-staging describe resourcequota

# === Vòng đời phát hành ===
helm list -n trading-staging
helm history trading -n trading-staging
helm get values trading -n trading-staging            # values đang dùng
helm get values trading -n trading-staging --all      # kể cả mặc định
helm get manifest trading -n trading-staging | head -30

# Rollback
helm rollback trading -n trading-staging
helm history trading -n trading-staging

# === Kiểm chứng ESO (nếu bật) ===
kubectl -n trading-prod get externalsecret
kubectl -n trading-prod describe externalsecret trading-secrets
kubectl -n trading-prod get secret trading-secrets -o jsonpath='{.metadata.annotations}'
#  -> KHÔNG có hash => Gap #19: cập nhật xong không pod nào restart

# Sau khi xoay secret ở AWS, phải restart thủ công:
kubectl -n trading-prod rollout restart deploy/order-service

# === Kiểm chứng TLS ===
kubectl -n trading-staging get certificate
kubectl -n trading-staging describe certificate trading-tls
kubectl -n trading-staging get issuer,clusterissuer
#  Kiểm tra thời hạn — phải ~90 ngày, không phải 10 năm
kubectl -n trading-staging get secret trading-tls -o jsonpath='{.data.tls\.crt}' \
  | base64 -d | openssl x509 -noout -dates -subject -issuer

# === Gỡ ===
helm uninstall trading -n trading-staging
#  LƯU Ý: PVC KHÔNG bị xoá (có chủ đích, để không mất dữ liệu)
kubectl -n trading-staging get pvc
```

---

## 10. Hạn chế đã biết

| # | Loại | Vị trí | Vấn đề | Hướng sửa |
|:--:|---|---|---|---|
| 19 | **Gap** | `templates/externalsecret.yaml` | Secret do ESO dựng không có hash ⇒ cập nhật xong không pod nào restart | Reloader, hoặc mount volume, hoặc `rollout restart` trong runbook |
| — | **Ops** | `helm/` ↔ `k8s/` | Duy trì **song song hai bộ manifest** cho cùng một hệ ⇒ mọi thay đổi làm hai lần, và **đã lệch** | Chọn một; hoặc Helm render + Kustomize patch |
| — | Gap | `values.yaml` ingress routes | Helm thiếu route `/api/v1/executions` so với Kustomize | Thêm vào `routes` |
| — | Gap | `values.yaml` | `selfSignedIssuer.enabled` phải khai dù mặc định `false`, nếu không `nil pointer` khi bật TLS | Dùng `dig`/`default dict`, và `helm template` mọi tổ hợp trong CI |
| — | Ops | `values-prod.yaml` | Digest placeholder `sha256:0000...` ⇒ `ErrImagePull` nếu CI không ghi đè | CI bắt buộc `--set *.image.digest`; thêm bước validate |
| — | Ops | `values-prod.yaml` networkPolicy | CIDR `/16` = 65.536 địa chỉ, quá rộng; NetworkPolicy không hỗ trợ DNS | Thu hẹp về subnet thật; cân nhắc Cilium `toFQDNs` |
| — | Ops | Toàn chart | Không có `helm test` hook | Thêm `templates/tests/` kiểm tra health sau deploy |
| — | Ops | CI | `k8s-validate.yml` chỉ validate Kustomize, **không** validate Helm | Thêm job `helm lint` + `helm template` + `kubeconform` cho cả 4 values |

---

## 11. Câu hỏi phỏng vấn

**Q: Repo có cả Kustomize lẫn Helm. Bạn thấy sao?**

A: Về mặt học tập thì tốt — hai cách tiếp cận cùng một bài toán, so sánh được trực tiếp. Về mặt vận hành thì đó là **nợ kỹ thuật thật**: mọi thay đổi phải làm hai lần, và chúng đã lệch nhau — bản Helm thiếu route `/api/v1/executions` mà bản Kustomize có. Nếu phải chọn một: Kustomize nếu đội dùng GitOps và muốn `kubectl diff -k` thấy chính xác thứ sắp đổi; Helm nếu cần rollback tức thì không qua Git, cần phân phối chart, hoặc cần logic điều kiện phức tạp như `routes` sinh ra cả Ingress path lẫn NetworkPolicy rule. Cách thứ ba cũng hợp lệ và khá phổ biến: Helm để đóng gói, Argo CD + Kustomize để đồng bộ và patch output của Helm.

**Q: Vì sao production không chạy PostgreSQL trong cluster?**

A: Không phải vì hiệu năng mà vì **trách nhiệm**. Một StatefulSet 1 replica không có backup tự động, không có point-in-time recovery, không có failover — pod chết là database chết. Vá bản vá bảo mật phải tự nâng image và tự test. Với dữ liệu giao dịch, mất là mất không lấy lại được, và trong tài chính đó là vấn đề pháp lý chứ không chỉ kỹ thuật. RDS/Cloud SQL chuyển những việc đó cho nhà cung cấp trong một SLA. Đáng chú ý là Redis thì **vẫn in-cluster ở prod** — nhất quán với triết lý xuyên suốt của hệ: Redis là cache tăng tốc, mất nó không ảnh hưởng tính đúng đắn, nên không cần trả tiền cho ElastiCache.

**Q: `helm rollback` có đưa hệ thống về trạng thái cũ hoàn toàn không?**

A: Không — nó rollback **chart**, không rollback **hệ thống**. Helm lưu manifest đầy đủ của từng revision vào một Secret trong namespace, nên `helm rollback trading 2` khôi phục chính xác manifest đã deploy ở revision 2. Nhưng ba thứ không undo được: Flyway đã chạy `V3__add_column.sql` thì schema vẫn ở V3; message đã publish lên Kafka vẫn nằm đó; row đã ghi vào `orders` vẫn còn. Nghĩa là với bất kỳ thay đổi nào chạm tới dữ liệu, rollback phải được **thiết kế trước**: migration phải tương thích ngược (thêm cột nullable thay vì đổi kiểu), và code phiên bản cũ phải đọc được schema phiên bản mới. Đó là lý do nguyên tắc "expand-contract" tồn tại.

**Q: Vì sao `selectorLabels` tách riêng khỏi `commonLabels`?**

A: Vì `spec.selector` của Deployment là **immutable**. `commonLabels` chứa `helm.sh/chart: trading-system-0.1.0` — bump `Chart.version` lên `0.1.1` là label đổi. Nếu label đó lọt vào selector, `helm upgrade` sẽ thất bại với `field is immutable: spec.selector`, và cách duy nhất để thoát là `helm uninstall` rồi install lại — tức là downtime. Đây là lỗi kinh điển với Helm chart tự viết, và Kustomize giải cùng vấn đề bằng `includeSelectors: false`. Nguyên tắc: selector chỉ nên chứa những label **bất biến theo định nghĩa**, ở đây là `app.kubernetes.io/name`.

**Q: Giải thích `certDuration: 2160h` — vì sao chứng chỉ nội bộ chỉ sống 90 ngày?**

A: Đây là ý hay nhất trong cấu hình TLS của repo. Comment nói "giống Let's Encrypt để lộ sớm lỗi gia hạn". Nếu đặt chứng chỉ nội bộ sống 10 năm, cơ chế gia hạn tự động của cert-manager **không bao giờ được kiểm chứng** — và khi lên production với Let's Encrypt 90 ngày, lỗi gia hạn sẽ xuất hiện lần đầu vào ngày thứ 60 của production, lúc 2 giờ sáng. Đặt 90 ngày ở local nghĩa là cơ chế đó được thực thi thật, sớm, ở nơi hỏng không ai chết. Đó là một trường hợp cụ thể của nguyên tắc chung: **môi trường thấp phải chạy qua đúng những cơ chế mà production sẽ dựa vào.**

**Q: ESO xoay mật khẩu DB lúc 3 giờ sáng. Ứng dụng có dùng mật khẩu mới không?**

A: **Không** — và đó là Gap #19. ESO cập nhật Kubernetes Secret, nhưng `env.valueFrom.secretKeyRef` chỉ đọc **một lần lúc container start**. Secret trong cluster đã mới, ứng dụng vẫn dùng mật khẩu cũ cho tới lần restart ngẫu nhiên sau đó — có thể là nhiều ngày. Tệ hơn: nếu mật khẩu cũ đã bị vô hiệu hoá ở phía AWS, ứng dụng sẽ chết ở lần restart tiếp theo, vào một thời điểm hoàn toàn không liên quan tới thao tác xoay, khiến việc chẩn đoán rất khó. Nguyên nhân gốc: cơ chế rollout của repo dựa vào **hash ConfigMap/Secret**, mà Secret do ESO dựng không qua generator nên không có hash. Ba cách sửa: `rollout restart` thủ công (đã vào runbook), Reloader của stakater watch Secret và tự restart, hoặc mount Secret thành volume — kubelet tự cập nhật file, nhưng ứng dụng phải biết đọc lại.

**Q: `kubectl describe node` báo 15,6 GiB nhưng pod liên tục bị OOMKilled. Chuyện gì xảy ra?**

A: Trên minikube chạy trong Docker Desktop, kubelet đọc `/proc/meminfo` nên nó thấy RAM của **host**, trong khi container `minikube` bị **cgroup** chặn ở một con số thấp hơn nhiều — ở máy trong tài liệu này là 6144 Mi. Scheduler tin kubelet nên vui vẻ xếp pod cho đủ 16 GiB vào cái hộp 6 GiB, rồi cả node thrashing và pod bị giết ngẫu nhiên — trong khi `describe node` vẫn báo "đủ chỗ". Con số thật chỉ lấy được từ phía Docker: `docker stats minikube --no-stream`. Cách làm đúng là lập **ngân sách RAM** có số liệu đo được: liệt kê chi phí cố định (kube-system, kube-prometheus-stack, ingress, KEDA) rồi tính phần còn lại cho ứng dụng — đúng cách mà `values-staging-local.yaml` đã làm.

---

## 12. Bảng tổng kết

| Khía cạnh | Lựa chọn | Lý do | Cái giá |
|---|---|---|---|
| Công cụ | Helm **và** Kustomize | So sánh được hai cách tiếp cận | ⚠️ Duy trì hai bộ, đã lệch nhau |
| Tái sử dụng | Named template trong `_helpers.tpl` | Hardening/labels/image định nghĩa một lần | Template khó đọc hơn YAML |
| Labels | Tách `commonLabels` / `selectorLabels` | 🔑 Selector immutable — chart version lọt vào là upgrade fail | — |
| Image | Template chọn digest hoặc tag | Prod tái lập được, dev tiện | ⚠️ Placeholder `0000...` nếu CI quên |
| Values | Phân tầng + chồng `-f` | Tách hình dạng môi trường khỏi điều chỉnh máy | Thứ tự `-f` có ý nghĩa, dễ nhầm |
| Mặc định | Hạ tầng in-cluster bật, operator tắt | `helm install` trần cũng chạy được | — |
| Prod DB/Kafka | **Managed** (`enabled: false`) | Backup, PITR, failover là trách nhiệm nhà cung cấp | Chi phí |
| Prod Redis | Vẫn in-cluster | Cache, mất được | — |
| Secret prod | **ESO** từ AWS Secrets Manager | Không nằm trong Git, xoay một nơi, có audit | ⚠️ Không trigger restart |
| TLS prod | Let's Encrypt qua cert-manager | Tự động gia hạn | — |
| TLS nội bộ | CA tự mang + cert 90 ngày | 🔑 Gia hạn được kiểm chứng sớm; CA sống độc lập với Helm | Phải tạo CA ngoài chart |
| Ingress route | Một mục sinh **hai** resource | Thêm API chỉ sửa một chỗ | ⚠️ Helm thiếu một route |
| Deploy | `--atomic --timeout 10m` | Upgrade hỏng tự rollback | Chờ tới timeout |
| Rollback | `helm rollback` từ Secret trong cluster | Khôi phục chính xác trạng thái đã deploy, không cần Git | ⚠️ Không undo migration/dữ liệu |
| Ngân sách RAM | Đo bằng `docker stats`, liệt kê chi phí cố định | 🔑 `kubectl describe node` nói dối trên minikube | Phải đo lại khi đổi máy |
| Heap | 3 cơ chế khác nhau cho 3 loại thành phần | Hạ limit mà quên heap = OOMKilled không manh mối | Phải nhớ sửa cặp đôi |

> **Đọc tiếp:** [Chương 12 — Bảo mật hệ thống](./12-bao-mat-he-thong.md).
