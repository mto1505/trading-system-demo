# Helm Chart: Trading System

Bản chuyển đổi chuẩn 1-1 từ cấu trúc Kustomize (`k8s/base` + `k8s/overlays/{dev,staging,prod}`) sang Helm Chart.

---

## 1. Cấu trúc thư mục

```
helm/trading-system/
├── Chart.yaml                 # Metadata của Helm chart
├── .helmignore                # File bỏ qua khi đóng gói package
├── README.md                  # Hướng dẫn chi tiết
├── values.yaml                # Giá trị mặc định (Base defaults)
├── values-dev.yaml            # Cấu hình môi trường DEV (minikube, in-cluster infra)
├── values-staging.yaml        # Cấu hình môi trường STAGING (KEDA test, in-cluster infra)
├── values-prod.yaml           # Cấu hình môi trường PROD (RDS, Managed Kafka, ESO, Digest)
├── values-staging-local.yaml  # Lớp phủ chạy staging trên minikube (image local, RAM nhỏ)
└── templates/
    ├── _helpers.tpl           # Named template, nhãn common/selector, hàm tính digest
    ├── namespace.yaml         # Namespace với label Pod Security Standards restricted
    ├── limitrange.yaml        # Giới hạn tài nguyên mặc định cho container
    ├── resourcequota.yaml     # Trần cứng tài nguyên cho toàn namespace
    ├── configmap.yaml         # trading-config sinh từ key-value
    ├── secret.yaml            # Secret thuần cho dev/staging
    ├── externalsecret.yaml    # ExternalSecret cho prod (AWS Secrets Manager)
    ├── priorityclass.yaml     # PriorityClass trading-critical cho prod
    ├── ingress.yaml           # Ingress routing vào order-service
    ├── issuer.yaml            # cert-manager Issuer tự ký + CA nội bộ (mục 6)
    ├── networkpolicy-default-deny.yaml
    ├── networkpolicy-infra.yaml
    ├── networkpolicy-apps.yaml
    ├── networkpolicy-external.yaml
    ├── apps/
    │   ├── market-gateway.yaml
    │   ├── pricing-risk-service.yaml
    │   ├── order-service.yaml
    │   └── matching-engine.yaml
    ├── infra/
    │   ├── postgres.yaml
    │   ├── kafka.yaml
    │   ├── kafka-topics-job.yaml
    │   ├── redis.yaml
    │   ├── schema-registry.yaml
    │   ├── zipkin.yaml
    │   └── prometheus.yaml
    ├── keda/
    │   └── scaled-objects.yaml
    └── monitoring/
        ├── podmonitor.yaml
        └── service-monitor.yaml
```

---

## 2. Bảng đối chiếu Kustomize vs Helm

| Thành phần Kustomize | File Helm tương đương | Ghi chú |
| :--- | :--- | :--- |
| `k8s/base/` | `templates/` + `values.yaml` | Toàn bộ object dùng chung được đưa vào template có điều kiện |
| `k8s/overlays/dev/` | `values-dev.yaml` | Hạ CPU/RAM, mở Prometheus tối giản, tắt TLS, chạy 1 replica |
| `k8s/overlays/staging/` | `values-staging.yaml` | Bật KEDA, bật monitoring, bám image tag `main` |
| `k8s/overlays/prod/` | `values-prod.yaml` | Bật RDS/Kafka ngoài, bật ESO, bật PriorityClass, ghim bằng Digest |
| `configMapGenerator` + hash | `checksum/config` annotation | Gắn mã hash của ConfigMap vào annotation của Pod để auto-restart |
| `components/external-*` | Cờ lật `postgres.enabled=false`, `kafka.enabled=false` | Tắt StatefulSet nội bộ và mở NetworkPolicy ra ngoài VPC |
| `components/keda` | `keda.enabled=true` | Render ScaledObject cho pricing và matching engine |

---

## 3. Lệnh triển khai theo từng môi trường

> **Hướng dẫn đầy đủ từ máy trắng tới pod Ready (prereq, secret, TLS, RDS, ESO,
> kiểm tra, rollback, tra lỗi) nằm ở [`../../HELM-DEPLOY.md`](../../HELM-DEPLOY.md).**
> Mục này chỉ là bảng tra lệnh nhanh.

### 3.0. BẪY NAMESPACE — đọc trước

Chart **có sẵn** object `Namespace` (để gắn nhãn Pod Security Standards
`restricted`). Vì vậy **KHÔNG được** dùng `--create-namespace`:

```
Error: Namespace "trading-staging" exists and cannot be imported into the current release
```

Phải tạo namespace trước rồi gắn nhãn/chú thích sở hữu để chart nhận nuôi nó:

```bash
NS=trading-staging; REL=trading
kubectl create namespace $NS --dry-run=client -o yaml | kubectl apply -f -
kubectl label    namespace $NS app.kubernetes.io/managed-by=Helm --overwrite
kubectl annotate namespace $NS \
  meta.helm.sh/release-name=$REL \
  meta.helm.sh/release-namespace=$NS --overwrite
```

Script `scripts/helm/Deploy-Staging.ps1` làm sẵn bước này.

### 3.0b. Thành phần cluster-wide chart KHÔNG tự cài

| Thiếu | Triệu chứng |
| :--- | :--- |
| KEDA | `helm` chết khi render: `no matches for kind "ScaledObject"` |
| Prometheus Operator | `helm` chết khi render: `no matches for kind "PodMonitor"` |
| ingress-nginx | helm **thành công**, pod Ready hết, nhưng gọi API **timeout** |
| External Secrets (prod) | `helm` chết: `no matches for kind "ExternalSecret"` |

```powershell
.\scripts\helm\Install-Prereqs.ps1 -Target minikube   # hoặc -Target cloud
```

### 3.1. Kiểm tra cú pháp (Dry-run template)
```bash
helm template trading ./helm/trading-system -f ./helm/trading-system/values-dev.yaml     -n trading-dev
helm template trading ./helm/trading-system -f ./helm/trading-system/values-staging.yaml -n trading-staging
helm template trading ./helm/trading-system -f ./helm/trading-system/values-prod.yaml    -n trading-prod
```

### 3.2. Triển khai Dev (Minikube / Local)
```bash
# namespace đã được tạo + gắn nhãn sở hữu theo mục 3.0
helm upgrade --install trading ./helm/trading-system \
  -f ./helm/trading-system/values-dev.yaml \
  -n trading-dev
```

### 3.3. Triển khai Staging
```powershell
# Đường có script (khuyên dùng): build + nạp image + secret + namespace + deploy
.\scripts\helm\Deploy-Staging.ps1

# Staging trên cluster thật, image đã có trên registry
.\scripts\helm\Deploy-Staging.ps1 -Mode registry -ImageTag main
```

Gõ tay:
```bash
helm upgrade --install trading ./helm/trading-system \
  -f ./helm/trading-system/values-staging.yaml \
  -n trading-staging \
  --set secrets.DB_PASSWORD="<staging-db-password>" \
  --set secrets.ORDER_API_KEY="<staging-api-key>" \
  --wait --atomic --timeout 900s
```

Chạy staging trên **minikube** thì phủ thêm `values-staging-local.yaml` (image
local không tiền tố registry, tắt TLS, hạ resource):
```bash
helm upgrade --install trading ./helm/trading-system \
  -f ./helm/trading-system/values-staging.yaml \
  -f ./helm/trading-system/values-staging-local.yaml \
  -n trading-staging --set secrets.DB_PASSWORD=... --wait --atomic
```

### 3.4. Triển khai Production (CI/CD Pipeline với Image Digest)
```powershell
.\scripts\helm\Deploy-Prod.ps1 -ImageTag v1.4.2 -DryRun   # BẮT BUỘC xem trước
.\scripts\helm\Deploy-Prod.ps1 -ImageTag v1.4.2
.\scripts\helm\Deploy-Prod.ps1 -Rollback
```

Trong CI, khi bước build đã biết digest:
```bash
helm upgrade --install trading ./helm/trading-system \
  -f ./helm/trading-system/values-prod.yaml \
  -n trading-prod \
  --set marketGateway.image.digest="sha256:..." \
  --set pricingRiskService.image.digest="sha256:..." \
  --set orderService.image.digest="sha256:..." \
  --set matchingEngine.image.digest="sha256:..." \
  --wait --timeout 900s
```

> Dùng **digest** chứ không phải tag: tag là con trỏ có thể ghi đè, nên
> `helm rollback` theo tag không bảo đảm quay về đúng binary đã chạy.

### 3.5. Kiểm tra sau khi deploy
```powershell
.\scripts\helm\Verify-Release.ps1 -Namespace trading-staging
.\scripts\helm\Verify-Release.ps1 -Namespace trading-prod
```

Kiểm 7 nhóm và chỉ đúng chỗ hỏng: pod (phân biệt chưa xếp được node /
ImagePullBackOff / CrashLoop / chờ init), Job tạo topic, ScaledObject (KEDA có
thực sự đọc được lag không), PodMonitor (có khớp selector của Prometheus thật
không), PDB (`ALLOWED DISRUPTIONS = 0` làm `kubectl drain` treo vĩnh viễn),
ExternalSecret.

---

## 4. Giá trị PHẢI khớp với code Java

Bốn nhóm giá trị dưới đây không được đặt tuỳ ý — chúng phải khớp với source,
và khi lệch thì hỏng theo kiểu **im lặng**:

| values | Giá trị đúng | Nguồn sự thật | Lệch thì sao |
| :--- | :--- | :--- | :--- |
| `marketGateway.port` | 8081 | `market-gateway/.../application.yml` | probe không bao giờ 200 → pod không bao giờ Ready |
| `pricingRiskService.port` | 8082 | `pricing-risk-service/.../application.yml` | như trên |
| `orderService.port` | 8083 | `order-service/.../application.yml` | như trên |
| `matchingEngine.port` | 8084 | `matching-engine/.../application.yml` | như trên |
| `keda.pricingRiskService.topic` | `stock.ticks` | `TickConsumer.java` | KEDA báo lag 0 vĩnh viễn → autoscale chết im lặng |
| `keda.pricingRiskService.consumerGroup` | `pricing-risk-group` | `pricing/config/KafkaConfig.java` | như trên |
| `keda.matchingEngine.topic` | `stock.orders` | `OrderConsumer.java` | như trên |
| `keda.matchingEngine.consumerGroup` | `matching-engine-group` | `OrderConsumer.java` | như trên |
| `keda.*.maxReplicaCount` | `KAFKA_PARTITIONS / 3` | `setConcurrency(3)` trong `KafkaConfig.java` | pod thừa join group và nhận **không partition nào** |
| `monitoring.extraLabels.release` | tên release của kube-prometheus-stack | `helm -n monitoring list` | Prometheus bỏ qua PodMonitor trong im lặng |

Muốn scale cao hơn `maxReplicaCount` thì phải **tăng partition trước**
(sửa `config.KAFKA_PARTITIONS`, Job `init-kafka-topics` sẽ tự `--alter`).

---

## 5. Named template và cách value được truyền vào / lấy ra

Phần này giải thích dòng hay gây bối rối nhất trong chart:

```gotemplate
{{ include "trading.appLabels" (dict "name" "order-service" "component" .Values.orderService.component "context" .) | nindent 4 }}
```

### 5.1. Value vào chart bằng đường nào

Helm gộp **nhiều nguồn** thành một cây duy nhất rồi mới render. Nguồn sau đè nguồn trước:

```
values.yaml  →  -f values-staging.yaml  →  -f values-staging-local.yaml  →  --set
  (luôn đọc)         (theo thứ tự -f trên dòng lệnh)                    (đè cuối cùng)
```

Cây đã gộp nằm ở `.Values`. Ngoài ra Helm bơm sẵn vài object **dựng sẵn**:

| Object | Là gì | Ví dụ trong chart này |
| :--- | :--- | :--- |
| `.Values` | Cây values đã gộp | `.Values.orderService.port` |
| `.Release` | Thông tin lần cài | `.Release.Name`, `.Release.Namespace`, `.Release.Service` |
| `.Chart` | Nội dung `Chart.yaml` | `.Chart.Name`, `.Chart.Version` |
| `.Template` | File template đang render | `.Template.BasePath` trong `configChecksum` |
| `.Capabilities` | Khả năng của API server | (chart này chưa dùng) |

Xem cây values thật sự đang được dùng:

```bash
helm template trading . -f values-staging.yaml -n trading-staging --debug | head -40   # trước khi cài
helm -n trading-staging get values trading --all                                       # sau khi cài
```

### 5.2. `define` / `include` — hàm của Helm

Named template khai báo bằng `define`, đặt trong file bắt đầu bằng dấu gạch dưới
(`_helpers.tpl`). **Dấu gạch dưới quan trọng**: Helm bỏ qua các file `_*` khi sinh
manifest, nếu không nó sẽ cố biến `_helpers.tpl` thành một object Kubernetes.

```gotemplate
{{- define "trading.selectorLabels" -}}
app.kubernetes.io/name: {{ .name }}
{{- end -}}
```

Gọi ra bằng `include` — **luôn dùng `include`, đừng dùng `template`**:

```gotemplate
{{ include "trading.selectorLabels" $arg }}     {{/* ĐÚNG: trả về chuỗi, nối ống được */}}
{{ template "trading.selectorLabels" $arg }}    {{/* SAI: ghi thẳng ra output, KHÔNG nối ống được */}}
```

`template` là một *statement*, không phải *expression*: nó in thẳng ra output nên
bạn **không** thể viết `{{ template "x" . | nindent 4 }}`. Mà `nindent` chính là
thứ làm cho indent YAML đúng — nên cả chart này chỉ dùng `include`.

### 5.3. Chỉ truyền được MỘT tham số — vì sao phải có `dict`

Go template cho `include` đúng **một** đối số dữ liệu. Bên trong template, đối số
đó trở thành dấu chấm `.`.

Muốn truyền ba thứ (`name`, `component`, và context gốc) thì phải gói chúng vào
một map. Đó là việc của hàm `dict`:

```gotemplate
(dict "name" "order-service" "component" .Values.orderService.component "context" .)
```

`dict` nhận các đối số theo cặp **khoá, giá trị** và trả về một map:

| Khoá | Giá trị truyền vào | Bên trong template đọc bằng |
| :--- | :--- | :--- |
| `"name"` | chuỗi `"order-service"` | `.name` |
| `"component"` | `.Values.orderService.component` → `"order"` | `.component` |
| `"context"` | `.` — **toàn bộ** context gốc | `.context` |

Dấu ngoặc đơn quanh `(dict ...)` là **bắt buộc**: nó bảo Go template "tính biểu
thức này trước, rồi lấy kết quả làm đối số". Thiếu ngoặc thì `dict` bị hiểu là
đối số đầu tiên của `include`.

### 5.4. Lần theo đúng ví dụ đó

**Chỗ gọi** — `templates/apps/order-service.yaml`:

```gotemplate
metadata:
  name: order-service
  labels:
    {{ include "trading.appLabels" (dict "name" "order-service" "component" .Values.orderService.component "context" .) | nindent 4 }}
```

Ở đây `.` là context gốc (có `.Values`, `.Release`, `.Chart`).

**Chỗ nhận** — `templates/_helpers.tpl`:

```gotemplate
{{- define "trading.appLabels" -}}
{{ include "trading.commonLabels" .context }}    {{/* chuyển tiếp context gốc */}}
{{ include "trading.selectorLabels" . }}         {{/* chuyển tiếp chính cái dict */}}
app.kubernetes.io/component: {{ .component }}    {{/* đọc thẳng từ dict */}}
{{- end -}}
```

Bên trong `trading.appLabels`, dấu chấm `.` **không còn là context gốc nữa** — nó
là cái dict vừa truyền vào. Nên:

- `.component` → `"order"` (lấy từ dict)
- `.context` → context gốc, được **chuyển tiếp** xuống `commonLabels`
- `.` → chính cái dict, được chuyển tiếp xuống `selectorLabels` (template đó đọc `.name`)

**Vì sao `commonLabels` cần `.context` chứ không phải `.`:**

```gotemplate
{{- define "trading.commonLabels" -}}
app.kubernetes.io/part-of: {{ .Values.global.partOf }}             {{/* cần .Values  */}}
app.kubernetes.io/managed-by: {{ .Release.Service }}               {{/* cần .Release */}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version ... }} {{/* cần .Chart   */}}
{{- end -}}
```

Nó đụng tới `.Values`, `.Release`, `.Chart` — mà dict thì **không có** những thứ
đó. Truyền `.` thay vì `.context` sẽ chết:

```
nil pointer evaluating interface {}.Values
```

Đó chính là lý do khoá `"context"` tồn tại trong dict. Không phải quy ước trang
trí — nó là cách duy nhất để một helper lồng nhau với tới được object dựng sẵn.

**Kết quả render:**

```yaml
metadata:
  name: order-service
  labels:
    app.kubernetes.io/part-of: trading-system      # từ commonLabels (.context)
    app.kubernetes.io/managed-by: Helm             # từ commonLabels (.context)
    helm.sh/chart: trading-system-0.1.0            # từ commonLabels (.context)
    app.kubernetes.io/name: order-service          # từ selectorLabels (.name)
    app.kubernetes.io/component: order             # từ .component
```

### 5.5. CÁI BẪY: `$` bên trong named template KHÔNG phải gốc chart

Ở template cấp cao nhất, `$` là context gốc. Nhiều người suy ra rằng `$` luôn là
"gốc chart, bất kể đang ở đâu". **Sai.**

Mỗi lần `include` tạo ra một lượt thực thi mới, và `$` được **gán lại** bằng đúng
đối số bạn vừa truyền:

```gotemplate
{{- define "dollarRoot" -}}
dollar-sees: {{ $.Chart.Name }}
{{- end -}}
```

```gotemplate
{{ include "dollarRoot" . }}                  {{/* OK   -> dollar-sees: probe */}}
{{ include "dollarRoot" (dict "name" "x") }}  {{/* CHẾT -> nil pointer evaluating interface {}.Name */}}
```

Vì vậy trong chart này:

```gotemplate
{{- define "trading.configChecksum" -}}
checksum/config: {{ include (print $.Template.BasePath "/configmap.yaml") . | sha256sum }}
{{- end -}}
```

`trading.configChecksum` **bắt buộc** phải được gọi bằng `include "trading.configChecksum" .`
(context gốc). Đổi sang gọi bằng dict là `$.Template` biến mất và template chết.
Đây là ràng buộc ngầm giữa helper và chỗ gọi — nhớ kiểm lại khi refactor.

### 5.6. Bốn kiểu truyền tham số đang dùng trong chart

Không phải helper nào cũng cần dict. Quy tắc: **chỉ gói vào dict khi cần nhiều
hơn một thứ**, và **chỉ kèm `context` khi helper thật sự đụng tới object dựng sẵn**.

| Kiểu | Ví dụ trong chart | Bên trong đọc bằng | Khi nào dùng |
| :--- | :--- | :--- | :--- |
| Context gốc | `include "trading.commonLabels" .` | `.Values`, `.Release` | Helper chỉ cần object dựng sẵn |
| Một nhánh values | `include "trading.image" .Values.orderService.image` | `.repository`, `.tag`, `.digest` | Helper chỉ thao tác trên một map con |
| dict, **không** context | `include "trading.topologySpread" (dict "spread" ... "name" "order-service")` | `.spread`, `.name` | Cần nhiều giá trị nhưng **không** đụng `.Values` |
| dict **có** context | `include "trading.appLabels" (dict ... "context" .)` | `.name`, `.component`, `.context` | Cần nhiều giá trị **và** gọi tiếp helper cần `.Values` |

Đối chiếu hai dòng này để thấy rõ:

```gotemplate
{{/* KHÔNG cần context: helper chỉ đọc .spread và .name, không đụng .Values */}}
{{- include "trading.topologySpread" (dict "spread" .Values.orderService.topologySpread "name" "order-service") | nindent 6 }}

{{/* CẦN context: helper gọi tiếp commonLabels, mà commonLabels cần .Values/.Release/.Chart */}}
{{ include "trading.appLabels" (dict "name" "order-service" "component" .Values.orderService.component "context" .) | nindent 4 }}
```

Lưu ý `.Values.orderService.topologySpread` được tính **ở chỗ gọi** rồi mới nhét
vào dict — nên bên trong helper chỉ còn `.spread`, không cần `.Values` nữa.

### 5.7. `nindent` và dấu `{{-`

`indent N` thêm N khoảng trắng vào **mọi** dòng. `nindent N` làm y hệt nhưng
**thêm một ký tự xuống dòng ở đầu**. Named template sinh ra nhiều dòng, nên gần
như luôn dùng `nindent`.

Dấu `-` trong `{{-` nuốt khoảng trắng **phía trước** (kể cả xuống dòng); `-}}`
nuốt khoảng trắng **phía sau**.

Hệ quả rất cụ thể — hai cách viết cho kết quả khác nhau:

```gotemplate
  labels:
    {{- include "trading.commonLabels" . | nindent 4 }}
```
```yaml
  labels:
    app.kubernetes.io/part-of: trading-system
```

```gotemplate
  labels:
    {{ include "trading.appLabels" (dict ...) | nindent 4 }}
```
```yaml
  labels:
                                               # <- dòng thừa, chỉ có 4 dấu cách
    app.kubernetes.io/part-of: trading-system
```

Cách thứ hai: 4 dấu cách viết tay **được giữ lại** (vì không có `-` để nuốt), rồi
`nindent` lại xuống dòng — thành ra một dòng trắng có đuôi khoảng trắng. YAML vẫn
hợp lệ và Kubernetes vẫn nhận, nên đây thuần tuý là vấn đề thẩm mỹ; bốn chỗ gọi
`appLabels` trong chart đang ở dạng này.

Quy tắc an toàn: viết `{{- include ... | nindent N }}` với N bằng đúng mức indent
của khoá cha cộng 2.

### 5.8. Cách gỡ rối khi dict truyền sai

```bash
# 1. Xem toàn bộ manifest đã render, KHÔNG cần cluster
helm template trading . -f values-staging.yaml -n trading-staging

# 2. --debug vẫn in ra manifest dang dở khi lỗi — rất hữu ích để biết chết ở đâu
helm template trading . -f values-staging.yaml --debug

# 3. Chỉ render một file
helm template trading . -f values-staging.yaml -s templates/apps/order-service.yaml

# 4. In thẳng cái dict ra để xem nó chứa gì.
#    Nhét tạm dòng này vào đầu helper rồi chạy helm template:
#      debug: {{ . | toYaml | nindent 2 }}
```

Khi dict truyền sai, có **hai nhóm hậu quả khác hẳn nhau** — và nhóm nguy hiểm
hơn lại là nhóm không báo gì cả.

**Nhóm 1 — chết ồn ào (dễ sửa):**

| Thông báo lỗi | Nghĩa thật |
| :--- | :--- |
| `nil pointer evaluating interface {}.Values` | Helper cần `.Values` nhưng bạn truyền dict không có `"context"` |
| `nil pointer evaluating interface {}.Name` | `$.Chart` / `$.Release` bên trong helper được gọi bằng dict (mục 5.5) |
| `wrong number of args for include: want 2 got 4` | Quên ngoặc đơn quanh `(dict ...)` — `dict`, `"name"`, `"x"` bị đếm thành các đối số rời của `include` |

**Nhóm 2 — hỏng IM LẶNG (nguy hiểm hơn nhiều):**

Tra khoá không tồn tại trên một map trong Go template **không phải là lỗi** — nó
trả về rỗng. Nên hai sai sót dưới đây render thành công, `helm lint` xanh,
`helm template` xanh, và bạn chỉ phát hiện khi object trên cluster thiếu nhãn:

| Sai sót | Kết quả thật |
| :--- | :--- |
| Sai tên khoá: dict đặt `"name"` nhưng helper đọc `.Name` | Render ra **chuỗi rỗng**, không lỗi |
| `dict` số đối số lẻ: `(dict "name" "x" "component")` | Khoá cuối được gán **rỗng**, không lỗi |

Minh hoạ — cả hai đều "chạy tốt":

```yaml
# helper đọc .Name trong khi dict đặt "name"
v: []
# dict thiếu giá trị cho "component"
name: x
comp:
```

Hậu quả thực tế trong chart này rất cụ thể: nhãn `app.kubernetes.io/name` rỗng →
`selector` của Service không khớp pod nào → Service có endpoint rỗng → gọi API
trả **503** trong khi pod vẫn `Running` và `Ready`. Không có dòng log nào ở phía
app nói cho bạn biết điều đó.

Vì vậy sau khi sửa helper, đừng chỉ nhìn `helm lint` — **soi nhãn đã render**:

```bash
helm template trading . -f values-staging.yaml -n trading-staging \
  -s templates/apps/order-service.yaml | grep -A6 "^  labels:"

# Hoặc chắc chắn hơn: tìm nhãn rỗng trong toàn bộ manifest
helm template trading . -f values-staging.yaml -n trading-staging \
  | grep -nE "app\.kubernetes\.io/[a-z-]+:\s*$"      # không ra dòng nào là đạt
```

### 5.9. Trường hợp đặc biệt: `configChecksum` — vì sao có dấu `.` ở cuối

```gotemplate
{{- define "trading.configChecksum" -}}
checksum/config: {{ include (print $.Template.BasePath "/configmap.yaml") . | sha256sum }}
{{- end -}}
```

Dòng này làm nhiều người khựng lại vì nó dùng `include` theo một kiểu khác hẳn
phần trên.

#### `include` luôn nhận ĐÚNG hai đối số

```
include   <TÊN TEMPLATE>   <DỮ LIỆU>
```

Tách dòng trên ra:

| Vị trí | Giá trị | Vai trò |
| :--- | :--- | :--- |
| đối số 1 | `(print $.Template.BasePath "/configmap.yaml")` | **tên** template, ghép ra lúc chạy |
| đối số 2 | `.` | **dữ liệu** truyền cho template đó |

Dấu `.` ở cuối **không phải** dấu câu, không phải cú pháp thừa — nó chính là
tham số dữ liệu, y hệt `.` trong `include "trading.commonLabels" .`.

#### Mỗi file trong `templates/` CŨNG là một named template

Đây là mảnh ghép làm cả chuyện này chạy được: Helm đăng ký mọi file dưới
`templates/` thành một named template, **tên chính là đường dẫn** của nó. Kiểm
chứng được:

```gotemplate
{{ $.Template.BasePath }}   →  trading-system/templates
{{ $.Template.Name }}       →  trading-system/templates/order-service.yaml
```

Nên `print $.Template.BasePath "/configmap.yaml"` ghép ra chuỗi
`trading-system/templates/configmap.yaml` — đúng bằng tên mà Helm đã đăng ký cho
`templates/configmap.yaml`. Nói cách khác: **ta đang gọi một file manifest như
gọi một hàm**, để lấy lại chính xác chuỗi YAML mà nó sinh ra.

Phải ghép bằng `$.Template.BasePath` chứ không viết cứng `"templates/configmap.yaml"`,
vì tên đăng ký có tiền tố là **tên chart**. Khi chart này được dùng làm
subchart của chart khác, tiền tố đó đổi — `BasePath` tự đúng, chuỗi viết cứng thì sai.

#### Vì sao dữ liệu BẮT BUỘC phải là context gốc

Xem thân `templates/configmap.yaml`:

```gotemplate
metadata:
  labels:
    {{- include "trading.commonLabels" . | nindent 4 }}   {{/* cần .Values .Release .Chart */}}
data:
  {{- range $key, $value := .Values.config }}             {{/* cần .Values            */}}
```

Nó đụng `.Values` và gọi tiếp `commonLabels`. Truyền thứ khác vào là chết ngay:

```gotemplate
{{ include (print $.Template.BasePath "/configmap.yaml") (dict "x" 1) | sha256sum }}
```
```
nil pointer evaluating interface {}.cfg
```

Mà `.` bên trong `trading.configChecksum` **đang là** context gốc — vì helper này
luôn được gọi bằng `{{ include "trading.configChecksum" . }}`. Dấu `.` cuối dòng
chỉ đơn giản là **chuyển tiếp** context gốc đó xuống cho `configmap.yaml`.

#### `$` và `.` ở đây là cùng một giá trị

Trong dòng đó, `$.Template.BasePath` dùng `$` còn đối số dữ liệu dùng `.` — trông
như hai thứ khác nhau, nhưng tại vị trí này chúng **bằng nhau**: cả hai đều là
đối số đã truyền vào `trading.configChecksum` (mục 5.5). Viết `.Template.BasePath`
cũng chạy y hệt.

`$` là lựa chọn phòng thủ: nếu sau này ai đó bọc dòng này vào trong `range` hay
`with`, `.` sẽ bị gán lại còn `$` thì không. Giữ `$` là thói quen tốt.

#### Helper này để làm gì

Gắn mã hash của ConfigMap vào annotation của Pod:

```yaml
template:
  metadata:
    annotations:
      checksum/config: 33d9c40e6625fbc9...
```

Sửa bất kỳ giá trị nào trong `config:` → ConfigMap render khác → hash khác →
**pod template đổi** → Deployment tự rolling restart. Không có nó thì
`kubectl apply` cập nhật ConfigMap nhưng pod vẫn giữ biến môi trường cũ cho tới
lần restart kế tiếp. Đây là thứ thay cho cơ chế name-suffix-hash của
`configMapGenerator` bên Kustomize (xem bảng đối chiếu mục 2).

#### Tác dụng phụ cần biết: đổi version chart cũng làm restart

Hash tính trên **toàn bộ manifest đã render**, mà manifest đó chứa nhãn
`helm.sh/chart: trading-system-0.1.0` do `commonLabels` sinh ra. Nên bump version
trong `Chart.yaml` là hash đổi theo, kể cả khi `config:` không hề thay đổi:

```
chart 0.1.0 -> checksum/config: 33d9c40e6625fbc90e0ca64a5143e60d3fd2956d6c1c8f1df528dc697375461e
chart 0.1.1 -> checksum/config: bcbb5cdc026419cf0f2dd79d7c139b34c3fed3e2335459f2d11c49b75082634b
```

Hệ quả: **mọi release có bump chart version đều rolling restart cả 4 service.**
Thường thì vô hại (và nhiều người còn muốn thế), nhưng cần biết trước khi ngạc
nhiên vì sao chỉ sửa mỗi `resources:` mà pod nào cũng restart.

Muốn hash chỉ phụ thuộc đúng phần dữ liệu thì hash `.Values.config` thay vì cả
file:

```gotemplate
checksum/config: {{ .Values.config | toYaml | sha256sum }}
```

Chart này cố ý **không** làm vậy: hash cả file bắt được cả những thay đổi ngoài
`config:` mà vẫn làm ConfigMap khác đi (ví dụ ai đó thêm một khoá vào
`templates/configmap.yaml`). Đánh đổi lấy việc thỉnh thoảng restart thừa.

#### Lưu ý khi đọc code

`configmap.yaml` được render **hai lần** mỗi lần deploy: một lần thành manifest
thật, một lần nữa ở đây để lấy hash. Template của Helm thuần tuý không có tác
dụng phụ nên điều này vô hại — nhưng đừng nhét `randAlphaNum` hay thứ gì sinh
giá trị ngẫu nhiên vào `configmap.yaml`: hai lần render sẽ ra hai kết quả khác
nhau, và pod sẽ restart ở **mọi** lần `helm upgrade`.

---

## 6. TLS tự động bằng cert-manager — cơ chế bên trong

Mục 4.8 của [`../../HELM-DEPLOY.md`](../../HELM-DEPLOY.md) nói **cách bật và kiểm
tra**. Mục này nói **nó hoạt động ra sao** — để khi hỏng bạn biết nhìn vào đâu.

### 6.1. Ai thực sự tạo ra Certificate

Chart này **không** chứa object `Certificate` nào cho Ingress. Chỉ có ba dòng
annotation trong `templates/ingress.yaml`:

```yaml
cert-manager.io/issuer: trading-local-issuer
cert-manager.io/duration: "2160h"
cert-manager.io/renew-before: "360h"
```

Thành phần **ingress-shim** bên trong cert-manager quan sát mọi Ingress. Thấy
annotation `cert-manager.io/issuer` (hoặc `cluster-issuer`) kèm `spec.tls`, nó tự
dựng một `Certificate`. Bằng chứng kiểm chứng được:

```bash
kubectl -n trading-staging get certificate trading-tls -o jsonpath='{.metadata.ownerReferences}'
```
```json
[{"kind":"Ingress","name":"trading-ingress","controller":true,"blockOwnerDeletion":true}]
```

`controller: true` nghĩa là **Ingress sở hữu Certificate**. Hệ quả thực tế: xoá
Ingress thì Certificate bị garbage-collect theo, không cần dọn tay. Và vì
Certificate không thuộc Helm release, `helm diff` sẽ **không** hiện nó.

Spec mà ingress-shim tự suy ra, không ai viết tay:

| Trường của Certificate | Lấy từ đâu |
| :--- | :--- |
| `dnsNames: [trading-staging.local]` | `spec.tls[0].hosts` của Ingress |
| `secretName: trading-tls` | `spec.tls[0].secretName` |
| `issuerRef.name` | annotation `cert-manager.io/issuer` |
| `duration: 2160h0m0s` | annotation `cert-manager.io/duration` |
| `renewBefore: 360h0m0s` | annotation `cert-manager.io/renew-before` |

Đổi `ingress.host` trong values → Helm cập nhật Ingress → ingress-shim thấy
`dnsNames` lệch → cert-manager cấp lại cert mới. Không có bước thủ công nào.

### 6.2. Secret chứa gì và nginx dùng nó thế nào

```bash
kubectl -n trading-staging get secret trading-tls -o json | jq '.data | keys'
```
```json
["ca.crt", "tls.crt", "tls.key"]
```

| Khoá | Là gì | Ai dùng |
| :--- | :--- | :--- |
| `tls.key` | Khoá riêng. **Không bao giờ rời cluster.** | nginx, để giải mã bắt tay TLS |
| `tls.crt` | Chứng chỉ của server (kèm chuỗi trung gian nếu có) | nginx gửi cho client |
| `ca.crt` | Chứng chỉ CA đã ký `tls.crt` | client, để xác thực |

Kiểu Secret là `kubernetes.io/tls` — đây là hợp đồng chuẩn giữa Kubernetes và
mọi Ingress controller. nginx đọc `spec.tls[*].secretName`, nạp cặp
`tls.crt`/`tls.key` vào SNI map theo host, rồi **kết thúc TLS tại nginx**. Từ
nginx vào pod là HTTP thường trong mạng cluster — đó là lý do NetworkPolicy
`allow-ingress-to-order-service` mở cổng **8083**, không phải 443.

cert-manager còn gắn nhãn xuất xứ lên chính Secret, rất tiện khi truy vết:

```bash
kubectl -n trading-staging get secret trading-tls -o jsonpath='{.metadata.annotations}'
```
```
cert-manager.io/certificate-name: trading-tls
cert-manager.io/issuer-name:      trading-local-issuer
cert-manager.io/issuer-kind:      Issuer
cert-manager.io/alt-names:        trading-staging.local
```

Thấy một Secret TLS mà **không có** mấy annotation này nghĩa là nó do người khác
tạo tay — cert-manager sẽ không gia hạn nó.

### 6.3. Vì sao phải hai Issuer, không phải một

#### Hiểu lầm thường gặp

`selfSigned` nghe như "một CA tự ký, rồi nó ký tiếp cho người khác". **Không
phải.** Trong cert-manager, `selfSigned` không phải một thực thể nắm khoá. Nó là
một **chế độ ký**:

> "Certificate nào xin, hãy ký bằng **chính khoá riêng của Certificate đó**."

Nghĩa là Issuer `selfSigned` **không có khoá nào cả**. Nó không ký gì hết — nó
bảo mỗi chứng chỉ tự ký lên chính mình. Mỗi chứng chỉ trở thành gốc tin cậy
riêng, độc lập hoàn toàn với các chứng chỉ khác do cùng Issuer đó "cấp".

So sánh:

| | Issuer `selfSigned` | Issuer `ca` |
| :--- | :--- | :--- |
| Có khoá riêng không? | **Không** | Có — nằm trong `spec.ca.secretName` |
| Ai ký lên chứng chỉ? | Chính chứng chỉ đó | Khoá của CA trong Secret |
| Hai cert cùng Issuer có chung gốc? | **Không** — hai gốc riêng | **Có** — cùng một gốc |

#### Chứng minh bằng thí nghiệm

Tạo một Issuer `selfSigned` rồi xin nó hai chứng chỉ `api-a` và `api-b`:

```yaml
kind: Issuer
metadata: { name: demo-selfsigned }
spec:
  selfSigned: {}
---
kind: Certificate
metadata: { name: api-a }
spec: { secretName: api-a-tls, dnsNames: ["api-a.local"], issuerRef: { name: demo-selfsigned } }
---
kind: Certificate
metadata: { name: api-b }
spec: { secretName: api-b-tls, dnsNames: ["api-b.local"], issuerRef: { name: demo-selfsigned } }
```

Hỏi OpenSSL ba câu:

```bash
# 1. api-a co tu xac thuc duoc chinh no khong?
openssl verify -CAfile api-a.crt api-a.crt
#   api-a.crt: OK          <- no LA goc cua chinh no

# 2. api-a co bao lanh duoc cho api-b khong?
openssl verify -CAfile api-a.crt api-b.crt
#   error 18 at 0 depth lookup: self-signed certificate
#   api-b.crt: verification failed    <- KHONG

# 3. Hai cai co chung goc khong?
openssl x509 -in api-a.crt -noout -pubkey | openssl dgst -sha256
#   2bf4ca45...
openssl x509 -in api-b.crt -noout -pubkey | openssl dgst -sha256
#   1d8b4e0c...             <- hai khoa KHAC nhau = hai goc khac nhau
```

Cùng một Issuer, nhưng ra **hai gốc tin cậy riêng biệt** không liên quan gì đến
nhau.

#### Vì sao điều đó là vấn đề

Để trình duyệt tin một chứng chỉ, bạn phải cài **gốc** của nó vào kho tin cậy.

Với `selfSigned`, gốc chính là từng chứng chỉ một:

```
cài api-a.crt vào Windows  ->  chỉ tin api-a.local
cài api-b.crt vào Windows  ->  chỉ tin api-b.local
thêm service thứ ba        ->  cài thêm lần nữa
cert hết hạn và xoay vòng  ->  gốc đổi -> cài LẠI tất cả
```

Mắt xích cuối mới là chỗ đau: cert xoay vòng 90 ngày một lần, mà gốc đổi theo
nên kho tin cậy hỏng mỗi quý.

#### Cách làm đúng: nhờ nó ký ra một CA trước

Vì `selfSigned` ký được cho *một* chứng chỉ, ta dùng nó đúng **một lần** — để
tạo ra thứ mà nó không tự làm được: một chứng chỉ có `isCA: true`.

```
Issuer <name>-root          kind: selfSigned
   │   dùng MỘT LẦN
   ▼
Certificate <name>-ca       isCA: true    ──► Secret <name>-ca-key-pair
                                                  │  (cert + KHOÁ RIÊNG của CA)
                                                  ▼
                                           Issuer <name>   kind: ca
                                                  │  ký bằng khoá đó
                                                  ▼
                                           Certificate trading-tls, và mọi cert sau này
```

`isCA: true` bật cờ `basicConstraints CA:TRUE` trong chứng chỉ — **đó là trường
duy nhất** cho phép một chứng chỉ ký lên chứng chỉ khác. Mắt (3) là Issuer kiểu
`ca`: nó **có** khoá (lấy từ Secret ở mắt 2) nên ký được cho người khác.

Kiểm chứng trên chính hệ thống này:

```bash
openssl verify -CAfile ca.crt leaf.crt      # leaf.crt: OK       <- CA bao lanh duoc
openssl verify -CAfile ca.crt ca.crt        # ca.crt: OK         <- CA la goc, tu ky
openssl verify -CAfile leaf.crt leaf.crt    # verification failed <- leaf KHONG phai goc

openssl x509 -in ca.crt   -noout -ext basicConstraints   # CA:TRUE
openssl x509 -in leaf.crt -noout -ext basicConstraints   # CA:FALSE
```

Kết quả: chỉ cần cài **một** file `ca.crt` vào Windows. Mọi chứng chỉ do CA đó ký
— hôm nay, và mọi lần xoay vòng sau — đều được tin ngay, không phải cài lại gì.

#### Tóm tắt khác biệt

| | `useCA: false` | `useCA: true` (mặc định) |
| :--- | :--- | :--- |
| Số object | 1 Issuer | 2 Issuer + 1 Certificate |
| Phải cài vào trust store | **Từng cert** của từng service | **Một** CA duy nhất |
| Cert xoay vòng 90 ngày | Gốc đổi → cài lại | CA giữ nguyên 1 năm → không đụng gì |
| Thêm service mới | Cài thêm một cert nữa | Không phải làm gì |
| Giống production | Không | Có — ACME cũng là CA ký cho leaf |

Chart mặc định `useCA: true`. Đặt `false` chỉ hợp khi bạn chỉ cần đúng một cert,
dùng một lần, và không định cài vào trust store.

#### Vì sao `Issuer` chứ không `ClusterIssuer`

`Issuer` là namespaced: mọi object nằm trong namespace của release, `helm
uninstall` là sạch, không đòi quyền cluster-scoped. Đổi lại mỗi namespace phải có
Issuer riêng.

Prod dùng `ClusterIssuer` vì một tài khoản ACME dùng chung cho cả cluster là hợp
lý hơn — và vì cert-manager chỉ tìm Secret của `Issuer` trong **cùng namespace**
với nó, nên CA nội bộ dùng chung nhiều namespace buộc phải là `ClusterIssuer`.

### 6.4. SAN mới là thứ được kiểm, không phải CN

Chứng chỉ do cert-manager cấp ở đây có `subject` **rỗng**:

```
subject=
issuer=CN=trading-local-ca
X509v3 Subject Alternative Name: critical
    DNS:trading-staging.local
```

Không phải lỗi. Common Name đã bị khai tử cho mục đích xác thực tên miền: từ
Chrome 58 (2017) và Go 1.15, client **chỉ đọc SAN**. cert-manager vì vậy để CN
trống và chỉ điền SAN từ `dnsNames`.

Hệ quả cần nhớ: cert chỉ hợp lệ cho **đúng** các host liệt kê trong
`spec.tls[*].hosts`. Gọi bằng IP, bằng `localhost`, hay bằng tên khác đều bị từ
chối — kể cả khi đã nạp đúng CA. Đó là lý do lệnh thử trong HELM-DEPLOY.md dùng
`--resolve trading-staging.local:8443:127.0.0.1` thay vì gọi thẳng `127.0.0.1`.

### 6.5. Vòng đời và gia hạn

Mỗi lần cấp phát đi qua một `CertificateRequest` — đây mới là nơi ghi **lý do
thật** khi hỏng, không phải `Certificate`:

```bash
kubectl -n trading-staging get certificaterequest
```
```
NAME                        APPROVED   READY
trading-local-issuer-ca-1   True       True
trading-tls-1               True       True
```

Hậu tố `-1` là `status.revision`. Mỗi lần gia hạn sinh một request mới
(`trading-tls-2`, `-3`...), nên đếm revision là biết cert đã xoay bao nhiêu lần.

Lịch gia hạn tính sẵn và đọc được:

```bash
kubectl -n trading-staging get certificate trading-tls \
  -o jsonpath='notBefore={.status.notBefore} notAfter={.status.notAfter} renewalTime={.status.renewalTime}'
```
```
notBefore=2026-11-15  notAfter=2027-02-13  renewalTime=2027-01-29
```

`renewalTime = notAfter − renewBefore` = 13/02 − 360h (15 ngày) = 29/01.
cert-manager tự cấp lại vào lúc đó rồi **ghi đè Secret tại chỗ**. nginx theo dõi
Secret và nạp lại cert mà không cần restart pod.

Vì sao chart đặt `certDuration: 2160h` (90 ngày) cho môi trường local, trong khi
có thể đặt 10 năm: **để lộ sớm lỗi gia hạn**. Cert 10 năm che mất mọi vấn đề của
đường gia hạn cho tới khi nó hỏng ở prod. 90 ngày là đúng bằng Let's Encrypt.

### 6.6. Ba loại Issuer và khi nào dùng cái nào

| Loại | Dùng khi | Chart này |
| :--- | :--- | :--- |
| `selfSigned` | Bootstrap một CA, hoặc test nhanh | Mắt (1), `useCA: true` |
| `ca` | Có CA nội bộ, không ra được Internet | Mắt (3) — staging/local |
| `acme` | Có domain công khai, cần trình duyệt tin sẵn | **prod** (`letsencrypt-prod`) |

`values-prod.yaml` giữ `cert-manager.io/cluster-issuer: letsencrypt-prod` và
**không** bật `selfSignedIssuer`. Cấu trúc YAML giống hệt — chỉ đổi nguồn ký. Đó
chính là giá trị của việc test luồng này ở local: cùng một cơ chế tự động hoá,
chỉ thay loại Issuer.

ACME cần cluster ra được Internet và domain trỏ đúng vào Ingress, vì Let's
Encrypt phải gọi ngược lại `http://<domain>/.well-known/acme-challenge/...` để
xác minh quyền sở hữu. Trên minikube điều đó bất khả thi — đó là lý do local
phải dùng CA tự ký, chứ không phải vì tiện.

### 6.7. Kiểu hỏng im lặng cần đề phòng

Nếu cert-manager **không** cấp được cert, Ingress vẫn tồn tại và nginx vẫn phục
vụ HTTPS — bằng **chứng chỉ mặc định tự ký của chính nginx**
(`Kubernetes Ingress Controller Fake Certificate`). Trình duyệt báo "không tin
cậy" y hệt như với CA nội bộ của bạn, nên rất dễ tưởng là bình thường.

Luôn kiểm `issuer` thực tế, đừng chỉ xem "có HTTPS hay không":

```bash
echo | openssl s_client -connect 127.0.0.1:8443 -servername trading-staging.local 2>/dev/null \
  | openssl x509 -noout -issuer
```
```
issuer=CN=trading-local-ca          # đúng
issuer=O=Acme Co, CN=Kubernetes Ingress Controller Fake Certificate   # SAI
```

`Verify-Release.ps1` tự so điều này giúp bạn.

### 6.8. Bảng tra lỗi TLS

| Triệu chứng | Nguyên nhân thật | Nhìn vào đâu |
| :--- | :--- | :--- |
| `no matches for kind "Issuer"` | cert-manager chưa cài | `Install-Prereqs.ps1` |
| `failed calling webhook ... connection refused` | Tạo Issuer khi `cert-manager-webhook` chưa Ready | `kubectl -n cert-manager get pods` |
| Certificate `Ready=False` | Issuer chưa sẵn sàng, hoặc CA secret thiếu | `kubectl get certificaterequest` — lý do thật ở đây |
| Issuer kiểu `ca` không Ready | Secret `...-ca-key-pair` chưa tồn tại | Kiểm Certificate `...-ca` trước |
| Có HTTPS nhưng cert lạ | cert-manager không cấp được, nginx dùng cert mặc định | So `issuer` (mục 6.7) |
| `schannel: the revocation status is unknown` | curl Windows đòi CRL/OCSP; CA tự ký không có | Thêm `--ssl-no-revoke`. **Không phải lỗi cert** |
| `UI is not allowed in this operation` | Thêm cert vào kho Root cần hộp thoại cảnh báo; phiên không tương tác không hiện được | Dùng `certutil -addstore -f -user Root` |
| `ErrGetKeyPair: unknown private key type: EC PARAMETERS` | Khoá CA sinh bằng `openssl ecparam -genkey` (ghi 2 khối PEM) | Dùng `openssl genpkey` -> PKCS#8 một khối |
| Đổi CA xong, `openssl verify` vẫn `error 20` | cert cũ chưa được ký lại — đổi CA KHÔNG tự kích hoạt cấp lại | `kubectl delete secret trading-tls` để ép cấp lại |
| Issuer `ca` báo không thấy Secret | Secret CA nằm khác namespace với Issuer | Issuer là namespaced — để Secret cùng namespace, hoặc dùng ClusterIssuer |
| Chrome vẫn `ERR_CERT_AUTHORITY_INVALID` sau khi cài CA | Chrome cache quyết định TLS | Đóng **hết** cửa sổ Chrome rồi mở lại |
| Cờ `--host-resolver-rules` của Chrome không có tác dụng | Chrome đang chạy nên chỉ mở thêm tab, bỏ qua mọi cờ | Thêm `--user-data-dir` để ép hồ sơ riêng |
| Chrome `This site can't be reached` dù curl cùng đường vẫn OK | Cú pháp `MAP host 127.0.0.1:443` — có `:port` ở vế phải thì không ăn | Bỏ port: `MAP host 127.0.0.1`, và port-forward đúng 443 |
| TLS qua được nhưng `504` | nginx không với được backend | `kubectl get endpoints order-service`; restart controller |
| Cert hợp lệ nhưng client vẫn từ chối | Gọi bằng IP/tên khác SAN | Dùng `--resolve <host>:<port>:<ip>` (mục 6.4) |
