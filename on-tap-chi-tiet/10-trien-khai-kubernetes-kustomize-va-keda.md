# Chương 10 — Kubernetes: Kustomize & KEDA

> **Câu hỏi trung tâm:** làm sao một bộ manifest phục vụ được ba môi trường khác nhau về quy mô và hạ tầng, mà thứ test ở dev vẫn giống về **cấu trúc** với thứ lên prod?
>
> **Tài liệu đối chiếu:** `k8s/README.md`, `.github/workflows/k8s-validate.yml`, `DEEP-DIVE.md` mục 6 & 10.

---

## 1. Nó là gì?

### 1.1. Cấu trúc thư mục

```
k8s/
├── base/                           ← nguồn sự thật chung, KHÔNG apply trực tiếp
│   ├── namespace.yaml
│   ├── limitrange.yaml             ← chặn pod thiếu resource bound
│   ├── resourcequota.yaml          ← trần cứng cho namespace
│   ├── networkpolicy-default-deny.yaml  ← nền zero-trust
│   ├── config/trading.env          ← sinh ConfigMap trading-config
│   ├── infra/                      postgres, kafka, kafka-topics-job,
│   │                               redis, schema-registry, zipkin, netpol
│   └── apps/                       4 Deployment + Service + SA + PDB,
│                                   netpol, ingress
├── components/                     ← bật/tắt theo môi trường
│   ├── keda/                       ScaledObject cho 2 consumer
│   ├── monitoring/                 ServiceMonitor + PrometheusRule
│   ├── in-cluster-prometheus/      Prometheus tối giản (dev)
│   ├── external-kafka/             GỠ Kafka in-cluster
│   └── external-postgres/          GỠ Postgres in-cluster
├── overlays/
│   ├── dev/                        1 pod, 3 partition, trace 100%
│   ├── staging/                    2 pod, 6 partition, KEDA bật
│   └── prod/                       3 pod, 12 partition, digest, ESO, infra ngoài
├── cluster/                        ClusterIssuer, ClusterSecretStore, Strimzi
├── labs/calico/                    bài tập NetworkPolicy
└── legacy/                         manifest cũ, giữ để đối chiếu
```

### 1.2. Ba cơ chế của Kustomize và khi nào dùng cái nào

| Cơ chế | Bản chất | Dùng khi |
|---|---|---|
| **Overlay** | Kế thừa `base` rồi patch | Mỗi môi trường một overlay |
| **Component** | Khối tính năng **bật/tắt được**, có thể dùng ở nhiều overlay | KEDA, monitoring, infra ngoài |
| **Patch** | Sửa một trường cụ thể | Resource, replica, ingress host |

🔑 **Component là thứ phân biệt Kustomize hiện đại với Kustomize "chỉ có overlay".** Một component có thể **thêm** resource *và* **xoá** resource — xem mục 4.2.

```yaml
# k8s/base/kustomization.yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

namespace: trading

resources:
  # --- nền namespace ---
  - namespace.yaml
  - limitrange.yaml
  - resourcequota.yaml
  - networkpolicy-default-deny.yaml
  # --- hạ tầng ---
  - infra/postgres.yaml
  - infra/kafka.yaml
  - infra/kafka-topics-job.yaml
  - infra/redis.yaml
  - infra/schema-registry.yaml
  - infra/zipkin.yaml
  - infra/infra-networkpolicy.yaml
  # --- 4 service nghiệp vụ ---
  - apps/market-gateway.yaml
  - apps/pricing-risk-service.yaml
  - apps/order-service.yaml
  - apps/matching-engine.yaml
  - apps/apps-networkpolicy.yaml
  - apps/ingress.yaml

configMapGenerator:
  # Sinh ra thay vì khai báo tĩnh: mỗi lần đổi config là tên ConfigMap đổi hash
  # -> Deployment đổi -> rolling restart tự động. ConfigMap tĩnh thì pod cứ chạy
  # với giá trị cũ cho tới khi có người restart tay.
  - name: trading-config
    envs:
      - config/trading.env

generatorOptions:
  disableNameSuffixHash: false

labels:
  - includeSelectors: false
    pairs:
      app.kubernetes.io/part-of: trading-system
      app.kubernetes.io/managed-by: kustomize
```

🔑 **`disableNameSuffixHash: false` là cơ chế rollout.** Kustomize thêm hash nội dung vào tên ConfigMap: `trading-config-7fc9b2k4d6`. Đổi một giá trị trong `trading.env` ⇒ hash đổi ⇒ tên ConfigMap đổi ⇒ `envFrom.configMapRef.name` trong Deployment đổi ⇒ **pod template đổi ⇒ rolling restart tự động**.

Không có cơ chế này, `kubectl apply` cập nhật ConfigMap nhưng pod **vẫn chạy với giá trị cũ** — biến môi trường chỉ được đọc một lần lúc container start. Và không có lỗi nào; chỉ là config mới không có tác dụng cho tới khi ai đó restart tay.

💡 **`includeSelectors: false`** — label được thêm vào metadata nhưng **không** vào `spec.selector`. Nếu thêm vào selector, thay đổi label sẽ làm Deployment mất dấu pod cũ của nó (selector là immutable trong Deployment ⇒ `kubectl apply` sẽ fail).

---

## 2. Bảo vệ ở tầng namespace

### 2.1. 🔑 LimitRange — và một quyết định về CPU limit đáng học

```yaml
# k8s/base/limitrange.yaml
# Bảo đảm KHÔNG pod nào lọt vào namespace mà thiếu resource bound, kể cả khi
# một manifest sau này quên khai báo.
apiVersion: v1
kind: LimitRange
metadata:
  name: defaults
  namespace: trading
spec:
  limits:
    - type: Container
      default:
        # Cố ý KHÔNG đặt default CPU limit. CPU limit = CFS throttling: pod cần
        # burst ngắn (JIT warmup, GC, poll Kafka batch lớn) sẽ bị treo nốt chu kỳ
        # 100ms -> đội p99 latency. Với hệ giao dịch latency-sensitive đây là
        # đánh đổi tệ. CPU *request* đã đủ cho scheduler; ResourceQuota chặn tổng.
        # Memory thì ngược lại: không nén được -> luôn phải cap.
        memory: 1Gi
      defaultRequest:
        cpu: 100m
        memory: 256Mi
      max:
        memory: 4Gi
      min:
        cpu: 10m
        memory: 64Mi
    - type: PersistentVolumeClaim
      max: { storage: 200Gi }
      min: { storage: 1Gi }
```

🔑 **Vì sao cố ý không đặt CPU limit** — đây là một trong những quyết định tinh tế nhất của bộ manifest.

CPU limit trong Kubernetes được thực thi bằng **CFS (Completely Fair Scheduler) quota**: mỗi chu kỳ 100ms, container được dùng tối đa `limit × 100ms` CPU-time. Hết quota ⇒ container bị **treo cho tới hết chu kỳ**.

Hậu quả cụ thể với JVM:

| Tình huống cần burst CPU | Nếu có CPU limit |
|---|---|
| JIT compilation lúc warmup | Bị throttle ⇒ warmup kéo dài, request đầu chậm bất thường |
| GC (dù là G1 concurrent) | Bị throttle **giữa chu kỳ GC** ⇒ pause dài hơn dự kiến |
| `poll()` trả về batch 1000 tick | Xử lý bị cắt khúc, kéo dài qua nhiều chu kỳ 100ms |

🔑 Với hệ latency-sensitive, điều này **trực tiếp đội p99**. Và nghịch lý: pod bị throttle trong khi node vẫn còn CPU rảnh — ta tự giới hạn mình.

| | CPU | Memory |
|---|---|---|
| Nén được (compressible)? | ✅ Có — thiếu thì chạy chậm | ❌ Không — thiếu thì **OOMKilled** |
| Cần limit không? | **Không** | **Bắt buộc** |
| Ai kiểm soát tổng? | `requests.cpu` trong ResourceQuota | `limits.memory` |

💡 Và trong manifest của app, `requests.memory == limits.memory`:

```yaml
# k8s/base/apps/order-service.yaml
resources:
  # memory request == limit (không bị evict); CPU không có limit
  # (không bị CFS throttle làm vọt p99).
  requests: { cpu: 250m, memory: 1Gi }
  limits:   { memory: 1Gi }
```

`request == limit` cho memory ⇒ pod thuộc QoS class **Guaranteed** ⇒ là nhóm **cuối cùng** bị evict khi node thiếu bộ nhớ. Với `request < limit` (Burstable), pod có thể bị evict khi node áp lực — không chấp nhận được cho service giao dịch.

### 2.2. ResourceQuota — và một cạm bẫy ít người biết

```yaml
# k8s/base/resourcequota.yaml
# Trần cứng cho cả namespace: KEDA/HPA có lỗi cũng không ăn hết cluster.
# LƯU Ý: liệt kê resource nào ở đây thì resource đó thành BẮT BUỘC trên mọi pod.
# `limits.cpu` cố ý bị bỏ ra để workload được phép chạy không có CPU limit
# (xem giải thích chống throttling ở limitrange.yaml).
apiVersion: v1
kind: ResourceQuota
metadata:
  name: namespace-quota
  namespace: trading
spec:
  hard:
    requests.cpu: "8"
    requests.memory: 16Gi
    limits.memory: 32Gi
    persistentvolumeclaims: "8"
    requests.storage: 120Gi
    pods: "50"
    services.loadbalancers: "1"
    services.nodeports: "0"      # không expose NodePort trong namespace này
```

🔑 **Cạm bẫy:** nếu `spec.hard` liệt kê `limits.cpu`, thì **mọi pod trong namespace bắt buộc phải khai `limits.cpu`** — pod nào thiếu sẽ bị API server từ chối. Nghĩa là liệt kê `limits.cpu` ở đây sẽ **mâu thuẫn trực tiếp** với quyết định không đặt CPU limit ở LimitRange. Hai file này phải nhất quán, và comment ở cả hai nơi nói rõ điều đó.

💡 `services.nodeports: "0"` — chặn hẳn việc ai đó tạo NodePort trong namespace này. NodePort mở một cổng trên **mọi node** của cluster, bỏ qua Ingress và bỏ qua mọi chính sách ở đó.

### 2.3. Zero-trust network

```yaml
# k8s/base/networkpolicy-default-deny.yaml
# Nền zero-trust: trong namespace này không gì nói chuyện được với gì, trừ khi
# có policy cho phép tường minh. Đây là biện pháp an ninh giá trị nhất của cả
# bộ manifest - một pod bị chiếm quyền không tự ý gọi ra ngoài được.
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: default-deny-all
  namespace: trading
spec:
  podSelector: {}
  policyTypes: [Ingress, Egress]
---
# DNS phải mở cho tất cả, nếu không mọi tên service đều không phân giải được.
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: allow-dns-egress
  namespace: trading
spec:
  podSelector: {}
  policyTypes: [Egress]
  egress:
    - to:
        - namespaceSelector:
            matchLabels: { kubernetes.io/metadata.name: kube-system }
          podSelector:
            matchLabels: { k8s-app: kube-dns }
      ports:
        - { protocol: UDP, port: 53 }
        - { protocol: TCP, port: 53 }
```

🔑 **`podSelector: {}` + `policyTypes: [Ingress, Egress]` với mảng rule rỗng = chặn hết.** Mọi kết nối phải được cho phép tường minh bởi một policy khác. NetworkPolicy là **additive** — nhiều policy cùng áp lên một pod thì kết quả là **hợp** của các rule cho phép.

🔑 **DNS phải mở trước tiên.** Đây là lỗi số một khi mới dùng default-deny: quên mở DNS ⇒ mọi tên service không phân giải được ⇒ ứng dụng lỗi `UnknownHostException` và người ta đi tìm nguyên nhân ở Kafka, ở Postgres, ở mọi nơi trừ DNS.

Chi tiết chính sách mạng ở [chương 12](./12-bao-mat-he-thong.md). Một ghi chú quan trọng:

```yaml
# k8s/base/apps/apps-networkpolicy.yaml
# LƯU Ý: probe của kubelet xuất phát từ NODE, không đi qua NetworkPolicy, nên
# liveness/readiness vẫn chạy dù không có rule ingress nào cho chúng.
```

---

## 3. Deployment — hardening từng lớp

```yaml
# k8s/base/apps/order-service.yaml (trích)
spec:
  replicas: 2
  revisionHistoryLimit: 5
  minReadySeconds: 15
  progressDeadlineSeconds: 600
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxSurge: 1
      maxUnavailable: 0          # không bao giờ tụt dưới số replica mong muốn
```

| Trường | Giá trị | Vì sao |
|---|---|---|
| `maxUnavailable: 0` | | 🔑 Rollout **tạo pod mới trước**, pod cũ chỉ bị xoá sau khi pod mới Ready. Không bao giờ tụt dưới số replica mong muốn. Bắt buộc với service duy nhất nhận traffic từ Ingress. |
| `minReadySeconds: 15` | | Pod phải Ready **liên tục 15 giây** mới được tính là available. Chặn trường hợp pod Ready rồi crash ngay — rollout sẽ dừng lại thay vì thay hết pod bằng pod hỏng. |
| `progressDeadlineSeconds: 600` | | Rollout không tiến triển trong 10 phút ⇒ đánh dấu thất bại. Không có nó, rollout treo vô hạn. |
| `revisionHistoryLimit: 5` | | Giữ 5 ReplicaSet cũ cho `kubectl rollout undo`. |

### 3.1. Security context

```yaml
securityContext:                   # cấp Pod
  runAsNonRoot: true
  runAsUser: 10001
  runAsGroup: 10001
  fsGroup: 10001
  seccompProfile: { type: RuntimeDefault }
...
  securityContext:                 # cấp Container
    allowPrivilegeEscalation: false
    readOnlyRootFilesystem: true
    capabilities: { drop: ["ALL"] }
  volumeMounts:
    - { name: tmp, mountPath: /tmp }
volumes:
  - name: tmp
    emptyDir: { medium: Memory, sizeLimit: 128Mi }
```

| Thiết lập | Chặn được gì |
|---|---|
| `runAsNonRoot` + UID 10001 | Container escape thành root trên node |
| `allowPrivilegeEscalation: false` | `setuid` binary leo thang quyền |
| `readOnlyRootFilesystem: true` | 🔑 Kẻ tấn công ghi webshell / sửa binary |
| `capabilities: drop ALL` | Mọi Linux capability (`NET_RAW` cho packet sniffing, `SYS_PTRACE`, …) |
| `seccompProfile: RuntimeDefault` | Hầu hết syscall nguy hiểm |
| `automountServiceAccountToken: false` | 🔑 Token API server bị dùng để gọi Kubernetes API |

🔑 **`readOnlyRootFilesystem: true` buộc phải mount `/tmp`** — JVM cần ghi vào đó (hsperfdata, temp file của một số thư viện). `emptyDir: { medium: Memory }` làm nó thành tmpfs (RAM), có `sizeLimit` để không ăn hết bộ nhớ node.

💡 `automountServiceAccountToken: false` được đặt **hai lần** (ở ServiceAccount và ở Pod spec) — thừa có chủ đích, phòng trường hợp một patch sau này ghi đè một trong hai. Bốn service này **không gọi Kubernetes API**, nên token chỉ là bề mặt tấn công thuần tuý.

### 3.2. Topology spread

```yaml
# Trải pod ra zone trước rồi tới node. Mất một node hoặc một AZ không
# được làm đứt consumer group.
topologySpreadConstraints:
  - maxSkew: 1
    topologyKey: topology.kubernetes.io/zone
    whenUnsatisfiable: ScheduleAnyway
    labelSelector:
      matchLabels: { app.kubernetes.io/name: order-service }
  - maxSkew: 1
    topologyKey: kubernetes.io/hostname
    whenUnsatisfiable: ScheduleAnyway
    labelSelector:
      matchLabels: { app.kubernetes.io/name: order-service }
```

`base` dùng `ScheduleAnyway` (mềm — cluster dev một node vẫn schedule được). Prod siết thành `DoNotSchedule` bằng **JSON patch**:

```yaml
# k8s/overlays/prod/kustomization.yaml
# Strategic merge sẽ NỐI THÊM vào topologySpreadConstraints chứ không thay
# mục zone, để lại hai luật mâu thuẫn trên cùng topologyKey. JSON patch sửa
# đúng một trường.
#
# DoNotSchedule ở prod: thà pod Pending còn hơn dồn cả consumer group vào
# một AZ rồi mất sạch khi AZ đó sự cố.
- target:
    group: apps
    version: v1
    kind: Deployment
    name: "pricing-risk-service|order-service|matching-engine"
  patch: |-
    - op: replace
      path: /spec/template/spec/topologySpreadConstraints/0/whenUnsatisfiable
      value: DoNotSchedule
```

🔑 **Vì sao phải dùng JSON patch chứ không strategic merge** — đây là một trong những bài học Kustomize giá trị nhất. `topologySpreadConstraints` là một **list không có merge key** được khai báo trong Kubernetes API. Strategic merge patch sẽ **nối thêm** phần tử mới thay vì thay thế phần tử cùng `topologyKey`, để lại **hai constraint mâu thuẫn trên cùng một `topologyKey`** — và scheduler sẽ xử lý theo cách khó đoán. JSON patch với `path` chỉ đúng một trường thì không có mơ hồ nào.

### 3.3. Graceful shutdown — ba con số phải cộng khớp

```yaml
terminationGracePeriodSeconds: 60
...
lifecycle:
  preStop:
    sleep: { seconds: 5 }
```

```yaml
# order-service/application.yml
server:
  shutdown: graceful
spring:
  lifecycle:
    timeout-per-shutdown-phase: 30s
```

```
preStop (5s) + timeout-per-shutdown-phase (30s) = 35s  ≤  terminationGracePeriodSeconds (60s)  ✅
```

**Trình tự khi pod bị xoá:**

| Bước | Diễn ra |
|:--:|---|
| 1 | Kubernetes gỡ pod khỏi endpoint Service **và** gửi `SIGTERM` — **song song**, không theo thứ tự |
| 2 | `preStop: sleep 5` giữ container sống thêm 5s để kube-proxy/ingress kịp cập nhật bảng định tuyến |
| 3 | `SIGTERM` → `server.shutdown: graceful` ngừng nhận request mới, xử lý nốt request đang chạy |
| 4 | Kafka listener (`SmartLifecycle`) dừng poll, xử lý nốt batch, commit offset cuối |
| 5 | Quá 30s chưa xong ⇒ Spring cưỡng chế dừng |
| 6 | Quá 60s container chưa thoát ⇒ `SIGKILL` |

🔑 **Bước 2 là thứ nhiều người bỏ sót.** Vì bước 1 xảy ra song song, có một khoảng vài trăm ms tới vài giây mà pod đã nhận `SIGTERM` nhưng kube-proxy/ingress vẫn gửi request tới. `preStop: sleep` lấp đúng khoảng đó.

⚠️ **Gap #20:** `market-gateway` và `pricing-risk-service` **thiếu** `server.shutdown: graceful` và `timeout-per-shutdown-phase`. Kafka listener vẫn dừng êm nhờ `SmartLifecycle`, nhưng **Tomcat đóng ngay** khi nhận `SIGTERM` ⇒ request REST đang bay bị cắt — đúng thứ mà `preStop: sleep 5` vừa cất công bảo vệ.

### 3.4. Probe và PDB

```yaml
startupProbe:
  # Rộng tay: JVM nguội + Flyway migration có thể mất vài chục giây.
  httpGet: { path: /actuator/health/liveness, port: http }
  periodSeconds: 5
  failureThreshold: 36
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
  failureThreshold: 6
```

Chi tiết về triết lý probe ở [chương 08](./08-observability-metrics-tracing-va-alerting.md) mục 5.

```yaml
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: order-service
spec:
  # Drain node khi nâng cấp cluster không được rút sạch consumer của một group
  # -> dừng xử lý lệnh giữa phiên giao dịch.
  #
  # maxUnavailable (không phải minAvailable): đúng ở MỌI số replica.
  # minAvailable: 1 với deployment 1 replica sẽ làm `kubectl drain` treo mãi;
  # còn với 3 replica thì lại cho phép rút tới 2 pod cùng lúc.
  maxUnavailable: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: order-service
```

🔑 **`maxUnavailable: 1` thay vì `minAvailable: 1`** — lý do ghi rõ trong comment và rất đáng nhớ:

| Số replica | `minAvailable: 1` | **`maxUnavailable: 1`** |
|:--:|---|---|
| 1 | 🔴 `kubectl drain` **treo vĩnh viễn** (không thể rút pod duy nhất mà vẫn giữ 1) | ✅ Cho rút 1 pod |
| 3 | 🔴 Cho rút tới **2 pod** cùng lúc | ✅ Chỉ cho rút 1 |
| N | Hành vi đổi theo N | ✅ **Luôn đúng** |

PDB chỉ ràng buộc **voluntary disruption** (drain, eviction). Node chết đột ngột hoặc `kubectl delete pod` thì PDB không can thiệp.

---

## 4. Ba môi trường từ một base

### 4.1. Bảng so sánh

| | **dev** | **staging** | **prod** |
|---|---|---|---|
| Namespace | `trading-dev` | `trading-staging` | `trading-prod` |
| `KAFKA_PARTITIONS` | **3** | **6** | **12** |
| Trần pod consumer (`/3`) | 1 | 2 | 4 |
| `KAFKA_TOPIC_REPLICAS` | 1 | 1 | 3 |
| Image | `trading/*:dev` (build tại chỗ) | `ghcr.io/*:main` (tag di động) | **`@sha256:...` (digest)** |
| Replica (gateway/pricing/order/matching) | 1/1/1/1 | 1/1/2/1 | 1/2/3/2 |
| KEDA | ❌ | ✅ | ✅ |
| Monitoring | Prometheus tối giản | ServiceMonitor (Operator) | ServiceMonitor (Operator) |
| Kafka | StatefulSet in-cluster | StatefulSet in-cluster | **Managed (MSK/Confluent)** |
| Postgres | StatefulSet in-cluster | StatefulSet in-cluster | **Managed (RDS/Cloud SQL)** |
| Secret | `secretGenerator` từ `secrets.env` | `secretGenerator` từ `secrets.env` | **ESO (External Secrets)** |
| `TRACING_PROBABILITY` | **1.0** | 0.3 | **0.05** |
| `SIMULATED_FEED` | `true` | `true` | **`false`** |
| Log level | `DEBUG` | `INFO` | `WARN` (root) / `INFO` (com.trading) |
| `topologySpread` | ScheduleAnyway | ScheduleAnyway | **DoNotSchedule** |
| PriorityClass | ❌ | ❌ | ✅ `trading-critical` |
| Ingress path `/api/v1/ticks` | ✅ | ✅ | **gỡ bỏ** |

### 4.2. 🔑 Component gỡ hạ tầng — kỹ thuật hay nhất của chương

```yaml
# k8s/components/external-postgres/kustomization.yaml
apiVersion: kustomize.config.k8s.io/v1alpha1
kind: Component

# Prod thật chạy database NGOÀI cluster (RDS / Cloud SQL / CloudNativePG ở
# namespace riêng): backup, PITR, failover, patch version là việc của managed
# service chứ không phải của một StatefulSet 1 replica.
#
# Bật component này rồi đặt DB_HOST (và DB_PORT/DB_NAME nếu khác) trong
# configMapGenerator của overlay.
resources:
  - allow-external-db-egress.yaml

patches:
  - path: delete-sts.yaml
    target: { kind: StatefulSet, name: postgres }
  - path: delete-svc.yaml
    target: { kind: Service, name: postgres }
  - path: delete-pdb.yaml
    target: { kind: PodDisruptionBudget, name: postgres }
  - path: delete-netpol.yaml
    target: { kind: NetworkPolicy, name: postgres }
```

🔑 **Component vừa thêm resource (`allow-external-db-egress.yaml`) vừa XOÁ resource** (bốn patch `$patch: delete`). Đây là năng lực mà overlay thuần không có một cách gọn gàng.

Nhờ vậy, `base` có thể khai báo Postgres in-cluster cho dev/staging, còn prod **gỡ nó đi hoàn toàn** chỉ bằng một dòng trong `components:` — không phải duy trì hai bộ manifest song song.

```yaml
# k8s/overlays/prod/kustomization.yaml
components:
  - ../../components/keda
  - ../../components/monitoring
  # Prod KHÔNG chạy Kafka/Postgres bằng StatefulSet 1 replica tự viết.
  # Hai component này gỡ chúng đi và mở đường ra dịch vụ quản trị bên ngoài.
  - ../../components/external-kafka
  - ../../components/external-postgres
```

### 4.3. Digest pinning ở prod

```yaml
# k8s/overlays/prod/kustomization.yaml
images:
  # Ghim theo DIGEST, không theo tag. Tag là MUTABLE: cùng một manifest ngày mai
  # có thể roll ra bits khác. Digest làm bản deploy tái lập được và đây chính là
  # thứ CI ghi đè ở mỗi lần release.
  - name: ghcr.io/trading-system/market-gateway
    digest: sha256:0000000000000000000000000000000000000000000000000000000000000000
  ...
```

🔑 **Tag là mutable.** `ghcr.io/x/order-service:v1.2.3` hôm nay và ngày mai có thể là hai image khác nhau — ai đó push đè lên tag đó. Hệ quả: cùng một manifest trong Git, hai lần deploy ra hai bits khác nhau. Digest `@sha256:...` là **content-addressed** — cùng digest thì vĩnh viễn cùng bits.

Và vì digest bất biến, cache image trở nên an toàn tuyệt đối:

```yaml
# k8s/overlays/prod/patch-apps.yaml
containers:
  - name: app
    # Digest-pinned nên cache image là an toàn tuyệt đối: cùng digest
    # thì luôn là cùng bits.
    imagePullPolicy: IfNotPresent
```

### 4.4. Secret — ba cách, ba môi trường

```yaml
# dev & staging
secretGenerator:
  - name: trading-secrets
    envs:
      - secrets.env    # CỐ Ý không có trong repo: thiếu thì build FAIL ngay,
                       # tốt hơn là deploy âm thầm với một mật khẩu placeholder.
                       # cp secrets.env.example secrets.env rồi điền.
```

```yaml
# prod
# CỐ Ý không có secretGenerator. Mật khẩu production không nằm trong git, kể cả
# dạng mã hoá mà ai đó giải được ở máy cá nhân. Dùng External Secrets Operator
# (externalsecret.yaml) hoặc Sealed Secrets.
```

🔑 **"Thiếu thì build FAIL ngay, tốt hơn là deploy âm thầm với placeholder"** — một nguyên tắc rất đáng áp dụng rộng. `secrets.env` bị `.gitignore`; nếu thiếu, `kustomize build` thất bại với lỗi rõ ràng. Phương án tệ hơn sẽ là có một giá trị mặc định — khi đó môi trường deploy thành công với mật khẩu `changeme` và không ai biết.

CI xử lý điều này bằng cách tạo file giả:

```yaml
# .github/workflows/k8s-validate.yml:29-36
# secrets.env bi gitignore nen CI khong co -> tao ban gia de build chay duoc.
# Gia tri khong quan trong: ta chi kiem tra manifest hop le.
- name: Tao secrets.env gia
  if: matrix.overlay != 'prod'
  run: |
    cp k8s/overlays/${{ matrix.overlay }}/secrets.env.example \
       k8s/overlays/${{ matrix.overlay }}/secrets.env
    sed -i 's/^\(DB_PASSWORD\|ORDER_API_KEY\)=$/\1=ci-placeholder/' \
       k8s/overlays/${{ matrix.overlay }}/secrets.env
```

---

## 5. 🔑 KEDA — scale theo đúng tín hiệu

### 5.1. Vì sao CPU là tín hiệu sai

```yaml
# k8s/components/keda/scaledobject-pricing-risk.yaml
# KEDA thay cho HPA-theo-CPU, vì với Kafka consumer CPU là tín hiệu SAI ở cả
# hai chiều:
#   - nghẽn I/O (chờ DB, chờ downstream ack): CPU thấp, lag hàng triệu
#     -> HPA ngồi yên trong khi hệ đang tụt hậu nghiêm trọng.
#   - GC nặng hoặc REST traffic cao: CPU cao, lag bằng 0 -> HPA scale vô ích.
# KEDA query thẳng Kafka: end offset của partition trừ committed offset của
# consumer group = lag. Đó đúng là đại lượng cần phản ứng.
```

| Tình huống | CPU | Lag | HPA-CPU làm gì | Đúng ra phải làm gì |
|---|:--:|:--:|---|---|
| Nghẽn I/O (chờ DB/ack) | Thấp | **Hàng triệu** | 😴 Ngồi yên | **Scale lên** |
| GC nặng / REST traffic cao | **Cao** | 0 | 📈 Scale lên | Không làm gì |
| Tải tick tăng thật | Cao | Tăng | 📈 Scale lên ✅ | Scale lên ✅ |

🔑 CPU chỉ đúng ở **một trong ba** tình huống. Lag đúng ở cả ba, vì nó đo **trực tiếp thứ cần phản ứng**: hệ đang tụt hậu bao nhiêu.

$$\text{lag} = \sum_{p \in \text{partitions}} (\text{end offset}_p - \text{committed offset}_p)$$

### 5.2. ScaledObject chi tiết

```yaml
# k8s/components/keda/scaledobject-pricing-risk.yaml
# maxReplicaCount KHÔNG được vượt partition / concurrency(3) - xem
# `setConcurrency(3)` trong pricing/config/KafkaConfig.java. Prod dùng
# 12 partition -> 4. Pod thứ 5 join group và nhận về KHÔNG partition nào.
# Muốn scale cao hơn phải TĂNG PARTITION trước (Job kafka-topics-init).
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: pricing-risk-service
  namespace: trading
spec:
  scaleTargetRef:
    name: pricing-risk-service
  minReplicaCount: 2
  maxReplicaCount: 4
  advanced:
    horizontalPodAutoscalerConfig:
      behavior:
        scaleDown:
          # ĐÂY mới là thứ chống flapping. `cooldownPeriod` của KEDA CHỈ áp cho
          # trường hợp co về 0 replica, mà min ở đây là 2 nên nó vô tác dụng.
          stabilizationWindowSeconds: 300
  triggers:
    - type: kafka
      metadata:
        # Đọc từ env của chính Deployment đích (qua envFrom trading-config),
        # nên không phải nhắc lại địa chỉ Kafka ở đây - một nguồn sự thật.
        bootstrapServersFromEnv: KAFKA_BOOTSTRAP
        consumerGroup: pricing-risk-group
        topic: stock.ticks
        # NGỮ NGHĨA: lagThreshold là lag TRÊN MỖI REPLICA, không phải mỗi
        # partition. KEDA tính desiredReplicas = ceil(tổng_lag / lagThreshold).
        # 1000 ở đây nghĩa là: tổng lag 4000 -> 4 pod.
        lagThreshold: "1000"
        # CHỈ có tác dụng cho ngưỡng kích hoạt 0 -> 1. Với minReplicaCount >= 1
        # giá trị này không bao giờ được dùng tới; giữ lại để nếu sau này đổi
        # sang scale-to-zero thì đã có sẵn con số đúng.
        activationLagThreshold: "100"
```

**Ba hiểu lầm phổ biến mà repo đã sửa** (xem `DEEP-DIVE.md` mục 11, các điểm #7-#9):

| # | Hiểu lầm | Sự thật |
|:--:|---|---|
| 7 | `lagThreshold` là lag **mỗi partition** | 🔑 Là lag **mỗi replica**. `desiredReplicas = ceil(tổng_lag / lagThreshold)`. Tổng lag 4000 với threshold 1000 ⇒ 4 pod. |
| 8 | `cooldownPeriod` chống flapping | 🔑 Nó **chỉ** áp cho trường hợp co về **0** replica. Với `minReplicaCount >= 1` nó vô tác dụng. Thứ chống flapping là `behavior.scaleDown.stabilizationWindowSeconds`. |
| 9 | `activationLagThreshold` là ngưỡng kích hoạt scale | 🔑 Chỉ dùng cho ngưỡng **0 → 1**. Với `minReplicaCount >= 1` không bao giờ được dùng. |

💡 **`bootstrapServersFromEnv: KAFKA_BOOTSTRAP`** — KEDA đọc biến môi trường của chính Deployment đích (được nạp qua `envFrom: trading-config`). Nhờ vậy địa chỉ Kafka chỉ khai một nơi. Nếu hardcode địa chỉ trong ScaledObject, overlay prod đổi `KAFKA_BOOTSTRAP` mà quên sửa ScaledObject ⇒ KEDA query sai cluster ⇒ lag luôn bằng 0 ⇒ **không bao giờ scale**, và không có lỗi nào.

Ngưỡng khác nhau giữa hai consumer:

```yaml
# k8s/components/keda/scaledobject-matching-engine.yaml
# Cùng nguyên lý với pricing-risk (xem chú thích chi tiết ở file đó), nhưng
# ngưỡng THẤP HƠN: một lệnh chờ khớp nhạy cảm hơn một tick giá trễ.
        lagThreshold: "500"
...
        scaleDown:
          # Co lại chậm hơn giãn ra: rebalance consumer group làm dừng xử lý
          # vài giây, nên đừng co ngay khi lag vừa hạ.
          stabilizationWindowSeconds: 120
```

🔑 **"Co lại chậm hơn giãn ra"** là nguyên tắc chung cho autoscale consumer group: mỗi lần số pod đổi là một lần rebalance, và rebalance làm dừng xử lý vài giây. Giãn ra nhanh (vì đang tụt hậu), co lại chậm (vì co nhầm còn tốn hơn giữ thừa một pod).

### 5.3. Bộ ba con số phải khớp nhau

$$\text{maxReplicaCount} \;\le\; \frac{\text{KAFKA\_PARTITIONS}}{\text{setConcurrency}}$$

Ba con số ở **ba file khác nhau**:

| Con số | File |
|---|---|
| `KAFKA_PARTITIONS` | `k8s/base/config/trading.env` + overlay |
| `setConcurrency(3)` | `pricing-risk-service/config/KafkaConfig.java:66` |
| `maxReplicaCount` | `k8s/components/keda/scaledobject-*.yaml` |

Không có gì trong runtime giữ chúng khớp nhau. Repo bịt bằng CI:

```yaml
# .github/workflows/k8s-validate.yml:59-78
# DEEP-DIVE.md #14: `maxReplicaCount` phai khop `partition / concurrency(3)`
# nhung hai con so nam o hai file khac nhau, khong gi cuong che. Cuong che o day.
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

💡 Đây là mẫu rất đáng học: **khi một ràng buộc đúng đắn trải qua nhiều file, hãy cưỡng chế nó ở CI.** Comment trong mã chỉ nhắc được người đang mở đúng file đó.

⚠️ Mặt trái: `concurrency = 3` bị **hardcode** trong CI (`P / 3`). Đổi `setConcurrency(4)` trong Java thì CI vẫn chia cho 3 và vẫn pass. Ràng buộc được bịt một nửa — và đây là ví dụ sống cho nguyên tắc *"ràng buộc giữa hai con số ở hai file khác nhau thì sớm muộn cũng lệch"*.

### 5.4. ⚠️ `replicas` trong Git đấu với autoscaler (Ops #21)

```yaml
# k8s/overlays/staging/kustomization.yaml
replicas:
  - { name: market-gateway, count: 1 }        # 1 feed session - xem chú thích ở base
  # pricing/matching do KEDA quản: giữ bằng minReplicaCount để mỗi lần
  # `kubectl apply` không kéo ngược số replica mà KEDA vừa đặt.
  - { name: pricing-risk-service, count: 1 }
  - { name: matching-engine, count: 1 }
  - { name: order-service, count: 2 }         # cửa ngõ REST -> luôn >= 2
```

Vấn đề: `spec.replicas` nằm trong Git **và** được KEDA/HPA điều khiển. Mỗi `kubectl apply` sẽ ghi lại giá trị từ Git, kéo ngược số replica mà autoscaler vừa đặt.

Đặt `replicas == minReplicaCount` chỉ **giảm xác suất** giằng co, không loại bỏ: apply đúng lúc KEDA đang giữ 4 pod vẫn kéo về 1 ⇒ KEDA lại đẩy lên 4 ⇒ **2 lần rebalance** cho một thao tác apply vô hại.

Cách sửa triệt để (khi chốt công cụ GitOps): **bỏ hẳn `replicas` khỏi overlay** và khai `ignoreDifferences` trên `/spec/replicas`:

```yaml
# Argo CD Application
spec:
  ignoreDifferences:
    - group: apps
      kind: Deployment
      jsonPointers:
        - /spec/replicas
```

---

## 6. CI validate — bắt lỗi trước khi lên cluster

```yaml
# .github/workflows/k8s-validate.yml
# Build cả ba overlay trên mỗi PR. Một patch sai `target`, một file bị đổi tên,
# một tham chiếu ConfigMap gãy - tất cả hỏng ở đây chứ không hỏng ở cluster.
name: k8s-validate

on:
  pull_request:
    paths: ["k8s/**", ".github/workflows/k8s-validate.yml"]
  push:
    branches: [main]
    paths: ["k8s/**"]

jobs:
  kustomize-build:
    strategy:
      fail-fast: false
      matrix:
        overlay: [dev, staging, prod]
```

Bốn bước kiểm tra:

#### (1) `kustomize build` — bắt lỗi cấu trúc

```yaml
- name: kustomize build
  run: kustomize build k8s/overlays/${{ matrix.overlay }} > /tmp/${{ matrix.overlay }}.yaml
```

Bắt: patch sai `target`, file bị đổi tên, tham chiếu ConfigMap gãy, component không tồn tại.

#### (2) `kubeconform` — bắt lỗi schema

```yaml
# kubeconform kiem tra schema. CRD cua KEDA/Prometheus Operator/ESO/cert-manager
# khong co trong schema chuan -> lay tu catalog cua datreeio.
- name: Kiem tra schema
  run: |
    kubeconform -strict -summary \
      -schema-location default \
      -schema-location 'https://raw.githubusercontent.com/datreeio/CRDs-catalog/main/{{.Group}}/{{.ResourceKind}}_{{.ResourceAPIVersion}}.json' \
      /tmp/${{ matrix.overlay }}.yaml
```

💡 `-strict` từ chối trường không có trong schema — bắt được lỗi gõ sai tên trường (`maxUnavailble` thay vì `maxUnavailable`), thứ mà `kubectl apply` sẽ **im lặng bỏ qua**.

#### (3) Prod phải ghim digest

```yaml
- name: Prod phai ghim image theo digest
  if: matrix.overlay == 'prod'
  run: |
    # Tag la mutable: cung manifest, ngay mai ra bits khac. Chan tu CI.
    # Dang digest cung chua dau ':' (@sha256:...) nen phai loai rieng.
    if grep -E '^\s+image: ghcr\.io/' /tmp/prod.yaml | grep -v '@sha256:'; then
      echo "::error::overlay prod dang dung TAG, phai dung @sha256:"
      exit 1
    fi
    echo "ok - moi image ung dung deu ghim theo digest"
```

#### (4) Ràng buộc KEDA ↔ partition — mục 5.3

🔑 **Bốn bước này biến "quy ước trong tài liệu" thành "ràng buộc chặn merge".** Đó là khác biệt giữa một tài liệu nói *nên làm* và một hệ thống *bắt phải làm*.

---

## 7. Đánh đổi

### 7.1. Ưu

1. **Một base, ba môi trường** — dev test đúng **cấu trúc** của prod.
2. **Component bật/tắt được**, kể cả **gỡ** resource.
3. **ConfigMap hash ⇒ rollout tự động.**
4. **KEDA scale theo lag** — tín hiệu đúng.
5. **Digest pinning** ⇒ deploy tái lập được.
6. **Secret không nằm trong Git**; thiếu thì fail ngay.
7. **Zero-trust mạng** làm nền.
8. **Hardening nhiều lớp** — nonroot, readonly rootfs, drop ALL, seccomp, no SA token.
9. **CI cưỡng chế ràng buộc liên-file.**
10. **Không CPU limit** ⇒ không CFS throttle đội p99.

### 7.2. Nhược

1. **Độ phức tạp cao** — phải hiểu overlay, component, patch, generator, JSON patch vs strategic merge.
2. ⚠️ **`replicas` trong Git đấu với autoscaler** (Ops #21).
3. ⚠️ **ESO không có hash** ⇒ secret xoay xong không pod nào restart (Gap #19).
4. ⚠️ **Xoay mật khẩu DB ở dev/staging có bẫy `initdb`** (Bug #18).
5. ⚠️ **`concurrency = 3` hardcode trong CI** — ràng buộc bịt một nửa.
6. ⚠️ **Partition khác nhau giữa môi trường phá tính "giống nhau"** (Gap #22).
7. ⚠️ **Thiếu graceful shutdown ở 2 service** (Gap #20).
8. **Trần scale cứng** = partition/concurrency.
9. **Không có `kubeval`/OPA/Kyverno** — chỉ kiểm schema, chưa kiểm chính sách.

### 7.3. Hai cạm bẫy vận hành cần biết

⚠️ **Bug #18 — xoay mật khẩu DB ở dev/staging.**

`POSTGRES_PASSWORD` **chỉ có tác dụng lúc `initdb`** (lần đầu khởi tạo volume). Sửa `secrets.env` ⇒ hash Secret đổi ⇒ restart cả Postgres lẫn 3 service ⇒ **ứng dụng dùng mật khẩu mới, DB giữ mật khẩu cũ** ⇒ sập cả môi trường. Và `wait-for-db` vẫn pass (nó dùng `pg_isready` không cần xác thực đầy đủ), nên triệu chứng chỉ lộ ở tầng ứng dụng.

Quy trình đúng:

```bash
# 1. Đổi mật khẩu TRONG DB trước
kubectl -n trading-dev exec sts/postgres -- \
  psql -U trading -c "ALTER ROLE trading WITH PASSWORD 'matkhaumoi';"

# 2. RỒI mới sửa secrets.env và apply
sed -i 's/^DB_PASSWORD=.*/DB_PASSWORD=matkhaumoi/' k8s/overlays/dev/secrets.env
kubectl apply -k k8s/overlays/dev
```

Triệt để: tách Secret của Postgres khỏi Secret của ứng dụng, hoặc dùng operator (CloudNativePG) biết xoay mật khẩu.

⚠️ **Gap #19 — ESO cập nhật secret nhưng không pod nào restart.**

Secret do External Secrets Operator dựng **không có hash** (nó không qua `secretGenerator`). ESO cập nhật xong thì Secret trong cluster đổi, nhưng `env.valueFrom.secretKeyRef` **chỉ đọc một lần lúc container start** ⇒ ứng dụng vẫn dùng mật khẩu cũ tới lần restart ngẫu nhiên sau đó.

Trước mắt: `kubectl rollout restart` thủ công sau mỗi lần xoay. Triệt để: Reloader (stakater) watch Secret và tự restart, hoặc **mount Secret thành volume** thay vì env — volume được kubelet cập nhật tự động (tuy có độ trễ tới ~1 phút).

---

## 8. Cách kiểm chứng

```bash
# === Build cục bộ, không apply ===
kustomize build k8s/overlays/dev     | head -50
kustomize build k8s/overlays/prod    > /tmp/prod.yaml
grep -c 'kind:' /tmp/prod.yaml

# Prod có digest không?
grep -E '^\s+image: ghcr\.io/' /tmp/prod.yaml

# Prod có thật sự GỠ Postgres/Kafka in-cluster không?
grep -E 'kind: StatefulSet' /tmp/prod.yaml    # -> rỗng

# Partition của từng overlay
for o in dev staging prod; do
  echo -n "$o: "
  kustomize build k8s/overlays/$o | grep -E '^\s+KAFKA_PARTITIONS:' | head -1
done

# Kiểm chứng công thức trần pod
for o in dev staging prod; do
  P=$(kustomize build k8s/overlays/$o | grep -E '^\s+KAFKA_PARTITIONS:' | head -1 | tr -d ' "' | cut -d: -f2)
  M=$(kustomize build k8s/overlays/$o | grep -E '^\s+maxReplicaCount:' | head -1 | tr -d ' ' | cut -d: -f2)
  echo "$o: partitions=$P tran=$((P/3)) maxReplica=${M:-'(khong co KEDA)'}"
done

# Schema
kubeconform -strict -summary -schema-location default \
  -schema-location 'https://raw.githubusercontent.com/datreeio/CRDs-catalog/main/{{.Group}}/{{.ResourceKind}}_{{.ResourceAPIVersion}}.json' \
  /tmp/prod.yaml

# So sánh hai overlay
diff <(kustomize build k8s/overlays/staging) <(kustomize build k8s/overlays/prod) | head -60

# === Deploy dev lên minikube ===
eval $(minikube docker-env)
for m in market-gateway pricing-risk-service order-service matching-engine; do
  docker build --build-arg MODULE=$m -t trading/$m:dev .
done
cp k8s/overlays/dev/secrets.env.example k8s/overlays/dev/secrets.env
# điền DB_PASSWORD và ORDER_API_KEY
kubectl apply -k k8s/overlays/dev

# === Quan sát ===
kubectl -n trading-dev get pods -w
kubectl -n trading-dev get all
kubectl -n trading-dev describe quota namespace-quota
kubectl -n trading-dev get limitrange defaults -o yaml
kubectl -n trading-dev logs job/kafka-topics-init

# ConfigMap hash
kubectl -n trading-dev get cm | grep trading-config
# -> trading-config-7fc9b2k4d6   (hash ở cuối)

# Chứng minh cơ chế hash: đổi một giá trị rồi apply
sed -i 's/TRACING_PROBABILITY=1.0/TRACING_PROBABILITY=0.5/' k8s/base/config/trading.env
kubectl apply -k k8s/overlays/dev
kubectl -n trading-dev get cm | grep trading-config     # -> hash MỚI
kubectl -n trading-dev rollout status deploy/order-service   # -> đang rolling restart

# === NetworkPolicy ===
kubectl -n trading-dev get networkpolicy
# Thử kết nối bị chặn
kubectl -n trading-dev run test --rm -it --image=busybox --restart=Never -- \
  wget -T 3 -O- http://order-service:8083/actuator/health
# -> timeout (pod test không có label component nên không được policy cho phép)

# === PDB ===
kubectl -n trading-dev get pdb
kubectl drain <node> --ignore-daemonsets --delete-emptydir-data --dry-run=server

# === KEDA (staging/prod) ===
kubectl -n trading-staging get scaledobject
kubectl -n trading-staging get hpa          # KEDA tạo HPA ở hậu trường
kubectl -n trading-staging describe scaledobject pricing-risk-service
kubectl -n keda logs -l app=keda-operator --tail=50

# Tạo lag để xem KEDA scale
kubectl -n trading-staging scale deploy/pricing-risk-service --replicas=0
#   (feed vẫn bơm tick -> lag tăng)
sleep 60
kubectl -n trading-staging get scaledobject pricing-risk-service
kubectl -n trading-staging get pods -w

# === Rollback ===
kubectl -n trading-prod rollout history deploy/order-service
kubectl -n trading-prod rollout undo deploy/order-service
kubectl -n trading-prod rollout undo deploy/order-service --to-revision=3
```

---

## 9. Hạn chế đã biết

| # | Loại | Vị trí | Vấn đề | Hướng sửa |
|:--:|---|---|---|---|
| 18 | **Bug** | `k8s/base/infra/postgres.yaml` | `POSTGRES_PASSWORD` chỉ tác dụng lúc `initdb`; xoay mật khẩu làm sập cả môi trường, và `wait-for-db` vẫn pass nên triệu chứng chỉ lộ ở tầng ứng dụng | `ALTER ROLE` trong DB **trước**, rồi mới sửa `secrets.env`. Triệt để: tách Secret, hoặc CloudNativePG |
| 19 | **Gap** | `k8s/overlays/prod/externalsecret.yaml` | Secret do ESO dựng không có hash ⇒ cập nhật xong không pod nào restart; `secretKeyRef` chỉ đọc một lần lúc start | `rollout restart` thủ công (đã vào runbook). Triệt để: Reloader, hoặc mount volume |
| 21 | **Ops** | `k8s/overlays/*/kustomization.yaml` | `replicas == minReplicaCount` chỉ giảm xác suất giằng co với autoscaler, không loại bỏ | Bỏ `replicas` khỏi overlay + `ignoreDifferences` trên `/spec/replicas` |
| 22 | **Gap** | Vùng phủ dev | 3 partition / 1 pod / concurrency 3 ⇒ một consumer không bao giờ ôm quá 1 partition | Dựa vào staging ở `minReplicaCount: 1`; thêm IT với 2 partition trên 1 consumer |
| 14 | ✅ | KEDA ↔ `setConcurrency(3)` | Đã bịt bằng CI — nhưng `concurrency` hardcode `/3` trong script | Đọc `setConcurrency` từ source, hoặc đưa thành property dùng chung |
| 20 | Gap | `market-gateway`, `pricing-risk-service` | Thiếu `server.shutdown: graceful` | Thêm hai dòng vào `application.yml` |
| — | Ops | CI | Chỉ kiểm schema, chưa kiểm chính sách | Thêm OPA Conftest / Kyverno (ví dụ: cấm `latest`, bắt buộc `runAsNonRoot`) |

---

## 10. Câu hỏi phỏng vấn

**Q: Giải thích công thức trần pod consumer.**

A: `maxReplicaCount ≤ KAFKA_PARTITIONS / setConcurrency`. Luật nền là của Kafka: trong một consumer group, một partition chỉ được gán cho đúng một consumer tại một thời điểm. `setConcurrency(3)` làm mỗi pod tạo 3 consumer thread, mỗi thread là một thành viên độc lập của group. Nên 12 partition / 3 = 4 pod là trần; pod thứ 5 join group và nhận **0 partition** — nó chiếm RAM, không làm gì, và việc nó join còn gây một lần rebalance làm cả group dừng xử lý. Điều đáng nói là ba con số này nằm ở ba file khác nhau — `trading.env`, `KafkaConfig.java`, `scaledobject-*.yaml` — và không gì trong runtime giữ chúng khớp nhau, nên repo cưỡng chế bằng CI. Dù vậy CI vẫn hardcode `/3`, nên ràng buộc mới bịt được một nửa.

**Q: Vì sao dùng KEDA thay vì HPA theo CPU?**

A: Vì với Kafka consumer, CPU là tín hiệu sai ở **cả hai chiều**. Khi nghẽn I/O — chờ DB, chờ downstream ack — CPU thấp nhưng lag có thể hàng triệu; HPA-CPU ngồi yên trong khi hệ đang tụt hậu nghiêm trọng. Ngược lại khi GC nặng hoặc REST traffic cao, CPU cao nhưng lag bằng 0; HPA scale lên vô ích và còn gây rebalance. KEDA query thẳng Kafka lấy `end offset − committed offset` = lag, đúng là đại lượng cần phản ứng. Nguyên tắc chung rút ra: **đo đúng thứ cần phản ứng**, đừng đo thứ dễ đo.

**Q: Vì sao cố ý không đặt CPU limit?**

A: Vì CPU limit trong Kubernetes được thực thi bằng CFS quota: mỗi chu kỳ 100ms, container dùng hết quota thì **bị treo cho tới hết chu kỳ**. JVM cần burst ngắn ở nhiều thời điểm — JIT compilation lúc warmup, GC, xử lý một batch 1000 tick vừa poll về. Bị throttle giữa những lúc đó trực tiếp đội p99, và nghịch lý là pod bị treo trong khi node vẫn còn CPU rảnh. Lý do sâu hơn: CPU là tài nguyên **nén được** — thiếu thì chạy chậm, không chết. Memory thì không nén được — thiếu là OOMKilled, nên memory luôn phải cap. `requests.cpu` đã đủ cho scheduler đặt pod đúng chỗ, và `ResourceQuota` chặn tổng của cả namespace. Một chi tiết đi kèm: vì vậy `ResourceQuota` **cố ý không liệt kê `limits.cpu`** — liệt kê nó sẽ làm mọi pod bắt buộc phải khai CPU limit, mâu thuẫn trực tiếp với quyết định này.

**Q: `maxUnavailable: 1` hay `minAvailable: 1` cho PDB?**

A: `maxUnavailable`, vì nó đúng ở **mọi** số replica. Với deployment 1 replica, `minAvailable: 1` làm `kubectl drain` treo vĩnh viễn — không thể rút pod duy nhất mà vẫn giữ 1 pod available. Với 3 replica, `minAvailable: 1` lại cho phép rút tới **2 pod cùng lúc** — quá rộng cho một consumer group. `maxUnavailable: 1` thì luôn là "chỉ được rút một pod một lúc", bất kể số replica. Và nhớ PDB chỉ ràng buộc voluntary disruption: node chết đột ngột thì nó không can thiệp.

**Q: ConfigMap hash là gì và vì sao quan trọng?**

A: Kustomize thêm hash nội dung vào tên ConfigMap: `trading-config-7fc9b2k4d6`. Đổi một giá trị trong `trading.env` ⇒ hash đổi ⇒ tên ConfigMap đổi ⇒ `envFrom.configMapRef.name` trong Deployment đổi ⇒ pod template đổi ⇒ **rolling restart tự động**. Không có nó, `kubectl apply` cập nhật ConfigMap nhưng pod vẫn chạy với giá trị cũ — biến môi trường chỉ được đọc một lần lúc container start, và không có lỗi nào báo rằng config mới chưa có tác dụng. Chỗ cơ chế này **thủng** là ở prod: Secret do External Secrets Operator dựng không qua generator nên không có hash, ESO cập nhật xong không pod nào restart. Hiện phải `rollout restart` thủ công; cách triệt để là Reloader hoặc mount Secret thành volume.

**Q: Vì sao prod ghim image theo digest chứ không theo tag?**

A: Vì tag là **mutable**. `v1.2.3` hôm nay và ngày mai có thể là hai image khác nhau nếu ai đó push đè. Hệ quả: cùng một manifest trong Git, hai lần deploy ra hai bits khác nhau — mất hoàn toàn tính tái lập, và một sự cố production không thể điều tra được vì không biết chính xác bits nào đang chạy. Digest `@sha256:...` là content-addressed nên vĩnh viễn trỏ tới cùng một nội dung. Hai hệ quả kéo theo: `imagePullPolicy: IfNotPresent` trở nên an toàn tuyệt đối (cùng digest thì luôn cùng bits), và CI phải ghi đè digest ở mỗi lần release — nghĩa là chuỗi CI→CD phải push image trước, điều mà pipeline hiện tại chưa làm vì bước push đang bị comment.

**Q: Vì sao prod dùng `DoNotSchedule` mà phải sửa bằng JSON patch?**

A: Về lý do nghiệp vụ: `ScheduleAnyway` chỉ là gợi ý, scheduler có thể dồn cả 3 pod của một consumer group vào một AZ nếu các AZ khác chật — và khi AZ đó sự cố thì mất sạch group. Ở prod, thà pod `Pending` còn hơn. Về lý do kỹ thuật phải dùng JSON patch: `topologySpreadConstraints` là một list **không có merge key** trong Kubernetes API, nên strategic merge patch sẽ **nối thêm** phần tử mới thay vì thay thế phần tử cùng `topologyKey`. Kết quả là hai constraint mâu thuẫn trên cùng một key và scheduler xử lý theo cách khó đoán. JSON patch với `path: /spec/template/spec/topologySpreadConstraints/0/whenUnsatisfiable` sửa đúng một trường, không có mơ hồ.

---

## 11. Bảng tổng kết

| Khía cạnh | Lựa chọn | Lý do | Cái giá |
|---|---|---|---|
| Cấu trúc | base + components + overlays | Một nguồn sự thật, ba môi trường | Phải hiểu 3 cơ chế Kustomize |
| Gỡ hạ tầng ở prod | **Component** với `$patch: delete` | Không duy trì hai bộ manifest | — |
| ConfigMap | `configMapGenerator` có hash | 🔑 Rollout tự động khi đổi config | ⚠️ ESO Secret không có hash |
| Image prod | **Digest** `@sha256:` | Tag mutable ⇒ deploy không tái lập được | CI phải push trước |
| Secret | dev/staging: `secretGenerator` (gitignored); prod: ESO | Không bao giờ nằm trong Git; thiếu thì fail ngay | ⚠️ ESO không trigger restart |
| CPU limit | **Không đặt** | CFS throttle đội p99 trên hệ latency-sensitive | Phải bỏ `limits.cpu` khỏi ResourceQuota |
| Memory | `request == limit` | QoS Guaranteed, không bị evict | Không tận dụng được bộ nhớ dư |
| Autoscale | **KEDA theo Kafka lag** | CPU là tín hiệu sai ở cả hai chiều | Thêm KEDA operator |
| Trần pod | `partition / concurrency(3)` | Luật Kafka: 1 partition = 1 consumer | ⚠️ CI hardcode `/3` |
| Chống flapping | `behavior.scaleDown.stabilizationWindowSeconds` | `cooldownPeriod` chỉ áp cho scale-to-zero | — |
| PDB | `maxUnavailable: 1` | Đúng ở mọi số replica | — |
| Rollout | `maxUnavailable: 0` + `minReadySeconds: 15` | Không tụt dưới replica mong muốn; chặn pod Ready-rồi-crash | Rollout chậm hơn |
| Shutdown | preStop 5s + graceful 30s ≤ grace 60s | Không cắt request đang bay | ⚠️ 2 service còn thiếu |
| Mạng | default-deny + allowlist | Pod bị chiếm quyền không gọi ra ngoài được | Phải nhớ mở DNS trước tiên |
| Container | nonroot, readonly rootfs, drop ALL, seccomp, no SA token | Giảm bề mặt tấn công nhiều lớp | Phải mount `/tmp` tmpfs |
| Topology | zone trước, node sau; prod `DoNotSchedule` | Mất một AZ không đứt consumer group | Pod có thể `Pending` |
| Validate | 4 bước CI trên 3 overlay | Quy ước thành ràng buộc chặn merge | Chưa có kiểm chính sách (OPA/Kyverno) |

> **Đọc tiếp:** [Chương 11 — Helm & Hạ tầng Production](./11-trien-khai-helm-va-ha-tang-production.md).
