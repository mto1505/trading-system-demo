# Triển khai Kubernetes — dev / staging / prod

Một bộ manifest, ba môi trường. `base/` mô tả hệ thống; mỗi overlay chỉ vặn các
**núm** khác nhau. Thứ chạy ở dev giống về **cấu trúc** với thứ lên prod — đó là
lý do tồn tại của kustomize ở đây.

```
k8s/
├── base/                      hệ thống, không phụ thuộc môi trường
│   ├── config/trading.env     ConfigMap chung (sinh ra, có hash)
│   ├── infra/                 kafka, postgres, redis, schema-registry, zipkin
│   └── apps/                  4 service + ingress + networkpolicy
├── components/                mảnh ghép bật/tắt theo môi trường
│   ├── keda/                  autoscale theo CONSUMER LAG
│   ├── monitoring/            ServiceMonitor + PrometheusRule (Prometheus Operator)
│   ├── in-cluster-prometheus/ Prometheus tối giản cho cluster chưa có stack
│   ├── external-kafka/        gỡ Kafka trong cluster, trỏ ra ngoài
│   └── external-postgres/     gỡ Postgres trong cluster, trỏ ra ngoài
├── overlays/{dev,staging,prod}
├── cluster/                   tài nguyên cấp cluster, apply một lần
└── legacy/                    bộ manifest phẳng cũ (không dùng nữa)
```

---

## 1. Khác biệt giữa ba môi trường

| | **dev** | **staging** | **prod** |
|---|---|---|---|
| Namespace | `trading-dev` | `trading-staging` | `trading-prod` |
| Image | `trading/<svc>:dev` (build tại chỗ) | `ghcr.io/...:main` (tag di động) | **ghim theo digest** |
| Kafka | 1 broker trong cluster | 1 broker trong cluster | **ngoài cluster** (Strimzi/MSK) |
| Postgres | 1 pod, PVC 3Gi | 1 pod, PVC 10Gi | **managed** (RDS/Cloud SQL) |
| Partition mỗi topic | 3 | 6 | 12 |
| Trần pod consumer | 1 | 2 | 4 |
| Autoscale | không | KEDA 1→2 | KEDA 2→4 |
| order-service | 1 pod | 2 pod | 3 pod |
| Tracing sampling | 1.0 | 0.3 | 0.05 |
| Feed | giả lập | giả lập | **feed thật** |
| Ingress | HTTP, `trading.dev.local` | TLS (LE staging) | TLS (LE prod) + rate limit |
| `/api/v1/ticks` | mở | mở | **đã gỡ khỏi Ingress** |
| Secret | `secrets.env` (gitignored) | `secrets.env` (gitignored) | **External Secrets Operator** |
| Monitoring | Prometheus trong namespace | ServiceMonitor | ServiceMonitor |
| PriorityClass | — | — | `trading-critical` |
| Trải pod theo AZ | ScheduleAnyway | ScheduleAnyway | **DoNotSchedule** |

### Hợp đồng quan trọng nhất: partition ↔ concurrency

Cả ba listener factory đều đặt `concurrency = 3` (`*/config/KafkaConfig.java`).
Vì vậy:

```
trần pod consumer hữu ích = số partition / 3
```

Pod vượt quá trần đó **không nhận được partition nào** — tốn tiền mà không tăng
thông lượng. `maxReplicaCount` của KEDA trong mỗi overlay đã đặt đúng bằng trần
này. Muốn scale cao hơn thì **tăng partition trước** (sửa `KAFKA_PARTITIONS`
trong overlay rồi chạy lại Job `kafka-topics-init`), xong mới nâng
`maxReplicaCount`.

Topic do Job `kafka-topics-init` tạo với số partition tường minh.
`auto.create.topics.enable` bị **tắt** có chủ ý: auto-create đẻ topic 1 partition
và làm sập toàn bộ bài toán trên mà không báo lỗi gì.

---

## 2. Yêu cầu trước khi deploy

**Kubernetes >= 1.29.** Hai tính năng được dùng và không có đường lùi dễ dàng:

- `lifecycle.preStop.sleep` (bật mặc định từ 1.29) — giữ pod phục vụ thêm 5s sau
  khi vào `Terminating`, vì việc gỡ Endpoints là bất đồng bộ. Cluster cũ hơn thì
  thay bằng `exec: ["sleep", "5"]`, nhưng image distroless/JRE không có `sleep`.
- `metadata.labels['apps.kubernetes.io/pod-index']` (GA từ 1.28) — Kafka lấy
  `KAFKA_NODE_ID` từ ordinal của pod.

Pod Security Standard **`restricted`** được `enforce` ngay ở namespace: mọi
container đều `runAsNonRoot`, `drop: ["ALL"]`, `seccompProfile: RuntimeDefault`.
Thêm manifest vi phạm thì API server **từ chối**, chứ không chỉ cảnh báo.

| Thành phần | dev | staging | prod | Cài bằng |
|---|---|---|---|---|
| ingress-nginx | nên có | **bắt buộc** | **bắt buộc** | `helm install ingress-nginx ingress-nginx/ingress-nginx -n ingress-nginx --create-namespace` |
| CNI hỗ trợ NetworkPolicy | nên có | **bắt buộc** | **bắt buộc** | Calico / Cilium (minikube: `--cni=calico`) |
| KEDA | — | **bắt buộc** | **bắt buộc** | `helm install keda kedacore/keda -n keda --create-namespace` |
| Prometheus Operator | — | **bắt buộc** | **bắt buộc** | `helm install kube-prometheus-stack prometheus-community/kube-prometheus-stack -n monitoring --create-namespace` |
| cert-manager | — | **bắt buộc** | **bắt buộc** | xem `cluster/README.md` |
| External Secrets Operator | — | — | **bắt buộc** | xem `cluster/README.md` |
| Kafka ngoài cluster | — | — | **bắt buộc** | `cluster/strimzi-kafka.yaml` hoặc MSK |
| Postgres ngoài cluster | — | — | **bắt buộc** | RDS / Cloud SQL / CloudNativePG |

> CNI **không** hỗ trợ NetworkPolicy (minikube mặc định chẳng hạn) thì các policy
> vẫn apply được nhưng **không có hiệu lực** — hệ chạy bình thường, chỉ là mất
> lớp phòng thủ. Đừng nhầm điều đó với "đã an toàn".

Staging chưa có kube-prometheus-stack thì đổi trong
`overlays/staging/kustomization.yaml`: `../../components/monitoring` →
`../../components/in-cluster-prometheus`.

---

## 3. Deploy

### dev (minikube / kind / Docker Desktop)

```bash
# 1. secret cho dev da co san gia tri mac dinh
cp k8s/overlays/dev/secrets.env.example k8s/overlays/dev/secrets.env

# 2. build 4 image vao docker daemon cua minikube
eval $(minikube docker-env)
for m in market-gateway pricing-risk-service order-service matching-engine; do
  docker build --build-arg MODULE=$m -t trading/$m:dev .
done

# 3. xem truoc roi apply
kubectl kustomize k8s/overlays/dev | less
kubectl apply -k k8s/overlays/dev

# 4. theo doi
kubectl -n trading-dev get pods -w
kubectl -n trading-dev logs job/kafka-topics-init

# 5. truy cap
echo "$(minikube ip) trading.dev.local" | sudo tee -a /etc/hosts
curl -H 'X-API-KEY: dev-key' http://trading.dev.local/api/v1/marketdata/VNM
kubectl -n trading-dev port-forward svc/prometheus 9090:9090
kubectl -n trading-dev port-forward svc/zipkin 9411:9411
```

### staging

`overlays/staging/secrets.env` **cố ý không có trong repo**: thiếu nó thì
`kustomize build` hỏng ngay, còn hơn là deploy âm thầm với một mật khẩu
placeholder mà rồi không ai đổi.

```bash
cp k8s/overlays/staging/secrets.env.example k8s/overlays/staging/secrets.env
$EDITOR k8s/overlays/staging/secrets.env            # dien mat khau that
$EDITOR k8s/overlays/staging/patch-ingress.yaml     # sua host

kubectl apply -k k8s/overlays/staging
kubectl -n trading-staging rollout status deploy/order-service
```

### prod

**Phải sửa trước khi apply** — mọi chỗ đều có dấu `>>> SỬA <<<` trong file:

| Giá trị | File |
|---|---|
| `KAFKA_BOOTSTRAP` (endpoint Kafka thật) | `overlays/prod/kustomization.yaml` |
| `DB_HOST` (endpoint RDS/Cloud SQL) | `overlays/prod/kustomization.yaml` |
| 4 image digest | `overlays/prod/kustomization.yaml` |
| Domain + `secretName` TLS | `overlays/prod/patch-ingress.yaml` |
| CIDR subnet database | `overlays/prod/patch-external-cidr-db.yaml` |
| CIDR subnet Kafka (2 file) | `overlays/prod/patch-external-cidr-kafka*.yaml` |
| Đường dẫn secret trong Vault/ASM | `overlays/prod/externalsecret.yaml` |
| Email ACME | `cluster/cert-manager-clusterissuer.yaml` |

```bash
kubectl apply -f k8s/cluster/           # mot lan cho ca cluster
kubectl diff  -k k8s/overlays/prod      # LUON xem diff truoc
kubectl apply -k k8s/overlays/prod
kubectl -n trading-prod rollout status deploy/order-service --timeout=10m
```

---

## 4. Quy trình release

Tag là **mutable** — cùng một manifest hôm nay và ngày mai có thể roll ra bits
khác nhau. Prod vì vậy ghim theo **digest**, và CI là thứ ghi đè nó:

```bash
# sau khi CI push image
DIGEST=$(docker buildx imagetools inspect ghcr.io/<org>/order-service:$SHA \
          --format '{{.Manifest.Digest}}')
cd k8s/overlays/prod
kustomize edit set image ghcr.io/trading-system/order-service@$DIGEST
git commit -am "release order-service $SHA" && git push
```

Có GitOps (Argo CD / Flux) thì commit đó chính là lệnh deploy. Không có thì
`kubectl apply -k` sau khi merge.

Workflow `.github/workflows/k8s-validate.yml` build cả ba overlay trên mỗi PR —
một patch sai target hay một file thiếu sẽ hỏng ở CI chứ không hỏng ở cluster.

---

## 5. Vận hành

```bash
# tang partition (buoc BAT BUOC truoc khi nang maxReplicaCount)
$EDITOR k8s/overlays/prod/kustomization.yaml        # KAFKA_PARTITIONS=24
kubectl -n trading-prod delete job kafka-topics-init
kubectl apply -k k8s/overlays/prod

# xem KEDA dang nghi gi
kubectl -n trading-prod get scaledobject,hpa
kubectl -n trading-prod describe scaledobject pricing-risk-service

# xoay mat khau DB o prod (ExternalSecret khong co hash -> khong tu restart)
kubectl -n trading-prod rollout restart deploy \
  -l app.kubernetes.io/part-of=trading-system

# rollback
kubectl -n trading-prod rollout undo deploy/order-service

# kiem tra NetworkPolicy co chan nham khong
kubectl -n trading-prod exec deploy/order-service -- sh -c 'nc -zv $DB_HOST 5432'
```

### Vì sao một vài lựa chọn trông lạ

- **Không có CPU limit, chỉ có CPU request.** CPU limit nghĩa là CFS throttling:
  pod cần burst ngắn (JIT warmup, GC, poll một batch Kafka lớn) bị treo nốt chu
  kỳ 100ms. Với hệ đo p99 theo mili-giây thì đó là đánh đổi tệ. ResourceQuota
  chặn tổng, LimitRange chặn từng pod về memory.
- **`memory request == limit`.** Pod không bị evict vì hàng xóm ồn ào.
- **liveness lỏng hơn readiness.** DB ốm thì pod phải **rời Service**
  (readiness), chứ không được **restart hàng loạt** (liveness) — restart không
  chữa được DB mà còn xoá JIT và gây rebalance consumer group.
- **`maxUnavailable: 1` chứ không `minAvailable: 1`** ở PDB: `minAvailable: 1`
  với deployment 1 replica làm `kubectl drain` treo vĩnh viễn, còn với 3 replica
  lại cho phép rút tới 2 pod cùng lúc.
- **market-gateway luôn 1 replica.** Nó gắn với **một** session feed. Chạy 2 pod
  với feed thật = nhận trùng tick và nhân đôi tải downstream. Muốn HA thì phải
  có leader election ở tầng ứng dụng trước.
- **ConfigMap/Secret được _sinh ra_ kèm hash.** Đổi config → tên đổi → Deployment
  đổi → rolling restart tự động. ConfigMap tĩnh thì pod cứ chạy giá trị cũ cho
  tới khi có người nhớ ra phải restart.
- **Tên service ngắn trong `trading.env`, FQDN trong overlay.** DNS trong pod tự
  gắn search domain nên `postgres` là đủ cho ứng dụng; nhưng KEDA chạy ở
  namespace khác nên `KAFKA_BOOTSTRAP` phải là FQDN — mỗi overlay đặt lại một
  dòng duy nhất.

---

## 6. Chưa làm (có chủ ý)

- **Debezium CDC** thay cho polling outbox relay. Postgres trong `base/` đã bật
  sẵn `wal_level=logical` để chuyển đổi về sau **không cần restart database**.
  Khi bật CDC nhớ đặt `OUTBOX_RELAY_ENABLED=false`, nếu không cả relay lẫn CDC
  cùng publish → event nhân đôi.
- **Zipkin ở prod vẫn là in-memory** — mất trace khi restart. Nên đổi
  `ZIPKIN_ENDPOINT` sang Grafana Tempo hoặc Zipkin + Elasticsearch.
- **Kafka TLS/SASL + ACL.** `security/create-acls.sh` trong repo đã có kịch bản;
  listener trong `base/` hiện là PLAINTEXT nội bộ cluster.
- **Grafana dashboard** trong `monitoring/dashboards/` chưa được đóng gói thành
  ConfigMap có nhãn `grafana_dashboard` để sidecar tự nạp.
