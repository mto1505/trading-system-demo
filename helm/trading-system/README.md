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

### 3.1. Kiểm tra cú pháp (Dry-run template)
```bash
# Render thử môi trường dev
helm template trading ./helm/trading-system -f ./helm/trading-system/values-dev.yaml -n trading-dev

# Render thử môi trường staging
helm template trading ./helm/trading-system -f ./helm/trading-system/values-staging.yaml -n trading-staging

# Render thử môi trường prod
helm template trading ./helm/trading-system -f ./helm/trading-system/values-prod.yaml -n trading-prod
```

### 3.2. Triển khai Dev (Minikube / Local)
```bash
helm upgrade --install trading ./helm/trading-system \
  -f ./helm/trading-system/values-dev.yaml \
  -n trading-dev \
  --create-namespace
```

### 3.3. Triển khai Staging
```bash
helm upgrade --install trading ./helm/trading-system \
  -f ./helm/trading-system/values-staging.yaml \
  -n trading-staging \
  --create-namespace \
  --set secrets.DB_PASSWORD="<staging-db-password>" \
  --set secrets.ORDER_API_KEY="<staging-api-key>"
```

### 3.4. Triển khai Production (CI/CD Pipeline với Image Digest)
```bash
# CI lấy digest sau khi build & push image, truyền vào Helm qua --set:
helm upgrade --install trading ./helm/trading-system \
  -f ./helm/trading-system/values-prod.yaml \
  -n trading-prod \
  --create-namespace \
  --set marketGateway.image.digest="sha256:7f83b1657ff1fc53b92dc18148a1d65dfc2d4b1fa3d677284addd200126d9069" \
  --set pricingRiskService.image.digest="sha256:8a1b2c3d..." \
  --set orderService.image.digest="sha256:9c8b7a6f..." \
  --set matchingEngine.image.digest="sha256:1a2b3c4d..."
```
