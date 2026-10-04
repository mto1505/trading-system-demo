# Tài nguyên cấp cluster

Những thứ ở đây **không** nằm trong overlay vì chúng là cluster-scoped hoặc
thuộc namespace khác: apply một lần, dùng chung cho cả dev/staging/prod.

| File | Khi nào cần | Lệnh |
|---|---|---|
| `cert-manager-clusterissuer.yaml` | staging + prod (TLS) | `kubectl apply -f cert-manager-clusterissuer.yaml` |
| `strimzi-kafka.yaml` | prod, nếu chạy Kafka trong cluster | `kubectl apply -f strimzi-kafka.yaml` |
| `external-secrets-store.yaml` | prod (secret từ Vault/ASM) | `kubectl apply -f external-secrets-store.yaml` |

## Nhãn namespace

NetworkPolicy trong `base/` chọn namespace bằng `kubernetes.io/metadata.name`.
Nhãn này do **kube-apiserver tự gắn** từ Kubernetes 1.21, nên không phải làm gì
thêm — miễn là Ingress controller nằm ở namespace tên `ingress-nginx` và
Prometheus stack ở namespace tên `monitoring`. Nếu cluster của bạn đặt tên khác,
sửa lại giá trị trong `base/apps/apps-networkpolicy.yaml`.
