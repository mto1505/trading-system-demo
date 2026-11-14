# kubectl Cheat Sheet — muốn kiểm tra gì thì chạy gì

> Viết theo cluster thật đang chạy: minikube (driver docker, WSL2, 6 GiB/4 CPU),
> k8s v1.37, containerd, namespace `trading-dev`, addon bật sẵn: `ingress`,
> `dashboard`, `metrics-server`. Lệnh ở đây đã chạy thử, không phải chép từ doc
> chung. Xem thêm [CNI-NOTES.md](CNI-NOTES.md) cho phần mạng, và
> [MINIKUBE-CHEATSHEET.md](MINIKUBE-CHEATSHEET.md) cho phần vỏ cluster (profile,
> addon, image, RAM cgroup).

Đặt namespace mặc định một lần cho đỡ gõ `-n trading-dev` mãi:

```bash
kubectl config set-context --current --namespace=trading-dev
kubectl config view --minify | grep namespace:     # xác nhận
```

Không chỉ để đỡ gõ: **IntelliJ và các k8s client khác lấy namespace từ
kubeconfig**. Để context ở `default` thì cây Kubernetes trong IDE chỉ hiện mỗi
`svc/kubernetes` — trông y hệt như chưa deploy gì, dù cluster vẫn đủ 10 Service
ở `trading-dev`. Đổi xong nhớ **Refresh** tool window; plugin cache kubeconfig
lúc khởi động.

`scripts/deploy-dev.ps1` đã tự đặt namespace này sau khi chuyển context, nên
chạy script thì khỏi làm tay.

---

## 1. Nhìn tổng thể trước khi làm gì

| Muốn biết | Chạy |
|---|---|
| Đang trỏ vào cluster nào | `kubectl config current-context` |
| Cluster còn sống không | `kubectl get --raw='/readyz?verbose'` |
| Node ổn không | `kubectl get nodes -o wide` |
| Mọi thứ trong namespace | `kubectl get all` |
| **Chỉ những thứ đang hỏng** | `kubectl get pods -A \| grep -v "Running\|Completed"` |
| Việc gì vừa xảy ra | `kubectl events --types=Warning --sort-by=.lastTimestamp` |

Lệnh đáng nhớ nhất ở trên là cái in đậm — nó trả lời "có gì hỏng không" trong
một dòng, thay vì đọc cả bảng.

`kubectl get all` **không** liệt kê ConfigMap, Secret, Ingress, NetworkPolicy,
PVC. Muốn thật sự thấy hết:

```bash
kubectl get $(kubectl api-resources --namespaced --verbs=list -o name | tr '\n' ',' | sed 's/,$//') 2>/dev/null
```

---

## 2. Pod không chạy — thang chẩn đoán theo thứ tự

Chạy từ trên xuống, dừng ở bước đầu tiên cho câu trả lời.

```bash
# 1. Nó đang ở trạng thái gì, restart bao nhiêu lần
kubectl get pods -o wide

# 2. Vì sao — phần Events ở cuối là nơi nói thật
kubectl describe pod <pod>

# 3. Chết vì lý do gì (OOMKilled? Error? exit code?)
kubectl get pod <pod> -o jsonpath='{.status.containerStatuses[0].lastState.terminated}' | jq

# 4. Log của LẦN CHẾT TRƯỚC — không có --previous là không thấy gì
kubectl logs <pod> --previous

# 5. Xem nhanh số restart của cả namespace
kubectl get pods -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.status.containerStatuses[0].restartCount}{"\n"}{end}'
```

### Đọc STATUS

| STATUS | Nghĩa | Xem tiếp |
|---|---|---|
| `Pending` | Chưa xếp được lên node nào | `describe pod` → Events → thường là thiếu RAM/CPU |
| `ContainerCreating` | Đang tạo — kẹt lâu thường là **CNI** hoặc mount volume | `describe pod`, log kubelet |
| `ErrImagePull` / `ImagePullBackOff` | Không kéo được image | mục 7 bên dưới |
| `CrashLoopBackOff` | Khởi động rồi chết, lặp lại | `logs --previous` |
| `Running` nhưng `0/1` | Chạy nhưng **readiness probe fail** | `describe pod` → probe nào fail |
| `OOMKilled` | Vượt memory limit | mục 6 — trên minikube hay là do cgroup, không phải app |
| `Completed` | Job chạy xong, bình thường | — |

### Khi log trống mà container vẫn chết

Đã gặp thật với `schema-registry` trong repo này: log chỉ in một dòng rồi exit 1.
Cách moi ra nguyên nhân là chạy lại entrypoint với `sh -x` để trace:

```bash
# Tạo pod cùng image + cùng env, override command để trace
kubectl run dbg --image=<image> --restart=Never --command -- sh -c 'sh -x /path/to/entrypoint'
kubectl logs dbg
```

Namespace này bật PodSecurity `restricted`, nên `kubectl run` trần sẽ bị từ chối.
Phải viết file pod có `securityContext` đầy đủ (`runAsNonRoot`,
`allowPrivilegeEscalation: false`, `capabilities.drop: ["ALL"]`,
`seccompProfile: RuntimeDefault`) rồi `kubectl apply -f`.

---

## 3. Log

```bash
kubectl logs <pod>                      # log hiện tại
kubectl logs <pod> --previous           # log của lần chết TRƯỚC  ← hay cần nhất
kubectl logs <pod> -f --tail=50         # theo dõi liên tục
kubectl logs deploy/order-service       # qua deployment, khỏi cần tên pod
kubectl logs -l app.kubernetes.io/name=kafka --all-containers   # theo label
kubectl logs <pod> --since=10m          # chỉ 10 phút gần đây
kubectl logs <pod> -c <container>       # pod nhiều container
```

Lọc cho nhanh — stack trace Java dài, grep thẳng vào chỗ cần:

```bash
kubectl logs deploy/market-gateway --tail=200 | grep -E "ERROR|Caused by|Exception"
```

---

## 4. Mạng, DNS, Service

```bash
# Service có trỏ đúng pod không — ENDPOINTS TRỐNG nghĩa là selector sai
kubectl get endpointslices -l kubernetes.io/service-name=<svc>
kubectl get endpoints <svc>      # cách cũ: v1.37 vẫn chạy nhưng in warning deprecated

# Service là headless hay ClusterIP (quan trọng với StatefulSet)
kubectl get svc <svc> -o jsonpath='{.spec.clusterIP}'      # None = headless

# DNS có phân giải được không — chạy từ TRONG cluster
kubectl exec kafka-0 -- getent hosts kafka-0.kafka.trading-dev.svc.cluster.local
kubectl exec kafka-0 -- getent hosts kafka

# Gọi thử service từ pod khác
kubectl exec <pod> -- curl -sS -m5 http://order-service:8083/actuator/health
```

`endpoints` trống là manh mối số một khi "service không gọi được": label của pod
không khớp `selector` của Service. Pod vẫn Running bình thường nên dễ bỏ sót.

**Lưu ý CNI:** cluster này dùng kindnet nên **NetworkPolicy không được thực thi**.
`kubectl get netpol` hiện đủ 11 policy nhưng không chặn gì cả. Chi tiết và cách
test thật ở [CNI-NOTES.md](CNI-NOTES.md) mục 5.

```bash
kubectl get netpol                       # hiện ra != có hiệu lực
```

---

## 5. Config và Secret — xem thứ pod THẬT SỰ nhận được

Đây là chỗ hay sai nhất: file trong repo khác thứ đang chạy trong cluster.

```bash
# ConfigMap/Secret đang tồn tại (kustomize thêm hash vào tên)
kubectl get cm,secret

# Nội dung ConfigMap
kubectl get cm trading-config-8ff54mdb49 -o jsonpath='{.data}' | jq

# Giải mã Secret
kubectl get secret trading-secrets-7944267b22 -o jsonpath='{.data.ORDER_API_KEY}' | base64 -d

# BIẾN MÔI TRƯỜNG THẬT bên trong container đang chạy  ← quyết định
kubectl exec <pod> -- env | sort
```

Lệnh cuối từng là thứ chỉ ra bug `schema-registry`: kubelet **tự tiêm** biến
`SCHEMA_REGISTRY_PORT=tcp://<clusterIP>:8081` cho mọi Service trong namespace
(cơ chế Docker-link cũ), và image Confluent hiểu nhầm đó là cấu hình `PORT` đã
bỏ rồi tự thoát. Không file manifest nào khai biến đó — chỉ `exec -- env` mới
thấy. Tắt bằng `enableServiceLinks: false` trong pod spec.

So manifest trong repo với thứ đang chạy:

```bash
kubectl diff -k k8s/overlays/dev
kubectl kustomize k8s/overlays/dev | less      # render ra xem trước khi apply
```

---

## 6. Tài nguyên — và cái bẫy riêng của minikube

```bash
# Node còn bao nhiêu chỗ (theo REQUEST, không phải mức dùng thật)
kubectl describe node minikube | grep -A8 "Allocated resources"

# Request/limit của từng pod
kubectl get pods -o custom-columns=\
'NAME:.metadata.name,MEM_REQ:.spec.containers[0].resources.requests.memory,MEM_LIM:.spec.containers[0].resources.limits.memory'

# Quota của namespace
kubectl describe resourcequota
```

```bash
# Mức dùng THẬT (cần metrics-server, đã bật trên cluster này)
kubectl top nodes
kubectl top pods --sort-by=memory
```

Nếu trả `error: Metrics API not available` thì addon đang tắt:
`minikube addons enable metrics-server`.

⚠️ **Bẫy quan trọng — `kubectl top` báo % SAI trên minikube.** kubelet lấy
`allocatable` bằng RAM của **cả VM WSL** (7.7 Gi), trong khi container minikube
bị cgroup giới hạn thật ở 6144 MiB. Cùng một thời điểm:

```
kubectl top nodes    ->  4350Mi   54%     <- mẫu số là 7.7Gi của VM, SAI
docker stats minikube -> 4.27GiB  71.19%  <- mẫu số là 6GiB cgroup, ĐÚNG
```

Hệ quả: scheduler tưởng còn nhiều chỗ nên vẫn xếp pod, rồi cgroup mới OOMKill.
Thấy `OOMKilled` thì so hai con số này trước khi nghi app rò rỉ bộ nhớ:

```bash
kubectl get node minikube -o jsonpath='{.status.allocatable.memory}{"\n"}'   # ảo (RAM cả VM)
docker inspect minikube --format '{{.HostConfig.Memory}}'                    # thật (cgroup)
docker stats --no-stream minikube                                            # % đáng tin
```

Muốn đổi giới hạn thật thì phải `minikube delete` rồi
`minikube start --memory=... --cpus=...` — driver docker không resize được.

---

## 7. Image

```bash
# Image nào đang có trong cluster
minikube image ls | grep trading

# Pod đang chạy image nào, pull policy gì
kubectl get pods -o jsonpath=\
'{range .items[*]}{.metadata.name}{"\t"}{.spec.containers[0].image}{"\n"}{end}'
```

Cluster này chạy **containerd**, nên `eval $(minikube docker-env)` vô dụng —
image build vào docker daemon thì containerd không thấy. Cách đúng:

```bash
docker tag trading-system-order-service:latest trading/order-service:dev
minikube image load trading/order-service:dev
kubectl rollout restart deploy/order-service
```

`imagePullPolicy` phải là `IfNotPresent` hoặc `Never`, nếu để `Always` thì k8s
vẫn đi hỏi Docker Hub và fail dù image đã nằm sẵn trong cluster.

---

## 8. Ingress và truy cập từ ngoài

```bash
kubectl get ingress
kubectl describe ingress trading            # xem rule + backend
kubectl -n ingress-nginx get pods
kubectl -n ingress-nginx logs deploy/ingress-nginx-controller --tail=30
```

**Kubernetes Dashboard (UI):**

```bash
minikube dashboard --url     # in ra URL, phải để lệnh chạy suốt
minikube dashboard           # tự mở trình duyệt luôn
```

URL có dạng `http://127.0.0.1:<port ngẫu nhiên>/api/v1/namespaces/
kubernetes-dashboard/services/http:kubernetes-dashboard:/proxy/` — port đổi mỗi
lần chạy. Đóng lệnh là mất, không cần token đăng nhập vì đi qua `kubectl proxy`.

Trên Windows, IP node `192.168.49.2` nằm trong VM WSL nên **không tới được từ
Windows** (cả NodePort cũng không). Dùng port-forward:

```bash
kubectl -n ingress-nginx port-forward svc/ingress-nginx-controller 80:80
kubectl port-forward svc/order-service 18083:8083        # bỏ qua ingress, vào thẳng service
```

Windows không cấm user thường bind port 80 (khác Linux), nên không cần admin và
không cần `minikube tunnel`.

**Gặp 404 qua ingress** mà service vẫn sống: gần như chắc chắn là sai header
`Host`. Ingress khai `host: trading.dev.local` thì nginx chỉ khớp đúng tên đó:

```bash
curl -H 'Host: trading.dev.local' http://localhost/api/v1/marketdata/HPG   # 200
curl http://localhost/api/v1/marketdata/HPG                                 # 404
```

---

## 9. Chui vào bên trong

```bash
kubectl exec -it <pod> -- sh                       # pod có shell
kubectl debug -it <pod> --image=nicolaka/netshoot --target=<container>   # pod distroless
kubectl cp <pod>:/path/file ./file                 # lấy file ra
kubectl port-forward <pod> 5005:5005               # mở cổng debug JVM
```

> **Windows/Git Bash:** mọi lệnh có path kiểu Linux phải thêm
> `MSYS_NO_PATHCONV=1`, không thì `/opt/kafka/...` bị đổi thành
> `C:/Program Files/Git/opt/kafka/...`:
> ```bash
> MSYS_NO_PATHCONV=1 kubectl exec kafka-0 -- /opt/kafka/bin/kafka-topics.sh \
>   --bootstrap-server localhost:9092 --list
> ```

---

## 10. Áp dụng thay đổi và dọn dẹp

```bash
kubectl diff -k k8s/overlays/dev          # xem sẽ đổi gì TRƯỚC khi apply
kubectl apply -k k8s/overlays/dev         # diff trả exit 1 khi CÓ khác biệt — không phải lỗi

kubectl rollout status deploy/order-service    # chờ rollout xong
kubectl rollout restart deploy/order-service   # khởi động lại, không đổi manifest
kubectl rollout undo deploy/order-service      # quay về bản trước
kubectl rollout history deploy/order-service

kubectl delete pod <pod>                  # xoá để Deployment tạo lại pod mới
kubectl delete -k k8s/overlays/dev        # gỡ toàn bộ namespace
```

`kubectl delete pod` với pod thuộc Deployment/StatefulSet thì controller tạo lại
ngay — đó là cách ép pod đọc lại config hoặc thử lại sau khi dependency đã sẵn
sàng. Muốn tắt hẳn thì `scale --replicas=0`.

---

## 11. Những lệnh gỡ bí hay quên

```bash
# Tài nguyên này có những field nào, nghĩa là gì
kubectl explain deployment.spec.template.spec.enableServiceLinks

# Mình có quyền làm việc này không
kubectl auth can-i create pods

# Xem YAML đầy đủ của thứ đang chạy (gồm cả default k8s tự điền)
kubectl get deploy order-service -o yaml

# Theo dõi thay đổi theo thời gian thực
kubectl get pods -w
```
