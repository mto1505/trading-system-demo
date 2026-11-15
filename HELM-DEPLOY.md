# Triển khai trading-system bằng Helm — STAGING & PRODUCTION

Tài liệu này đi từ máy trắng tới pod Ready, cho cả hai môi trường. Phần dev vẫn
dùng Kustomize (`scripts\deploy-dev.ps1`); Helm dùng cho staging và prod.

Mọi lệnh chạy từ **repo root** (`D:\JavaCode\trading-system-v4\trading-system`).

---

## Mục lục

1. [Tóm tắt 5 dòng](#1-tóm-tắt-5-dòng)
2. [Hiểu chart trước khi chạy](#2-hiểu-chart-trước-khi-chạy)
3. [Chuẩn bị cluster (chung cho staging & prod)](#3-chuẩn-bị-cluster)
4. [STAGING](#4-staging)
5. [PRODUCTION](#5-production)
6. [Vận hành hằng ngày](#6-vận-hành-hằng-ngày)
7. [Bảng tra lỗi](#7-bảng-tra-lỗi)
8. [Những thứ đã sửa trong chart](#8-những-thứ-đã-sửa-trong-chart)

---

## 1. Tóm tắt 5 dòng

```powershell
# Staging trên minikube, từ đầu:
.\scripts\helm\Install-Prereqs.ps1 -Target minikube     # KEDA + Prometheus + ingress
.\scripts\helm\Deploy-Staging.ps1                       # build + deploy + chờ Ready
.\scripts\helm\Verify-Release.ps1 -Namespace trading-staging

# Prod:
.\scripts\helm\Deploy-Prod.ps1 -ImageTag v1.4.2 -DryRun # BẮT BUỘC xem trước
.\scripts\helm\Deploy-Prod.ps1 -ImageTag v1.4.2
```

---

## 2. Hiểu chart trước khi chạy

### 2.1. Bốn file values và quan hệ giữa chúng

| File | Vai trò | Chạy được một mình? |
| :--- | :--- | :--- |
| `values.yaml` | Mặc định chung. Helm **luôn** nạp file này trước. | Không — thiếu mật khẩu |
| `values-dev.yaml` | Minikube, Prometheus tối giản, không KEDA | Có |
| `values-staging.yaml` | KEDA + monitoring thật, hạ tầng trong cluster | Cần registry + TLS |
| `values-staging-local.yaml` | **Lớp phủ** lên staging để chạy trên minikube | Không — phải đi kèm `values-staging.yaml` |
| `values-prod.yaml` | RDS + Managed Kafka + ESO + digest | Cần hạ tầng AWS |

`-f` sau đè `-f` trước. Chart luôn đọc `values.yaml` rồi mới tới các `-f`.

### 2.2. Khác biệt staging ↔ prod, và vì sao

| | STAGING | PROD |
| :--- | :--- | :--- |
| Postgres | StatefulSet trong cluster | **RDS** (`postgres.enabled=false`) |
| Kafka | StatefulSet trong cluster | **Managed** (`kafka.enabled=false`) |
| Tạo topic | Job hook `init-kafka-topics` | Tắt — managed Kafka tự lo |
| Secret | Secret thuần từ `--set` | **ExternalSecret** kéo từ AWS Secrets Manager |
| Image | tag `main` / `staging-local` | **digest** `sha256:...` |
| Replica order-service | 2 | 3 |
| KEDA | 1→2 pod, lag 100 | 2→4 pod, lag 1000/500 |
| Partition Kafka | 6 | 12 |
| TopologySpread | `ScheduleAnyway` | `DoNotSchedule` |
| PriorityClass | không | `trading-critical` |
| Tracing | 30% | 5% |
| Feed | `SIMULATED_FEED=true` | **false** — feed thật |

### 2.3. Ba thứ chart KHÔNG tự cài

Đây là nguồn gốc của phần lớn lần deploy hỏng đầu tiên:

| Thiếu | Triệu chứng |
| :--- | :--- |
| **KEDA** | `helm` chết ngay khi render: `no matches for kind "ScaledObject"` |
| **Prometheus Operator** | `helm` chết ngay khi render: `no matches for kind "PodMonitor"` |
| **ingress-nginx** | helm **thành công**, pod Ready hết, nhưng gọi API **timeout** |
| **ESO** (chỉ prod) | helm chết: `no matches for kind "ExternalSecret"` |

Hai cái đầu hỏng ồn ào nên dễ sửa. Cái thứ ba im lặng — đó là cái tốn thời gian.

`Install-Prereqs.ps1` lo cả bốn.

### 2.4. Bẫy namespace — đọc trước khi tự gõ lệnh helm

Chart **có sẵn** object `Namespace` (để gắn nhãn Pod Security Standards
`restricted`). Vì vậy:

```powershell
# SAI — sẽ chết:
helm upgrade --install trading ./helm/trading-system -n trading-staging --create-namespace
#   Error: Namespace "trading-staging" exists and cannot be imported
#          into the current release
```

`--create-namespace` tạo namespace "trần", rồi manifest `Namespace` của chart
đụng phải nó và Helm từ chối nhận nuôi tài nguyên nó không sở hữu.

Nhưng bỏ `--create-namespace` cũng không xong: Helm phải ghi được release record
*vào* namespace đó **trước** khi apply manifest — chicken-and-egg.

Lối ra: tạo namespace trước, rồi gắn đúng bộ nhãn/chú thích sở hữu của Helm để
chart nhận nuôi nó. `Deploy-Staging.ps1` làm sẵn; nếu tự gõ tay:

```powershell
kubectl create namespace trading-staging --dry-run=client -o yaml | kubectl apply -f -
kubectl label    namespace trading-staging app.kubernetes.io/managed-by=Helm --overwrite
kubectl annotate namespace trading-staging `
    meta.helm.sh/release-name=trading `
    meta.helm.sh/release-namespace=trading-staging --overwrite
# RỒI mới helm upgrade --install, KHÔNG có --create-namespace
```

Thiếu bất kỳ cái nào trong ba dòng label/annotate đó là Helm vẫn từ chối.

---

## 3. Chuẩn bị cluster

### 3.1. Công cụ trên máy

| Công cụ | Tối thiểu | Kiểm tra |
| :--- | :--- | :--- |
| helm | 3.8 | `helm version` |
| kubectl | 1.27 | `kubectl version --client` |
| docker | 24 | `docker version` |
| minikube | 1.32 | `minikube version` |

Rất nên cài thêm `helm-diff` — nó là công cụ đáng giá nhất trước mỗi lần deploy prod:

```powershell
helm plugin install https://github.com/databus23/helm-diff
```

### 3.2. Cluster cho staging local
Kiểm tra profile hiện có trước khi đập đi xây lại:

```powershell
minikube profile list
cat $env:USERPROFILE\.minikube\profiles\minikube\config.json | Select-String '"CPUs"|"Memory"|"CNI"'
```

Cần tối thiểu **4 CPU**, **8192 MB**, **CNI calico**. Nếu thiếu thì phải tạo lại —
`minikube start` trên profile đã tồn tại **bỏ qua `--cpus/--memory/--cni` trong
im lặng**:

```powershell
minikube delete
minikube start --cpus=4 --memory=8192 --cni=calico
```

`--cni=calico` **quan trọng**: chart tạo 11 NetworkPolicy trong đó có
`default-deny-all`. Với kindnet (CNI mặc định của minikube), chúng apply được
nhưng **không được thực thi** — bạn tưởng đã cô lập mạng trong khi chưa.

#### Phép tính RAM — chỗ hay hụt nhất

| Thành phần | Requests |
| :--- | ---: |
| Hạ tầng staging (postgres, kafka, redis, schema-registry, zipkin) | ~960 Mi |
| 4 service (order-service 2 replica) | ~1.5 Gi |
| kube-system + calico | ~1.0 Gi |
| KEDA | ~200 Mi |
| **kube-prometheus-stack** | **~1.5–2 Gi** |
| **Tổng** | **~5.2–5.7 Gi** |

Với node **6144 MB** thì allocatable còn ít hơn con số đó, và bạn sẽ chạm trần.
Hai lối ra:

```powershell
# (a) Bỏ monitoring — tiết kiệm ~1.5-2GB
.\scripts\helm\Install-Prereqs.ps1 -Target minikube -SkipMonitoring
# rồi deploy kèm:
.\scripts\helm\Deploy-Staging.ps1   # tự phát hiện thiếu CRD và dừng, nên thêm:
#   --set monitoring.enabled=false   (sửa trong values-staging-local.yaml cho gọn)

# (b) Cấp thêm RAM — tạo lại profile với 8192
minikube delete; minikube start --cpus=4 --memory=8192 --cni=calico
```

Docker Desktop phải được cấp nhiều hơn minikube ít nhất 1GB. Nếu thiếu, sửa
`memory=` trong `%USERPROFILE%\.wslconfig` rồi `wsl --shutdown`.

#### Cluster đang tắt

Cả ba script đều kiểm tra API server **trước mọi lệnh `kubectl` khác**, vì khi
cluster tắt các lệnh thăm dò kiểu `kubectl get crd ... --ignore-not-found`
**không chết** mà trả về **rỗng** — script sẽ kết luận nhầm là "thiếu KEDA, hãy
chạy Install-Prereqs" trong khi thật ra chỉ là cluster chưa bật.

`Install-Prereqs.ps1` và `Deploy-Staging.ps1` (chế độ `local`) tự bật Docker
Desktop rồi `minikube start` nếu cần. `Deploy-Prod.ps1` thì **không bao giờ** tự
khởi động gì — prod là cluster thật, nó chỉ báo lỗi và dừng.

### 3.3. Cài KEDA + Prometheus Operator + ingress

```powershell
.\scripts\helm\Install-Prereqs.ps1 -Target minikube
```

Trên cluster cloud thật (EKS/GKE/AKS):

```powershell
.\scripts\helm\Install-Prereqs.ps1 -Target cloud -MonitoringRelease kube-prometheus-stack
```

Script sẽ bắt bạn gõ đúng tên context để xác nhận.

Nếu máy yếu, bỏ monitoring:

```powershell
.\scripts\helm\Install-Prereqs.ps1 -Target minikube -SkipMonitoring
# rồi deploy kèm:  -HelmExtraArgs  hoặc  --set monitoring.enabled=false
```

Cuối script có bảng tổng kết CÓ / CHƯA CÓ cho 5 thành phần.

> **Lưu ý về nhãn `release`**: kube-prometheus-stack mặc định **chỉ** nhận
> PodMonitor mang nhãn `release: <tên-release-của-stack>`. `values-staging.yaml`
> và `values-prod.yaml` đặt sẵn `monitoring.extraLabels.release: kube-prometheus-stack`.
> Nếu bạn cài stack với tên release khác, **phải sửa giá trị này**, nếu không
> Prometheus bỏ qua PodMonitor trong im lặng và dashboard trống trơn.
> `Verify-Release.ps1` so hai giá trị này giúp bạn.

---

## 4. STAGING

### 4.1. Đường nhanh nhất

```powershell
.\scripts\helm\Deploy-Staging.ps1
```

Script làm 9 bước: kiểm tra công cụ → kiểm tra CRD → build 4 image (tuần tự) →
`minikube image load` → sinh/đọc secret → chuẩn bị namespace → `helm upgrade
--install --wait --atomic` → `rollout restart` → in cách truy cập.

Lần đầu mất ~10–15 phút (phần lớn là build Maven trong Docker).

### 4.2. Xem trước mà không đụng cluster

**Luôn làm bước này lần đầu.** Nó bắt được mọi lỗi template, lỗi thiếu CRD, lỗi
sai giá trị — trước khi tốn 10 phút build:

```powershell
.\scripts\helm\Deploy-Staging.ps1 -DryRun
```

Hoặc render thuần, không cần cluster:

```powershell
helm template trading .\helm\trading-system `
  -f .\helm\trading-system\values-staging.yaml `
  -f .\helm\trading-system\values-staging-local.yaml `
  -n trading-staging > out.yaml
```

### 4.3. Secret của staging

`values-staging.yaml` cố ý để `DB_PASSWORD: ""`. Script sinh mật khẩu **một lần**
rồi lưu vào `helm\.secrets.staging.env` (đã gitignore theo mẫu `secrets.env`).

> **Đừng xoá file này rồi deploy lại.** Postgres ghi mật khẩu vào PVC ở lần khởi
> tạo **đầu tiên** và không bao giờ đọc lại `POSTGRES_PASSWORD` nữa. Sinh mật
> khẩu mới = app không đăng nhập được vào chính DB của nó, và lỗi hiện ra ở
> **app** (`password authentication failed`) chứ không phải ở postgres — rất dễ
> đi lạc hướng. Muốn đổi thật thì phải `ALTER USER` trong DB.

Nếu tự gõ tay:

```powershell
helm upgrade --install trading .\helm\trading-system `
  -f .\helm\trading-system\values-staging.yaml `
  -n trading-staging `
  --set secrets.DB_PASSWORD="<mat-khau>" `
  --set secrets.ORDER_API_KEY="<api-key>" `
  --wait --atomic --timeout 900s
```

### 4.4. Staging trên cluster thật (có registry)

Khi CI đã push image lên GHCR:

```powershell
.\scripts\helm\Deploy-Staging.ps1 -Mode registry -ImageTag main -Namespace trading-staging
```

Chế độ `registry` **không build gì cả**, không `minikube image load`, không
`rollout restart` (tag từ CI đổi theo commit nên kubelet tự thấy image mới).

Cần thêm hai thứ ở cluster thật:

**TLS** — `values-staging.yaml` bật TLS với secret `trading-tls`. Có cert-manager:

```yaml
# thêm vào values-staging.yaml
ingress:
  annotations:
    cert-manager.io/cluster-issuer: letsencrypt-staging
```

Không có cert-manager thì tạo secret bằng tay:

```powershell
kubectl -n trading-staging create secret tls trading-tls `
  --cert=path\to\tls.crt --key=path\to\tls.key
```

**Quyền pull registry riêng tư**:

```powershell
kubectl -n trading-staging create secret docker-registry ghcr-pull `
  --docker-server=ghcr.io --docker-username=<user> --docker-password=<token>
```

> Chart hiện **chưa** có trường `imagePullSecrets`. Cách nhanh nhất là gắn vào
> ServiceAccount của từng service:
> ```powershell
> foreach ($s in 'market-gateway','pricing-risk-service','order-service','matching-engine') {
>   kubectl -n trading-staging patch serviceaccount $s `
>     -p '{"imagePullSecrets":[{"name":"ghcr-pull"}]}'
> }
> ```

### 4.5. Kiểm tra sau khi deploy

```powershell
.\scripts\helm\Verify-Release.ps1 -Namespace trading-staging
```

Script kiểm 7 nhóm và **chỉ đúng chỗ hỏng** thay vì chỉ báo "chưa Ready": pod
(phân biệt chưa xếp được node / ImagePullBackOff / CrashLoop / đang chờ init),
Job tạo topic, ScaledObject, PodMonitor (có khớp selector của Prometheus thật
không), PDB, Secret.

Kiểm bằng tay những thứ đáng nghi nhất:

```powershell
# KEDA có THỰC SỰ đọc được lag không — READY phải là True.
# False = autoscale đã chết im lặng, deployment vẫn chạy nên không ai để ý.
kubectl -n trading-staging get scaledobject
kubectl -n trading-staging describe scaledobject pricing-risk-service

# Job tạo topic. Kẹt ở đây gần như luôn là NetworkPolicy, không phải Kafka.
kubectl -n trading-staging logs job/init-kafka-topics --tail=30

# Topic có đúng 6 partition không
kubectl -n trading-staging exec kafka-0 -- `
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic stock.ticks
```

### 4.6. Gọi thử API

```powershell
# Cửa sổ 1 — giữ mở
kubectl -n ingress-nginx port-forward svc/ingress-nginx-controller 80:80
```

Thêm vào `C:\Windows\System32\drivers\etc\hosts` (cần quyền Administrator):

```
127.0.0.1  trading-staging.local
```

```powershell
# Cửa sổ 2
$key = (Select-String -Path helm\.secrets.staging.env -Pattern '^ORDER_API_KEY=(.*)$').Matches.Groups[1].Value
curl.exe -H "X-API-KEY: $key" http://trading-staging.local/api/v1/marketdata/FPT
```

> IP node của minikube **không** tới được từ Windows: mạng docker bridge nằm
> trong VM WSL2 và Windows không có route tới đó. Sửa `hosts` trỏ vào node IP sẽ
> **timeout**, không phải 404.

Đọc kết quả — ba lỗi khác hẳn nhau:

| Kết quả | Nghĩa là |
| :--- | :--- |
| timeout | chưa tới được nginx (port-forward chết, hoặc `hosts` trỏ sai) |
| 404 | tới nginx rồi nhưng `Host` header không khớp rule Ingress |
| 401 | đã vào tới app, sai `X-API-KEY` |
| 503 | nginx tới được nhưng không có endpoint — pod chưa Ready, hoặc NetworkPolicy chặn |

Feed giả lập chỉ sinh 4 symbol: `VN30F2607`, `HPG`, `VCB`, `FPT`. Symbol khác trả
404 đúng nghĩa "không có dữ liệu", không phải lỗi route.

### 4.7. Thử KEDA có scale thật không

`values-staging.yaml` cố ý hạ `lagThreshold` xuống 100 để quan sát được:

```powershell
# Cửa sổ 1 — theo dõi
kubectl -n trading-staging get hpa,pods -w

# Cửa sổ 2 — bơm tải
kubectl -n trading-staging exec kafka-0 -- sh -c `
  "seq 1 50000 | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic stock.ticks"
```

Tổng lag vượt ~200 thì `pricing-risk-service` lên 2 pod. Co lại mất
`scaleDownStabilizationSeconds` = 300s (5 phút) — đó là thiết kế, không phải treo.

Trần 2 pod ở staging không tuỳ tiện: `KAFKA_PARTITIONS=6` chia cho
`setConcurrency(3)` trong `pricing-risk-service/config/KafkaConfig.java`. Pod thứ 3
join group và nhận về **không partition nào**.


### 4.8. TLS tự động bằng cert-manager với Issuer nội bộ

Mục này test **luồng tự động hoá YAML** của cert-manager mà không cần Internet.
(Giải thích cơ chế bên trong — ingress-shim, chuỗi tin cậy, SAN, gia hạn — nằm ở
[mục 6 của README chart](helm/trading-system/README.md#6-tls-tự-động-bằng-cert-manager--cơ-chế-bên-trong).)
Staging/prod thật dùng ACME (Let's Encrypt), nhưng minikube không có domain công
khai nên không qua được thử thách HTTP-01. Issuer tự ký test được **đúng cùng cơ
chế**: object YAML → cert-manager quan sát → tự sinh Secret.

#### Chuỗi bốn mắt xích

```
(1) Issuer  trading-local-issuer-root     kind: selfSigned
         │  chỉ biết tự ký, dùng một lần
         ▼
(2) Certificate  trading-local-issuer-ca  isCA: true
         │  → Secret trading-local-issuer-ca-key-pair
         ▼
(3) Issuer  trading-local-issuer          kind: ca
         │  ký bằng CA ở (2)
         ▼
(4) Ingress  annotation cert-manager.io/issuer
            → cert-manager TỰ tạo Certificate → Secret trading-tls
```

**Mắt (4) là thứ đáng test nhất**: chart **không** chứa object `Certificate` nào
cho Ingress. cert-manager đọc annotation + `spec.tls` rồi tự tạo. Ba mắt đầu chỉ
là cách dựng một CA nội bộ để có *một* chứng chỉ gốc đáng tin dùng chung.

#### Vì sao cần hai Issuer, không phải một

Chỗ này hay gây khựng, vì chữ `selfSigned` gợi ý sai.

`selfSigned` **không phải** "một CA tự ký rồi ký tiếp cho người khác". Nó là một
**chế độ ký**, không phải một thực thể nắm khoá:

> "Certificate nào xin, hãy ký bằng **chính khoá riêng của Certificate đó**."

Issuer `selfSigned` **không có khoá nào cả**. Nó không ký gì hết — nó bảo mỗi
chứng chỉ tự ký lên chính mình. Hệ quả: hai chứng chỉ do **cùng một** Issuer
`selfSigned` cấp ra là **hai gốc tin cậy riêng biệt**, không liên quan gì nhau.

Kiểm chứng được bằng một Issuer `selfSigned` và hai Certificate `api-a`, `api-b`:

```bash
openssl verify -CAfile api-a.crt api-a.crt    # api-a.crt: OK  -> no LA goc cua chinh no
openssl verify -CAfile api-a.crt api-b.crt    # verification failed (error 18)
                                              #                -> KHONG bao lanh duoc cho ai
```

Hai khoá công khai khác nhau → hai gốc khác nhau.

**Vì sao đó là vấn đề:** để trình duyệt tin một chứng chỉ, phải cài **gốc** của
nó vào kho tin cậy. Với `selfSigned`, gốc chính là từng chứng chỉ một:

```
cài api-a.crt  -> chỉ tin api-a.local
cài api-b.crt  -> chỉ tin api-b.local
thêm service   -> cài thêm lần nữa
cert xoay vòng 90 ngày -> gốc đổi -> cài LẠI tất cả     ← chỗ đau nhất
```

**Cách giải:** `selfSigned` ký được cho *một* chứng chỉ, nên dùng nó đúng **một
lần** — để tạo ra thứ nó không tự làm được: một chứng chỉ có `isCA: true`.
Trường đó bật cờ `basicConstraints CA:TRUE`, và **chỉ** chứng chỉ có cờ này mới
ký được cho chứng chỉ khác. Mắt (3) là Issuer kiểu `ca`: nó **có** khoá (đọc từ
Secret ở mắt 2) nên ký được cho người khác.

Kiểm trên chính cluster này:

```bash
openssl verify -CAfile ca.crt leaf.crt     # OK                  <- CA bao lanh duoc
openssl verify -CAfile ca.crt ca.crt       # OK                  <- CA la goc, tu ky
openssl verify -CAfile leaf.crt leaf.crt   # verification failed <- leaf KHONG phai goc
```

Kết quả: chỉ cần cài **một** file `ca.crt` vào Windows. Mọi chứng chỉ do CA đó
ký — hôm nay và mọi lần xoay vòng sau — đều được tin ngay.

| | `useCA: false` | `useCA: true` (mặc định) |
| :--- | :--- | :--- |
| Phải cài vào trust store | **Từng cert** của từng service | **Một** CA duy nhất |
| Cert xoay vòng 90 ngày | Gốc đổi → cài lại | CA giữ nguyên 1 năm → không đụng gì |
| Thêm service mới | Cài thêm một cert | Không phải làm gì |
| Giống production | Không | Có — ACME cũng là CA ký cho leaf |

> Phần giải thích đầy đủ, kèm toàn bộ output của thí nghiệm, ở
> [mục 6.3 của README chart](helm/trading-system/README.md#63-vì-sao-phải-hai-issuer-không-phải-một).


Dùng `Issuer` (namespaced) chứ không `ClusterIssuer`: mọi thứ nằm gọn trong
namespace của release, `helm uninstall` là sạch, không cần quyền cluster-scoped.


#### Tự tạo CA thay vì để cert-manager sinh (khuyên dùng)

##### Vì sao

CA do cert-manager sinh **gắn với vòng đời Helm release**. `helm uninstall` rồi
cài lại, xoá namespace, hay dựng lại minikube — đều ra một CA **mới** với
thumbprint **mới**. Cái đã cài vào kho tin cậy Windows thành vô dụng, phải cài
lại mỗi lần.

CA tự tạo sống trên **đĩa**, độc lập với Helm và với cluster:

| | cert-manager sinh (`useCA: true`) | Tự tạo (`existingCaSecret`) |
| :--- | :--- | :--- |
| Dựng lại cluster | CA mới → cài lại vào Windows | **Vẫn CA cũ** |
| `helm uninstall` | Mất CA | Secret ngoài Helm, **không mất** |
| Sao lưu / chia cho team | Không | Có — chỉ là 2 file |
| Kiểm soát hạn, thuật toán, tên | Hạn chế | Toàn quyền |

---

##### Toàn bộ quy trình — 7 bước

Có script làm sẵn, nhưng mỗi bước dưới đây đều kèm lệnh thủ công tương đương để
bạn hiểu nó làm gì (và làm tay được khi không có PowerShell).

###### Bước 1 — Sinh khoá riêng của CA

```powershell
# Đường có script:
.\scripts\helm\New-LocalCA.ps1
```

Làm tay (openssl đi kèm Git for Windows, ở `C:\Program Files\Git\usr\bin\openssl.exe`):

```bash
mkdir -p helm/.ca
openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out helm/.ca/ca.key
```

> **Bắt buộc dùng `genpkey`, KHÔNG dùng `ecparam -genkey`.** Lý do ở mục bẫy số 1
> bên dưới. Kiểm ngay:
> ```bash
> head -1 helm/.ca/ca.key
> # -----BEGIN PRIVATE KEY-----      <- ĐÚNG (PKCS#8)
> # -----BEGIN EC PARAMETERS-----    <- SAI, cert-manager sẽ từ chối
> ```

Muốn RSA thay vì ECDSA (tương thích thiết bị cũ hơn):

```bash
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:4096 -out helm/.ca/ca.key
```

###### Bước 2 — Tự ký chứng chỉ CA

Cần một file cấu hình vì phần mở rộng `basicConstraints` không truyền được qua
dòng lệnh `req -x509`:

```bash
cat > helm/.ca/ca.cnf <<'EOF'
[req]
distinguished_name = dn
prompt = no
x509_extensions = v3_ca

[dn]
CN = trading-local-ca

[v3_ca]
basicConstraints = critical,CA:true,pathlen:0
keyUsage = critical,keyCertSign,cRLSign
subjectKeyIdentifier = hash
EOF

openssl req -x509 -new -key helm/.ca/ca.key -sha256 -days 3650 \
  -out helm/.ca/ca.crt -config helm/.ca/ca.cnf

rm helm/.ca/ca.cnf
```

Ba trường quyết định:

| Trường | Ý nghĩa |
| :--- | :--- |
| `basicConstraints = CA:true` | **Trường duy nhất** cho phép chứng chỉ này ký chứng chỉ khác. Thiếu nó thì Issuer tạo được nhưng mọi lần ký đều hỏng |
| `pathlen:0` | CA này chỉ ký được cho cert lá, không ký được CA con |
| `keyUsage = keyCertSign,cRLSign` | Quyền ký chứng chỉ và ký danh sách thu hồi |

`-days 3650` = 10 năm. CA nội bộ nên để hạn dài: hết hạn nghĩa là phải cài lại
vào **mọi** máy.

Kiểm:

```bash
openssl x509 -in helm/.ca/ca.crt -noout -subject -dates -ext basicConstraints
```
```
subject=CN=trading-local-ca
notBefore=Nov 15 15:17:28 2026 GMT
notAfter=Nov 12 15:17:28 2036 GMT
X509v3 Basic Constraints: critical
    CA:TRUE, pathlen:0
```

###### Bước 3 — Nạp CA vào cluster thành Secret

```bash
kubectl create secret tls trading-ca \
  --cert=helm/.ca/ca.crt --key=helm/.ca/ca.key \
  -n trading-staging --dry-run=client -o yaml | kubectl apply -f -
```

`--dry-run=client | apply` để lệnh **idempotent** — chạy lại cập nhật được Secret
đã có, thay vì báo `AlreadyExists`.

> Secret **phải ở cùng namespace với Issuer** — `Issuer` là namespaced nên
> cert-manager chỉ tìm trong chính namespace đó. Muốn dùng chung nhiều namespace
> thì phải đổi sang `ClusterIssuer`.

> Secret này **không do Helm quản** → `helm uninstall` không xoá nó. Đó chính là
> điều ta muốn.

###### Bước 4 — Trỏ chart vào CA đó

```yaml
# helm/trading-system/values-staging-local.yaml
ingress:
  tls:
    enabled: true
    secretName: trading-tls
    selfSignedIssuer:
      enabled: true
      name: trading-local-issuer
      existingCaSecret: trading-ca     # <-- dòng quyết định
```

Đặt `existingCaSecret` thì chart **bỏ qua** `useCA`: nó không sinh
`-root`/`-ca` nữa, chỉ tạo **một** `Issuer kind: ca` trỏ vào Secret.

Xem trước khi deploy:

```bash
helm template t ./helm/trading-system \
  -f ./helm/trading-system/values-staging.yaml \
  -f ./helm/trading-system/values-staging-local.yaml \
  -n trading-staging | grep -A6 "kind: Issuer"
```

###### Bước 5 — Deploy

```powershell
.\scripts\helm\Deploy-Staging.ps1 -SkipBuild
```

Chờ Issuer sẵn sàng:

```bash
kubectl -n trading-staging get issuer trading-local-issuer
```
```
NAME                   READY   AGE
trading-local-issuer   True    30s
```

`READY=False` thì xem lý do — gần như luôn là định dạng khoá (bẫy số 1):

```bash
kubectl -n trading-staging get issuer trading-local-issuer \
  -o jsonpath='{.status.conditions[0].reason}: {.status.conditions[0].message}'
```

###### Bước 6 — Ép cấp lại chứng chỉ (chỉ khi CHUYỂN từ CA cũ)

Cài mới từ đầu thì **bỏ qua bước này**.

Nếu trước đó đã có cert do CA khác ký, nó **không tự được ký lại** — cert-manager
chỉ cấp lại khi `spec` đổi hoặc tới hạn gia hạn. Xoá Secret để ép:

```bash
kubectl -n trading-staging delete secret trading-tls
```

cert-manager thấy thiếu và sinh lại ngay. Kiểm bằng CA của bạn:

```bash
kubectl -n trading-staging get secret trading-tls -o jsonpath='{.data.tls\.crt}' \
  | base64 -d > /tmp/leaf.crt
openssl verify -CAfile helm/.ca/ca.crt /tmp/leaf.crt
```
```
/tmp/leaf.crt: OK
```

Ra `error 20: unable to get local issuer certificate` nghĩa là cert cũ còn đó —
xoá Secret lần nữa.

###### Bước 7 — Cài CA vào Windows

Làm **một** lần, dùng cho mọi lần dựng lại cluster sau này.

```powershell
certutil -addstore -f -user Root helm\.ca\ca.crt
```

Hoặc dùng script (nó tự lấy đúng CA mà Issuer đang dùng, và kiểm chứng luôn):

```powershell
.\scripts\helm\Trust-LocalCA.ps1
```

> Cờ `-f` bắt buộc trong phiên không tương tác — xem bẫy số 3.

Xác nhận:

```powershell
Get-ChildItem Cert:\CurrentUser\Root | Where-Object { $_.Subject -like '*trading-local-ca*' }
```
```
Thumbprint                                Subject
----------                                -------
4D2533C62F1A6682040F5EF3F7E50335ACF56FE7  CN=trading-local-ca
```

Thumbprint này phải **khớp** với file trên đĩa:

```powershell
(New-Object System.Security.Cryptography.X509Certificates.X509Certificate2 `
  ((Resolve-Path .\helm\.ca\ca.crt).Path)).Thumbprint
```

---

##### Kiểm tra cuối

```powershell
.\scripts\helm\Verify-Release.ps1 -Namespace trading-staging
```
```
== TLS tu dong hoa (cert-manager)
   [OK]   Issuer trading-local-issuer  Ready (KeyPairVerified)
   [OK]   Certificate trading-tls  Ready  -> secret/trading-tls
   [OK]   Certificate cho Ingress da duoc cert-manager TU sinh (secret/trading-tls).
            issuer : CN=trading-local-ca
            het han: 2027-02-13 22:22 (90 ngay nua)
            SAN    : DNS Name=trading-staging.local
```

Gọi HTTPS **không truyền `--cacert`** — nếu kho tin cậy đã ăn thì nó dùng CA từ
kho Windows:

```powershell
kubectl -n ingress-nginx port-forward svc/ingress-nginx-controller 443:443   # cửa sổ 1

curl.exe --ssl-no-revoke --resolve trading-staging.local:443:127.0.0.1 `
  -X POST https://trading-staging.local/api/v1/orders `
  -H "X-API-KEY: <key>" -H "Content-Type: application/json" `
  -d '{\"clientOrderId\":\"t1\",\"symbol\":\"FPT\",\"side\":\"BUY\",\"price\":105000,\"quantity\":100,\"accountId\":\"ACC1\"}'
```
```
{"status":"REJECTED","reason":"RISK_LEVEL_HIGH volatility=273.09", ...}
HTTP 422
```

`422` nghĩa là đã đi hết chuỗi TLS → nginx → order-service, và risk engine từ
chối lệnh theo nghiệp vụ. **TLS đã đạt.**

Mở Chrome kiểm bằng mắt (không cần sửa hosts):

```powershell
& "$env:ProgramFiles\Google\Chrome\Application\chrome.exe" `
  --user-data-dir="$env:TEMP\chrome-tls-check" `
  --host-resolver-rules="MAP trading-staging.local 127.0.0.1" `  # KHONG them :443
  https://trading-staging.local/api/v1/orders
```

Ổ khoá → Connection is secure → Certificate → **Issued By: trading-local-ca**.

---

##### Ba bẫy đã gặp khi làm thật

**1. `ErrGetKeyPair: unknown private key type: EC PARAMETERS`**

`openssl ecparam -genkey` ghi ra **hai** khối PEM — `EC PARAMETERS` rồi mới
`EC PRIVATE KEY`. cert-manager đọc khối **đầu tiên** và chịu thua. Issuer đứng
mãi ở `Ready=False`, Certificate không bao giờ được ký, còn Ingress thì vẫn chạy
nên nhìn bề ngoài không thấy gì sai.

```bash
openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out ca.key   # ĐÚNG
openssl ecparam -genkey -name prime256v1 -out ca.key                          # SAI
```

**2. Đổi CA phía sau Issuer KHÔNG làm cert cũ được ký lại**

cert-manager chỉ cấp lại khi `spec` của Certificate đổi hoặc tới hạn gia hạn.
Thay ruột Secret CA thì Issuer nhận CA mới ngay (`Ready=True`), nhưng
`trading-tls` **vẫn là cert cũ do CA cũ ký**. Triệu chứng: Issuer xanh, Certificate
xanh, nhưng `openssl verify` bằng CA mới trượt với `error 20`.

Xoá Secret để ép cấp lại (bước 6).

**3. `UI is not allowed in this operation` khi cài vào kho Root**

Windows muốn hiện hộp thoại cảnh báo bảo mật; trong phiên không tương tác
(script, CI, terminal của agent) nó chết luôn. `certutil` với cờ `-f` bỏ qua hộp
thoại đó. `Import-Certificate` của PowerShell thì **không** có đường vòng này.

---

##### Sao lưu và rủi ro

`helm/.ca/` đã được gitignore. **Giữ cả `ca.key` lẫn `ca.crt`** ở chỗ an toàn —
mất `ca.key` thì không ký tiếp được và phải làm lại từ bước 1, kể cả cài CA mới
vào mọi máy.

Ai có `ca.key` thì ký được chứng chỉ cho **bất kỳ tên miền nào**, và mọi máy đã
cài CA này sẽ tin. Đừng commit, đừng gửi qua chat/mail. Dùng cho cluster local
một mình thì chấp nhận được; **không** nên làm trên máy dùng chung.

Tạo CA mới (`New-LocalCA.ps1 -Force`) làm **mọi** chứng chỉ đã cấp hết hợp lệ và
mọi máy đã cài CA cũ phải cài lại — script mặc định từ chối ghi đè vì lý do đó.

Chia CA cho máy khác trong team: chỉ cần gửi `ca.crt` (công khai) để họ cài vào
trust store. `ca.key` thì **không** — trừ khi máy đó cũng cần ký, và khi đó phải
gửi qua kênh an toàn.

#### Bật

```powershell
.\scripts\helm\Install-Prereqs.ps1 -Target minikube   # đã gồm cert-manager
```

```yaml
# values-staging-local.yaml
ingress:
  tls:
    enabled: true
    secretName: trading-tls
    selfSignedIssuer:
      enabled: true
      name: trading-local-issuer
      useCA: true
```

Thiếu cert-manager thì `helm` chết ngay ở render: `no matches for kind "Issuer"`.
`Deploy-Staging.ps1` quét values và bắt lỗi này **trước** khi build image.

#### Kiểm tra

```powershell
.\scripts\helm\Verify-Release.ps1 -Namespace trading-staging
```

Kết quả đạt trông như sau:

```
== TLS tu dong hoa (cert-manager)
   [OK]   Issuer trading-local-issuer  Ready (KeyPairVerified)
   [OK]   Issuer trading-local-issuer-root  Ready (IsReady)
   [OK]   Certificate trading-local-issuer-ca  Ready  -> secret/...-ca-key-pair
   [OK]   Certificate trading-tls  Ready  -> secret/trading-tls
   [OK]   Certificate cho Ingress da duoc cert-manager TU sinh (secret/trading-tls).
            issuer : CN=trading-local-ca
            het han: 2027-02-13 21:13 (90 ngay nua)
            SAN    : DNS Name=trading-staging.local
```

Xem tay:

```powershell
kubectl -n trading-staging get issuer,certificate
kubectl -n trading-staging get certificaterequest     # nơi ghi LÝ DO khi cấp phát hỏng
```

> **Hỏng kiểu im lặng cần biết**: nếu cert-manager **không** sinh được
> Certificate, Ingress vẫn tồn tại và nginx vẫn phục vụ HTTPS — bằng **cert mặc
> định tự ký của chính nginx**. Trình duyệt báo "không tin cậy" y hệt trường hợp
> CA nội bộ của ta, nên rất dễ tưởng là bình thường. Luôn kiểm `issuer` của cert
> thực tế, đừng chỉ nhìn "có HTTPS".

#### Gọi thử HTTPS

```powershell
# Lấy CA ra file
kubectl -n trading-staging get secret trading-local-issuer-ca-key-pair `
  -o jsonpath='{.data.tls\.crt}' | ForEach-Object { [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($_)) } > ca-local.crt

# Cửa sổ 1
kubectl -n ingress-nginx port-forward svc/ingress-nginx-controller 8443:443

# Cửa sổ 2 — KHÔNG dùng -k, xác thực bằng chính CA của ta
curl.exe --ssl-no-revoke --cacert ca-local.crt `
  --resolve trading-staging.local:8443:127.0.0.1 `
  -X POST https://trading-staging.local:8443/api/v1/orders `
  -H "X-API-KEY: <key>" -H "Content-Type: application/json" `
  -d '{\"clientOrderId\":\"t1\",\"symbol\":\"FPT\",\"side\":\"BUY\",\"price\":105000,\"quantity\":100,\"accountId\":\"ACC1\"}'
```

> **`--ssl-no-revoke` là bắt buộc trên Windows.** curl của Windows dùng Schannel,
> vốn đòi kiểm tra thu hồi (CRL/OCSP). CA tự ký không có điểm phân phối CRL nên
> Schannel từ chối với `schannel: the revocation status is unknown` — **không
> phải lỗi chứng chỉ**. curl trong Git Bash / WSL (OpenSSL) không cần cờ này.

Phép thử ngược để chắc chắn chuỗi tin cậy được thực thi thật:

| Lệnh | Kết quả đúng |
| :--- | :--- |
| `--cacert ca-local.crt` | `422` hoặc `401` — qua TLS, tới app |
| `--cacert <CA khác>` | `000` — TLS từ chối |
| không có `--cacert` | `000` — CA nội bộ không nằm trong trust store hệ thống |


#### Cài CA vào Windows để Chrome tin

Chrome trên Windows **không có kho chứng chỉ riêng** — nó đọc thẳng kho của hệ
điều hành. Đưa CA vào "Trusted Root Certification Authorities" là Chrome, Edge,
.NET, PowerShell đều tin theo.

```powershell
.\scripts\helm\Trust-LocalCA.ps1              # cài + kiểm chứng
.\scripts\helm\Trust-LocalCA.ps1 -OpenChrome  # cài + mở Chrome luôn
.\scripts\helm\Trust-LocalCA.ps1 -Uninstall   # gỡ khi không cần nữa
```

Script lấy CA thẳng từ Secret trong cluster (đọc tên Issuer từ annotation của
Ingress, không đoán), ghi ra `ca-local.crt`, nạp vào kho, rồi tự kiểm chứng bằng
`curl` **không truyền `--cacert`** — nếu ra `401` thay vì `000` thì kho tin cậy
đã ăn.

> **Dùng kho CurrentUser, không phải kho máy.** Không cần Administrator, và phạm
> vi ảnh hưởng gói trong đúng tài khoản đang dùng.

##### Hai bẫy khi tự gõ tay

**`Import-Certificate` thất bại với `UI is not allowed in this operation`.**
Thêm cert vào kho Root làm Windows muốn hiện hộp thoại cảnh báo bảo mật; trong
phiên không tương tác (script, CI, terminal của agent) nó chết luôn. `certutil`
với cờ `-f` bỏ qua hộp thoại đó:

```powershell
certutil -addstore -f -user Root ca-local.crt
```

**Chrome bỏ qua mọi cờ dòng lệnh nếu đã có cửa sổ đang mở.** Nó chỉ mở thêm tab
trong tiến trình cũ. Phải ép một hồ sơ riêng bằng `--user-data-dir`, nếu không
`--host-resolver-rules` không có tác dụng gì và bạn sẽ tưởng là cờ sai.

##### Không cần sửa hosts

`hosts` cần quyền Administrator. Tránh được bằng cách để Chrome tự phân giải tên:

```powershell
kubectl -n ingress-nginx port-forward svc/ingress-nginx-controller 443:443   # giữ mở

& "$env:ProgramFiles\Google\Chrome\Application\chrome.exe" `
  --user-data-dir="$env:TEMP\chrome-trading-staging" `
  --host-resolver-rules="MAP trading-staging.local 127.0.0.1" `  # KHONG them :443
  https://trading-staging.local/api/v1/orders
```

Cổng 443 **bind được không cần admin** trên Windows (khác Linux, nơi cổng dưới
1024 cần root).

Cách này chỉ áp dụng cho cửa sổ Chrome đó. Muốn mọi trình duyệt và mọi công cụ
(curl, Postman, IDE) dùng được tên này thì thêm dòng vào hosts — cần Administrator:

```powershell
Start-Process powershell -Verb RunAs -ArgumentList '-NoProfile','-Command',
  'Add-Content -Path "$env:SystemRoot\System32\drivers\etc\hosts" -Value "127.0.0.1 trading-staging.local"'
```

##### Nhìn gì để biết đã đạt

| Dấu hiệu | Ý nghĩa |
| :--- | :--- |
| Ổ khoá, không có "Not secure" | TLS đã đạt |
| Ổ khoá → Connection is secure → **Issued By: trading-local-ca** | Đúng cert của ta, không phải cert mặc định của nginx |
| Nội dung trang là `401` JSON | **Đúng** — trình duyệt không gửi `X-API-KEY`. Mục tiêu của bước này là TLS, không phải gọi API thành công |
| `ERR_CERT_AUTHORITY_INVALID` | Kho tin cậy chưa ăn, hoặc Chrome chưa khởi động lại |
| `ERR_CONNECTION_REFUSED` | port-forward đã chết |

##### Rủi ro và cách gỡ

Một CA trong kho Root ký được chứng chỉ cho **bất kỳ tên miền nào** và máy này
sẽ tin. Khoá riêng của nó nằm trong Secret `<issuer>-ca-key-pair` của cluster —
ai đọc được Secret đó thì giả mạo được mọi site đối với máy này.

Chấp nhận được cho cluster local dùng một mình. **Không** nên làm trên máy dùng
chung, và nên gỡ khi xong:

```powershell
.\scripts\helm\Trust-LocalCA.ps1 -Uninstall
```

Chrome cache quyết định TLS, nên phải đóng hết cửa sổ Chrome rồi mở lại mới thấy
nó quay về báo "không tin cậy".

Kiểm tra bằng tay lúc nào cũng được:

```powershell
Get-ChildItem Cert:\CurrentUser\Root | Where-Object { $_.Subject -like '*trading-local-ca*' }
certmgr.msc    # Trusted Root Certification Authorities -> Certificates
```

> **CA hết hạn sau 1 năm** (`caDuration: 8760h`). Khi cert-manager cấp CA mới,
> thumbprint đổi và CA cũ trong kho thành vô dụng — chạy lại script để nạp cái
> mới. Nó so theo thumbprint nên chạy lại nhiều lần vô hại.
#### Bẫy đã gặp khi chạy thật

**`504 Gateway Time-out` sau khi TLS đã qua.** TLS xong nghĩa là nginx đã nhận
request; 504 là nginx không với được backend. Nguyên nhân gặp phải: nginx giữ
**upstream cũ** (IP pod order-service đã đổi sau lần deploy trước) vì controller
mất watch Endpoints trong một đợt cluster nghẽn. Kiểm và vá:

```powershell
kubectl -n ingress-nginx logs -l app.kubernetes.io/component=controller --tail=20 |
  Select-String "upstream"
kubectl -n trading-staging get endpoints order-service     # IP đúng là cái này
kubectl -n ingress-nginx rollout restart deploy ingress-nginx-controller
```

**cert-manager webhook chưa Ready.** Tạo `Issuer` khi webhook chưa lên sẽ chết với
`failed calling webhook ... connection refused`, dù cert-manager "đã cài xong".
`Install-Prereqs.ps1` cảnh báo trường hợp này.

#### Lên prod thì đổi gì

`values-prod.yaml` giữ `cert-manager.io/cluster-issuer: letsencrypt-prod` và
**không** bật `selfSignedIssuer` — prod có domain công khai nên dùng ACME thật.
Cấu trúc YAML giống hệt; chỉ khác loại Issuer. Đó chính là lý do test luồng này ở
local có giá trị: cùng một cơ chế, chỉ thay nguồn ký.

### 4.9. Bản đồ API và cách xem lệnh đã đặt

Mỗi service có prefix **riêng**, nhưng Ingress chỉ route **một** đường:

| Đường | Service | Qua Ingress? |
| :--- | :--- | :--- |
| `POST /api/v1/orders` | order-service `:8083` | **Có** |
| `GET  /api/v1/orders` | order-service `:8083` | **Có** — liệt kê, phân trang |
| `GET  /api/v1/orders/{clientOrderId}` | order-service `:8083` | **Có** — tra một lệnh |
| `GET  /api/v1/marketdata` | pricing-risk-service `:8082` | **Có** — snapshot mọi symbol |
| `GET  /api/v1/marketdata/{symbol}` | pricing-risk-service `:8082` | **Có** |
| `GET  /api/v1/marketdata/{symbol}/history` | pricing-risk-service `:8082` | **Có** |
| `GET  /api/v1/ticks` | market-gateway `:8081` | Không — port-forward |
| `GET  /api/v1/executions/{clientOrderId}` | matching-engine `:8084` | Không — port-forward |

Ingress route theo danh sách `ingress.routes` trong values, `pathType: Prefix` nên
mọi đường con cũng vào (`/api/v1/orders/{id}`, `/api/v1/marketdata/{symbol}`).



#### Thêm một route mới vào Ingress

Sửa **một chỗ duy nhất** — `ingress.routes` trong values:

```yaml
ingress:
  routes:
    - path: /api/v1/orders
      service: order-service
    - path: /api/v1/marketdata
      service: pricing-risk-service
    - path: /api/v1/executions        # ví dụ thêm mới
      service: matching-engine
```

Mỗi mục sinh ra **hai** object, không phải một:

1. một `path` trong Ingress
2. một NetworkPolicy `allow-ingress-to-<service>` mở đúng cổng cho nginx

> **Vì sao phải sinh cả hai từ cùng một chỗ.** Chart có `default-deny-all`, nên
> thêm route mà quên mở NetworkPolicy thì nginx nhận request rồi treo tới timeout
> → **HTTP 504**, và log của app **không có gì cả** vì gói tin chưa bao giờ tới
> nơi. Đây đúng là lỗi đã gặp khi chạy thật. Sinh từ một danh sách thì hai thứ
> không thể lệch nhau.

Cổng tra từ `trading.servicePorts` trong `_helpers.tpl`, lấy thẳng
`.Values.<service>.port` — không viết cứng, nên đổi cổng trong values là
NetworkPolicy đổi theo.

`service` phải là một trong bốn tên có trong bảng tra đó
(`market-gateway`, `pricing-risk-service`, `order-service`, `matching-engine`);
sai tên thì render chết ngay với lỗi `index` trên nil, không âm thầm tạo rule rỗng.

Kiểm trước khi deploy:

```bash
helm template t ./helm/trading-system -f ./helm/trading-system/values-staging.yaml \
  -f ./helm/trading-system/values-staging-local.yaml -n trading-staging \
  | grep -E "allow-ingress-to-|path:|port: 80"
```
#### Xem lệnh đã đặt

```powershell
$key = (Select-String -Path helm\.secrets.staging.env -Pattern '^ORDER_API_KEY=(.*)$').Matches.Groups[1].Value

# Liệt kê, mới nhất trước
curl.exe -H "X-API-KEY: $key" "https://trading-staging.local/api/v1/orders?size=20"

# Lọc theo trạng thái / tài khoản
curl.exe -H "X-API-KEY: $key" "https://trading-staging.local/api/v1/orders?status=REJECTED"
curl.exe -H "X-API-KEY: $key" "https://trading-staging.local/api/v1/orders?accountId=ACC1"

# Tra một lệnh theo clientOrderId
curl.exe -H "X-API-KEY: $key" "https://trading-staging.local/api/v1/orders/ownca-1"
```

Dạng trả về của endpoint liệt kê là `Page` chuẩn của Spring Data:

```json
{
  "content": [ { "orderId": "...", "clientOrderId": "ownca-1", "status": "REJECTED",
                 "reason": "RISK_LEVEL_HIGH volatility=273.09", "symbol": "FPT",
                 "side": "BUY", "price": 105000.0000, "quantity": 100,
                 "createdAt": "2026-11-15T15:22:45.626477Z" } ],
  "totalElements": 3, "totalPages": 1, "size": 20, "number": 0
}
```

| Tham số | Mặc định | Ghi chú |
| :--- | :--- | :--- |
| `page` | 0 | Âm bị ép về 0 |
| `size` | 20 | **Chặn cứng ở 200** |
| `status` | — | So khớp hoa/thường tuỳ ý (`rejected` cũng được) |
| `accountId` | — | Kết hợp được với `status` |

Sắp xếp cố định `createdAt DESC`.

> **`size` bị chặn ở 200 là cố ý.** Bảng `ordersvc.orders` giữ **mọi** lệnh kể cả
> `REJECTED` để phục vụ audit, nên nó chỉ có tăng. Một truy vấn không giới hạn sẽ
> kéo cả bảng vào heap 336Mi của pod và giết nó bằng OOM — sự cố chỉ xảy ra ở môi
> trường có dữ liệu thật, không bao giờ lộ ra lúc dev.

Kết quả rỗng trả **200 với `content: []`**, không phải 404. Chỉ tra theo
`clientOrderId` mà không thấy mới trả **404**.

#### Trước khi có endpoint GET: đọc thẳng DB

Vẫn hữu ích khi cần truy vấn phức tạp hơn API:

```powershell
kubectl -n trading-staging exec postgres-0 -- psql -U trading -d trading -c `
  "SELECT client_order_id, symbol, side, price, quantity, status, reason, created_at
   FROM ordersvc.orders ORDER BY created_at DESC LIMIT 20;"
```

Hoặc mở bằng GUI (DBeaver, IntelliJ):

```powershell
kubectl -n trading-staging port-forward svc/postgres 5432:5432
#   host=127.0.0.1  db=trading  user=trading
#   pass = DB_PASSWORD trong helm\.secrets.staging.env
```

Ba schema đáng quan tâm:

| Schema | Bảng | Nội dung |
| :--- | :--- | :--- |
| `ordersvc` | `orders` | Mọi lệnh, kể cả REJECTED |
| `ordersvc` | `outbox_event` | Sự kiện chờ đẩy sang Kafka |
| `matchsvc` | `execution` | Khớp lệnh |
| `public` | `market_snapshot` | Giá + chỉ số rủi ro do pricing-risk ghi |
### 4.10. Gỡ staging

```powershell
helm uninstall trading -n trading-staging
kubectl delete namespace trading-staging     # xoá luôn PVC
```

Hoặc làm lại từ đầu, có xác nhận:

```powershell
.\scripts\helm\Deploy-Staging.ps1 -Recreate
```

---

## 5. PRODUCTION

### 5.1. Khác biệt căn bản với staging

Prod **không build gì cả**. Image phải đã được CI build, test, push. Script chỉ
phân giải tag → digest rồi deploy theo digest.

> **Vì sao digest chứ không phải tag**: tag là con trỏ có thể ghi đè. Deploy
> `:v1.4.2` hôm nay và `:v1.4.2` tuần sau có thể ra hai binary khác nhau nếu ai
> đó push đè. Với digest, `helm rollback` đưa bạn về đúng **bit** đã chạy, không
> phải "cái mà tag đó đang trỏ tới lúc này".

### 5.2. Checklist hạ tầng trước lần deploy đầu

**(a) RDS** — `postgres.enabled=false`, app nối ra ngoài cluster.

```yaml
# values-prod.yaml — PHẢI sửa, đang là giá trị mẫu
config:
  DB_HOST: "trading-prod.<id-that>.ap-southeast-1.rds.amazonaws.com"
```

Security group của RDS phải mở 5432 cho CIDR của node group, và
`networkPolicy.externalDb.cidr` phải **chứa** IP của RDS:

```yaml
networkPolicy:
  externalDb:
    enabled: true
    cidr: 10.100.0.0/16    # subnet chứa RDS
```

Schema do Flyway tự tạo khi app khởi động (`spring.flyway.enabled=true`), nhưng
user `trading` phải có quyền `CREATE` trên database.

**(b) Managed Kafka (MSK/Confluent)** — `kafka.enabled=false`, Job tạo topic cũng
tắt. Topic phải được tạo **trước** bằng tay, đúng 8 cái, đúng partition/retention:

```bash
# 12 partition, RF 3 — khớp KAFKA_PARTITIONS / KAFKA_TOPIC_REPLICAS
for T in stock.ticks stock.enriched stock.orders stock.executions; do
  kafka-topics.sh --bootstrap-server $BS --create --if-not-exists \
    --topic $T --partitions 12 --replication-factor 3 \
    --config retention.ms=604800000
done
for T in stock.ticks.DLT stock.enriched.DLT stock.orders.DLT stock.executions.DLT; do
  kafka-topics.sh --bootstrap-server $BS --create --if-not-exists \
    --topic $T --partitions 12 --replication-factor 3 \
    --config retention.ms=2592000000
done
```

> `maxReplicaCount: 4` trong KEDA **không tuỳ tiện**: bằng `12 partition / setConcurrency(3)`.
> Muốn scale cao hơn thì phải **tăng partition trước**, không phải nâng max.

**(c) External Secrets Operator**

```powershell
helm repo add external-secrets https://charts.external-secrets.io
helm upgrade --install external-secrets external-secrets/external-secrets `
  -n external-secrets --create-namespace --set installCRDs=true --wait
```

Tạo secret trên AWS đúng tên và đúng property mà chart đọc:

```bash
aws secretsmanager create-secret --name trading/prod/db \
  --secret-string '{"username":"trading","password":"<...>"}'
aws secretsmanager create-secret --name trading/prod/api \
  --secret-string '{"order-api-key":"<...>"}'
```

> Tên property phải **chính xác** `username`, `password`, `order-api-key` — chúng
> được hardcode trong `templates/externalsecret.yaml`. Sai tên thì ExternalSecret
> báo `Ready=False` và pod kẹt `CreateContainerConfigError` mà thông báo lỗi
> **không hề nhắc tới ESO**.

Rồi tạo `ClusterSecretStore` (IRSA — không dùng access key tĩnh):

```yaml
apiVersion: external-secrets.io/v1beta1
kind: ClusterSecretStore
metadata:
  name: aws-secrets-manager
spec:
  provider:
    aws:
      service: SecretsManager
      region: ap-southeast-1
      auth:
        jwt:
          serviceAccountRef:
            name: external-secrets
            namespace: external-secrets
```

ServiceAccount đó cần IAM role với `secretsmanager:GetSecretValue` trên hai ARN trên.

**(d) Ingress + TLS**

```yaml
ingress:
  host: trading.<ten-mien-that>.com     # đang là trading.example.com
  annotations:
    cert-manager.io/cluster-issuer: letsencrypt-prod
```

**(e) PriorityClass** — `priorityClass.enabled=true` tạo `trading-critical`
(value 1000000), đảm bảo pod trading được ưu tiên khi node thiếu chỗ. Object này
**cluster-scoped**: nếu cluster đã có PriorityClass cùng tên do thứ khác tạo,
Helm sẽ từ chối nhận nuôi nó.

### 5.3. Tiền kiểm tra — bắt buộc

```powershell
.\scripts\helm\Deploy-Prod.ps1 -ImageTag v1.4.2 -DryRun
```

Script kiểm theo thứ tự và **dừng ngay** khi gặp lỗi cứng:

1. In context + node, bắt gõ đúng tên context để xác nhận
2. CRD: KEDA, PodMonitor, ExternalSecret — thiếu là dừng
3. `ClusterSecretStore` tồn tại và `Ready=True`
4. Quét giá trị mẫu còn sót trong `values-prod.yaml` (`abcdefg...rds`,
   `trading.example.com`, `sha256:000...`)
5. Phân giải tag → digest qua `docker buildx imagetools inspect` (đọc manifest,
   **không** pull image)
6. `helm --dry-run` + `helm diff upgrade` nếu có plugin

### 5.4. Deploy

```powershell
.\scripts\helm\Deploy-Prod.ps1 -ImageTag v1.4.2
```

Script bắt gõ `deploy` để xác nhận, in revision hiện tại (dùng cho rollback),
rồi chạy `helm upgrade --wait`.

Sau khi xong nó **xác minh image đang chạy đúng bằng digest vừa deploy** — bước
này bắt được trường hợp pod cũ chưa bị thay thế vì PDB chặn, vì quota, hoặc vì
node hết chỗ.

> **Vì sao mặc định KHÔNG có `--atomic`**: `--atomic` tự rollback khi `--wait`
> hết giờ. Ở prod điều đó nguy hiểm — một rollout chậm (kéo image lớn, node scale
> chậm) bị hiểu nhầm là hỏng, và Helm rollback ngay giữa lúc bạn đang có hai
> phiên bản chạy song song. Mặc định ở đây là deploy rồi **quan sát**, người vận
> hành tự quyết. Bật `-Atomic` nếu bạn thật sự muốn.

### 5.5. Trong CI (GitHub Actions)

Khi CI đã biết digest từ bước build, không cần phân giải lại:

```yaml
- name: Deploy prod
  run: |
    helm upgrade --install trading ./helm/trading-system \
      -f ./helm/trading-system/values-prod.yaml \
      -n trading-prod \
      --set marketGateway.image.digest="${{ needs.build.outputs.digest_market_gateway }}" \
      --set pricingRiskService.image.digest="${{ needs.build.outputs.digest_pricing }}" \
      --set orderService.image.digest="${{ needs.build.outputs.digest_order }}" \
      --set matchingEngine.image.digest="${{ needs.build.outputs.digest_matching }}" \
      --wait --timeout 900s
```

Lấy digest ở bước build:

```yaml
- id: push
  uses: docker/build-push-action@v5
  with:
    push: true
    tags: ghcr.io/${{ github.repository }}/order-service:${{ github.sha }}
- run: echo "digest_order=${{ steps.push.outputs.digest }}" >> $GITHUB_OUTPUT
```

Namespace prod phải được tạo + gắn nhãn sở hữu Helm **một lần** trước đó (mục 2.4).

### 5.6. Rollback

```powershell
helm -n trading-prod history trading          # xem các revision
.\scripts\helm\Deploy-Prod.ps1 -Rollback                  # về revision liền trước
.\scripts\helm\Deploy-Prod.ps1 -Rollback -RollbackTo 7    # về revision cụ thể
```

> **Rollback KHÔNG lùi schema database.** Flyway đã chạy migration lên rồi. Nếu
> bản mới có migration phá vỡ tương thích ngược, rollback code sẽ làm app chết ở
> `ddl-auto: validate`. Quy tắc: mọi migration phải tương thích ngược được **một**
> phiên bản (thêm cột nullable trước, xoá cột ở release sau).

---

## 6. Vận hành hằng ngày

```powershell
# Trạng thái release
helm -n trading-staging list
helm -n trading-staging history trading
helm -n trading-staging get values trading           # values ĐANG áp dụng

# So sánh trước khi đổi
helm diff upgrade trading .\helm\trading-system `
  -f .\helm\trading-system\values-staging.yaml -n trading-staging

# Đổi một giá trị mà không deploy lại image
helm upgrade trading .\helm\trading-system `
  -f .\helm\trading-system\values-staging.yaml -n trading-staging `
  --reuse-values --set config.LOGGING_LEVEL_COM_TRADING=DEBUG

# Scale tay (KEDA sẽ ghi đè lại trong vòng pollingInterval=15s)
kubectl -n trading-staging scale deployment order-service --replicas=3
```

> `--reuse-values` giữ nguyên các `--set` trước đó (kể cả mật khẩu). Dùng
> `--reset-values` thì mật khẩu sẽ về rỗng và pod chết — đó là lý do script luôn
> truyền lại mật khẩu từ file thay vì dựa vào `--reuse-values`.

Thay đổi `config.*` làm ConfigMap đổi → annotation `checksum/config` trên pod đổi
→ rolling restart tự động. Đó là cơ chế thay cho name-suffix-hash của Kustomize.

---

## 7. Bảng tra lỗi

| Triệu chứng | Nguyên nhân thật | Cách sửa |
| :--- | :--- | :--- |
| `no matches for kind "ScaledObject"` | KEDA chưa cài | `Install-Prereqs.ps1` |
| `no matches for kind "PodMonitor"` | Prometheus Operator chưa cài | `Install-Prereqs.ps1` hoặc `--set monitoring.enabled=false` |
| `Namespace ... cannot be imported` | Dùng `--create-namespace` với chart có Namespace | Mục 2.4 |
| Pod `Pending`, `PodScheduled=False` | Hết CPU/RAM node, hoặc vướng ResourceQuota | `kubectl describe quota`; cấp thêm cho minikube |
| `ImagePullBackOff` trên minikube | Image chưa `minikube image load`, hoặc repository còn tiền tố `ghcr.io` | Dùng `values-staging-local.yaml` |
| `CreateContainerConfigError` | Thiếu Secret — ở prod là ExternalSecret chưa sync | `kubectl get externalsecret trading-secrets -o yaml` |
| Pod kẹt ở init `wait-for-db` | `DB_HOST` sai, hoặc NetworkPolicy chặn 5432 | Kiểm `config.DB_HOST` + `networkPolicy.externalDb.cidr` |
| Job `init-kafka-topics` lặp mãi "broker chua san sang" | NetworkPolicy chặn egress của Job (không phải Kafka hỏng) | `kubectl get netpol init-kafka-topics-egress` |
| Pod Ready nhưng gọi API **timeout** | Không có ingress controller | `Install-Prereqs.ps1` |
| Gọi API trả **503** | Pod chưa Ready, hoặc namespace `ingress-nginx` thiếu nhãn `kubernetes.io/metadata.name` | `kubectl label ns ingress-nginx kubernetes.io/metadata.name=ingress-nginx` |
| Grafana trống, không có metric | PodMonitor thiếu nhãn `release` khớp selector của Prometheus | Sửa `monitoring.extraLabels`; `Verify-Release.ps1` so giúp |
| `ScaledObject` `READY=False` | Sai topic/consumer group, hoặc KEDA không nối được broker | Đối chiếu với `@KafkaListener` trong code |
| Deployment không scale dù lag cao | Đã chạm `maxReplicaCount`, hoặc pod thừa không nhận được partition | Tăng `KAFKA_PARTITIONS` trước, rồi mới tăng max |
| `kubectl drain` treo vĩnh viễn | PDB `ALLOWED DISRUPTIONS = 0` | `kubectl get pdb`; xem mục 8 |
| `is forbidden: unable to create new content in namespace ... because it is being terminated` | Namespace còn ở trạng thái `Terminating` từ lần xoá trước | Chờ nó biến mất hẳn — script nay tự chờ (xem 7.1) |
| Nhiều pod ở **nhiều namespace** cùng `CrashLoopBackOff` | CNI chết → pod không gọi được API server qua ClusterIP | Xem 7.2 — kiểm `calico-node` trước mọi thứ khác |
| NetworkPolicy "không có tác dụng" | CNI không thực thi (kindnet), hoặc `calico-node` không Ready | `kubectl -n kube-system get pods -l k8s-app=calico-node` |
| `password authentication failed` sau khi deploy lại | Mật khẩu mới vs mật khẩu cũ trong PVC của postgres | Khôi phục `helm\.secrets.staging.env`, hoặc `ALTER USER` |

Bước đầu tiên gần như luôn là:

```powershell
.\scripts\helm\Verify-Release.ps1 -Namespace trading-staging
```

### 7.1. Namespace kẹt ở `Terminating`

`kubectl delete namespace` trả về **ngay** khi API server nhận lệnh, nhưng
namespace còn sống hàng chục giây để gỡ PVC và finalizer. Trong khoảng đó nó
**vẫn tồn tại**, nên `kubectl create/apply/label` đều báo thành công — rồi Helm
mới chết ở bước ghi release record:

```
Error: create: failed to create: secrets "sh.helm.release.v1.trading.v1" is forbidden:
unable to create new content in namespace trading-staging because it is being terminated
```

Thông báo đó rơi vào **sau** khi đã build xong 4 image, nên mất cả chục phút mới
thấy. `Deploy-Staging.ps1` nay tự phát hiện và chờ (`Wait-NamespaceGone` trong
`_Common.ps1`); chạy lại là xong.

Nếu quá 5 phút vẫn `Terminating` thì là finalizer treo thật:

```powershell
kubectl get namespace trading-staging -o jsonpath='{.spec.finalizers}'
kubectl -n trading-staging get all,pvc
# Tìm mọi object còn sót trong namespace:
kubectl api-resources --verbs=list --namespaced -o name |
  ForEach-Object { kubectl -n trading-staging get $_ --ignore-not-found -o name }
```

### 7.2. Nhiều pod ở nhiều namespace cùng CrashLoopBackOff

Khi thấy KEDA, metrics-server, prometheus-operator, dashboard **cùng** chết, đừng
sửa từng cái — chúng là triệu chứng, không phải bệnh. Mẫu số chung: tất cả đều
cần gọi API server từ trong pod qua ClusterIP `10.96.0.1:443`. Mất CNI là mất hết.

Kiểm tra theo đúng thứ tự này:

```powershell
# 1. CNI trước tiên — nếu nó không Ready thì dừng ở đây
kubectl -n kube-system get pods -l k8s-app=calico-node

# 2. Lỗi của các pod kia sẽ đều có hình dạng "không với tới 10.96.0.1"
kubectl -n kube-system logs deploy/metrics-server --tail=5
#   panic: ... Get "https://10.96.0.1:443/...": net/http: TLS handshake timeout

# 3. Nguyên nhân calico chết
kubectl -n kube-system describe pod -l k8s-app=calico-node | Select-String -Pattern "Exit Code|Liveness|Readiness|Killing"
```

**Ca đã gặp trên máy này:** `calico-node` cần **>90 giây** để khởi động xong
(trên Windows/WSL2 đĩa chậm, lại phải chờ API server lên), nhưng DaemonSet mặc
định **không có `startupProbe`** — liveness chỉ cho `initialDelay 10s + 6 × 10s
= 70s` rồi kubelet giết (`Exit Code: 137`). Vòng lặp vô hạn, triệu chứng là
`Liveness probe failed: command timed out`.

Cách vá, không mất dữ liệu:

```powershell
kubectl -n kube-system patch ds calico-node --type=json -p '[
  {"op":"add","path":"/spec/template/spec/containers/0/startupProbe",
   "value":{"exec":{"command":["/bin/calico-node","-felix-live"]},
            "periodSeconds":10,"timeoutSeconds":30,"failureThreshold":30}},
  {"op":"replace","path":"/spec/template/spec/containers/0/livenessProbe/timeoutSeconds","value":30},
  {"op":"replace","path":"/spec/template/spec/containers/0/readinessProbe/timeoutSeconds","value":30}
]'
```

`startupProbe` **treo liveness lại** cho tới khi khởi động xong, nên ngân sách
thành `30 × 10s = 300s`. Hoàn tác: `kubectl -n kube-system rollout undo ds/calico-node`.

Khi calico Ready trở lại, các pod kia thường tự hồi phục theo trong vài phút —
không cần xoá tay. Kiểm:

```powershell
kubectl get pods -A | Select-String -NotMatch "Running|Completed"
```

> **Vì sao điều này quan trọng với chart trading-system:** `calico-node` không
> Ready nghĩa là felix không chạy, nghĩa là **NetworkPolicy không được thực thi**.
> Chart sẽ cài thành công, pod sẽ Ready, nhưng 11 NetworkPolicy trong đó có
> `default-deny-all` chỉ là object trang trí trong etcd. Dễ nhầm thành "đã cô lập
> mạng" trong khi chưa.
```

---

## 8. Những thứ đã sửa trong chart

Chart ban đầu render sạch nhưng **không chạy được** ở staging/prod. Sáu chỗ dưới
đây đã được sửa; ghi lại vì mỗi cái hỏng theo một kiểu khác nhau.

**(1) Sai port của 3/4 service.** `values.yaml` khai market-gateway 8080,
pricing 8081, matching 8082. `application.yml` của chúng là **8081, 8082, 8084**
(khớp với `k8s/base`). Probe `httpGet` trỏ vào port không ai nghe → `startupProbe`
không bao giờ 200 → **pod không bao giờ Ready**, và `helm --wait` hết giờ sau 15
phút mà log app hoàn toàn bình thường.

**(2) Thiếu NetworkPolicy egress cho Job tạo topic.** `default-deny-all` chặn mọi
egress; policy `trading-apps-egress` chọn pod theo nhãn `part-of`, mà pod của Job
**không mang nhãn đó**. Job lặp mãi "broker chua san sang" rồi chết sau
`backoffLimit`, kéo cả `helm upgrade` fail ở post-install hook — trong khi Kafka
hoàn toàn khoẻ mạnh. `k8s/base` có policy này (`kafka-topics-init`); bản Helm làm
rơi mất.

**(3) Thiếu NetworkPolicy ingress cho Prometheus scrape.** `k8s/base` có
`trading-apps-ingress` mở port 8081–8084 cho namespace `monitoring`; bản Helm chỉ
giữ rule cho order-service. Hậu quả: PodMonitor tạo ra target trong Prometheus,
mọi scrape **timeout**, dashboard trống trơn, **không có log lỗi nào** ở phía app.

**(4) KEDA trỏ vào topic và consumer group không tồn tại.** Chart dùng
`market.ticks.enriched` / `pricing-risk-service` và `orders.validated` /
`matching-engine`. Code thật dùng `stock.ticks` / `pricing-risk-group` và
`stock.orders` / `matching-engine-group`. KEDA báo lag = 0 vĩnh viễn → **autoscale
chết im lặng** còn Deployment vẫn chạy nên không ai để ý. Đồng thời khôi phục
`lagThreshold` 1000/500 và `stabilizationWindowSeconds: 300` từ `k8s/components/keda`,
và đổi sang `bootstrapServersFromEnv` để chỉ còn một nguồn sự thật cho địa chỉ broker.

**(5) PDB của order-service đặt `maxUnavailable: 0`.** Chặn **mọi** eviction tự
nguyện → `kubectl drain` treo vĩnh viễn khi nâng cấp node, không có cảnh báo nào
từ Kubernetes. `k8s/base` dùng `maxUnavailable: 1` kèm hẳn một đoạn comment giải
thích vì sao không dùng `minAvailable`. (Trường `pdb.minAvailable` trong values
còn chưa hề được template đọc tới.)

**(6) PodMonitor/ServiceMonitor thiếu nhãn `release`.** kube-prometheus-stack mặc
định chỉ nhận object mang nhãn `release: <tên-stack>`. Thêm `monitoring.extraLabels`
để khai báo được, và `monitoring.namespace` để NetworkPolicy ở (3) mở đúng namespace.

Ngoài ra, `ServiceMonitor trading-infra` chọn Service có đồng thời
`component: infra` và port tên `metrics` — **không Service nào trong chart có cả
hai**. Nó hiện không khớp target nào. Đã giữ lại làm điểm cắm sẵn cho
kafka-exporter / postgres-exporter và ghi chú rõ trong template.

### Hai thứ cố ý chưa sửa

- **`imagePullSecrets`**: chart chưa có trường này. Với registry riêng tư, cách
  vá nhanh ở mục 4.4.
- **Giá trị mẫu trong `values-prod.yaml`** (`abcdefg...rds`, `trading.example.com`,
  `sha256:000...`): để nguyên vì chúng phụ thuộc hạ tầng thật của bạn.
  `Deploy-Prod.ps1` quét và cảnh báo khi chúng còn sót.
