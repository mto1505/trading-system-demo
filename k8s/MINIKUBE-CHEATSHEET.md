# minikube Cheat Sheet — kiểm tra cái vỏ, không phải cái ruột

> Viết theo cluster thật trên máy này: minikube CLI v1.39.0, profile `minikube`
> (driver docker, 6144 MB / 4 CPU, containerd, CNI **Calico**, k8s v1.37.0),
> namespace của project là `trading-dev`.
>
> Ranh giới với [KUBECTL-CHEATSHEET.md](KUBECTL-CHEATSHEET.md): file này lo phần
> **cluster có tồn tại và chạy được không** (VM, profile, addon, image, RAM
> cgroup). Pod không lên, log, DNS, Service, config — sang file kia.

---

## 0. Máy này có HAI profile

```
PROFILE    DRIVER   RUNTIME      IP             ACTIVE
minikube   docker   containerd   192.168.49.2   *      <- project trading-system
taskapp    docker   containerd   192.168.58.2
```

Hầu hết lệnh `minikube` chạy trên profile **active**, không phải profile bạn
đang nghĩ trong đầu. Khi nghi ngờ thì luôn thêm `-p minikube`. Các lệnh dưới
đây đều ghi rõ `-p` vì lý do đó.

```bash
minikube profile list              # cả 2 profile + cái nào active
minikube profile minikube          # đổi profile active
kubectl config current-context     # kubectl có đang trỏ cùng chỗ không
```

Hai thứ đó **độc lập nhau**: đổi `minikube profile` không tự đổi kubecontext và
ngược lại. Lệch nhau là lúc `minikube image load` nạp vào cluster này còn
`kubectl` nói chuyện với cluster kia — không có cảnh báo nào cả.

---

## 1. Trạng thái — hỏi đầu tiên

```bash
minikube status -p minikube
```

In 4 dòng vì 4 thứ đó chết độc lập được:

```
host: Running        <- container docker tên "minikube"
kubelet: Running
apiserver: Running   <- kubectl sống nhờ dòng này
kubeconfig: Configured
```

`host: Running` mà `apiserver: Stopped` là cluster còn đó nhưng `kubectl` sẽ
treo tới timeout. Đọc hết 4 dòng, đừng chỉ nhìn dòng đầu.

```bash
minikube ip -p minikube            # IP để sửa hosts cho trading.dev.local
minikube node list -p minikube
```

---

## 2. Vòng đời

```bash
minikube start -p minikube --wait=apiserver,system_pods
minikube stop -p minikube
minikube pause -p minikube         # đóng băng control plane, giữ nguyên state
minikube delete -p minikube        # XOÁ HẲN — mất PVC (postgres, kafka)
```

⚠️ **Bẫy lớn nhất của minikube.** Profile đã tồn tại thì các cờ cấu hình của
`minikube start` bị **bỏ qua trong im lặng**:

```bash
minikube start -p minikube --memory=12g --cpus=8 --cni=cilium
# profile đã có -> KHÔNG có gì đổi, KHÔNG có cảnh báo, exit code 0
```

Muốn đổi RAM / CPU / CNI / phiên bản k8s thì bắt buộc `minikube delete` rồi tạo
lại — driver docker không resize được container đang có. Đây là lý do
`scripts/deploy-dev.ps1` cố ý **không** truyền mấy cờ đó khi start.

Xem cấu hình thật của profile (nguồn sự thật, không phải thứ bạn nhớ đã gõ):

```bash
grep -E '"(Memory|CPUs|Driver|ContainerRuntime|CNI|KubernetesVersion)"' \
  ~/.minikube/profiles/minikube/config.json
```

Lệnh tạo lại đúng với cluster hiện tại:

```bash
minikube start -p minikube \
  --driver=docker --container-runtime=containerd --cni=calico \
  --memory=6144 --cpus=4 --kubernetes-version=v1.37.0
```

---

## 3. Addon

```bash
minikube addons list -p minikube | grep enabled
minikube addons enable ingress -p minikube
minikube addons disable dashboard -p minikube
```

Đang bật trên cluster này: `ingress`, `metrics-server`, `dashboard`,
`default-storageclass`, `storage-provisioner`.

Hai chỗ hay tìm nhầm:

- ingress-nginx nằm ở namespace **`ingress-nginx`**, không phải `kube-system`.
  Tìm nhầm chỗ thì lúc nào cũng tưởng "chưa bật":
  `kubectl -n ingress-nginx get pods`
- `addons list` nói addon đã **enable**, không nói controller đã **Ready**.
  Enable xong mà pod chưa lên thì `trading.dev.local` trả 503, không phải 404.

---

## 4. Image — nơi dễ sai nhất

```bash
minikube image ls -p minikube | grep trading     # image có THẬT trong cluster chưa
minikube image load trading/order-service:dev -p minikube
minikube image rm  trading/order-service:dev -p minikube
```

Runtime là **containerd**, nên `eval $(minikube docker-env)` vô dụng ở đây —
image build vào docker daemon thì containerd không nhìn thấy. (Trên Windows nó
còn hỏng hẳn vì minikube không khởi động được ssh-agent.) `minikube image load`
là đường duy nhất.

Nạp xong **vẫn phải** restart:

```bash
kubectl -n trading-dev rollout restart deploy/order-service
```

Tag `:dev` không đổi + `imagePullPolicy: IfNotPresent` ⇒ kubelet thấy "đã có
image tên này rồi" và giữ nguyên container đang chạy. Không restart thì bạn
deploy xong vẫn đang soi log của binary cũ, mà không có dấu hiệu gì báo.

Muốn chắc container đang chạy đúng bits vừa build:

```bash
minikube ssh -p minikube -- sudo crictl images | grep trading
kubectl -n trading-dev get pod -l app.kubernetes.io/name=order-service \
  -o jsonpath='{.items[0].status.containerStatuses[0].imageID}{"\n"}'
```

---

## 5. RAM và CPU — con số nào đáng tin

```bash
# Giới hạn THẬT (cgroup của container minikube)
docker inspect minikube --format 'mem={{.HostConfig.Memory}} cpu={{.HostConfig.NanoCpus}}'

# Mức dùng thật — mẫu số ĐÚNG
docker stats --no-stream --format 'table {{.Name}}\t{{.MemUsage}}\t{{.MemPerc}}\t{{.CPUPerc}}' minikube

# Góc nhìn của kubelet — mẫu số SAI (xem dưới)
kubectl get node minikube -o jsonpath='{.status.allocatable.cpu} {.status.allocatable.memory}{"\n"}'
kubectl top nodes
```

kubelet lấy `capacity`/`allocatable` bằng RAM và CPU của **cả VM WSL**, chứ
không phải giới hạn cgroup mà container `minikube` thực sự bị chặn. Đo thật
trên cluster này, cùng một thời điểm:

| | RAM | CPU |
|---|---|---|
| `docker stats` — **ĐÚNG** | 4.779 GiB / 6 GiB = **79.65%** | **56.36%** |
| `kubectl top nodes` — **ẢO** | 4877 Mi = 30% | 400m = 5% |
| Mẫu số kubelet đang dùng | 16377456Ki ≈ **15.6 GiB** | **8 CPU** |
| Mẫu số thật (cgroup) | **6 GiB** | **4 CPU** |

**CPU nói dối y hệt RAM** — chỗ này hay bị bỏ qua vì mọi tài liệu chỉ nhắc tới
bộ nhớ. Scheduler tin node có 8 CPU trong khi cgroup chỉ cho 4.

Con số 15.6 GiB không phải hằng số: nó đổi theo `memory=` trong `.wslconfig`.
Thứ cần nhớ là **cách đo**, không phải con số.

### Vì sao lệch — hai con số nằm ở hai nơi

Chạy trong node sẽ thấy ngay nguồn gốc:

```bash
minikube ssh -p minikube -- "cat /sys/fs/cgroup/memory.max; cat /sys/fs/cgroup/cpu.max; \
  grep MemTotal /proc/meminfo; grep -c ^processor /proc/cpuinfo"
```
```
6442450944          <- /sys/fs/cgroup/memory.max   = 6 GiB    GIỚI HẠN THẬT
400000 100000       <- /sys/fs/cgroup/cpu.max      = 4 CPU    GIỚI HẠN THẬT
MemTotal: 16377456 kB   <- /proc/meminfo                      KUBELET ĐỌC CÁI NÀY
8                       <- /proc/cpuinfo                      KUBELET ĐỌC CÁI NÀY
```

Hai dòng dưới **khớp từng chữ số** với `allocatable` mà kubectl báo
(`mem=16377456Ki`, `cpu=8`). Không phải suy đoán — kubelet lấy capacity thẳng
từ procfs.

Với driver docker, "node" **không phải VM** mà là một container Docker chạy
trên VM WSL. Mà trong container:

- **cgroup** giới hạn tài nguyên — nằm ở `/sys/fs/cgroup/`
- **`/proc`** mô tả phần cứng — và **Linux không có "memory namespace"**

Container có mount / PID / network namespace, nhưng **không hề có** namespace
cho RAM hay CPU. Nên `/proc/meminfo` bên trong hiển thị nguyên si phần cứng của
host. `free -m` trong node báo 15993 MB cũng vì vậy — nó chỉ là bộ đọc
`/proc/meminfo`.

kubelet (qua cAdvisor) tính capacity bằng cách hỏi "máy này có bao nhiêu
RAM/CPU" → vào `/proc` → nhận thông số cả VM. Nó **không** đọc `memory.max` của
chính mình, vì kubelet vốn thiết kế để chạy trên **máy thật**, nơi nó *là* cả
cái máy. Chạy kubelet trong container là mẹo của kind/minikube, nằm ngoài giả
định đó. Còn `docker stats` đúng vì đọc thẳng bộ đếm cgroup — Docker biết chính
xác limit nó đã đặt.

### RAM và CPU lệch theo hai kiểu khác nhau

| | Cơ chế cgroup | Khi vượt | Container "thấy" |
|---|---|---|---|
| `memory.max` | **tường cứng** | OOM killer giết process | vẫn 15.6 GiB |
| `cpu.max` | **van tiết lưu** (quota/chu kỳ) | bị throttle, chạy chậm | vẫn 8 CPU |

`400000 100000` nghĩa là: mỗi chu kỳ 100 ms được dùng tối đa 400 ms CPU-time —
bằng 4 CPU chạy full. Tiến trình **vẫn lập lịch trên cả 8 core**, chỉ bị chặn
tổng thời gian. Nên con số 8 của `/proc/cpuinfo` thật ra **không sai**; cái sai
là kubelet diễn giải nó thành "ngân sách 8 CPU để chia cho pod".

Hệ quả khác hẳn nhau:

- **Vượt RAM** → `OOMKilled`, pod chết đột ngột, không báo trước
- **Vượt CPU** → mọi thứ chậm đi, p99 vọt lên, **không pod nào chết**

Bug RAM dễ thấy, bug CPU thì âm thầm — nó không bao giờ hiện ra trong
`kubectl get pods`.

### Vì sao k8s không tự sửa

Scheduler quyết định xếp pod dựa trên `allocatable` do kubelet báo. Không thành
phần nào trong k8s đối chiếu lại với cgroup:

```
scheduler: "còn 11 GiB trống, xếp thêm pod"    (sổ sách)
cgroup:    "6 GiB là 6 GiB"                    -> OOMKill
kubelet:   ghi nhận pod chết, báo "còn trống"  -> scheduler xếp lại
```

Không ai nói dối ai — hai bên dùng hai nguồn số liệu không liên quan gì nhau.

Hướng tổng quát để đóng khe hở là `kubelet --system-reserved` / `--kube-reserved`
để ép `allocatable` về gần 6 GiB. Trên minikube thì cách gọn hơn là **đặt limit
cho pod sát trần cgroup thật** — đúng cái bảng quy đổi ngay dưới đây: tổng
limits ≤ 6144 Mi thì khe hở này không bao giờ gây hại, dù kubelet vẫn báo sai.

(Môi trường khác dùng LXCFS để giả lập `/proc/meminfo` theo cgroup — minikube
không dùng.)

### Quy đổi lại phần đã cam kết

`kubectl describe node minikube | grep -A8 "Allocated resources"` cũng in %
theo mẫu số ảo. Quy về mẫu số thật thì bức tranh khác hẳn:

| | k8s báo (mẫu số ảo) | Thực tế (mẫu số cgroup) |
|---|---|---|
| CPU requests | 2200m — 27% của 8 | **55%** của 4 CPU |
| RAM requests | 3148 Mi — 19% của 15.6G | **51%** của 6144 Mi |
| RAM **limits** | 6570 Mi — 41% | **107%** của 6144 Mi ⚠️ |

Dòng cuối là chỗ nguy hiểm: tổng limit đã **vượt trần cgroup**. Các pod cùng
lúc dùng tới limit của mình thì cgroup OOMKill trước khi k8s kịp thấy vấn đề —
theo sổ sách của nó thì mới dùng 41%.

Hệ quả chung: scheduler tưởng còn ~11 GiB trống (thật ra còn ~1.2 GiB) nên vẫn
vui vẻ xếp thêm pod, rồi cgroup mới ra tay. Thấy `OOMKilled` thì so hai con số
này **trước** khi nghi app rò rỉ bộ nhớ.

### Đổi giới hạn

Hai tầng, sửa tầng nào cũng phải sửa tầng kia:

| Tầng | Chỉnh ở | Áp dụng bằng |
|---|---|---|
| WSL2 cấp cho Docker Desktop | `%USERPROFILE%\.wslconfig` → `memory=` | `wsl --shutdown` |
| cgroup của container minikube | `--memory=` / `--cpus=` lúc start | `minikube delete` + start lại |

`minikube delete` **mất PVC** (postgres, kafka). Lệnh tạo lại đúng cấu hình nằm
ở mục 2.

### Disk thì không cần lo

`DiskSize` trong profile config (20000 MB) **không có hiệu lực** với driver
docker — node dùng thẳng filesystem của host:

```
minikube ssh -p minikube -- df -h /var
/dev/sdd    1007G    44G   912G   5%   /var
```

---

## 6. Chui vào node

```bash
minikube ssh -p minikube
```

Trong node, containerd dùng `crictl` chứ không phải `docker`:

```bash
sudo crictl ps                 # container đang chạy
sudo crictl images
sudo crictl logs <id>
sudo journalctl -u kubelet -n 100 --no-pager
free -m                        # RAM theo góc nhìn của node
```

Chạy một lệnh rồi thoát luôn: `minikube ssh -p minikube -- sudo crictl ps`

> Trên Git Bash nhớ `MSYS_NO_PATHCONV=1` khi lệnh có đường dẫn tuyệt đối kiểu
> `/var/log/...`, nếu không Git Bash đổi nó thành `C:\...` trước khi gửi đi.

---

## 7. Truy cập và gỡ bí

```bash
minikube service list -p minikube          # Service nào ra được ngoài
minikube dashboard -p minikube             # UI, tự mở browser
minikube logs -p minikube --problems       # lọc sẵn phần đáng ngờ
minikube logs -p minikube -f
minikube tunnel -p minikube                # cấp IP thật cho Service LoadBalancer
```

`minikube tunnel` phải chạy ở terminal riêng và **giữ nguyên** — đóng terminal
là mất route. Project này dùng Ingress nên thường không cần tới nó.

`minikube tunnel` phải chạy ở terminal riêng và **giữ nguyên** — đóng terminal
là mất route. Nó chỉ phục vụ Service kiểu `LoadBalancer`; ingress-nginx của
minikube là **NodePort**, nên tunnel *không* chữa được lỗi ở mục 8.

---

## 8. Ingress không vào được — bẫy lớn nhất trên Windows

Triệu chứng: hosts đã sửa đúng, pod Ready hết, Ingress có ADDRESS, mà
`curl http://trading.dev.local/...` vẫn **treo rồi timeout**.

### Vì sao

Driver `docker` ⇒ network `minikube` (`192.168.49.0/24`) là một **docker bridge
nằm bên trong VM WSL2**. Windows không có route tới subnet đó. `minikube ip`
trả về địa chỉ **có thật nhưng không tới được từ host**.

Đây là giới hạn của docker driver trên Windows/macOS — **trên Linux thì dòng
hosts trỏ vào node IP chạy bình thường**. Mọi tài liệu chung đều viết cho
Linux, kể cả mục 3 của [README.md](README.md).

Đo thử để tự thấy:

```bash
# từ Windows -> timeout, http_code=000
curl.exe --max-time 8 -o NUL -w "%{http_code}\n" -H "Host: trading.dev.local" http://192.168.49.2/

# từ TRONG node -> 200
minikube ssh -p minikube -- "curl -s -o /dev/null -w '%{http_code}\n' -H 'Host: trading.dev.local' http://localhost/"
```

Hai kết quả lệch nhau ⇒ cluster không hỏng, chỉ là Windows không tới được.

### Cách chạy được

```powershell
# terminal riêng, GIỮ NGUYÊN không đóng
kubectl -n ingress-nginx port-forward svc/ingress-nginx-controller 80:80
```

```
# C:\Windows\System32\drivers\etc\hosts   (bước này mới cần Administrator)
127.0.0.1  trading.dev.local
```

Windows **không cấm user thường bind port 80** (khác Linux), nên bản thân lệnh
port-forward không cần quyền admin. Xong là mở được bằng trình duyệt:

```
http://trading.dev.local/api/v1/marketdata/FPT
```

Không muốn đụng file hosts thì dùng cổng cao, Host header vẫn khớp vì nginx
so `$host` đã bỏ phần `:port`:

```bash
kubectl -n ingress-nginx port-forward svc/ingress-nginx-controller 8080:80
curl.exe -H "Host: trading.dev.local" -H "X-API-KEY: dev-key" \
  http://127.0.0.1:8080/api/v1/marketdata/FPT
```

Chỉ cần gọi API, bỏ qua Ingress luôn:

```bash
kubectl -n trading-dev port-forward svc/pricing-risk-service 8082:8082
curl.exe -H "X-API-KEY: dev-key" http://localhost:8082/api/v1/marketdata/FPT
```

Đường này **không đi qua ingress-nginx**. Nếu nó chạy mà đường Ingress hỏng thì
lỗi nằm ở Ingress/nginx, không phải ở app — mẹo khoanh vùng nhanh nhất.

### Đọc mã lỗi — ba triệu chứng, ba tầng khác hẳn nhau

| Thấy gì | Dừng ở đâu | Nghi gì |
|---|---|---|
| **timeout** (`http_code=000`) | chưa tới nginx | port-forward chết, hoặc hosts trỏ node IP |
| **404** | tới nginx rồi | Host header không khớp `host:` trong Ingress |
| **503** | nginx khớp rule rồi | controller hoặc backend Service chưa Ready |
| **401** | đã vào tới app | sai `X-API-KEY` (dev là `dev-key`, **không phải** `demo-key`) |

Nhầm timeout với 404 là chỗ tốn thời gian nhất: 404 nghĩa là **mạng đã thông**,
chỉ sai tên; timeout nghĩa là chưa có gì thông cả.

---

## 9. Thứ tự kiểm tra khi "deploy xong mà không vào được"

```bash
minikube status -p minikube                      # 1. cluster còn sống?
kubectl config current-context                   # 2. kubectl trỏ đúng cluster?
minikube image ls -p minikube | grep trading     # 3. image vào cluster chưa?
kubectl -n trading-dev get pods                  # 4. pod Ready chưa?
kubectl -n ingress-nginx get pods                # 5. ingress controller Ready?
kubectl -n trading-dev get ingress               # 6. Ingress có ADDRESS chưa?
```

Sáu dòng này loại được gần hết nguyên nhân phía cluster. Tất cả đều xanh mà vẫn
không vào được thì **gần như chắc chắn là mục 8** — thử `minikube ssh ... curl`
để xác nhận trước khi debug tiếp.

Lỗi nằm trong cluster thì sang [KUBECTL-CHEATSHEET.md](KUBECTL-CHEATSHEET.md)
mục 2 — thang chẩn đoán pod.
