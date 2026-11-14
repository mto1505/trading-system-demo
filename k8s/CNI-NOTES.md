# CNI — Ghi chú học + lệnh check

> Viết dựa trên cluster thật đang chạy trên máy: minikube (driver docker, WSL2),
> Kubernetes v1.37.0, containerd 2.3.4.
>
> **CNI hiện tại: Calico** (đổi ngày 2026-10-18 bằng
> `minikube start --cni=calico`). Trước đó là **kindnet** — phần mô tả kindnet
> ở mục 3 được giữ lại để đối chiếu, vì so hai plugin cạnh nhau là cách hiểu
> CNI nhanh nhất. CNI chỉ chọn được lúc tạo cluster, không hoán đổi nóng được.

> **Muốn tự tay làm thay vì đọc:** `labs/calico/README.md` — 12 bài thực hành
> dựng lại đúng các bẫy ghi ở mục 5 (DNAT, AND/OR, conntrack, ipset) trong một
> namespace riêng, kèm 4 policy hỏng sẵn để tự chẩn.

---

## 1. CNI là cái gì (và không là cái gì)

CNI = **Container Network Interface** — một *bản hợp đồng* (spec), không phải
một phần mềm. Nó quy định: "runtime sẽ gọi một file binary, truyền config qua
stdin + biến môi trường, binary đó cắm mạng cho container rồi trả JSON về."

Ba thứ tạo nên CNI trên một node:

| Thành phần | Vị trí trên node | Vai trò |
|---|---|---|
| File config | `/etc/cni/net.d/*.conflist` | Mô tả dùng plugin nào, IP range nào |
| Binary plugin | `/opt/cni/bin/` | Code thật sự tạo veth, gán IP, set route |
| IPAM state | `/run/cni-ipam-state` | Nhớ IP nào đã cấp cho container nào |

**Kubernetes không tự làm mạng pod.** kubelet chỉ ra lệnh; toàn bộ việc "pod này
có IP 10.244.0.9 và ping được pod kia" là do CNI plugin làm.

### Kubernetes yêu cầu gì ở CNI

1. Mọi pod có IP riêng, duy nhất trong cluster.
2. Pod ↔ pod nói chuyện được **không NAT** (IP nguồn nhìn thấy đúng là IP pod).
3. Node ↔ pod nói chuyện được không NAT.

Lưu ý: NetworkPolicy **không** nằm trong spec CNI — nó là tính năng *thêm*,
plugin có thể làm hoặc không (xem mục 5).

---

## 2. Luồng khi một pod khởi tạo

```
kubelet  →  containerd (CRI)  →  tạo sandbox (pause container) + netns rỗng
                              →  đọc /etc/cni/net.d/10-kindnet.conflist
                              →  chạy /opt/cni/bin/ptp  (ADD)
                                   ├── gọi /opt/cni/bin/host-local → xin 1 IP
                                   ├── tạo cặp veth: 1 đầu trong netns pod (eth0),
                                   │   1 đầu trên node (vethXXXX)
                                   └── set route mặc định trong pod
                              →  chạy /opt/cni/bin/portmap (ADD)   ← chain
                              →  trả IP về cho kubelet → ghi vào pod.status.podIP
```

Khi xoá pod thì chạy lại y hệt với lệnh `DEL` để trả IP về IPAM. Nếu bước DEL
hỏng → IP bị rò rỉ (leak), lâu dài cạn IP pool → pod mới kẹt
`ContainerCreating`.

---

## 3. Hai plugin, đặt cạnh nhau

### 3a. Calico — CNI đang chạy hiện tại

`/etc/cni/net.d/10-calico.conflist`:

```json
{
  "name": "k8s-pod-network",
  "cniVersion": "1.0.0",
  "plugins": [
    { "type": "calico",
      "datastore_type": "kubernetes",
      "ipam": { "type": "calico-ipam" },
      "policy": { "type": "k8s" },
      "kubernetes": { "kubeconfig": "/etc/cni/net.d/calico-kubeconfig" } },
    { "type": "portmap", "snat": true, "capabilities": { "portMappings": true } }
  ]
}
```

Khác kindnet ở bốn điểm cốt lõi:

| | kindnet | Calico |
|---|---|---|
| IPAM | `host-local`, state là file trên node | `calico-ipam`, state trong **etcd/CRD** → dùng được multi-node |
| Lưu cấu hình | file trên đĩa | Kubernetes API (`datastore_type: kubernetes`) |
| **NetworkPolicy** | **không thực thi** | **có** (`"policy": {"type": "k8s"}`) |
| cniVersion | 0.3.1 | 1.0.0 |

Dòng `"policy": { "type": "k8s" }` chính là thứ biến 11 NetworkPolicy trong repo
từ vật trang trí thành luật có hiệu lực.

Route trên node cũng khác hẳn — Calico cấp IP theo **block /26** cho mỗi node và
đặt một route `blackhole` cho cả block:

```
blackhole 10.244.120.64/26 proto bird        <- block cua node nay
10.244.120.65 dev cali887503c500b scope link metric 1024
10.244.120.66 dev cali5437f8a056e scope link metric 1024
```

`blackhole` là để gói tin gửi tới IP **trong block nhưng chưa cấp cho pod nào**
bị vứt ngay tại node, thay vì lọt ra default route rồi đi lang thang. `proto
bird` cho biết route do BIRD (daemon BGP của Calico) tạo — ở cluster nhiều node,
chính BGP quảng bá block của từng node cho các node khác. Interface đổi tiền tố
từ `vethXXXX` sang `caliXXXX`.

Binary trong `/opt/cni/bin` có thêm `calico` và `calico-ipam`.

### 3b. kindnet — plugin cũ, giữ lại để đối chiếu

Nội dung `/etc/cni/net.d/10-kindnet.conflist` hồi còn dùng kindnet:

```json
{
  "cniVersion": "0.3.1",
  "name": "kindnet",
  "plugins": [
    { "type": "ptp", "ipMasq": false, "mtu": 1500,
      "ipam": { "type": "host-local",
                "dataDir": "/run/cni-ipam-state",
                "ranges": [[{ "subnet": "10.244.0.0/24" }]],
                "routes": [{ "dst": "0.0.0.0/0" }] } },
    { "type": "portmap", "capabilities": { "portMappings": true } }
  ]
}
```

Đọc ra được:

- **`plugins` là một chain** — chạy tuần tự, kết quả plugin trước truyền vào
  plugin sau. `ptp` cắm dây, `portmap` lo `hostPort`.
- **`ptp`** (point-to-point): mỗi pod một cặp veth nối thẳng vào node, **không có
  bridge `cni0`**. Vì thế trên node bạn thấy route `/32` cho từng pod:

  ```
  10.244.0.9  dev veth48f35ef0 scope host
  10.244.0.10 dev vethebe2ad43 scope host
  ```

  (Khác với plugin `bridge` mà Flannel/Calico hay dùng — chúng gom pod vào một
  bridge chung rồi route cả subnet một lần.)
- **`host-local`**: IPAM cấp IP tuần tự, lưu state ra file trên đĩa node. Đơn
  giản, nhưng **chỉ biết node của chính nó** → chỉ hợp cluster 1 node như
  minikube. Cluster nhiều node cần IPAM tập trung (Calico IPAM, Cilium IPAM).
- **`ipMasq: false`**: CNI không NAT. Pod → internet được NAT ở tầng khác
  (iptables của node).
- **`cniVersion 0.3.1`**: phiên bản spec. Có bản mới hơn (1.0.0, 1.1.0) thêm lệnh
  `CHECK`/`GC`, nhưng plugin cũ vẫn chạy được — tương thích ngược.

### Ba dải IP đừng nhầm lẫn

| Dải | Giá trị trên máy bạn | Ai cấp |
|---|---|---|
| **Pod CIDR** | `10.244.0.0/24` | CNI (host-local) |
| **Service CIDR** | `10.96.0.0/12` | kube-apiserver — IP ảo, kube-proxy dịch bằng iptables/ipvs |
| **Node IP** | `192.168.49.2` | Docker bridge của minikube |

Service IP **không** do CNI cấp và không tồn tại trên interface nào cả — nó chỉ
là rule iptables. Đây là chỗ hay nhầm nhất khi debug.

---

## 4. Bảng lệnh check

### Xem CNI đang dùng là gì

```bash
# Pod CNI chạy ở kube-system (kindnet / calico-node / cilium / flannel ...)
kubectl get pods -n kube-system -o wide

# Node Ready = CNI đã cắm xong.
# NotReady kèm "cni plugin not initialized" = CNI hỏng
kubectl get nodes -o wide
kubectl describe node minikube | grep -A5 Conditions
```

> **Windows / Git Bash:** lệnh `docker exec` có path Linux phải thêm
> `MSYS_NO_PATHCONV=1`, không thì Git Bash đổi `/etc/cni` thành
> `C:/Program Files/Git/etc/cni` và báo "No such file or directory".

### Xem config + binary trên node

```bash
MSYS_NO_PATHCONV=1 docker exec minikube ls -la /etc/cni/net.d
MSYS_NO_PATHCONV=1 docker exec minikube cat /etc/cni/net.d/10-kindnet.conflist
MSYS_NO_PATHCONV=1 docker exec minikube ls /opt/cni/bin
```

File đuôi `.mk_disabled` / `.disabled` là đã bị tắt. **Nếu có nhiều hơn 1 file
`.conflist` còn active, kubelet lấy file đầu theo thứ tự alphabet** — nguồn gốc
của rất nhiều bug kiểu "pod lấy nhầm IP range".

### Xem IPAM đã cấp IP nào

> ⚠️ Hai lệnh `/run/cni-ipam-state/kindnet` ở bản ghi chú cũ **chỉ đúng với
> kindnet**. Calico không ghi state ra file trên node mà giữ trong CRD của
> apiserver, nên phải hỏi bằng `kubectl`.

```bash
# Pool tổng + chế độ overlay
kubectl get ippool default-ipv4-ippool -o yaml | grep -E 'cidr|blockSize|ipipMode|vxlanMode|natOutgoing'

# Block mà node này được cấp (mỗi node xin nguyên một /26 rồi tự chia)
kubectl get ipamblock -o custom-columns=\
'BLOCK:.spec.cidr,NODE:.spec.affinity'

# Từng IP trong block đang thuộc về pod nào
kubectl get ipamblock -o jsonpath='{.items[0].spec.allocations}'
```

### Xem IP và route thật trên node

Ba lệnh trả lời ba câu khác nhau — đừng nhầm:

```bash
# 1. Node có những IP nào (eth0 ra ngoài, tunl0 là đầu hầm IPIP)
MSYS_NO_PATHCONV=1 docker exec minikube ip -4 -br addr show
#   lo            UNKNOWN   127.0.0.1/8
#   eth0@if9      UP        192.168.49.2/24      ← ra Docker bridge, MTU 1500
#   tunl0@NONE    UNKNOWN   10.244.120.64/32     ← IPIP, MTU 1480

# 2. Bảng route — mỗi pod MỘT dòng /32, không có dòng nào cho cả subnet
MSYS_NO_PATHCONV=1 docker exec minikube ip route
#   default via 192.168.49.1 dev eth0
#   blackhole 10.244.120.64/26 proto bird        ← BIRD cài, chặn IP chưa cấp
#   10.244.120.66 dev calia255da68055 scope link ← một dòng cho mỗi pod
#   ...

# 3. Đếm veth — phải khớp số pod KHÔNG dùng hostNetwork
MSYS_NO_PATHCONV=1 docker exec minikube sh -c 'ip -o link | grep -c cali'
#   -> 16

# Pod hostNetwork có podIP == hostIP; pod thường thì khác nhau.
# (ĐỪNG lọc bằng jsonpath '?(@.spec.hostNetwork!=true)' — pod thường KHÔNG CÓ
#  trường hostNetwork chứ không phải bằng false, filter sẽ trả về rỗng.)
kubectl get pods -A \
  -o jsonpath='{range .items[*]}{.status.podIP}{" "}{.status.hostIP}{"\n"}{end}' \
  | awk 'NF==2 && $1!=$2' | wc -l
#   -> 16    khớp số veth ✓

kubectl get pods -A \
  -o jsonpath='{range .items[*]}{.status.podIP}{" "}{.status.hostIP}{"\n"}{end}' \
  | awk 'NF==2 && $1==$2' | wc -l
#   -> 7     pod hostNetwork, không có veth và không bị NetworkPolicy chạm tới
```

Lệch giữa hai số đầu = veth mồ côi (pod chết mà veth còn) hoặc ngược lại.

> Dưới Calico **đừng dùng `grep veth`** như thời kindnet — veth của Calico tên
> `cali<hash>`, không có chữ "veth" nào trong tên.

Soi một sợi cụ thể:

```bash
MSYS_NO_PATHCONV=1 docker exec minikube ip -d link show calia255da68055
#   16: calia255da68055@if2: ... mtu 1480
#   link/ether ee:ee:ee:ee:ee:ee ... link-netns cni-22eaeed3-...
#    ↑ ifindex phía host        ↑ @if2 = đầu kia là ifindex 2 trong netns pod
#   MAC ee:ee:... GIỐNG NHAU trên cả 16 sợi — xem mẹo proxy ARP ở mục 5b.
#   Cả hai ifindex đều đổi sau mỗi lần khởi động lại; đừng nhớ con số.

# proxy_arp phải = 1, nếu 0 thì pod không ra được khỏi chính nó
MSYS_NO_PATHCONV=1 docker exec minikube \
  cat /proc/sys/net/ipv4/conf/calia255da68055/proxy_arp
```

### Tra ngược: IP ↔ veth ↔ tên pod

Bảng route chỉ cho biết `IP → tên veth`; nó **không** biết pod nào. Ghép với
`kubectl` để ra bảng đầy đủ:

```bash
MSYS_NO_PATHCONV=1 docker exec minikube \
  sh -c "ip route | awk '/scope link/ {print \$1, \$3}'" | sort > /tmp/r.txt
kubectl get pods -A \
  -o jsonpath='{range .items[?(@.status.podIP)]}{.status.podIP}{" "}{.metadata.name}{"\n"}{end}' \
  | sort > /tmp/p.txt
join /tmp/r.txt /tmp/p.txt
#   10.244.120.122 calif643f7bd1c3 zipkin-7ff4d65b7c-rvjb5
#   10.244.120.123 cali2f8c8c5d815 schema-registry-7dd78dbb6-h7hk8
#   ...
```

Đi từ phía pod ngược ra (ảnh JRE/distroless không có lệnh `ip`):

```bash
POD=$(kubectl -n trading-dev get pod -l app.kubernetes.io/name=order-service \
        -o jsonpath='{.items[0].metadata.name}')
MSYS_NO_PATHCONV=1 kubectl -n trading-dev exec $POD -c app -- cat /sys/class/net/eth0/iflink
#   -> 16          (ifindex của ĐẦU KIA, nằm trên host)
MSYS_NO_PATHCONV=1 docker exec minikube sh -c 'ip -o link | grep "^16:"'
#   -> cali...     khớp với @if3 ở trên là đúng cặp
```

> **IP pod đổi sau mỗi lần cluster khởi động lại, tên veth thì không.**
> Kiểm chứng 22-10: `calif643f7bd1c3` vẫn là zipkin qua hai lần reboot, nhưng
> IP nhảy từ `.102` sang `.122`. Vì vậy đừng bao giờ chép cứng IP vào script
> hay tài liệu — luôn tra lại bằng mấy lệnh trên.

### Xem mạng từ bên trong pod

```bash
kubectl get pods -A -o wide          # cột IP = podIP do CNI cấp

# Pod có sẵn shell:
kubectl exec -it <pod> -- ip addr
kubectl exec -it <pod> -- ip route

# Pod distroless không có shell → bật pod debug tạm:
kubectl run netshoot --rm -it --image=nicolaka/netshoot -- bash
#   trong đó:  ip a / ip r / dig kubernetes.default / curl <svc>:<port>

# Chui vào netns của pod đang chạy (không cần pod có shell):
kubectl debug -it <pod> --image=nicolaka/netshoot --target=<container-name>
```

### Kiểm chứng 3 yêu cầu của Kubernetes

```bash
kubectl run p1 --image=nicolaka/netshoot --restart=Never -- sleep 3600
kubectl run p2 --image=nicolaka/netshoot --restart=Never -- sleep 3600

# 1. Pod ping pod, không NAT
kubectl exec p1 -- ping -c3 $(kubectl get pod p2 -o jsonpath='{.status.podIP}')

# 2. DNS (CoreDNS) — hỏng DNS hay bị tưởng nhầm là hỏng CNI
kubectl exec p1 -- dig +short kubernetes.default.svc.cluster.local

# 3. Pod ra internet (NAT ở node)
kubectl exec p1 -- curl -sS -o /dev/null -w '%{http_code}\n' https://example.com

kubectl delete pod p1 p2
```

### Log khi nghi CNI lỗi

```bash
kubectl describe pod <pod> | tail -20   # Events: "failed to setup network for sandbox"
kubectl logs -n kube-system kindnet-hvqg4
MSYS_NO_PATHCONV=1 docker exec minikube \
  journalctl -u kubelet -n 100 --no-pager | grep -i cni
```

---

## 5. NetworkPolicy — thứ KHÔNG nằm trong spec CNI

| Có enforce NetworkPolicy | Không enforce |
|---|---|
| **Calico**, Cilium, Antrea, Weave | **kindnet**, flannel |

(flannel thường ghép với Calico thành "Canal" để có policy.)

Plugin không enforce thì API server vẫn nhận object, `kubectl get netpol` vẫn
hiện ra đầy đủ, nhưng **không có gì bị chặn cả** — fail *mở*, im lặng, không một
dòng warning. Đó là lý do `kubectl get netpol` **không** trả lời được câu
"policy có hiệu lực chưa"; chỉ có thử đường mạng thật mới trả lời được.

### Phép thử đối chứng — đã chạy thật ngày 2026-10-18

Policy `redis` trong `k8s/base/infra/infra-networkpolicy.yaml` chỉ cho phép pod
có `component=order` nối tới Redis. Vậy lấy một pod ĐƯỢC phép và một pod KHÔNG
được phép, thử cùng một đích:

```bash
# KHONG duoc phep theo policy
kubectl -n trading-dev exec deploy/market-gateway -- \
  timeout 6 bash -c 'cat < /dev/null > /dev/tcp/redis/6379 && echo OK'

# DUOC phep theo policy
kubectl -n trading-dev exec deploy/order-service -- \
  timeout 6 bash -c 'cat < /dev/null > /dev/tcp/redis/6379 && echo OK'
```

| | kindnet | Calico |
|---|---|---|
| market-gateway → redis:6379 | OK (policy bị bỏ qua) | **timeout — chặn đúng** |
| order-service → redis:6379 | OK | OK |

Mẹo `bash -c 'cat < /dev/null > /dev/tcp/host/port'` tiện vì image JRE không có
`curl` hay `nc`, nhưng bash thì có sẵn. Bị chặn thì **treo rồi timeout** chứ
không phải `connection refused` — gói tin bị âm thầm vứt (DROP), không ai trả
lời. Phân biệt được hai thứ này là chẩn đoán nhanh hơn hẳn: `refused` = tới nơi
nhưng không ai nghe cổng đó; `timeout` = policy hoặc firewall chặn.

### ⚠️ Bẫy đã dính thật: NetworkPolicy xét cổng SAU khi kube-proxy DNAT

Bật Calico xong, Prometheus chết service discovery, log:

```
dial tcp 10.96.0.1:443: i/o timeout
```

Policy của nó mở egress cổng 443 và 6443 rồi — mà vẫn bị chặn. Lý do: client nối
tới **Service IP** `10.96.0.1:443`, nhưng kube-proxy DNAT thành
**`192.168.49.2:8443`** (API server của minikube nghe 8443) **trước khi**
NetworkPolicy được xét. Calico thấy gói tin đi tới cổng 8443, không khớp rule
nào → DROP.

Quy tắc rút ra: **rule egress phải khớp cổng ĐÍCH THẬT sau DNAT, không phải cổng
Service.** Kiểm tra cổng thật bằng:

```bash
kubectl get endpointslices -n default -l kubernetes.io/service-name=kubernetes \
  -o jsonpath='{.items[0].ports[0].port}{"\n"}'     # -> 8443
```

Đã sửa bằng cách thêm `{ protocol: TCP, port: 8443 }` vào policy `prometheus`
trong `k8s/components/in-cluster-prometheus/prometheus.yaml`. Sau khi sửa,
`activeTargets` từ 0 lên 4, cả 4 đều `health: up`.

Bẫy này **chỉ lộ ra dưới Calico** — dưới kindnet mọi thứ trông vẫn hoàn hảo.

### Kiểm tra nhanh policy có đang được thực thi không

```bash
kubectl get pods -n kube-system | grep -E "calico|cilium|kindnet"
docker exec minikube sh -c 'grep -o "\"policy\".*" /etc/cni/net.d/10-calico.conflist'
curl -s localhost:19090/api/v1/targets?state=active | grep -c scrapeUrl   # Prometheus con thay target khong
```

---

## 5b. Giải phẫu networking của cluster (khảo sát 2026-10-18, Calico)

### Bản đồ IP

| Dải | Giá trị | Ai cấp |
|---|---|---|
| Pod pool | `10.244.0.0/16`, blockSize 26 | calico-ipam |
| Block của node | `10.244.120.64/26` | calico-ipam |
| Service | `10.96.0.0/12` | apiserver (chỉ là rule iptables) |
| Node | `192.168.49.2/24`, gw `192.168.49.1` | Docker bridge |

`ipipMode=Always`, `natOutgoing=true`, kube-proxy **mode iptables**.

### Interface trên node

```
eth0@if23            192.168.49.2/24      ra Docker bridge
tunl0@NONE           10.244.120.64/32     đầu đường hầm IPIP
docker0              172.17.0.1/16 DOWN   rác base image, KHÔNG dùng
cali<hash>@if3       x16                  mỗi pod một cái, MAC ee:ee:ee:ee:ee:ee, MTU 1480
```

**Không có bridge cho pod.** Calico dùng veth point-to-point + định tuyến L3,
khác mô hình `cni0` của Flannel (mọi pod chung bridge, nói chuyện ở L2).
MTU 1480 = 1500 − 20 byte header IPIP.

### Bên trong pod — mẹo proxy ARP

```
default via 169.254.1.1 dev eth0
169.254.1.1/32 dev eth0 scope link
# arp: 169.254.1.1 -> ee:ee:ee:ee:ee:ee
```

`169.254.1.1` **không tồn tại trên interface nào**. Calico bật `proxy_arp=1`
trên mọi `cali*`, đầu veth phía node trả lời ARP hộ bằng MAC giả. Nhờ vậy pod
không cần biết subnet hay gateway thật — cứ gửi ra node, node lo định tuyến.

Kiểm chứng cặp veth (image JRE không có lệnh `ip`, dùng `/sys` và `/proc`):

```bash
kubectl -n trading-dev exec <pod> -c app -- cat /sys/class/net/eth0/iflink   # -> 18
kubectl -n trading-dev exec <pod> -c app -- cat /proc/net/route              # route dạng hex
kubectl -n trading-dev exec <pod> -c app -- cat /proc/net/arp
MSYS_NO_PATHCONV=1 docker exec minikube sh -c 'ip -o link | grep "^18:"'     # -> cali8dde687ca05
```

### Hành trình gói tin

**Pod → pod cùng node (podIP):**

```
eth0 pod → ARP 169.254.1.1 (proxy ARP) → veth → cali<src> trên node
  → cali-from-wl-dispatch → cali-fw-cali<src>     ← POLICY EGRESS
  → node route /32 (ip_forward=1)
  → cali-to-wl-dispatch   → cali-tw-cali<dst>     ← POLICY INGRESS
  → veth → eth0 pod đích
```

Không đụng `tunl0`, không đóng gói.

**Pod → Service:** thêm DNAT **trước** khi route —
`KUBE-SERVICES → KUBE-SVC-xxx (chọn endpoint) → KUBE-SEP-xxx -j DNAT`, rồi
giống hệt trên. ⚠️ DNAT chạy TRƯỚC khi policy được xét → xem bẫy ở mục 5.

**Pod → internet:** `cali-POSTROUTING → cali-nat-outgoing`, MASQUERADE khi
*nguồn thuộc pool pod* VÀ *đích ngoài pool*. Đó là ý nghĩa của `natOutgoing`:
pod↔pod giữ nguyên IP nguồn, chỉ ra ngoài mới NAT.

**Pod → pod khác node:** mới dùng `tunl0`, bọc header IPIP rồi gửi theo route
BGP. Cluster 1 node nên chưa xảy ra.

### Policy nằm ở đâu trong iptables

Mỗi pod hai chuỗi, đặt tên theo interface:

- `cali-fw-<iface>` — **f**rom **w**orkload = pod gửi đi = **egress**
- `cali-tw-<iface>` — **t**o **w**orkload = vào pod = **ingress**

Thứ tự bên trong:

```
1. conntrack RELATED,ESTABLISHED -j ACCEPT   <- traffic hồi đáp qua ngay
2. conntrack INVALID -j DROP
3. MARK --set-xmark 0x0/0x30000
4. "Start of tier default"
5. -j cali-pi-<hash>                          <- từng NetworkPolicy một
6. mark 0x10000 -j RETURN                     <- khớp thì thoát
   ... không khớp cái nào -> DROP
```

Dòng 1 là lý do không cần viết rule cho chiều về. Cuối chuỗi là **DROP** (không
phải REJECT) nên bị chặn sẽ **treo rồi timeout**, không `connection refused`.

```bash
MSYS_NO_PATHCONV=1 docker exec minikube sh -c \
  "iptables-save -t filter | grep -E '^-A cali-tw-<iface>'"
```

---

## 6. Phân biệt CNI vs kube-proxy (rất hay nhầm)

| | CNI | kube-proxy |
|---|---|---|
| Lo cái gì | IP cho **pod**, đường đi pod ↔ pod | IP ảo của **Service** → pod |
| Làm bằng | veth, route, (vxlan/bgp nếu multi-node) | iptables / ipvs / eBPF |
| Hỏng thì | Pod kẹt `ContainerCreating`, không có IP | Pod có IP, ping được, nhưng `curl <service>` fail |

Mẹo chẩn đoán: **ping thẳng podIP được mà gọi Service không được → lỗi
kube-proxy / Service, không phải CNI.** Ngược lại pod còn chẳng có IP → lỗi CNI.

(Cilium ở chế độ `kubeProxyReplacement` gộp cả hai vào eBPF, bỏ hẳn kube-proxy.)

---

## 7. Đọc thêm theo thứ tự

1. Spec CNI — <https://github.com/containernetworking/cni/blob/main/SPEC.md>
2. Plugin chuẩn — <https://www.cni.dev/plugins/current/>
3. Kubernetes network model — <https://kubernetes.io/docs/concepts/cluster-administration/networking/>
4. NetworkPolicy + plugin nào hỗ trợ — <https://kubernetes.io/docs/concepts/services-networking/network-policies/>
5. Lab thực hành của repo này — [`labs/calico/README.md`](labs/calico/README.md)
