# Lab Calico — học bằng cách đo, không bằng cách đọc

> Bài lab này nối tiếp `k8s/CNI-NOTES.md`. Ghi chú đó **mô tả** cluster; lab này
> bắt bạn **tự làm hỏng rồi tự sửa**, vì phần lớn hiểu biết về Calico đến từ việc
> nhìn một đường mạng chết đi rồi sống lại.
>
> Chạy trên chính cluster minikube đang có. Mọi thứ nằm trong hai namespace
> `calico-lab` / `calico-lab-ext` — **không đụng tới `trading-dev`**, vì
> NetworkPolicy chỉ có hiệu lực trong namespace của chính nó.
>
> ⚠️ Lab được viết lúc cluster đang tắt nên **chưa chạy kiểm chứng end-to-end**.
> Nếu có lệnh nào ra khác dự đoán, đó cũng là bài học — ghi lại rồi sửa file.

**Thời lượng:** Bài 0–7 khoảng 60–75 phút. Bài 8–11 + thử thách thêm 45 phút.

---

## Bản đồ lab

```
  namespace calico-lab  (tier=app)            namespace calico-lab-ext (tier=external)
  ┌──────────────────────────────────────┐    ┌───────────────────────────────┐
  │  frontend   app=frontend  (netshoot) │    │  ext-client   app=ext-client  │
  │  attacker   app=attacker  (netshoot) │    │  ext-attacker app=ext-attacker│
  │                                      │    └───────────────────────────────┘
  │  backend    app=backend   (nginx:80) │
  │    └─ svc backend   8080 ──► 80  ⚠️  │   chênh cổng: nguyên liệu Bài 7
  │  database   app=database  (nginx:80) │
  │    └─ svc database    80 ──► 80      │
  └──────────────────────────────────────┘
```

Bốn vai: `frontend` = client hợp lệ, `attacker` = pod không được phép gì cả,
`backend`/`database` = đích. `attacker` tồn tại để trả lời câu hỏi quan trọng
nhất sau mỗi bài: **policy có thực sự chặn ai không, hay chỉ trông như đang chặn?**

---

## Bài 0 — Dựng lab và kiểm tra Calico có thật sự enforce

```bash
minikube start                       # nếu cluster đang tắt
cd trading-system/k8s/labs/calico
source ./lab.sh                      # nạp các lệnh up/matrix/probe/reset/...
up
```

Trước khi làm gì khác, xác nhận CNI đang chạy là Calico **và** nó bật phần policy:

```bash
kubectl get pods -n kube-system | grep -E "calico|kindnet|cilium"
MSYS_NO_PATHCONV=1 docker exec minikube \
  grep -o '"policy".*' /etc/cni/net.d/10-calico.conflist
# -> "policy": {"type": "k8s"},
```

> `kubectl get netpol` **không** trả lời được câu "policy có hiệu lực chưa".
> Dưới kindnet, policy vẫn hiện ra đầy đủ mà không chặn gì — fail *mở*, im lặng.
> Chỉ có đo đường mạng thật mới trả lời được.

---

## Bài 1 — Đường cơ sở: chưa có policy thì mọi thứ thông nhau

```bash
reset      # đảm bảo không còn policy nào
matrix
```

Cột giữa phải ra `ALLOW` ở **tất cả** các dòng. Ghi lại cả cột thời gian — bạn
sẽ cần nó để so sánh.

> **Câu hỏi.** Vì sao Kubernetes mặc định cho mọi pod nói chuyện với mọi pod,
> kể cả khác namespace?

<details><summary>Đáp án</summary>

Vì mô hình mạng của Kubernetes **yêu cầu** như vậy: mọi pod phải nối được tới
mọi pod, không NAT. Namespace là ranh giới *quản trị* (tên, quota, RBAC), hoàn
toàn **không phải** ranh giới mạng. Muốn có ranh giới mạng thì phải tự dựng bằng
NetworkPolicy — và phải có một CNI chịu thực thi nó.
</details>

---

## Bài 2 — `default-deny` và cú sốc DNS

```bash
kubectl apply -f 10-default-deny.yaml
matrix
```

Giờ hầu hết các dòng chuyển sang `DNS-FAIL` chứ không phải `DROP`. Hai dòng
`thẳng podIP` thì `DROP`. Hãy nhìn kỹ **thời gian**:

```bash
raw frontend backend 8080      # xem lỗi gốc: không phân giải được tên
raw frontend $(kubectl -n calico-lab get pod backend -o jsonpath='{.status.podIP}') 80
```

> **Câu hỏi 1.** `default-deny-all` không hề nhắc tới DNS. Vì sao DNS chết?
>
> **Câu hỏi 2.** Vì sao gọi theo tên thì fail **nhanh**, còn gọi thẳng podIP thì
> treo ~4 giây rồi mới fail?

<details><summary>Đáp án</summary>

**1.** `podSelector: {}` chọn mọi pod, `policyTypes: [Ingress, Egress]` khoá cả
hai chiều, và không có mảng `egress` nào nghĩa là **không có đích nào được phép**.
CoreDNS nằm ở namespace `kube-system` → gói UDP/53 gửi tới nó là egress → bị vứt.
Trong 11 policy của repo, `allow-dns-egress` tồn tại chính vì lý do này.

**2.** Đây là kỹ năng chẩn đoán đáng giá nhất của cả bài lab:

| Triệu chứng | Nghĩa là |
|---|---|
| `could not resolve host` (nhanh) | Hỏng **DNS** — gói tới CoreDNS bị chặn, hoặc CoreDNS chết |
| `connection refused` (nhanh) | Gói **tới nơi** rồi, nhưng không ai nghe cổng đó. Không phải lỗi policy |
| treo rồi **timeout** | Gói bị **vứt âm thầm** — policy hoặc firewall |

Cuối chuỗi `cali-tw-*`/`cali-fw-*` là `DROP` chứ không phải `REJECT`, nên không
ai gửi lại gói báo lỗi → bên gửi chỉ biết ngồi đợi hết giờ.
</details>

---

## Bài 3 — Mở DNS, và chỉ DNS

```bash
kubectl apply -f 11-allow-dns.yaml
matrix
```

Bây giờ `DNS-FAIL` phải biến thành `DROP` ở mọi dòng: tên phân giải được rồi,
nhưng kết nối thì vẫn bị chặn đúng như mong muốn.

> **Câu hỏi.** `allow-dns-egress` dùng `podSelector: {}` giống hệt
> `default-deny-all`. Hai policy cùng chọn một tập pod thì cái nào thắng?

<details><summary>Đáp án</summary>

Không cái nào "thắng" — NetworkPolicy của Kubernetes **không có thứ tự ưu tiên và
không có luật deny**. Mọi policy chọn cùng một pod được **gộp lại (OR)**: gói tin
đi qua nếu **có ít nhất một** rule cho phép. "Deny" chỉ là hệ quả của việc *không
rule nào khớp*.

Vì vậy quy tắc viết policy là: một policy `default-deny` làm nền, rồi cộng dồn
các policy "allow" hẹp lên trên. Muốn **chặn tường minh**, hoặc thứ tự ưu tiên,
thì phải dùng `GlobalNetworkPolicy` của Calico — xem Bài 11.
</details>

---

## Bài 4 — Mở đúng một đường, và bài học conntrack

```bash
kubectl apply -f 20-backend-ingress.yaml      # vào backend: chỉ frontend, cổng 80
kubectl apply -f 22-frontend-egress-ok.yaml   # ra khỏi frontend: chỉ tới backend
matrix
```

Kết quả mong đợi:

| Đường | Kết quả | Vì sao |
|---|---|---|
| frontend → backend:8080 | ALLOW | cả hai đầu đều mở |
| attacker → backend:8080 | DROP | ingress của backend không nhận `app=attacker` |
| frontend → database:80 | DROP | egress của frontend không có database |
| backend → database:80 | DROP | backend chưa có policy egress nào |

> **Câu hỏi 1.** Phải mở **mấy** policy cho một đường frontend → backend? Vì sao?
>
> **Câu hỏi 2.** `backend` đang bị `default-deny` khoá cả **Egress**, và không có
> policy nào cho phép nó gửi đi. Vậy gói tin **trả lời** cho frontend đi ra bằng
> đường nào?

<details><summary>Đáp án</summary>

**1.** Hai. Một đường TCP đi qua **hai** bộ lọc: `cali-fw-<iface nguồn>` (egress,
from workload) rồi `cali-tw-<iface đích>` (ingress, to workload). Thiếu bên nào
cũng DROP. Đây là lỗi hay gặp nhất khi gỡ NetworkPolicy — thấy ingress mở đúng
rồi nên không ngờ tới đầu kia (chính là Thử thách 4).

**2.** Nhờ **conntrack**. Rule đầu tiên trong mọi chuỗi `cali-*` là:

```
-m conntrack --ctstate RELATED,ESTABLISHED -j ACCEPT
```

Kết nối đã được chấp nhận lúc khởi tạo thì mọi gói sau đó của *cùng dòng* đi
thẳng qua, không xét policy nữa. Vì vậy **chỉ cần cho phép chiều khởi tạo**,
không bao giờ phải viết rule cho chiều về. Ai viết rule cho chiều về là đang
hiểu sai mô hình và tự mở thêm lỗ hổng.

Tự kiểm chứng:
```bash
chains backend      # dòng đầu của cali-tw-* và cali-fw-*
```
</details>

---

## Bài 5 — Bẫy AND/OR: khác nhau đúng một dấu gạch đầu dòng

Đây là lỗi NetworkPolicy phổ biến nhất trên đời, và nó **không gây sự cố** — nó
âm thầm mở quá rộng.

```bash
reset
kubectl apply -f 10-default-deny.yaml -f 11-allow-dns.yaml
kubectl apply -f 30-cross-ns-AND.yaml

probe ext-client   backend.calico-lab 8080 calico-lab-ext
probe ext-attacker backend.calico-lab 8080 calico-lab-ext
```

Giờ đổi sang bản OR và đo lại **đúng hai lệnh đó**:

```bash
diff 30-cross-ns-AND.yaml 31-cross-ns-OR.yaml
kubectl apply -f 31-cross-ns-OR.yaml

probe ext-client   backend.calico-lab 8080 calico-lab-ext
probe ext-attacker backend.calico-lab 8080 calico-lab-ext
```

> **Câu hỏi.** Hai file chỉ khác nhau một ký tự `-`. Vì sao kết quả của
> `ext-attacker` lại đổi?

<details><summary>Đáp án</summary>

`from` là một **mảng**. Các *phần tử* của mảng gộp bằng **OR**; các *selector nằm
trong cùng một phần tử* gộp bằng **AND**.

```yaml
from:
  - namespaceSelector: {...}     # ┐ cùng MỘT phần tử
    podSelector: {...}           # ┘ -> AND: pod đúng nhãn VÀ ở đúng namespace

from:
  - namespaceSelector: {...}     # phần tử 1
  - podSelector: {...}           # phần tử 2
                                 # -> OR: CẢ namespace vào được
```

Bản OR cho **toàn bộ** namespace `calico-lab-ext` vào backend, nên `ext-attacker`
lọt. Dấu `-` thứ hai biến một rule "một pod cụ thể" thành rule "cả một namespace".

Mẹo đọc nhanh khi review: trong `from`/`to`, đếm số dấu `-`. Mỗi dấu `-` là một
cửa độc lập.
</details>

---

## Bài 6 — namespaceSelector nhìn vào nhãn của namespace, không phải tên

```bash
kubectl get ns --show-labels | grep calico-lab
kubectl label ns calico-lab-ext tier-          # gỡ nhãn tier=external
probe ext-client backend.calico-lab 8080 calico-lab-ext     # -> DROP
kubectl label ns calico-lab-ext tier=external  # gắn lại
probe ext-client backend.calico-lab 8080 calico-lab-ext     # -> ALLOW
```

Không sửa policy, không khởi động lại pod — chỉ đổi nhãn namespace là đường mạng
đổi theo trong vài giây. Felix đang watch API server liên tục và viết lại
iptables ngay.

> **Câu hỏi.** Policy `taskapp` trong repo dùng
> `namespaceSelector: {kubernetes.io/metadata.name: ingress-nginx}`. Nhãn đó ai
> gắn, và tin được không?

<details><summary>Đáp án</summary>

apiserver tự gắn `kubernetes.io/metadata.name` cho **mọi** namespace từ
Kubernetes 1.21, giá trị luôn bằng tên namespace và **không sửa được**. Vì vậy
đó là cách chọn namespace theo tên an toàn nhất. Ngược lại, nhãn tự đặt như
`tier=external` thì **bất kỳ ai có quyền sửa namespace đều có thể tự cấp quyền
vào mạng của bạn** — đúng như vừa làm ở trên. Cân nhắc điều này khi phân quyền RBAC.
</details>

---

## Bài 7 — Bẫy DNAT: cổng Service không phải cổng đích thật

Đây chính là sự cố Prometheus đã dính ngày 18-10 (xem mục 5 của `CNI-NOTES.md`),
dựng lại ở quy mô nhỏ.

```bash
reset
kubectl apply -f 10-default-deny.yaml -f 11-allow-dns.yaml -f 20-backend-ingress.yaml
kubectl apply -f 21-frontend-egress-WRONG.yaml     # mở cổng 8080 — nghe rất hợp lý

probe frontend backend 8080        # -> DROP ?!
```

Policy nói rõ "cho phép tới backend cổng 8080", client cũng gọi đúng
`backend:8080`. Vậy mà vẫn bị vứt.

```bash
kubectl get endpointslices -n calico-lab -l kubernetes.io/service-name=backend \
  -o jsonpath='{.items[0].ports[0].port}{"\n"}'    # -> 80, KHÔNG phải 8080

kubectl apply -f 22-frontend-egress-ok.yaml        # sửa thành 80
probe frontend backend 8080        # -> ALLOW
```

> **Câu hỏi.** Vì sao rule 8080 không khớp, trong khi lệnh curl rõ ràng ghi 8080?

<details><summary>Đáp án</summary>

kube-proxy **DNAT trước, policy xét sau**. Thứ tự thật:

```
pod gọi 10.96.x.x:8080
   → iptables nat: KUBE-SERVICES → KUBE-SVC-… → KUBE-SEP-… -j DNAT  (thành 10.244.x.y:80)
   → routing
   → filter: cali-fw-<iface>                        ← Calico mới nhìn gói ở ĐÂY
```

Tới lúc Calico nhìn, gói đã mang đích `:80`. Rule `port: 8080` không khớp cái gì
cả → rơi xuống cuối chuỗi → DROP.

**Quy tắc nhớ đời: rule egress phải khớp `targetPort` (cổng container), không
phải `port` của Service.** Tra cổng thật luôn bằng `endpointslices` như trên,
đừng đọc Service rồi đoán.

Hệ quả hay bị quên: `ipBlock` trong egress **không bao giờ khớp Service IP**, vì
Service IP đã bị DNAT thành podIP trước khi policy nhìn thấy gói.
</details>

---

## Bài 8 — ipBlock và ý nghĩa thật của `natOutgoing`

```bash
kubectl apply -f 40-egress-internet.yaml
probe frontend example.com 443
probe frontend $(kubectl -n calico-lab get pod backend -o jsonpath='{.status.podIP}') 80
```

Ra được Internet, nhưng **không** vào được podIP — dù `cidr: 0.0.0.0/0` nghe như
"cho phép tất cả".

> **Câu hỏi.** `0.0.0.0/0` đã bao trùm cả IP pod. Vì sao vẫn bị chặn?

<details><summary>Đáp án</summary>

Vì `except: [10.0.0.0/8, ...]` khoét thủng dải đó, mà pool pod của bạn là
`10.244.0.0/16` — nằm gọn trong `10.0.0.0/8`. Đây là mẫu chuẩn để viết "cho ra
Internet nhưng cấm đi ngang trong cluster".

Hai điều cần nhớ về `ipBlock`:
1. Nó là selector **duy nhất** làm việc bằng IP; nó không hiểu pod/namespace, và
   IP pod thì đổi sau mỗi lần pod khởi động lại → đừng dùng ipBlock cho đích
   trong cluster.
2. IP nguồn mà bên ngoài nhìn thấy là IP **node**, không phải IP pod, do
   `natOutgoing=true` MASQUERADE ở `cali-nat-outgoing`. Chỉ NAT khi *nguồn trong
   pool pod* và *đích ngoài pool* — nên pod ↔ pod vẫn giữ nguyên IP nguồn, đúng
   yêu cầu "không NAT" của Kubernetes.
</details>

---

## Bài 9 — Nhìn policy của mình dưới dạng iptables

```bash
iface                    # bảng pod -> veth, do chính Calico khai báo
chains backend           # hai chuỗi của riêng pod backend
```

Đọc `cali-tw-<iface>` (to workload = **ingress**) từ trên xuống:

```
1. conntrack RELATED,ESTABLISHED -j ACCEPT   <- Bài 4 câu 2
2. conntrack INVALID -j DROP
3. MARK --set-xmark 0x0/0x30000              <- xoá mark cũ
4. "Start of tier default"
5. -j cali-pi-_<hash>                        <- MỖI NetworkPolicy một chuỗi con
6. mark 0x10000 -j RETURN                    <- khớp rồi thì thoát
   ... không chuỗi con nào khớp -> DROP
```

Thử nghiệm: đếm số chuỗi policy trước và sau khi apply thêm một policy.

```bash
MSYS_NO_PATHCONV=1 docker exec minikube sh -c 'iptables-save | grep -c "^:cali-pi-"'
kubectl apply -f 31-cross-ns-OR.yaml
MSYS_NO_PATHCONV=1 docker exec minikube sh -c 'iptables-save | grep -c "^:cali-pi-"'
```

Còn tập pod mà selector chọn ra thì nằm trong **ipset**, không nằm trong iptables:

```bash
kubectl -n kube-system exec ds/calico-node -- ipset list -n | head
kubectl -n kube-system exec ds/calico-node -- ipset list <tên-ipset-cali40s-…>
```

> **Câu hỏi.** Vì sao Calico đẩy tập IP vào ipset thay vì sinh thẳng rule
> iptables cho từng pod?

<details><summary>Đáp án</summary>

iptables duyệt rule **tuyến tính**. Mỗi pod một rule thì cluster 5.000 pod sẽ có
chuỗi dài 5.000 bước cho **mỗi** gói tin — không dùng được. ipset tra cứu bằng
hash, **O(1)**, và quan trọng hơn: khi một pod sinh ra hay chết đi, Felix chỉ cần
thêm/bớt một phần tử trong ipset chứ **không phải viết lại chuỗi iptables**.
Đó là lý do policy bám kịp pod đang scale lên xuống liên tục.

(Cùng vấn đề này là động cơ của chế độ eBPF dataplane trong Calico và của Cilium.)
</details>

---

## Bài 10 — IPAM: Calico cấp IP khác kindnet ở chỗ nào

```bash
kubectl get ippool default-ipv4-ippool -o yaml | grep -E 'cidr|blockSize|ipipMode|natOutgoing'
kubectl get ipamblock -o custom-columns='BLOCK:.spec.cidr,NODE:.spec.affinity'
kubectl -n calico-lab get pod -o wide                      # ghi lại IP của frontend
kubectl -n calico-lab delete pod frontend
kubectl apply -f 01-workloads.yaml
kubectl -n calico-lab get pod frontend -o wide             # IP mới
iface frontend                                             # veth mới
```

> **Câu hỏi.** Node chỉ có ~20 pod, vì sao Calico cấp hẳn một block `/26` (64 IP)
> cho node thay vì cấp lẻ từng IP?

<details><summary>Đáp án</summary>

Để **quảng bá route theo block**. Ở cluster nhiều node, BIRD (daemon BGP của
Calico) chỉ cần quảng bá *một* prefix `/26` cho cả node thay vì 64 route `/32` —
bảng route của các node khác nhỏ đi hàng chục lần. Route `blackhole` cho cả block
tồn tại để gói gửi tới IP *trong block nhưng chưa cấp cho pod nào* bị vứt ngay
tại node, thay vì lọt ra default route rồi đi lang thang.

Khác biệt cốt lõi với kindnet: `host-local` lưu state ra **file trên đĩa node**
nên chỉ biết node của chính nó → chỉ hợp cluster một node. `calico-ipam` lưu
state trong **CRD trên apiserver** (`ipamblock`, `blockaffinity`) nên mọi node
nhìn thấy cùng một sự thật.
</details>

---

## Bài 11 (tuỳ chọn) — Thứ Calico làm được mà NetworkPolicy chuẩn thì không

⚠️ `GlobalNetworkPolicy` **không có namespace** — nó áp lên toàn cluster. File
`50-globalnetworkpolicy-OPTIONAL.yaml` đã khoá selector vào namespace lab; đừng
nới nó ra. Xoá ngay sau khi làm xong.

Kiểm tra trước xem cluster có API nào:

```bash
kubectl api-resources | grep projectcalico
```

Nếu chỉ thấy `crd.projectcalico.org/v1` (không có `projectcalico.org/v3`) thì
minikube chưa cài `calico-apiserver`. Vẫn apply được bằng dạng CRD thô, nhưng
**không có validation** — gõ sai tên trường thì policy im lặng không chạy, nên
luôn đo lại bằng `probe` chứ đừng tin "apply thành công".

```bash
reset
kubectl apply -f 10-default-deny.yaml -f 11-allow-dns.yaml -f 20-backend-ingress.yaml
kubectl apply -f 22-frontend-egress-ok.yaml
kubectl apply -f 50-globalnetworkpolicy-OPTIONAL.yaml

probe frontend backend 8080        # vẫn ALLOW
probe attacker backend 8080        # DROP — lần này do rule Deny tường minh

kubectl delete -f 50-globalnetworkpolicy-OPTIONAL.yaml    # DỌN NGAY
```

> **Câu hỏi.** `attacker` vốn đã bị chặn bởi `default-deny` rồi. Vậy `action: Deny`
> thêm được giá trị gì?

<details><summary>Đáp án</summary>

Ba thứ NetworkPolicy chuẩn không có:

1. **Deny tường minh** — chặn được *bất chấp* có policy allow nào khác. Với
   NetworkPolicy chuẩn, chỉ cần một team nào đó apply một policy allow rộng tay
   là lỗ hổng mở ra, và bạn không có cách nào đóng lại từ trên.
2. **`order`** — số nhỏ chạy trước. Có thứ tự ưu tiên thật, nên viết được kiểu
   "luật của tổ chức luôn thắng luật của team".
3. **Phạm vi toàn cluster** — một policy nền cho mọi namespace, kể cả namespace
   sẽ được tạo trong tương lai. NetworkPolicy chuẩn thì namespace mới = mặc định
   mở toang cho tới khi ai đó nhớ ra phải apply default-deny.

Đổi lại: đây là API riêng của Calico, không portable sang CNI khác. Nguyên tắc
thực dụng là dùng NetworkPolicy chuẩn cho luật của ứng dụng, dành
GlobalNetworkPolicy cho các luật nền toàn cluster.
</details>

---

## Thử thách — bốn policy hỏng, tự chẩn

Với mỗi file: **đoán trước** chuyện gì sẽ xảy ra, apply, đo, rồi mới mở đáp án.
Mỗi lần bắt đầu bằng trạng thái sạch:

```bash
reset && kubectl apply -f 10-default-deny.yaml -f 11-allow-dns.yaml
```

### Thử thách 1 — `bugs/bug-1.yaml`
Đã apply ingress cho backend, nhưng frontend vẫn DROP.
```bash
kubectl apply -f 22-frontend-egress-ok.yaml -f bugs/bug-1.yaml
probe frontend backend 8080
```
<details><summary>Đáp án</summary>

Sai **khoá nhãn**: policy chọn `app.kubernetes.io/name: backend`, còn pod mang
`app: backend`. Selector không khớp pod nào → policy tồn tại nhưng áp lên tập
rỗng → backend vẫn chỉ còn `default-deny`.

Cách phát hiện trong 5 giây — hỏi chính API server xem selector chọn được ai:
```bash
kubectl -n calico-lab get pod -l app.kubernetes.io/name=backend   # -> No resources found
kubectl -n calico-lab get pod -l app=backend                      # -> backend
```
Phản xạ cần có: **mọi lần debug policy, việc đầu tiên là `kubectl get pod -l <selector>`.**
Repo này dùng `app.kubernetes.io/name` ở `deploy_example/` nhưng `app` ở
`base/` — trộn hai bộ nhãn là nguồn lỗi kinh điển.
</details>

### Thử thách 2 — `bugs/bug-2.yaml`
```bash
kubectl apply -f 20-backend-ingress.yaml -f bugs/bug-2.yaml
probe frontend backend 8080
```
<details><summary>Đáp án</summary>

`policyTypes: [Ingress]` nhưng lại viết mảng `egress`. Khi `policyTypes` được ghi
tường minh, mọi mảng không được liệt kê sẽ **bị bỏ qua hoàn toàn** — apply không
báo lỗi, `kubectl get netpol -o yaml` vẫn hiện đủ mảng egress, mà nó không có
tác dụng gì.

Tệ hơn: policy này còn *thêm* một luật ingress rỗng cho frontend (có
`policyTypes: [Ingress]` mà không có mảng `ingress`) → siết frontend chặt hơn.

Nhớ: nếu **bỏ trống** `policyTypes`, Kubernetes tự suy ra (luôn có Ingress; thêm
Egress nếu có mảng `egress`). Đã ghi tường minh thì phải ghi cho đúng:
```bash
kubectl -n calico-lab get netpol frontend-allow-to-backend -o jsonpath='{.spec.policyTypes}'
```
</details>

### Thử thách 3 — `bugs/bug-3.yaml`
Lần này **không có gì hỏng** cả. Tìm lỗ hổng.
```bash
kubectl apply -f 22-frontend-egress-ok.yaml -f bugs/bug-3.yaml
probe ext-client   backend.calico-lab 8080 calico-lab-ext
probe ext-attacker backend.calico-lab 8080 calico-lab-ext
```
<details><summary>Đáp án</summary>

Hai lỗi chồng lên nhau:

1. **Bẫy AND/OR của Bài 5** — hai dấu `-` nên `ext-attacker` cũng vào được, dù ý
   định chỉ là cho pod monitoring.
2. **Không có mảng `ports`** — thiếu `ports` nghĩa là **mọi cổng**, không phải
   "cổng mặc định". Nếu backend có cổng admin, cổng debug, JMX… thì mở hết.

Thói quen nên có: luôn viết `ports` tường minh, và review `from`/`to` bằng cách
đếm dấu `-`. Lỗi loại này không bao giờ tự lộ ra — không ai mở ticket vì "mạng
quá thông".
</details>

### Thử thách 4 — `bugs/bug-4.yaml`
```bash
kubectl apply -f bugs/bug-4.yaml
kubectl -n calico-lab get pod -l app=frontend      # nhãn khớp
kubectl -n calico-lab get netpol                   # policy có đó
probe frontend backend 8080                        # vẫn DROP
```
<details><summary>Đáp án</summary>

Policy **đúng hoàn toàn** — nhưng mới mở có một đầu. `default-deny` đang khoá
**Egress** của frontend, mà không policy nào cho phép frontend gửi đi.

Một đường TCP cần **hai** giấy phép: `cali-fw-<nguồn>` (egress) và
`cali-tw-<đích>` (ingress). Đây là lỗi số một khi gỡ NetworkPolicy, vì ingress
trông đúng nên không ai nghĩ tới đầu bên kia. Sửa: thêm `22-frontend-egress-ok.yaml`.
</details>

---

## Dọn dẹp

```bash
reset     # chỉ xoá policy, giữ pod để làm lại
down      # xoá hẳn hai namespace
```

`down` không đụng gì tới `trading-dev` hay `kube-system`. Nếu có apply
`GlobalNetworkPolicy` ở Bài 11, nhớ xoá tay — nó **không** nằm trong namespace
nên `down` không dọn giúp:

```bash
kubectl delete globalnetworkpolicies.crd.projectcalico.org lab-deny-attacker
```

---

## Rút gọn lại thành 7 điều

1. `kubectl get netpol` không chứng minh được policy có hiệu lực. Chỉ phép đo mới chứng minh được.
2. Một đường TCP cần **hai** giấy phép: egress ở nguồn, ingress ở đích.
3. Chiều về luôn tự thông nhờ conntrack. Đừng bao giờ viết rule cho chiều về.
4. Trong `from`/`to`: mỗi dấu `-` là một cửa độc lập (OR); các selector trong cùng một dấu `-` là AND.
5. Cổng trong rule phải là **cổng container thật**, không phải cổng Service — DNAT chạy trước policy.
6. `timeout` = bị vứt (policy). `refused` = tới nơi nhưng không ai nghe. `could not resolve` = DNS.
7. Policy chuẩn chỉ cộng dồn allow, không có deny và không có thứ tự. Cần deny/ưu tiên → `GlobalNetworkPolicy` của Calico.
