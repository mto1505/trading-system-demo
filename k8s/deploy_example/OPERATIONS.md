# Lệnh vận hành hằng ngày

Cheatsheet cho **chính bộ deploy này**, không phải kubectl tổng quát. Mỗi lệnh kèm
*khi nào dùng* và *vì sao*, cộng những cái bẫy đã thực sự dính khi chạy.

- Cái gì đã dựng → [`../../README_ARC.md`](README.md)
- Vì sao dựng như vậy → [`DECISIONS.md`](DECISIONS.md)

---

## 0. Đặt biến cho gọn

Mọi lệnh bên dưới dùng `$CTX` và `$NS`. Đặt một lần cho mỗi phiên terminal:

```powershell
# PowerShell
$CTX = 'taskapp'; $NS = 'taskapp-dev'
```

```bash
# Git Bash / WSL
CTX=taskapp; NS=taskapp-dev
```

**Vì sao luôn ghi `--context` tường minh.** `kubectl` dùng context đang active toàn
cục. Bạn có 2 profile minikube; và ở công ty thì context prod và dev nằm cạnh nhau
trong cùng file kubeconfig. Gõ thiếu `--context` một lần là chạy nhầm lệnh lên prod.
Thói quen ghi rõ rẻ hơn nhiều so với sự cố.

```bash
kubectl config get-contexts              # xem có những context nào, cái nào đang active
kubectl config current-context           # đang trỏ vào đâu — kiểm tra trước lệnh nguy hiểm
```

---

## 1. Vòng đời cluster local

```powershell
minikube start -p taskapp                    # bật lại (giữ nguyên dữ liệu, PVC, image)
minikube stop  -p taskapp                    # tắt, không mất gì
minikube delete -p taskapp                   # XOÁ HẲN: mất PVC, mất DB, mất image
minikube profile list                        # liệt kê mọi profile + trạng thái
minikube -p taskapp status                   # host / kubelet / apiserver
minikube -p taskapp ip                       # IP của node (xem cảnh báo ở mục 4)
```

**`stop` vs `delete`.** `stop` chỉ tắt container, bật lại mất vài chục giây và dữ
liệu còn nguyên. `delete` xoá sạch — chỉ dùng khi muốn lấy lại dung lượng đĩa hoặc
cần cluster hoàn toàn sạch.

### Nạp image vào cluster

```powershell
docker build -t k8s-practice:dev .
minikube -p taskapp image load k8s-practice:dev
minikube -p taskapp image ls | Select-String k8s-practice     # kiểm tra đã vào chưa
```

**Vì sao phải `image load`.** minikube chạy containerd **riêng** bên trong node,
không dùng chung image store với Docker Desktop. Build xong ở host thì trong cluster
chưa có gì. Overlay dev đặt `imagePullPolicy: Never` để lỗi hiện ra ngay
(`ErrImageNeverPull`) thay vì âm thầm đi kéo từ Docker Hub một image trùng tên.

> Sau khi load image **cùng tag**, pod đang chạy không tự đổi. Phải
> `kubectl rollout restart` (mục 2) — tag không đổi thì pod spec không đổi, k8s
> không có lý do gì để tạo pod mới.

---

## 2. Deploy, kiểm tra trước khi deploy, rollback

### Ba mức kiểm tra, từ rẻ tới đắt

```bash
# 1. Render thuần, KHÔNG cần cluster. Bắt lỗi cú pháp kustomize.
kubectl kustomize deploy/overlays/dev

# 2. So sánh với cluster thật: chính xác cái gì sẽ đổi.
kubectl --context $CTX diff -k deploy/overlays/dev

# 3. Chạy qua toàn bộ admission (quota, Pod Security, webhook) nhưng KHÔNG ghi.
kubectl kustomize deploy/overlays/dev | kubectl --context $CTX apply --dry-run=server -f -
```

**`diff -k` là lệnh quan trọng nhất trong file này.** `apply` báo "configured" nhưng
không nói đã đổi gì. `diff` cho bạn thấy chính xác trước khi bấm. Bắt buộc với prod.

**`--dry-run=server` khác `=client` ở đâu.** `client` chỉ kiểm tra cú pháp ở máy bạn.
`server` gửi lên API server chạy thật qua ResourceQuota, Pod Security Admission,
validating webhook — rồi bỏ đi thay vì ghi. Chính lỗi
`configuration-snippet annotation cannot be used` chỉ lộ ra ở mức này, không mức nào
thấp hơn bắt được.

### Apply và theo dõi

```bash
kubectl --context $CTX apply -k deploy/overlays/dev

kubectl --context $CTX -n $NS rollout status deployment/taskapp --timeout=300s
kubectl --context $CTX -n $NS rollout status statefulset/postgres --timeout=300s
```

**Luôn chạy `rollout status`.** `apply` trả về ngay lập tức — nó chỉ ghi ý định vào
API server, không chờ pod lên. Không có `rollout status` thì CI báo xanh trong khi
pod đang CrashLoopBackOff.

### Rollout, restart, rollback

```bash
# Buộc tạo pod mới mà không đổi gì trong manifest (dùng sau khi load image cùng tag,
# hoặc sau khi Secret đổi — xem mục 8).
kubectl --context $CTX -n $NS rollout restart deployment/taskapp

kubectl --context $CTX -n $NS rollout history deployment/taskapp          # các revision
kubectl --context $CTX -n $NS rollout history deployment/taskapp --revision=3
kubectl --context $CTX -n $NS rollout undo deployment/taskapp             # về bản ngay trước
kubectl --context $CTX -n $NS rollout undo deployment/taskapp --to-revision=3

kubectl --context $CTX -n $NS rollout pause  deployment/taskapp   # dừng giữa chừng để quan sát
kubectl --context $CTX -n $NS rollout resume deployment/taskapp
```

**`rollout restart` làm gì.** Nó ghi annotation `kubectl.kubernetes.io/restartedAt`
vào pod template → pod spec đổi → rolling update bình thường, tôn trọng
`maxUnavailable: 0`. Đây là cách restart **không gây downtime**, khác hẳn
`delete pod` (giết ngay, mất luôn pod đang phục vụ).

**`undo` tin cậy được đến đâu.** Ở prod image pin theo digest, nên `undo` quay về
đúng những bytes đã từng chạy. Nếu dùng tag mutable thì `undo` chỉ quay về *cùng một
tag* — có thể đã là binary khác. Đó là lý do prod pin digest.

---

## 3. Nhìn trạng thái

```bash
kubectl --context $CTX -n $NS get pods -o wide        # thêm IP pod và node
kubectl --context $CTX -n $NS get pods -w             # theo dõi thay đổi liên tục
kubectl --context $CTX -n $NS get all                 # KHÔNG hiện netpol/pvc/ingress — xem dưới

# Cái nên dùng thay cho `get all`:
kubectl --context $CTX -n $NS get pods,svc,ingress,hpa,pdb,netpol,pvc,cronjob
```

> `get all` gây hiểu nhầm: nó **không** liệt kê NetworkPolicy, PVC, Ingress,
> ResourceQuota, Secret. Tưởng namespace trống trong khi policy vẫn còn nguyên.

### Events — nơi đọc đầu tiên khi có gì đó không lên

```bash
kubectl --context $CTX -n $NS get events --sort-by=.lastTimestamp | tail -20

# Dạng mới, dễ đọc hơn (k8s >= 1.28):
kubectl --context $CTX -n $NS events --for statefulset/postgres
```

**Vì sao events quan trọng hơn logs lúc đầu.** Pod không khởi động thì **chưa có
log nào cả**. Mọi thứ chặn nó — quota từ chối PVC, image kéo không được, scheduler
không xếp được node, admission webhook từ chối — đều nằm ở events. Chính event này
lộ ra lỗi quota storage của dev:

```
create Claim data-postgres-0 ... failed: exceeded quota: namespace-quota,
requested: requests.storage=2Gi, used: requests.storage=20Gi, limited: 20Gi
```

StatefulSet **không** báo lỗi đó ra pod — nó chỉ ghi event rồi im lặng không tạo gì.

### Describe

```bash
kubectl --context $CTX -n $NS describe pod <ten-pod>
kubectl --context $CTX -n $NS describe pod <ten-pod> | tail -30    # phần Events ở cuối
```

`describe` gộp spec + trạng thái + events của riêng object đó. Đọc từ **dưới lên**:
phần Events cuối cùng thường là câu trả lời.

### Headlamp — nhìn bằng mắt (chỉ dùng ở cụm local)

```powershell
.\scripts\headlamp.ps1              # cài + sinh token + mở http://localhost:8081
.\scripts\headlamp.ps1 -TokenOnly   # token hạn 24 giờ, lấy lại khi hết hạn
```

Đỡ mỏi tay khi phải nhìn nhiều loại object cùng lúc: NetworkPolicy, PVC ↔ PV,
Events theo thời gian, lần chạy gần nhất của CronJob backup.

Nó gọi đúng những API mà kubectl gọi, bằng token bạn dán vào — nên nó **không**
thay được ba thứ dưới đây, và đây mới là chỗ quyết định:

| Không có trong giao diện | Vì sao vẫn phải dùng CLI |
|---|---|
| `kubectl diff -k` | biết chính xác cái gì sẽ đổi **trước khi** apply |
| `apply --dry-run=server` | chạy qua quota + Pod Security + webhook mà không ghi |
| `kubectl auth can-i ... --as` | trả lời "ai được làm gì" — nguồn sự thật về RBAC |

> `scripts\headlamp-rbac.yaml` có một ServiceAccount gắn `cluster-admin`. Chỉ hợp
> lệ ở cụm học trên máy. Trên staging/prod, quyền vào cụm đi qua OIDC của tổ chức,
> không qua ServiceAccount token sinh tại chỗ.

---

## 4. Logs

```bash
POD=$(kubectl --context $CTX -n $NS get pod -l app.kubernetes.io/name=taskapp -o name | head -1)

kubectl --context $CTX -n $NS logs $POD -c app --tail=-1     # TOÀN BỘ log
kubectl --context $CTX -n $NS logs $POD -c app -f            # bám theo realtime
kubectl --context $CTX -n $NS logs $POD -c app --previous    # log của lần chạy TRƯỚC khi crash
kubectl --context $CTX -n $NS logs $POD -c wait-for-db       # log của init container
kubectl --context $CTX -n $NS logs $POD -c app --since=10m
```

> **Bẫy đã dính.** Dùng selector `-l` thì kubectl mặc định `--tail=10`, còn dùng tên
> pod thì mặc định là **toàn bộ**. Lúc test tôi tưởng Flyway không chạy vì log không
> có dòng nào — thực ra chỉ đang xem 10 dòng cuối. Dùng `-l` thì luôn thêm `--tail=-1`:
>
> ```bash
> kubectl --context $CTX -n $NS logs -l app.kubernetes.io/name=taskapp -c app --tail=-1 --prefix
> ```
> `--prefix` thêm tên pod vào đầu mỗi dòng — cần khi có nhiều replica.

**`--previous` là lệnh cứu mạng khi CrashLoopBackOff.** Container đã chết thì
`logs` thường trả về của container **mới**, chưa có gì. `--previous` lấy log của lần
chạy đã crash — tức là chỗ chứa nguyên nhân.

### Log JSON đọc cho dễ

App chạy profile `k8s` nên log là JSON một dòng. Lọc lấy phần message:

```bash
kubectl --context $CTX -n $NS logs $POD -c app --tail=-1 | grep -oE '"message":"[^"]*"'
```

```powershell
# PowerShell, lọc và parse tử tế hơn
kubectl --context $CTX -n $NS logs $POD -c app --tail=-1 |
  ForEach-Object { try { ($_ | ConvertFrom-Json) } catch {} } |
  Select-Object timestamp, level, message | Format-Table -AutoSize
```

---

## 5. Vào bên trong / kết nối tới pod

### port-forward — cách truy cập đáng tin cậy nhất trên Windows

```bash
kubectl --context $CTX -n $NS port-forward svc/taskapp 8080:8080
curl http://localhost:8080/api/info

# Cổng actuator (NetworkPolicy chặn từ pod khác, nhưng port-forward thì qua được)
kubectl --context $CTX -n $NS port-forward svc/taskapp 9090:9090
curl http://localhost:9090/actuator/health
```

**Vì sao port-forward không bị NetworkPolicy chặn.** Nó đi qua API server → kubelet →
pod, không phải traffic pod-to-pod trong mạng cluster. Rất tiện để debug, nhưng
**cũng có nghĩa là nó không chứng minh được NetworkPolicy đúng hay sai** — muốn test
policy phải dùng pod thật (mục 7).

> **Trên Windows + docker driver, IP của minikube (`192.168.x.x`) KHÔNG route được
> từ host.** Thêm IP đó vào file `hosts` là vô ích, curl sẽ treo. Ba cách đi vào:
>
> ```powershell
> # 1. port-forward (dễ nhất)
> kubectl --context taskapp -n taskapp-dev port-forward svc/taskapp 8080:8080
>
> # 2. Qua Ingress, chạy curl từ bên trong node
> minikube -p taskapp ssh -- curl -s -H 'Host: tasks.local' http://localhost/api/info
>
> # 3. Qua Ingress từ Windows: cần `minikube tunnel` chạy ở terminal Administrator riêng
> minikube -p taskapp tunnel
> # rồi thêm vào C:\Windows\System32\drivers\etc\hosts:  127.0.0.1  tasks.local
> ```

### exec

```bash
kubectl --context $CTX -n $NS exec -it $POD -c app -- sh
kubectl --context $CTX -n $NS exec $POD -c app -- ls -la /tmp      # chạy một lệnh rồi thoát
```

**Nhớ `readOnlyRootFilesystem: true`.** Vào được nhưng **không ghi được** ở đâu ngoài
`/tmp`. `apk add` sẽ fail. Đó là chủ ý, không phải lỗi.

### kubectl debug — khi image không có công cụ cần thiết

```bash
# Gắn thêm một container tạm vào pod ĐANG CHẠY, dùng chung network namespace
kubectl --context $CTX -n $NS debug -it $POD --image=nicolaka/netshoot --target=app -- sh
```

**Vì sao dùng cái này.** Image runtime cố tình tối giản: không `curl`, không `dig`,
không `tcpdump`. `debug` gắn một container có đủ đồ vào **đúng pod đó**, nhìn thấy
đúng network và process của nó — khác hẳn việc bật một pod mới ở chỗ khác. Container
tạm biến mất khi pod restart.

---

## 6. Tài nguyên, scale, disruption

```bash
kubectl --context $CTX -n $NS top pods        # cần metrics-server
kubectl --context $CTX top nodes

kubectl --context $CTX -n $NS get hpa
kubectl --context $CTX -n $NS describe hpa taskapp        # xem vì sao nó quyết định vậy

kubectl --context $CTX -n $NS describe resourcequota namespace-quota   # đã dùng bao nhiêu
kubectl --context $CTX -n $NS describe limitrange defaults
```

**`describe resourcequota` nên chạy trước khi tăng replica hay thêm PVC.** Nó hiện
cột `Used / Hard`. Vượt quota thì object bị từ chối ở admission, và — như đã thấy —
controller có thể nuốt lỗi chỉ ghi event.

### Scale tay

```bash
kubectl --context $CTX -n $NS scale deployment/taskapp --replicas=3
```

> Nếu HPA đang bật, nó sẽ **kéo về** theo tính toán của nó trong vòng ~30 giây.
> Muốn cố định số replica thì phải sửa `minReplicas`/`maxReplicas` của HPA, hoặc xoá
> HPA. `scale` tay chỉ có tác dụng tạm.

### Drain và PDB

```bash
kubectl --context $CTX drain <node> --ignore-daemonsets --delete-emptydir-data
kubectl --context $CTX uncordon <node>
kubectl --context $CTX -n $NS get pdb          # cột ALLOWED DISRUPTIONS
```

**PDB của postgres là `maxUnavailable: 0`, nên `drain` sẽ TREO** và báo lỗi PDB.
Đó là chủ ý — xem [DECISIONS 4.5](DECISIONS.md). Muốn drain thật thì phải xoá PDB
đó bằng tay, tức là chấp nhận downtime DB một cách có ý thức:

```bash
kubectl --context $CTX -n $NS delete pdb postgres     # rồi drain, rồi apply -k lại
```

---

## 7. Kiểm tra bảo mật

### NetworkPolicy có thực sự chặn không

Đây là cách duy nhất chứng minh policy hoạt động — `get netpol` chỉ nói policy *tồn
tại*, không nói nó *được thực thi*:

```bash
kubectl --context $CTX -n $NS run rogue --rm -it --restart=Never \
  --image=busybox:1.36 \
  --overrides='{"spec":{"securityContext":{"runAsNonRoot":true,"runAsUser":65534,"seccompProfile":{"type":"RuntimeDefault"}},"containers":[{"name":"rogue","image":"busybox:1.36","stdin":true,"tty":true,"command":["sh"],"securityContext":{"allowPrivilegeEscalation":false,"readOnlyRootFilesystem":true,"capabilities":{"drop":["ALL"]}}}]}}' 

# rồi bên trong:
#   nc -w 4 -z postgres 5432     -> phải THẤT BẠI
#   nc -w 4 -z taskapp 8080      -> phải THẤT BẠI
#   nslookup postgres            -> phải THÀNH CÔNG (policy DNS cho qua)
```

**Vì sao `--overrides` dài dòng như vậy.** Namespace gắn nhãn Pod Security
`restricted`, nên một `kubectl run` thường sẽ bị **từ chối thẳng**. Phần override
chính là bộ `securityContext` tối thiểu để pod được chấp nhận. Việc nó *phải* dài
như vậy chính là bằng chứng PSA đang hoạt động.

> **CNI phải thực thi NetworkPolicy.** CNI mặc định của minikube thì **không** —
> policy vẫn apply thành công, `get netpol` vẫn liệt kê, và không có gì bị chặn.
> Vì thế `dev-up.ps1` dùng `--cni=calico`. Đây là kiểu lỗi nguy hiểm nhất: an toàn giả.

### Quyền và cấu hình bảo mật

```bash
# ServiceAccount của app có quyền gì (kỳ vọng: gần như không có gì)
kubectl --context $CTX auth can-i --list --as=system:serviceaccount:$NS:taskapp -n $NS

# Nhãn Pod Security của namespace
kubectl --context $CTX get ns $NS -o jsonpath='{.metadata.labels}' ; echo

# securityContext thực tế đang chạy
kubectl --context $CTX -n $NS get pod $POD -o jsonpath='{.spec.containers[0].securityContext}' ; echo
```

---

## 8. Config và Secret

```bash
# Tên ConfigMap/Secret có hậu tố hash — tra tên thật đang dùng:
kubectl --context $CTX -n $NS get cm,secret

kubectl --context $CTX -n $NS get cm app-config-<hash> -o yaml

# Đọc giá trị secret (base64)
kubectl --context $CTX -n $NS get secret db-credentials-<hash> \
  -o jsonpath='{.data.APP_DB_PASSWORD}' | base64 -d ; echo
```

```powershell
# PowerShell
$b = kubectl --context $CTX -n $NS get secret db-credentials-<hash> -o jsonpath='{.data.APP_DB_PASSWORD}'
[Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($b))
```

**Vì sao tên có hash.** `configMapGenerator` gắn hash nội dung vào tên. Đổi config →
đổi tên → pod spec đổi → **rolling update tự động**. Đó là lý do bạn không bao giờ
phải `rollout restart` sau khi sửa file trong `base/files` rồi `apply -k`.

**Ngoại lệ ở prod:** Secret prod do External Secrets Operator tạo, **không có hash**.
Vault xoay mật khẩu thì Secret đổi nhưng pod vẫn giữ giá trị cũ (biến môi trường
được chụp lúc container khởi động). Lúc đó phải restart tay:

```bash
kubectl --context $CTX -n $NS rollout restart deployment/taskapp
```

---

## 9. Database

```bash
# psql bằng superuser (quản trị)
kubectl --context $CTX -n $NS exec -it postgres-0 -- psql -U pgadmin -d taskdb

# Vài lệnh kiểm tra nhanh
kubectl --context $CTX -n $NS exec postgres-0 -- psql -U pgadmin -d taskdb \
  -c "\dt app.*" \
  -c "select * from app.flyway_schema_history;" \
  -c "select count(*) from pg_stat_activity;"
```

**Dùng `pgadmin` hay `taskapp`?** App chạy bằng `taskapp` (không phải superuser).
Khi debug quyền hạn, hãy đăng nhập **đúng bằng `taskapp`** để tái hiện cái app thấy —
`pgadmin` là superuser nên sẽ không bao giờ gặp lỗi phân quyền.

### Backup và restore

```bash
# Chạy backup ngay, không đợi lịch 02:15
kubectl --context $CTX -n $NS create job backup-now --from=cronjob/postgres-backup
kubectl --context $CTX -n $NS wait --for=condition=complete job/backup-now --timeout=600s
kubectl --context $CTX -n $NS logs job/backup-now

# Liệt kê các bản dump đang có
kubectl --context $CTX -n $NS exec postgres-0 -- ls -lh /backups 2>/dev/null \
  || echo "PVC backup chỉ mount vào pod của CronJob, không mount vào postgres-0"

kubectl --context $CTX -n $NS get cronjob postgres-backup    # LAST SCHEDULE / SUSPEND

# Tạm dừng backup (ví dụ khi bảo trì)
kubectl --context $CTX -n $NS patch cronjob postgres-backup -p '{"spec":{"suspend":true}}'
kubectl --context $CTX -n $NS patch cronjob postgres-backup -p '{"spec":{"suspend":false}}'
```

**Restore là thao tác phá huỷ — đọc kỹ trước khi chạy.** `--clean --if-exists` sẽ
**xoá dữ liệu hiện tại** rồi nạp bản dump đè lên:

```bash
kubectl --context $CTX -n $NS exec -it postgres-0 -- \
  pg_restore -U pgadmin -d taskdb --clean --if-exists /backups/<ten-file>.dump
```

Luôn tập restore trên dev trước. Một backup chưa từng được restore thử thì chưa phải
backup.

---

## 10. Dọn dẹp

```bash
kubectl --context $CTX delete -k deploy/overlays/dev

# PVC KHÔNG bị xoá theo — phải xoá riêng:
kubectl --context $CTX -n $NS get pvc
kubectl --context $CTX -n $NS delete pvc --all
```

**Vì sao `delete -k` để lại PVC.** PVC do `volumeClaimTemplates` của StatefulSet
sinh ra **cố ý sống lâu hơn** StatefulSet — đó là tính năng bảo vệ dữ liệu, không
phải lỗi. Xoá nhầm StatefulSet thì apply lại là dữ liệu còn nguyên. Hệ quả: dọn
namespace mà quên xoá PVC thì vẫn đang trả tiền cho volume, và lần deploy sau sẽ
**dùng lại dữ liệu cũ** — nguồn gốc của kiểu lỗi "sao migration không chạy".

Hoặc dùng script (tự xử lý PVC):

```powershell
.\scripts\dev-down.ps1              # thêm -KeepData nếu muốn giữ volume
```

---

## 11. Lệnh cần cẩn thận

| Lệnh | Rủi ro | Làm trước khi chạy |
|---|---|---|
| `kubectl delete -k ...` | Xoá cả namespace và mọi thứ trong đó | `kubectl diff -k` xem phạm vi |
| `kubectl delete pvc` | **Mất dữ liệu vĩnh viễn** | Chạy backup trước |
| `pg_restore --clean` | Xoá dữ liệu hiện tại | Dump một bản ngay trước đó |
| `kubectl delete pdb postgres` | Mở đường cho drain hạ DB | Chỉ làm trong cửa sổ bảo trì |
| `minikube delete` | Mất toàn bộ cluster + PVC | Dùng `stop` nếu chỉ muốn tắt |
| `kubectl apply` lên nhầm context | Chạm vào prod | `kubectl config current-context` |
| `kubectl edit` | Sửa trực tiếp trên cluster, git không biết | Sửa file rồi `apply -k` |

**Về `kubectl edit`.** Nó sửa live object, nên cluster lệch khỏi git. Lần
`apply -k` sau sẽ âm thầm ghi đè thay đổi của bạn, hoặc tệ hơn là không ghi đè và
để lại một khác biệt không ai nhớ. Dùng để *thử nhanh* thì được; sửa thật thì phải
qua file trong ``.

---

## 12. Một chuỗi debug điển hình

Khi có người báo "app hỏng rồi", chạy theo đúng thứ tự này:

```bash
# 1. Pod có chạy không?
kubectl --context $CTX -n $NS get pods

# 2. Nếu KHÔNG Running/Ready -> đọc events, không phải logs (chưa có log nào cả)
kubectl --context $CTX -n $NS get events --sort-by=.lastTimestamp | tail -20
kubectl --context $CTX -n $NS describe pod <pod> | tail -30

# 3. Nếu ĐANG Running nhưng lỗi -> đọc logs
kubectl --context $CTX -n $NS logs <pod> -c app --tail=-1 | grep -i error
kubectl --context $CTX -n $NS logs <pod> -c app --previous     # nếu có restart

# 4. App có tự thấy mình khoẻ không? (port-forward ở terminal khác, rồi curl)
kubectl --context $CTX -n $NS port-forward <pod> 9090:9090
curl -s http://localhost:9090/actuator/health

# 5. Có phải vấn đề mạng?
kubectl --context $CTX -n $NS exec <pod> -c app -- nc -zv postgres 5432

# 6. Hết cách -> quay về bản chạy được, điều tra sau
kubectl --context $CTX -n $NS rollout undo deployment/taskapp
```

**Nguyên tắc: khôi phục dịch vụ trước, tìm nguyên nhân sau.** `rollout undo` mất vài
chục giây. Đọc log trên một hệ thống đang chết thì tốn nhiều hơn thế rất nhiều — và
log vẫn còn đó sau khi đã rollback.
