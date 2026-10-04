# Vì sao lại làm như vậy

`../../README_ARC.md` nói **cái gì** đã được dựng. File này nói **tại sao**, và quan trọng hơn:
**đánh đổi gì** và **khi nào nên chọn khác**.

Đọc file này trước khi sửa bất kỳ giá trị nào trong `base`. Phần lớn con số ở đây
không phải chọn bừa — sửa một chỗ thường làm hỏng một chỗ khác.

**Mục lục**

1. [Công cụ & cấu trúc](#1-công-cụ--cấu-trúc)
2. [Vòng đời pod: probe, shutdown, rollout](#2-vòng-đời-pod-probe-shutdown-rollout)
3. [Tài nguyên: CPU, RAM, QoS](#3-tài-nguyên-cpu-ram-qos)
4. [Tầng dữ liệu](#4-tầng-dữ-liệu)
5. [Image & JVM](#5-image--jvm)
6. [Bảo mật](#6-bảo-mật)
7. [Khả dụng & co giãn](#7-khả-dụng--co-giãn)
8. [Cấu hình & bí mật](#8-cấu-hình--bí-mật)
9. [Quan trắc](#9-quan-trắc)
10. [Những thứ cố tình KHÔNG làm](#10-những-thứ-cố-tình-không-làm)

---

## 1. Công cụ & cấu trúc

### 1.1 Kustomize, không phải Helm

**Vì sao.** Manifest ở đây luôn là YAML hợp lệ — mở ra đọc được, IDE validate được,
`kubectl diff -k` so sánh được với cluster thật. Helm chart là *template* sinh ra
YAML: muốn biết thực sự apply cái gì phải render trước. Với một dịch vụ nội bộ do
chính đội mình vận hành, lớp template đó là chi phí không đổi lại gì. Thêm nữa
`kustomize` đã nằm sẵn trong `kubectl` — không cần cài thêm binary, không có
release state lưu ở server-side như Helm.

**Đánh đổi.** Không có điều kiện (`if`), không có vòng lặp, không có cơ chế đóng gói
để phát hành cho người ngoài. Và strategic-merge của kustomize xử lý list khá bất
ngờ — chính nó đã nhân đôi `topologySpreadConstraints` ở overlay prod, phải chữa
bằng JSON6902 patch (xem comment trong `overlays/prod/kustomization.yaml`).

**Đổi sang Helm khi.** Bạn phát hành chart cho bên thứ ba cài, hoặc cần bật/tắt cả
mảng tính năng theo giá trị đầu vào.

### 1.2 Base + overlay, tuyệt đối không sửa base cho một môi trường

**Vì sao.** Giá trị của staging nằm ở chỗ nó *giống* prod về cấu trúc. Nếu prod có
NetworkPolicy mà staging thì không, thì staging không chứng minh được điều gì về
prod. Nên base chứa toàn bộ **cấu trúc**, overlay chỉ chứa **con số và tên miền**.

Quy tắc kiểm tra nhanh: nếu một patch của overlay thêm/bớt *loại* object hoặc
*loại* cơ chế (chứ không phải đổi giá trị), thì nó nên là một **component**
(`components/monitoring`, `components/external-database`), không phải patch.

### 1.3 Mỗi môi trường một namespace, không phải một cluster

**Vì sao.** Phạm vi của tài liệu này. Namespace cho ta ranh giới về quota, NetworkPolicy,
RBAC và tên DNS — đủ để tách dev/staging.

**Đánh đổi — cần nói thẳng.** Namespace **không** phải ranh giới bảo mật thật.
Control plane dùng chung, node dùng chung, CRD dùng chung, và một lỗi ở
cluster-scoped resource (`PriorityClass`, webhook, CRD) ảnh hưởng tất cả.

**Prod thật nên có cluster riêng.** Khi đó `overlays/prod` trỏ sang một context khác,
cấu trúc file không đổi.

---

## 2. Vòng đời pod: probe, shutdown, rollout

Đây là phần nhiều người bỏ qua nhất và cũng là phần gây downtime nhiều nhất.

### 2.1 Ba loại probe, mỗi loại một câu hỏi khác nhau

| Probe | Câu hỏi | Trượt thì sao |
|---|---|---|
| `startupProbe` | "Đã khởi động xong chưa?" | Chưa làm gì cả, chỉ chờ tiếp |
| `readinessProbe` | "Nhận request được chưa?" | Gỡ khỏi Service — **không** restart |
| `livenessProbe` | "Có cần giết và tạo lại không?" | **Restart container** |

**Vì sao cần `startupProbe` riêng.** JVM + Flyway migration mất 30–90 giây. Nếu chỉ
có liveness với `initialDelaySeconds` lớn, ta phải chọn: delay đủ dài cho lần khởi
động chậm nhất (và khi đó một pod chết thật cũng mất chừng đó thời gian mới được
phát hiện), hoặc delay ngắn (và pod khởi động chậm bị giết giữa chừng →
CrashLoopBackOff). `startupProbe` tách hai mối quan tâm: cho phép tới 150s để lên,
nhưng khi đã lên rồi thì liveness kiểm tra mỗi 10s.

### 2.2 Liveness LỎNG hơn readiness — và không được kiểm tra DB

Trong `application.yml`:

```yaml
readiness: { include: readinessState, db }   # có kiểm tra DB
liveness:  { include: livenessState }        # KHÔNG kiểm tra DB
```

Và `failureThreshold`: readiness 3 (≈15s), liveness 6 (≈60s).

**Vì sao.** Giả sử DB restart 40 giây. Nếu liveness cũng kiểm tra DB:

1. Cả 4 pod app trượt liveness cùng lúc → Kubernetes giết cả 4.
2. DB sống lại, 4 pod khởi động lại từ đầu: JVM nguội, connection pool rỗng,
   Flyway chạy lại, JIT phải warm-up lại.
3. Cả 4 đồng loạt mở connection vào DB vừa hồi phục → **thundering herd**, DB sập lại.

Một sự cố DB 40 giây trở thành sự cố toàn hệ thống vài phút. Restart pod **không**
sửa được DB hỏng, nên liveness không được quan tâm tới DB.

Ngược lại readiness *phải* quan tâm: mọi endpoint của app này đều cần DB, pod không
có DB thì trả 500. Gỡ nó khỏi Service để Ingress trả 503 là hành vi đúng.

### 2.3 `preStop.sleep 5s` — chữa một race condition có thật

Khi pod bị xoá, **hai việc chạy song song, không ai chờ ai**:

```
(A) kubelet:              preStop  →  SIGTERM  →  (chờ grace)  →  SIGKILL
(B) endpoints controller: gỡ pod khỏi EndpointSlice → kube-proxy/ingress cập nhật
```

(B) là *eventually consistent* — phải lan qua controller, rồi tới từng kube-proxy
trên từng node, rồi tới ingress controller. Thường mất 1–3 giây, có khi hơn.

Nếu không có `preStop`, (A) chạy trước: app đóng listener trong khi ingress vẫn còn
đang gửi request tới IP đó → client nhận `connection refused` / RST. Đây chính là
"cứ deploy là có vài lỗi 502" mà nhiều đội chấp nhận sống chung.

`preStop.sleep 5` cho (B) 5 giây chạy trước. Trong 5 giây đó app **vẫn phục vụ bình
thường** — nó còn chưa nhận SIGTERM.

Dùng `sleep` dạng khai báo (k8s ≥1.30) chứ không phải `exec: ["sleep","5"]`, vì
runtime image không có shell/coreutils.

### 2.4 `terminationGracePeriodSeconds: 60` — con số này là phép cộng

```
 5s  preStop.sleep
30s  spring.lifecycle.timeout-per-shutdown-phase  (xử nốt request đang dở)
25s  dự phòng
───
60s
```

Grace period tính từ **lúc bắt đầu terminate**, tức là bao gồm cả preStop. Nếu đặt
30s thì preStop ăn mất 5s, Spring chỉ còn 25s để làm việc mà nó tưởng mình có 30s →
SIGKILL giữa chừng một transaction. **Sửa một trong ba số thì phải sửa cả ba.**

### 2.5 `maxUnavailable: 0` + `maxSurge: 1` + `minReadySeconds: 15`

**Vì sao `maxUnavailable: 0`.** Mặc định (25%/25%) cho phép tụt xuống dưới số replica
mong muốn trong lúc deploy. Với 4 replica được tính toán cho tải thật, tụt còn 3
giữa giờ cao điểm là tự gây quá tải. `maxSurge: 1` tạo pod mới *trước*, chỉ xoá pod
cũ sau khi pod mới Ready.

**Vì sao `minReadySeconds: 15`.** Không có nó, pod chỉ cần Ready *một lần* là rollout
tính là xong. Một container Ready rồi chết sau 3 giây vẫn làm rollout tiếp tục cho
tới khi hỏng cả 4. 15 giây buộc pod phải trụ được một lát trước khi rollout đi tiếp.

**Đánh đổi.** Deploy chậm hơn: 4 replica × (startup + 15s) tuần tự. Đổi lại rollout
hỏng sẽ tự dừng khi mới thay 1 pod.

### 2.6 `initContainer: wait-for-db` thay vì để CrashLoopBackOff

**Vì sao.** Flyway fail cứng khi không kết nối được DB. Không có init container,
pod app sẽ crash, vào CrashLoopBackOff, và backoff **tăng theo cấp số nhân** tới
5 phút. Nghĩa là DB hồi phục sau 30 giây nhưng app có thể còn nằm chờ 5 phút nữa.

Init container chờ bằng vòng lặp `pg_isready` giữ pod ở `Init:0/1` — trạng thái
này nhìn log ra ngay là "đang chờ DB", và khi DB lên thì app khởi động **ngay lập tức**.

---

## 3. Tài nguyên: CPU, RAM, QoS

### 3.1 CPU có request, KHÔNG có limit

Đây là quyết định gây tranh cãi nhất trong bộ này, nên giải thích kỹ.

**CPU limit hoạt động thế nào.** Kernel cấp cho cgroup một hạn ngạch trên mỗi chu kỳ
100ms. Dùng hết hạn ngạch thì **mọi thread bị đóng băng cho tới đầu chu kỳ sau** —
đó là CFS throttling.

**Vì sao điều đó tệ với app này.** Xử lý một HTTP request không phải tải đều: nó là
một cụm burst ngắn (parse, query, serialize) xen kẽ chờ I/O. Thêm GC thread của JVM
chạy song song. Với limit 500m, chỉ cần một GC pause trùng với vài request là hết
hạn ngạch → **mọi thread đứng im tới 100ms**, kể cả thread đang phục vụ request
hoàn toàn bình thường. Kết quả: p99 latency spike trong khi node vẫn còn CPU rảnh.

**Không có limit thì lấy gì bảo vệ?** Ba lớp:

- **`requests.cpu`** vẫn là thứ scheduler dùng để xếp pod → node không bị overcommit.
- Khi node tranh chấp CPU thật, kernel chia theo **tỉ lệ cpu.shares = request**. Pod
  request 500m vẫn được gấp đôi pod request 250m. Không ai đói.
- **`ResourceQuota.requests.cpu`** chặn trần cho cả namespace.

CPU là tài nguyên **nén được** — thiếu thì chạy chậm. RAM thì không — thiếu là bị
giết. Nên RAM phải có limit, CPU thì không.

**Hệ quả phải nhớ.** Đặt tên một resource trong `ResourceQuota` là biến nó thành
**bắt buộc** với mọi pod. Vì thế `limits.cpu` bị **cố ý bỏ khỏi** cả ResourceQuota
lẫn `LimitRange.default`. Thêm `limits.cpu` vào quota là mọi pod lập tức bị từ chối
hoặc bị LimitRange nhét limit vào — quay lại đúng vấn đề ban đầu.

**Đổi khi nào.** Cluster multi-tenant chạy workload của bên khác mà bạn không tin
tưởng, hoặc bạn cần chi phí dự đoán được tuyệt đối hơn là p99 thấp.

### 3.2 RAM: `requests == limits`

**Vì sao.** Khi node thiếu RAM, kubelet xếp hạng ứng viên bị evict theo lượng dùng
**vượt quá request**. Container không bao giờ dùng quá request thì luôn nằm cuối
danh sách.

**Nói cho chính xác:** vì không có CPU limit nên pod ở QoS **Burstable**, không phải
`Guaranteed` (Guaranteed đòi hỏi cả CPU lẫn RAM đều request == limit). Nhưng về
mặt evict do RAM thì hiệu quả gần như nhau — đó là thứ ta cần.

### 3.3 `LimitRange` và `ResourceQuota` giải quyết hai việc khác nhau

- **LimitRange** = *mặc định và chặn trên cho từng container*. Chống việc ai đó
  thêm một manifest quên khai resource rồi ăn hết node.
- **ResourceQuota** = *trần cho cả namespace*. Chống HPA loạn scale lên 20 pod và
  nuốt cluster.

Cần cả hai: LimitRange không biết gì về tổng, ResourceQuota không biết gì về từng pod.

**Bẫy đã dính khi chạy thật:** `requests.storage` trong quota tính **tổng mọi PVC**,
kể cả PVC không thuộc StatefulSet. Ở overlay dev, PVC backup 20Gi (giá trị kế thừa
từ base) một mình nuốt hết quota 20Gi, khiến `data-postgres-0` bị từ chối:

```
create Claim data-postgres-0 ... failed: exceeded quota: namespace-quota,
requested: requests.storage=2Gi, used: requests.storage=20Gi, limited: 20Gi
```

StatefulSet **không** báo lỗi này ra pod — nó chỉ ghi Event rồi im lặng không tạo
pod nào. Bài học: hạ quota của một môi trường thì phải hạ **mọi** PVC của môi trường
đó, không chỉ PVC chính. Và dung lượng PVC chỉ **tăng** được — muốn giảm phải xoá
rồi tạo lại.

---

## 4. Tầng dữ liệu

### 4.1 `StatefulSet` chứ không phải `Deployment`

**Lỗi thật trong `k8s/03-postgres.yaml`.** `Deployment` + PVC `ReadWriteOnce` +
chiến lược RollingUpdate mặc định = **deadlock**:

1. Rollout tạo pod mới trước khi xoá pod cũ.
2. Pod mới xin attach PVC — nhưng RWO nghĩa là chỉ một node được mount.
3. Pod cũ chưa chết nên chưa nhả volume.
4. Pod mới kẹt `ContainerCreating` vĩnh viễn, rollout treo.

`StatefulSet` giải quyết bằng thứ tự có đảm bảo: xoá `postgres-0` xong hẳn, mới tạo
`postgres-0` mới. Kèm theo:

- **Danh tính ổn định**: luôn là `postgres-0`, DNS
  `postgres-0.postgres-headless` không đổi — điều kiện cần khi sau này thêm replica.
- **`volumeClaimTemplates`**: mỗi replica một PVC riêng, không phải một PVC dùng chung.

Headless Service tồn tại chỉ để cấp DNS cho từng pod; `publishNotReadyAddresses: true`
để pod vẫn có DNS trong lúc khởi động (cần cho bootstrap replication sau này).

### 4.2 `PGDATA` phải là thư mục CON của mount point

```yaml
- name: PGDATA
  value: /var/lib/postgresql/data/pgdata   # không phải /var/lib/postgresql/data
```

**Vì sao.** Hai lý do, cả hai đều làm Postgres không khởi động được:

1. Nhiều loại volume tạo sẵn `lost+found` ở thư mục gốc. `initdb` từ chối chạy trên
   thư mục không rỗng.
2. `fsGroup: 999` làm thư mục gốc của mount có quyền group-write. `initdb` từ chối
   data directory có quyền cho group/other. Thư mục con do entrypoint tạo với
   `chmod 0700` thì không dính.

### 4.3 App không dùng superuser

Init script tạo role `taskapp` sở hữu riêng schema `app`, và thu hồi quyền trên
`public`.

**Vì sao.** Trong image chính thức, `POSTGRES_USER` **là superuser**. App connect
bằng nó nghĩa là một lỗ SQL injection cho phép `COPY ... FROM PROGRAM` (thực thi
lệnh shell trên container DB), đọc mọi database, tạo extension. Với role riêng, thiệt
hại tối đa giới hạn trong schema `app`.

Flyway vẫn chạy được vì `taskapp` **sở hữu** schema đó — đủ quyền DDL trong phạm vi
của mình, không hơn.

### 4.4 Flyway, và Hibernate chỉ `validate`

**Vì sao bỏ `ddl-auto: update`.** Nó suy ra thay đổi schema từ entity lúc chạy:
không review được trong PR, không rollback được, không test trước được, và
**không bao giờ xoá hay đổi cột** — chỉ thêm. Schema prod sẽ trôi dần khỏi code
theo cách không ai kiểm soát được.

`validate` biến sai lệch schema thành **lỗi khởi động** — pod không Ready, rollout
dừng, bản cũ vẫn chạy. Hỏng sớm và an toàn, thay vì hỏng lúc 2 giờ sáng ở một
endpoint ít dùng.

### 4.5 `PodDisruptionBudget` của DB là `maxUnavailable: 0` — cố ý chặn drain

**Vì sao.** Postgres một instance. `kubectl drain` một node đồng nghĩa với downtime
DB. Đặt 0 khiến drain **treo** và báo lỗi PDB, buộc một con người phải quyết định
thay vì tự động làm mất DB lúc 3 giờ sáng.

**Đánh đổi thật — phải biết trước.** Nó cũng chặn cả node upgrade tự động
(cluster-autoscaler, managed node group rotation). Sẽ có lúc bạn phải xoá PDB này
bằng tay để nâng cấp cluster. Đó là chủ ý: tạm thời mất DB nên là một hành động có
ý thức.

PDB của app thì khác — `maxUnavailable: 1` + `unhealthyPodEvictionPolicy: AlwaysAllow`,
vì app stateless, drain thoải mái, và nếu pod đã hỏng sẵn thì đừng chặn drain.

### 4.6 Backup có verify, và có NetworkPolicy riêng

`pg_dump` xong chạy ngay `pg_restore --list` trên file vừa tạo.

**Vì sao.** Một dump hỏng vẫn có kích thước hợp lý và exit code 0 ở những tình huống
nhất định. Backup chưa từng đọc lại được thì không phải backup — nó là một file.
`--list` là cách rẻ nhất để chứng minh file đọc được ngay tại thời điểm tạo.

**Chỗ dễ sót.** Policy `default-deny-all` chặn cả **egress**, nên pod backup cũng
cần policy riêng để mở đường tới `postgres:5432` — nếu quên, CronJob treo rồi fail
im lặng mỗi đêm. Cảnh báo `PostgresBackupMissing` tồn tại chính vì loại lỗi này.

---

## 5. Image & JVM

### 5.1 Multi-stage, layered jar, UID dạng số

**Build trong image.** `k8s/` cũ `COPY target/*.jar` từ máy dev: build không tái lập
được (phụ thuộc JDK/Maven trên máy ai chạy), và glob có thể match nhiều file nếu
`target/` còn rác. Multi-stage đưa toàn bộ build vào trong image.

**Layered jar.** Đổi một dòng code mà phải đẩy lại 60MB dependency là lãng phí.
`layertools` tách dependency (gần như không đổi) khỏi application class (đổi mỗi
commit) thành các layer riêng → Docker chỉ đẩy/kéo lớp thật sự thay đổi.

**`USER 10001:10001` dạng số, không phải tên.** Quan trọng: kubelet chỉ xác minh
được `runAsNonRoot` khi UID trong image là **số**. Nếu image khai `USER app`,
kubelet không phân giải được tên và pod fail thẳng với
*"container has runAsNonRoot and image has non-numeric user"*.

### 5.2 Ngân sách RAM của JVM — chỗ rất dễ sai

`MaxRAMPercentage` là phần dành cho **heap**, tính trên limit của container.
Phần còn lại vẫn phải đủ cho metaspace, code cache, thread stack và native của JVM —
nếu không kernel OOM-kill container, và JVM không hề biết chuyện gì xảy ra.

Cấu hình hiện tại: heap 65%, metaspace ≤128Mi, code cache ≤64Mi.

| Limit | Heap (65%) | Metaspace | Code cache | Thread + native | Tổng | Dư |
|---|---|---|---|---|---|---|
| 768Mi (dev) | 499 | 128 | 64 | ~50 | ~741 | 27 |
| 1Gi (base/staging) | 665 | 128 | 64 | ~60 | ~917 | 107 |
| 1536Mi (prod) | 998 | 128 | 64 | ~60 | ~1250 | 286 |

> Bản đầu của bộ này để 75% heap + metaspace 192Mi trên container 768Mi:
> 576 + 192 + 64 = 832Mi > 768Mi. Container sẽ bị OOM-kill giữa lúc khởi động, và
> triệu chứng nhìn ra là `Exit Code 137` chứ không phải `OutOfMemoryError` — rất
> khó đoán nếu không biết trước phép tính này. Đã sửa.

Suy ra: **768Mi là sàn** của app này. Muốn giảm nữa thì phải hạ `MaxMetaspaceSize`
và đo lại bằng `jcmd VM.native_memory`.

**Vì sao dùng phần trăm chứ không phải `-Xmx`.** Một image chạy cho cả ba môi trường
với ba mức limit khác nhau. `-Xmx` cố định thì phải build ba image, hoặc chọn một
con số sai ở hai nơi.

**`-XX:+ExitOnOutOfMemoryError`.** Mặc định, JVM gặp OOM heap sẽ ném exception rồi
cố chạy tiếp — thường là lết lay lắt: request fail ngẫu nhiên, GC quay 100% CPU,
nhưng liveness vẫn pass. Thoát hẳn để Kubernetes restart cho sạch, và biến sự cố
thành một tín hiệu nhìn thấy được (restart count).

**Chưa đặt `-XX:MaxDirectMemorySize`.** Mặc định nó bằng max heap, về lý thuyết có
thể nhân đôi RAM. Tomcat NIO dùng rất ít direct buffer nên thực tế không chạm tới,
nhưng nếu sau này thêm Netty / gRPC / Kafka client thì phải đặt tường minh.

---

## 6. Bảo mật

Nguyên tắc chung: **mặc định là từ chối**, rồi mở đúng thứ cần.

### 6.1 Pod Security Admission mức `restricted`

Nhãn trên namespace, không phải trên pod.

**Vì sao đặt ở namespace.** `securityContext` trên từng pod là *tự giác* — ai quên
thì thôi. Nhãn PSA là **admission controller**: pod không đạt chuẩn bị từ chối
ngay lúc `kubectl apply`, kể cả khi nó do một operator hay một Helm chart bên thứ
ba tạo ra. Đây là khác biệt giữa "chúng tôi cố gắng" và "không thể làm sai".

### 6.2 `readOnlyRootFilesystem: true` và cái giá của nó

Malware chạy được trong container thường cần ghi file đâu đó. Rootfs read-only cắt
phần lớn khả năng đó.

**Cái giá:** phải biết chính xác app ghi vào đâu và mount `emptyDir` đúng chỗ.

- App: `/tmp` (Tomcat work dir, hsperfdata)
- Postgres: `/var/run/postgresql` (socket) và `/tmp`

**Lưu ý về `emptyDir` của `/tmp`:** dùng `medium: Memory`, tức tmpfs — nhanh, không
chạm disk, nhưng **dung lượng dùng tính vào memory limit của pod**. Vì thế có
`sizeLimit: 128Mi`: không có nó, một file tạm lớn có thể ăn hết ngân sách RAM ở
mục 5.2 và OOM-kill container.

### 6.3 NetworkPolicy: `default-deny-all` trước, rồi allow-list

**Vì sao thứ tự đó quan trọng.** NetworkPolicy là cộng dồn và không có "deny rule".
Cách duy nhất để chặn một thứ là **không tồn tại policy nào cho phép nó**. Nên phải
bắt đầu bằng một policy chặn tất cả, sau đó mở từng đường.

Không có nó, mọi pod trong cluster — kể cả pod của đội khác, kể cả một container
bị chiếm — đều mở được socket tới `postgres:5432`.

**Ba điều phải biết:**

1. **CNI phải thực thi NetworkPolicy.** CNI mặc định của minikube **không**. Policy
   vẫn apply thành công, `kubectl get netpol` vẫn liệt kê ra, và **không có gì bị
   chặn cả**. Vì thế `dev-up.ps1` dùng `--cni=calico`. Đây là kiểu lỗi nguy hiểm
   nhất: an toàn giả.
2. **Probe của kubelet không bị chặn.** Nguồn là node, và Calico/Cilium mặc định
   cho phép host → pod trên cùng node. Đây là hành vi của CNI chứ không phải bảo
   đảm trong spec — đổi CNI thì phải kiểm tra lại.
3. **Egress cũng bị chặn.** Dễ quên nhất. Chính vì vậy có policy DNS riêng cho toàn
   bộ namespace (không có nó thì không pod nào phân giải được tên) và policy egress
   riêng cho pod backup.

### 6.4 Tách actuator sang cổng 9090

**Vì sao.** Ingress chỉ route cổng 8080. Actuator nằm ở 9090 nên
`/actuator/health`, `/actuator/metrics`, `/actuator/prometheus` **về mặt vật lý
không tới được từ Internet** — không phụ thuộc vào việc có ai đó cấu hình đúng
path rule hay không. NetworkPolicy chỉ mở 9090 cho namespace `monitoring`.

So sánh: `k8s/` cũ để chung 8080, nên `/actuator/health` (với `show-details: always`)
lộ ra ngoài cả chi tiết kết nối DB.

### 6.5 Header bảo mật đặt trong app, KHÔNG phải `configuration-snippet`

Bản đầu của bộ này dùng annotation `nginx.ingress.kubernetes.io/configuration-snippet`
để chèn `X-Content-Type-Options`, HSTS… Khi apply thật lên cluster, admission
webhook **từ chối thẳng**:

```
admission webhook "validate.nginx.ingress.kubernetes.io" denied the request:
configuration-snippet annotation cannot be used. Snippet directives are disabled
by the Ingress administrator
```

**Vì sao ingress-nginx chặn.** Từ bản 1.9, `allow-snippet-annotations` mặc định là
`false`. Lý do: snippet cho phép nhét **config nginx bất kỳ** vào controller. Ai
tạo được một Ingress — tức là bất kỳ ai có quyền write trong *bất kỳ namespace nào* —
đều có thể đọc file trên container controller, hoặc tự cấp quyền định tuyến sang
service của namespace khác. Đó là leo thang đặc quyền xuyên namespace.

**Cách xử lý sai:** bật `allow-snippet-annotations: true` cho controller. Làm vậy
là đánh đổi một ranh giới bảo mật thật lấy vài cái header.

**Cách xử lý đúng:** đặt header trong app (`SecurityHeadersFilter`). Ưu điểm cộng
thêm: header còn nguyên khi đổi ingress controller, và có cả khi gọi từ trong cluster
— chỗ mà ingress hoàn toàn không tham gia.

Các annotation còn lại (`limit-rps`, `proxy-*-timeout`, `ssl-redirect`) là annotation
thường, không phải snippet, nên vẫn dùng được.

### 6.6 `automountServiceAccountToken: false`

App này không gọi Kubernetes API. Mặc định k8s vẫn mount một token vào
`/var/run/secrets/...`. Pod bị chiếm là kẻ tấn công có ngay token để dò API server.
Không dùng thì đừng mount.

### 6.7 Prod pin image bằng **digest**, không phải tag

**Vì sao.** Tag là con trỏ, có thể bị đẩy đè. `:1.0.0` hôm nay và `:1.0.0` ngày mai
có thể là hai binary khác nhau — cùng một manifest, cùng một commit, ra kết quả
khác. Digest là hash của nội dung, bất biến.

Điều này làm rollback trở nên thật sự tin cậy được: `rollout undo` quay về đúng
những bytes đã từng chạy. CI có bước fail build nếu manifest prod còn tag.

Staging **được phép** dùng tag `main` — mục đích của staging là luôn chạy bản mới nhất.

---

## 7. Khả dụng & co giãn

### 7.1 HPA chỉ theo CPU — cố ý không dùng memory

**Vì sao không dùng memory với JVM.** JVM xin RAM từ OS rồi **giữ lại**, kể cả khi
object đã được GC. RSS tăng đơn điệu tới gần `MaxRAMPercentage` rồi nằm đó bất kể
tải cao hay thấp. HPA theo memory sẽ:

- Scale **lên** vì heap đầy — dù đó chỉ là GC chưa cần chạy, không phải quá tải.
- Không bao giờ scale **xuống** vì RSS không giảm.

Kết quả là một HPA chỉ biết tăng. Với ngôn ngữ có GC, memory là metric tồi.

**Metric tốt hơn CPU (nếu có Prometheus Adapter):** request trên giây mỗi pod, hoặc
độ dài hàng đợi. Chúng đo trực tiếp thứ ta quan tâm. CPU là xấp xỉ, nhưng là xấp xỉ
dùng được và không cần cài thêm gì.

**Lưu ý:** HPA tính utilization trên **request**, không phải limit. Đó là một lý do
nữa để `requests.cpu` phải phản ánh mức dùng thật.

### 7.2 `behavior`: lên nhanh, xuống chậm

```yaml
scaleUp:   stabilizationWindowSeconds: 30    # phản ứng nhanh
scaleDown: stabilizationWindowSeconds: 300   # chờ 5 phút
```

**Vì sao bất đối xứng.** Hai hướng có chi phí sai lệch rất khác nhau. Scale up thừa
= tốn tiền vài phút. Scale down sớm = mất dung lượng đúng lúc tải quay lại, rồi lại
phải scale up, và pod mới cần 60–90s để JVM ấm — trong khoảng đó người dùng chịu
độ trễ. Không có bất đối xứng này thì HPA dao động qua lại (flapping) với tải nhấp nhô.

### 7.3 `topologySpreadConstraints`: `ScheduleAnyway` ở base, `DoNotSchedule` ở prod

**Vì sao khác nhau.** `DoNotSchedule` là ràng buộc cứng — pod không trải đều được
thì ở `Pending` mãi. Trên minikube (một node, không có nhãn zone) điều đó có nghĩa
là **không có pod nào chạy**. Nên base để `ScheduleAnyway` (gợi ý), prod nâng lên
`DoNotSchedule` cho `topology.kubernetes.io/zone`.

Ở prod, "trải đều qua zone" là yêu cầu bắt buộc: 4 replica nằm chung một AZ thì
mất AZ đó là mất dịch vụ, mà HPA lại báo mọi thứ bình thường.

Riêng `kubernetes.io/hostname` giữ `ScheduleAnyway` ở cả hai — ràng buộc cứng theo
node dễ làm pod kẹt Pending khi cluster đang co giãn.

---

## 8. Cấu hình & bí mật

### 8.1 `configMapGenerator` (có hash) thay vì ConfigMap tĩnh

**Vì sao — đây là một cái bẫy thật.** Biến môi trường được **chụp một lần lúc
container khởi động**. Sửa một ConfigMap tĩnh rồi `kubectl apply`:

- ConfigMap trong cluster đổi ✓
- Pod đang chạy **vẫn giữ giá trị cũ** ✗
- Không có sự kiện nào, không có cảnh báo nào

Cấu hình trôi lệch trong im lặng cho tới lần restart tiếp theo vì một lý do không
liên quan — và khi đó hành vi đột nhiên đổi, không ai hiểu vì sao.

`configMapGenerator` gắn hash nội dung vào tên (`app-config-4tdcd9f87c`). Đổi nội
dung → đổi tên → pod spec đổi → **rolling update tự động**. Cấu hình và pod luôn khớp.

### 8.2 Ba cơ chế secret khác nhau cho ba môi trường

| Môi trường | Cơ chế | Vì sao |
|---|---|---|
| dev | `secrets.env` đã gitignore | Toàn giá trị giả, tiện là chính |
| staging | như trên | Dữ liệu không thật |
| **prod** | External Secrets Operator | Credential thật |

**Vì sao prod cố tình KHÔNG có `secretGenerator`.** Không có cách nào an toàn để
credential prod nằm trong git. Sealed Secrets và SOPS mã hoá được, nhưng:

- Ciphertext nằm trong git **vĩnh viễn** — xoay khoá không xoá được lịch sử.
- Đọc được ciphertext + lấy được khoá sau này = đọc được secret (harvest-now-decrypt-later).
- Không có audit log "ai đã đọc secret này".
- Xoay secret phải sửa file, tạo PR, deploy.

External Secrets Operator kéo từ Vault lúc chạy: có audit log, xoay khoá là chuyện
của Vault không phải của repo, và không có bản mã nào nằm trong lịch sử git.

**Hệ quả vận hành:** apply prod khi chưa có Secret `db-credentials` thì pod đứng ở
`CreateContainerConfigError`. Đó là hành vi đúng — thà không khởi động còn hơn khởi
động với credential sai.

---

## 9. Quan trắc

### 9.1 Cảnh báo theo **triệu chứng**, không theo **nguyên nhân**

Toàn bộ alert trong `components/monitoring/prometheusrule.yaml` đo thứ người dùng
cảm nhận được: tỉ lệ 5xx, p99 latency, pod restart lặp, HPA hết trần, đĩa DB gần đầy,
backup không chạy.

**Vì sao không cảnh báo "CPU > 80%".** CPU cao có thể hoàn toàn bình thường (đang
phục vụ tốt) hoặc là dấu hiệu sự cố. Cảnh báo theo nguyên nhân sinh ra rất nhiều
báo động giả, và báo động giả dẫn tới việc người trực học cách phớt lờ — lúc đó
hệ thống cảnh báo trở thành vô dụng đúng lúc cần nhất.

Quy tắc: **cảnh báo thứ đánh thức người ta dậy phải là thứ người dùng đang chịu.**
CPU, memory, connection pool thuộc về dashboard để *chẩn đoán* sau khi đã có
triệu chứng, không phải để đánh thức.

Ngoại lệ có chủ ý: `PostgresBackupMissing` và `PostgresDiskFillingUp` — người dùng
chưa cảm thấy gì, nhưng khi họ cảm thấy thì đã quá muộn để sửa.

### 9.2 Log JSON, và chỉ ở trong cluster

Profile `k8s` bật JSON; chạy local vẫn log người đọc được.

**Vì sao.** Trong cluster, log đi vào Loki/ELK và phải query được theo trường
(`level`, `service`, `traceId`). Log dạng text phải parse bằng regex — mong manh và
tốn. Nhưng JSON thì người đọc trực tiếp rất khó chịu, nên local giữ nguyên định
dạng cũ. Cùng một image, khác profile.

---

## 10. Những thứ cố tình KHÔNG làm

Quan trọng không kém phần đã làm.

| Không làm | Vì sao |
|---|---|
| **Postgres HA tự viết** | Replication + failover + fencing bằng YAML thuần là cách chắc chắn nhất để mất dữ liệu. Dùng CloudNativePG operator hoặc DB managed. Xem `components/external-database`. |
| **Service mesh (Istio/Linkerd)** | Với một service và một DB, mesh thêm sidecar vào mọi pod, thêm một control plane, thêm một lớp nữa để debug — đổi lấy mTLS mà NetworkPolicy đã che phần lớn nhu cầu. Đáng cân nhắc từ ~10 service trở lên. |
| **GitOps (ArgoCD/Flux)** | Đúng đắn hơn `kubectl apply` trong CI, nhưng cần hạ tầng riêng. Cấu trúc thư mục hiện tại đã sẵn sàng: trỏ Argo Application vào `overlays/prod` là xong, không phải sửa gì. |
| **Distributed tracing** | Một service thì trace không cho biết thêm gì so với log. Cần ngay khi có service thứ hai gọi service thứ nhất. |
| **`ddl-auto` cho test** | Test cũng phải chạy đúng migration Flyway như prod, nếu không test không chứng minh được migration đúng. |
| **VerticalPodAutoscaler** | Xung đột với HPA trên cùng một metric, và VPA restart pod để đổi kích thước. Dùng để *khuyến nghị* (`updateMode: Off`) thì tốt, để tự động thì không. |

---

## Tham chiếu chéo

- Cái gì đã được dựng, cách chạy → [`../../README_ARC.md`](README.md)
- Phiên bản k8s tối thiểu và add-on cần có → [`cluster/namespace-labels.md`](cluster/namespace-labels.md)
- Giá trị placeholder phải sửa trước khi dùng thật → [`../../README_ARC.md` mục 6](README.md#6-phải-sửa-trước-khi-dùng-thật)
