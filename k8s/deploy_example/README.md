# Production-grade Kubernetes deployment

Bộ manifest này là phiên bản "chạy thật" của ứng dụng trong `k8s/`.
Thư mục `k8s/` giữ nguyên làm tài liệu học; thư mục `` là thứ đem đi prod.

> Ba file tài liệu, ba câu hỏi khác nhau:
>
> | File | Trả lời |
> |---|---|
> | `../../README_ARC.md` (file này) | **Cái gì** đã dựng, chạy thế nào |
> | [`ARCHITECTURE.md`](ARCHITECTURE.md) | **Sơ đồ** — topology, lớp bảo mật, vòng đời pod, CI/CD |
> | [`DECISIONS.md`](DECISIONS.md) | **Tại sao** lại làm vậy, đánh đổi gì, khi nào nên chọn khác |
> | [`OPERATIONS.md`](OPERATIONS.md) | **Lệnh hằng ngày** — deploy, debug, backup, rollback |
>
> Đọc `DECISIONS.md` trước khi sửa bất kỳ giá trị nào trong `base/`: nhiều con số
> phụ thuộc lẫn nhau, sửa một chỗ thường làm hỏng chỗ khác.

---

## 1. `k8s/` thiếu gì so với production

| # | Vấn đề trong `k8s/` | Hậu quả thật | Cách xử lý trong `` |
|---|---|---|---|
| 1 | Mật khẩu DB plaintext trong `01-postgres-secret.yaml` | Ai clone repo cũng có credential | `secretGenerator` đọc file đã gitignore (dev/staging); prod dùng External Secrets Operator |
| 2 | Postgres chạy bằng `Deployment` + PVC `ReadWriteOnce` | Rolling update **deadlock**: pod mới không attach được volume pod cũ đang giữ | `StatefulSet` + `volumeClaimTemplates` + headless Service |
| 3 | Không có `securityContext` | Container chạy root, ghi được rootfs, giữ đủ Linux capabilities | `runAsNonRoot`, `readOnlyRootFilesystem`, `drop: [ALL]`, `seccompProfile: RuntimeDefault` |
| 4 | Không có Pod Security Admission | Một manifest sai là chạy privileged được | Namespace gắn nhãn `pod-security.kubernetes.io/enforce: restricted` |
| 5 | Không có NetworkPolicy | Mọi pod trong cluster đều mở được socket tới Postgres | `default-deny-all` + allow-list từng chiều |
| 6 | `Service` kiểu `NodePort` | Mở cổng trên **mọi** node, không TLS, không rate limit | `ClusterIP` + Ingress nginx + cert-manager + HSTS + limit-rps |
| 7 | `ddl-auto: update` | Hibernate tự sửa schema prod: không review được, không rollback được | Flyway migration, Hibernate chỉ `validate` |
| 8 | App connect bằng superuser Postgres | Một lỗ SQL injection = mất toàn bộ instance | Init script tạo role `taskapp` quyền tối thiểu, sở hữu riêng schema `app` |
| 9 | Không có PodDisruptionBudget | `kubectl drain` / upgrade node có thể hạ hết replica cùng lúc | PDB cho cả app và DB |
| 10 | Không graceful shutdown | Mỗi lần deploy là client ăn connection reset | `preStop.sleep 5s` + `server.shutdown: graceful` + `terminationGracePeriodSeconds: 60` |
| 11 | Không có `startupProbe` | JVM khởi động chậm bị liveness giết → CrashLoopBackOff | `startupProbe` 150s; liveness lỏng hơn readiness |
| 12 | Probe và traffic chung cổng 8080 | `/actuator/*` lộ ra Internet qua Ingress | Actuator tách sang cổng 9090, NetworkPolicy chỉ mở cho namespace `monitoring` |
| 13 | Image tag `1.0.0` mutable | Cùng manifest, hôm nay và mai ra 2 binary khác nhau | Prod pin theo **digest** `@sha256:...`; CI fail nếu thấy tag |
| 14 | Dockerfile copy `target/*.jar` từ máy dev | Build không tái lập được, glob có thể match nhiều file | Multi-stage build trong image, layered jar, UID số `10001` |
| 15 | Không có `topologySpreadConstraints` | 3 replica có thể nằm chung 1 node / 1 AZ | Spread bắt buộc theo zone ở prod |
| 16 | Không tách môi trường | Sửa dev là đụng prod | Kustomize base + 3 overlay |
| 17 | ConfigMap tĩnh | Đổi config xong pod vẫn chạy giá trị cũ | `configMapGenerator` → đổi hash → tự rolling restart |
| 18 | Không có backup | Mất PVC là mất sạch dữ liệu | CronJob `pg_dump` hằng đêm + `pg_restore --list` verify + retention 14 ngày |
| 19 | Không có ResourceQuota / LimitRange | Một HPA loạn là ăn hết cluster | Quota + LimitRange theo namespace |
| 20 | Không metrics, không alert | Hỏng mới biết khi khách báo | `/actuator/prometheus`, ServiceMonitor, PrometheusRule theo triệu chứng |
| 21 | Không CI/CD | Deploy bằng tay | test → build → Trivy scan → cosign sign → staging → prod (có approval) |
| 22 | Log dạng text | Không query được khi có sự cố | Log JSON (profile `k8s`) cho Loki/ELK |
| 23 | Mount ServiceAccount token | Pod bị chiếm là có token gọi API server | `automountServiceAccountToken: false` |

---

## 2. Cấu trúc

```
deploy/
├── base/                  # nguồn sự thật duy nhất, không apply trực tiếp
│   ├── files/             # nội dung sinh ConfigMap (app.env, init script)
│   ├── app-*.yaml         # Deployment, Service, HPA, PDB, Ingress, NetworkPolicy
│   ├── db-*.yaml          # StatefulSet, Service, PDB, NetworkPolicy, backup CronJob
│   └── namespace / limitrange / resourcequota / default-deny
├── overlays/
│   ├── dev/               # 1 replica, minikube, image side-load, HTTP thuần
│   ├── staging/           # 2 replica, tag main, cert Let's Encrypt staging
│   └── prod/              # 4 replica, pin digest, spread theo zone, PriorityClass
├── components/
│   ├── monitoring/        # ServiceMonitor + PrometheusRule (cần Prometheus Operator)
│   └── external-database/ # gỡ Postgres in-cluster, trỏ sang RDS / Cloud SQL
└── cluster/               # cài 1 lần mỗi cluster: ClusterIssuer, ExternalSecret mẫu
```

Quy tắc: **không sửa thẳng `base` cho một môi trường**. Khác biệt môi trường phải
nằm ở overlay, nếu không thì "test ở staging" mất hết ý nghĩa.

---

## 3. Chạy local (minikube)

```powershell
# Docker Desktop phải đang chạy
.\scripts\dev-up.ps1
```

Script sẽ start minikube với **Calico** — CNI mặc định của minikube *không* thực thi
NetworkPolicy, thiếu Calico thì toàn bộ policy bị bỏ qua trong im lặng — bật
`ingress` + `metrics-server`, build image trong daemon của minikube, rồi apply overlay dev.

```powershell
.\scripts\verify.ps1
kubectl -n taskapp-dev get pods,hpa,pdb,netpol
kubectl -n taskapp-dev port-forward svc/taskapp 8080:8080
curl http://localhost:8080/api/info

.\scripts\dev-down.ps1            # thêm -KeepData nếu muốn giữ volume
```

---

### Đã kiểm chứng trên cluster thật

Overlay dev đã chạy trọn vẹn trên minikube v1.39 / Kubernetes v1.37 / Calico:

| Kiểm tra | Kết quả |
|---|---|
| Postgres non-root (uid 999) + rootfs read-only + `PGDATA` thư mục con | Ready, `data-checksums` bật |
| Init script tạo role quyền tối thiểu | `tasks` thuộc schema `app`, owner `taskapp`, `rolsuper = f` |
| Flyway migration | `Successfully applied 1 migration` |
| `initContainer: wait-for-db` | Chờ đúng, app lên ngay khi DB sẵn sàng, **không** CrashLoopBackOff |
| API qua Ingress | `200`, kèm `X-Content-Type-Options` / `X-Frame-Options` / `Referrer-Policy` |
| CRUD `POST` + `GET /api/tasks` | Ghi/đọc DB thành công |
| `/actuator/health` qua Ingress | `404` — actuator không lộ ra ngoài |
| **NetworkPolicy** (pod lạ cùng namespace) | DNS qua được; `postgres:5432` và `taskapp:8080` **bị chặn** |
| HPA | `cpu: 16%/70%` — đọc được metric thật |
| PDB | postgres `ALLOWED DISRUPTIONS = 0` đúng như thiết kế |
| CronJob backup | dump + `pg_restore --list` verify OK |
| **Rolling restart** | **799 request, 0 lỗi** — zero downtime |

Ba lỗi thật lộ ra lúc chạy và đã sửa: annotation `configuration-snippet` bị ingress-nginx
từ chối ([DECISIONS 6.5](DECISIONS.md#65-header-bảo-mật-đặt-trong-app-không-phải-configuration-snippet)),
PVC backup ăn hết quota storage của dev ([DECISIONS 3.3](DECISIONS.md#33-limitrange-và-resourcequota-giải-quyết-hai-việc-khác-nhau)),
và ngân sách RAM của JVM vượt limit container ([DECISIONS 5.2](DECISIONS.md#52-ngân-sách-ram-của-jvm--chỗ-rất-dễ-sai)).

---

## 4. Deploy staging / prod

```bash
kubectl apply -k deploy/overlays/staging

# prod: pin digest trước, không bao giờ apply bằng tag
cd deploy/overlays/prod
kustomize edit set image ghcr.io/example/k8s-practice=ghcr.io/you/app@sha256:<digest>
cd -
kubectl diff  -k deploy/overlays/prod      # luôn xem diff trước
kubectl apply -k deploy/overlays/prod
kubectl -n taskapp-prod rollout status deployment/taskapp --timeout=600s
```

Bật monitoring cho một overlay — thêm vào `kustomization.yaml` của overlay đó:

```yaml
components:
  - ../../components/monitoring
```

Dùng DB managed (RDS / Cloud SQL) thay Postgres in-cluster:

```yaml
components:
  - ../../components/external-database
```

---

## 5. Secrets

| Môi trường | Nguồn | File |
|---|---|---|
| dev / staging | `secretGenerator` đọc `secrets.env` (đã gitignore) | copy từ `secrets.env.example` |
| prod | External Secrets Operator kéo từ Vault | `cluster/external-secret-example.yaml` |

Overlay prod **cố tình không có `secretGenerator`**. Credential prod không nằm trong
git dưới bất kỳ dạng nào. Secret tên `db-credentials` phải tồn tại sẵn trong namespace
trước khi apply, nếu không pod sẽ đứng ở `CreateContainerConfigError`.

---

## 6. Phải sửa trước khi dùng thật

Đây là placeholder có chủ ý, không phải lỗi:

- `ghcr.io/example/k8s-practice` → registry thật.
- Digest `sha256:0000...` trong `overlays/prod` → CI ghi đè mỗi lần release.
- `tasks.example.com`, `tasks-staging.example.com` → domain thật.
- `platform@example.com` trong ClusterIssuer.
- `storageClassName: ssd-retain` trong `overlays/prod` → StorageClass thật có
  `reclaimPolicy: Retain`. Mặc định là `Delete`, tức **xoá PVC là xoá luôn volume DB**.
- CIDR `10.20.0.0/24` trong `components/external-database`.
- `release: kube-prometheus-stack` phải khớp `serviceMonitorSelector` của Prometheus.

---

## 7. Giới hạn đã biết

**Postgres vẫn là single-instance.** StatefulSet + PDB + backup đã tốt hơn
`Deployment` rất nhiều, nhưng mất node là mất DB cho tới khi volume re-attach —
đó là *high durability*, không phải *high availability*.

Production thật cần một trong hai:

1. **Managed database** (RDS Multi-AZ, Cloud SQL HA) — dùng
   `components/external-database`. Đây là lựa chọn nên chọn trong đa số trường hợp.
2. **CloudNativePG operator** nếu bắt buộc chạy trong cluster. Operator lo streaming
   replication, tự failover, backup WAL liên tục lên S3, PITR. Tự dựng Postgres HA
   bằng YAML thuần là việc không nên làm.

Ngoài phạm vi bộ này: service mesh / mTLS nội bộ, distributed tracing
(OpenTelemetry), và GitOps (ArgoCD/Flux) thay cho `kubectl apply` trong CI.

---

## 8. Runbook

```bash
NS=taskapp-prod

# Vì sao pod không lên?
kubectl -n $NS describe pod -l app.kubernetes.io/name=taskapp | tail -40
kubectl -n $NS logs -l app.kubernetes.io/name=taskapp --tail=200 --prefix

# Rollback
kubectl -n $NS rollout undo deployment/taskapp
kubectl -n $NS rollout history deployment/taskapp

# NetworkPolicy có chặn nhầm không?
kubectl -n $NS run netcheck --rm -it --image=nicolaka/netshoot --restart=Never -- \
  nc -zv postgres 5432

# Backup đêm qua có chạy không?
kubectl -n $NS get cronjob postgres-backup
kubectl -n $NS logs job/$(kubectl -n $NS get job \
  -l app.kubernetes.io/name=postgres-backup \
  --sort-by=.metadata.creationTimestamp -o name | tail -1 | cut -d/ -f2)

# Restore
kubectl -n $NS exec -it postgres-0 -- \
  pg_restore -U "$POSTGRES_USER" -d taskdb --clean --if-exists /backups/<file>.dump
```
