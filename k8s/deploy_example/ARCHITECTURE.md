# Sơ đồ hệ thống

Sáu góc nhìn vào cùng một hệ thống. Sơ đồ render được trên GitHub, IDE và mọi trình
xem Markdown hỗ trợ Mermaid.

- Cái gì đã dựng → [`../../README_ARC.md`](README.md)
- Vì sao dựng vậy → [`DECISIONS.md`](DECISIONS.md)
- Lệnh vận hành → [`OPERATIONS.md`](OPERATIONS.md)

---

## 1. Đường đi của một request

```mermaid
flowchart TB
    client["Người dùng<br/>HTTPS :443"]

    subgraph nsing["namespace: ingress-nginx"]
        ing["ingress-nginx<br/>kết thúc TLS · limit 50 rps"]
    end

    subgraph nsmon["namespace: monitoring"]
        prom["Prometheus"]
    end

    subgraph nsapp["namespace: taskapp-prod — PodSecurity: restricted"]
        svc["Service taskapp<br/>ClusterIP :8080 :9090"]
        app["Pod taskapp × 4<br/>:8080 traffic · :9090 actuator"]
        svcpg["Service postgres<br/>ClusterIP :5432"]
        pg[("StatefulSet postgres-0")]
        pvc[("PVC data-postgres-0<br/>100Gi · Retain")]
        cron["CronJob postgres-backup<br/>02:15 hằng ngày"]
        bkp[("PVC postgres-backups<br/>20Gi")]
    end

    client -->|"tasks.example.com"| ing
    ing -->|":8080"| svc
    svc --> app
    app -->|":5432"| svcpg
    svcpg --> pg
    pg --- pvc
    cron -->|"pg_dump"| svcpg
    cron -->|"dump + verify"| bkp
    prom -.->|"scrape :9090"| app
```

**Điểm cần thấy:** không có đường nào từ ngoài vào thẳng `:9090`. Actuator chỉ tới
được từ namespace `monitoring`, và `/actuator/health` qua Ingress trả `404`.
Cổng `:5432` không có đường nào ra khỏi namespace.

---

## 2. Bốn lớp phòng thủ

Mỗi lớp giả định lớp trước đã thủng.

```mermaid
flowchart TB
    L1["**1 · Pod Security Admission** — nhãn trên namespace<br/>Từ chối ngay lúc apply: pod chạy root, privileged, hostPath, thiếu seccomp"]
    L2["**2 · NetworkPolicy** — default-deny cả ingress lẫn egress<br/>Không policy nào cho phép = không đi được. Không có luật deny."]
    L3["**3 · securityContext** — trên từng container<br/>runAsNonRoot · readOnlyRootFilesystem · drop ALL capabilities"]
    L4["**4 · Quyền trong PostgreSQL**<br/>App đăng nhập bằng role taskapp, không phải superuser. Sở hữu đúng schema app."]

    L1 --> L2 --> L3 --> L4

    A1["Manifest sai vẫn không chạy được"] -.- L1
    A2["Pod bị chiếm vẫn không mở được socket tới DB"] -.- L2
    A3["RCE trong container vẫn không ghi được rootfs"] -.- L3
    A4["SQL injection vẫn không đọc được database khác"] -.- L4
```

---

## 3. Ai được nói chuyện với ai

`default-deny-all` chặn tất cả trước, rồi mở đúng 5 đường:

| Từ | Tới | Cổng | Vì sao |
|---|---|---|---|
| `ingress-nginx` (namespace) | pod `taskapp` | 8080 | Traffic công khai |
| `monitoring` (namespace) | pod `taskapp` | 9090 | Prometheus scrape |
| pod `taskapp` | pod `postgres` | 5432 | Truy vấn dữ liệu |
| pod `postgres-backup` | pod `postgres` | 5432 | `pg_dump` hằng đêm |
| **mọi pod** | `kube-dns` | 53 | Không có thì không pod nào phân giải được tên |

Mọi thứ khác — pod này sang pod khác, egress ra Internet, namespace khác gọi vào —
đều bị chặn.

```mermaid
flowchart LR
    rogue["Pod bất kỳ<br/>cùng namespace"]
    app["pod taskapp"]
    pg["pod postgres"]
    dns["kube-dns"]

    rogue -.->|"❌ :8080 CHẶN"| app
    rogue -.->|"❌ :5432 CHẶN"| pg
    rogue -->|"✅ :53 cho qua"| dns
```

> Đã kiểm chứng thật trên Calico: pod lạ phân giải được DNS, nhưng `nc` tới
> `postgres:5432` và `taskapp:8080` đều thất bại. **CNI mặc định của minikube không
> thực thi NetworkPolicy** — policy vẫn apply thành công và không chặn gì cả.

---

## 4. Khởi động một pod

```mermaid
sequenceDiagram
    autonumber
    participant K as kubelet
    participant I as initContainer<br/>wait-for-db
    participant A as container app
    participant P as PostgreSQL

    K->>I: khởi chạy
    loop mỗi 2 giây
        I->>P: pg_isready
        P--xI: chưa sẵn sàng
    end
    P-->>I: accepting connections
    I-->>K: exit 0
    Note over I,A: Không CrashLoopBackOff:<br/>app chưa từng khởi động và fail

    K->>A: khởi chạy JVM
    A->>P: Flyway migrate (schema app)
    P-->>A: applied 1 migration
    A->>A: Hibernate validate schema
    A->>A: Tomcat :8080 + :9090

    loop startupProbe · tối đa 150s
        K->>A: GET :9090/health/liveness
    end
    Note over K,A: startupProbe đạt →<br/>readiness + liveness mới bắt đầu

    K->>A: GET :9090/health/readiness
    A-->>K: UP (readinessState + db)
    Note over K: Pod vào Service<br/>sau minReadySeconds 15s
```

---

## 5. Tắt một pod — vì sao cần `preStop`

Hai việc chạy **song song, không ai chờ ai**. Đó là gốc của lỗi 502 khi deploy.

```mermaid
sequenceDiagram
    participant C as Ingress
    participant E as EndpointSlice<br/>controller
    participant K as kubelet
    participant A as container app

    Note over E,K: t=0 · Pod bị xoá — hai nhánh chạy song song

    par Nhánh A — kubelet
        K->>A: preStop.sleep 5s
        Note over A: vẫn phục vụ bình thường,<br/>chưa nhận SIGTERM
    and Nhánh B — mạng
        E->>E: gỡ pod khỏi EndpointSlice
        E->>C: cập nhật (eventually consistent, 1–3s)
    end

    Note over C: t≈3s · Ingress ngừng gửi request tới pod này

    K->>A: t=5s · SIGTERM
    A->>A: graceful shutdown ≤30s<br/>xử nốt request đang dở
    A-->>K: thoát sạch

    Note over K,A: terminationGracePeriodSeconds 60<br/>= 5 (preStop) + 30 (graceful) + 25 dự phòng
```

**Không có `preStop`:** nhánh A chạy trước nhánh B — app đóng listener trong khi
Ingress vẫn đang gửi request tới IP đó → client nhận RST.

> Đã đo thật: rolling restart với cấu hình này cho **799 request, 0 lỗi**.

---

## 6. Kustomize: một base, ba môi trường

```mermaid
flowchart TB
    base["**base/**<br/>toàn bộ CẤU TRÚC<br/>Deployment · StatefulSet · NetworkPolicy<br/>HPA · PDB · Ingress · Quota · CronJob"]

    dev["**overlays/dev**<br/>1 replica · 768Mi<br/>image tag dev · HTTP thuần"]
    stg["**overlays/staging**<br/>2 replica · 1Gi<br/>image tag main · cert staging"]
    prod["**overlays/prod**<br/>4 replica · 1536Mi<br/>image pin @sha256 · spread theo zone"]

    mon["components/monitoring<br/>ServiceMonitor + PrometheusRule"]
    extdb["components/external-database<br/>gỡ Postgres in-cluster → RDS"]

    base --> dev
    base --> stg
    base --> prod
    mon -.->|"thêm tuỳ chọn"| prod
    extdb -.->|"thêm tuỳ chọn"| prod
```

**Quy tắc.** Overlay chỉ đổi **con số và tên miền**. Thêm/bớt *loại* cơ chế thì phải
là **component**, không phải patch — nếu không, staging thôi không còn chứng minh
được điều gì về prod.

---

## 7. Từ commit tới production

```mermaid
flowchart LR
    pr["Pull request"] --> test["mvn verify"]
    test --> render["Render cả 3 overlay<br/>kubeval + kube-score"]
    render --> gate1{"Prod có<br/>pin digest?"}
    gate1 -->|"không"| fail["❌ fail build"]
    gate1 -->|"có"| build["Build image<br/>multi-stage · layered"]
    build --> scan["Trivy scan<br/>CRITICAL/HIGH = fail"]
    scan --> sign["cosign sign<br/>keyless"]
    sign --> stg["Deploy staging<br/>tự động khi merge main"]
    stg --> tag{"Có tag v*?"}
    tag -->|"có"| approve["Chờ người duyệt"]
    approve --> prod["Deploy prod<br/>pin theo digest"]
    prod --> verify["rollout status<br/>fail → tự rollback"]
```

**Vì sao prod pin digest.** Tag là con trỏ, có thể bị đẩy đè — `:1.0.0` hôm nay và
mai có thể là hai binary khác nhau. Digest là hash nội dung, bất biến. Đó là điều
kiện để `rollout undo` quay về **đúng** những bytes đã từng chạy.
