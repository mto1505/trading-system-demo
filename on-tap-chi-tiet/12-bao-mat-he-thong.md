# Chương 12 — Bảo Mật Hệ Thống

> **Câu hỏi trung tâm:** repo này bảo vệ được gì, **không** bảo vệ được gì, và đâu là khoảng cách giữa cấu hình demo và cấu hình production cho một hệ thống tiền thật?
>
> **Nguyên tắc nền:** tài liệu này **nói thẳng** những chỗ chưa đủ. Bảo mật nửa vời mà tưởng là đủ còn nguy hiểm hơn không có.
>
> **Tài liệu đối chiếu:** `security/`, `k8s/base/*networkpolicy*`, `DESIGN-NOTES.md` mục 15, `DEEP-DIVE.md` mục 9.7.

---

## 1. Tổng quan: bốn lớp, bốn mức độ hoàn thiện

```
┌──────────────────────────────────────────────────────────────────┐
│ LỚP 1 — Biên ngoài (Ingress)                            🟡 Một phần│
│   TLS (cert-manager) · rate limit nginx · proxy-body-size 1m      │
│   ❌ Không WAF, không mTLS client, không IP allowlist             │
├──────────────────────────────────────────────────────────────────┤
│ LỚP 2 — Ứng dụng                                       🔴 Tối thiểu│
│   API key tĩnh (X-API-KEY) · Bean Validation · RateLimiter        │
│   ❌ Không OAuth2/JWT, không phân quyền theo account, không audit │
├──────────────────────────────────────────────────────────────────┤
│ LỚP 3 — Mạng nội bộ (NetworkPolicy)                     🟢 Tốt    │
│   default-deny + allowlist tường minh cho từng cặp                │
├──────────────────────────────────────────────────────────────────┤
│ LỚP 4 — Container runtime                               🟢 Tốt    │
│   nonroot · readonly rootfs · drop ALL caps · seccomp · no SA token│
├──────────────────────────────────────────────────────────────────┤
│ LỚP 5 — Kafka & Secret                            🟡 Có mẫu, chưa bật│
│   SASL_SSL + SCRAM-SHA-512 + ACL (mẫu trong security/)            │
│   ESO ở prod · secrets.env gitignored ở dev/staging               │
└──────────────────────────────────────────────────────────────────┘
```

🔑 **Đánh giá thẳng thắn:** hai lớp *hạ tầng* (mạng và container) được làm rất tốt, ở mức production thật. Lớp *ứng dụng* mới ở mức demo và đó là khoảng cách lớn nhất.

---

## 2. Lớp ứng dụng — API Key

```java
// order-service/src/main/java/com/trading/order/config/ApiKeyFilter.java:12-23
/**
 * Bảo vệ tối thiểu API đặt lệnh bằng API key (header X-API-KEY).
 *
 * ĐÂY LÀ SÀN, KHÔNG PHẢI TRẦN. Production tài chính cần:
 * - OAuth2/JWT (Spring Security + Authorization Server) gắn quyền theo account
 * - mTLS giữa các service (service mesh: Istio/Linkerd, hoặc Kafka SASL_SSL)
 * - Rate limiting per-account (chặn máy đặt lệnh loạn)
 * Ưu điểm của filter đơn giản: 0 phụ thuộc, ~0 latency, đủ chặn truy cập vu vơ.
 * Nhược: key tĩnh, lộ là hết; không phân quyền theo user.
 */
@Component
public class ApiKeyFilter extends OncePerRequestFilter {

    @Value("${security.api-key:demo-key}")
    private String apiKey;

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
                                    FilterChain chain) throws ServletException, IOException {
        String path = request.getRequestURI();
        // Chỉ bảo vệ API nghiệp vụ; actuator để K8s probe + Prometheus scrape
        if (path.startsWith("/api/") && !apiKey.equals(request.getHeader("X-API-KEY"))) {
            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            response.getWriter().write("{\"error\":\"invalid or missing X-API-KEY\"}");
            return;
        }
        chain.doFilter(request, response);
    }
}
```

💡 **Comment tự thừa nhận giới hạn** — đó là một phẩm chất tốt của tài liệu kỹ thuật, và cũng là cách đúng để nói về bảo mật nửa vời.

### 2.1. Phân tích từng điểm

| Điểm | Đánh giá |
|---|---|
| Chỉ bảo vệ `/api/**` | ✅ Có chủ đích — `/actuator/**` phải mở cho kubelet probe và Prometheus (cả hai không mang được header) |
| `@Value` từ Secret | ✅ Prod lấy từ ESO, không hardcode |
| Mặc định `demo-key` | ⚠️ Nếu quên đặt `ORDER_API_KEY`, hệ chạy với key công khai mà **không có cảnh báo nào** |
| So sánh bằng `.equals()` | ⚠️ Không chống **timing attack**. Nên dùng `MessageDigest.isEqual(byte[], byte[])` |
| Key tĩnh, dùng chung | 🔴 Không xoay được không downtime; lộ là mất toàn bộ |
| Không phân quyền | 🔴 Mọi key xem được lệnh của **mọi** `accountId` |
| Không audit | 🔴 Không biết key nào gọi gì |

🔴 **Lỗ hổng phân quyền cụ thể:**

```java
// order-service/controller/OrderController.java:62-66
public Page<OrderView> list(
        @RequestParam(required = false) String status,
        @RequestParam(required = false) String accountId,   // ← chỉ là BỘ LỌC
        ...
```

`accountId` ở đây là **bộ lọc**, không phải **ràng buộc quyền**. Bất kỳ ai có API key đều gọi được `GET /api/v1/orders?accountId=ACC_CUA_NGUOI_KHAC` và xem toàn bộ lệnh của tài khoản đó. Với hệ thống tài chính thật, đây là vi phạm nghiêm trọng về bảo mật dữ liệu khách hàng.

### 2.2. Lộ trình tới production

```java
// Bước 1 — So sánh constant-time (sửa ngay, 1 dòng)
import java.security.MessageDigest;
import java.nio.charset.StandardCharsets;

boolean valid = MessageDigest.isEqual(
        apiKey.getBytes(StandardCharsets.UTF_8),
        String.valueOf(request.getHeader("X-API-KEY")).getBytes(StandardCharsets.UTF_8));
```

```java
// Bước 2 — JWT mang accountId, phân quyền ở tầng service
@PostMapping
public ResponseEntity<OrderResult> place(@Valid @RequestBody OrderRequest request,
                                         @AuthenticationPrincipal Jwt jwt) {
    String accountId = jwt.getClaimAsString("account_id");
    if (!accountId.equals(request.accountId())) {
        throw new AccessDeniedException("khong duoc dat lenh cho tai khoan khac");
    }
    ...
}
```

```java
// Bước 3 — Lọc theo quyền ở endpoint đọc
@GetMapping
public Page<OrderView> list(@AuthenticationPrincipal Jwt jwt, ...) {
    String accountId = jwt.getClaimAsString("account_id");   // KHÔNG lấy từ query param
    return orderRepository.findAllByAccountId(accountId, p).map(OrderView::from);
}
```

| Bước | Công sức | Giá trị |
|---|---|---|
| Constant-time compare | 1 dòng | Chặn timing attack |
| Từ chối `demo-key` ở prod profile | 3 dòng | Chặn deploy nhầm |
| JWT + phân quyền theo account | Vừa (Spring Security + Resource Server) | 🔑 **Đóng lỗ hổng nghiêm trọng nhất** |
| Rate limit per-account | Vừa | Một khách không làm hỏng trải nghiệm của mọi khách |
| Audit log | Vừa | Yêu cầu tuân thủ bắt buộc với tài chính |

### 2.3. ⚠️ Ba service còn lại không có filter nào

Chỉ `order-service` có `ApiKeyFilter`. `market-gateway`, `pricing-risk-service`, `matching-engine` **hoàn toàn mở** ở tầng ứng dụng.

| Service | Endpoint | Rủi ro nếu lộ |
|---|---|---|
| `market-gateway` | `POST /api/v1/ticks` | 🔴 **Bơm giá giả vào hệ thống** ⇒ `refPrice` sai ⇒ lệnh khớp ở giá sai |
| `pricing-risk-service` | `GET /api/v1/marketdata` | 🟡 Lộ dữ liệu thị trường nội bộ |
| `matching-engine` | `GET /api/v1/executions/{id}` | 🟡 Lộ kết quả khớp của người khác |

🔑 **`POST /api/v1/ticks` là endpoint nguy hiểm nhất trong hệ.** Ai bơm được tick giả thì điều khiển được `refPrice`, và `refPrice` là giá mà `matching-engine` dùng để khớp lệnh (`MatchingService.java:79`). Đây là một đường tấn công thao túng giá hoàn chỉnh.

Hiện nó được bảo vệ bằng **hai lớp hạ tầng**, không phải bằng ứng dụng:
1. **Overlay prod gỡ route `/api/v1/ticks` khỏi Ingress** — `k8s/base/apps/ingress.yaml` ghi rõ *"Bơm tick thủ công - CÔNG CỤ DEV. Overlay prod gỡ path này đi."*
2. **NetworkPolicy** chỉ cho ingress-nginx tới các service.

⚠️ Nhưng trong namespace, **bất kỳ pod nào** cũng gọi được `http://market-gateway:8081/api/v1/ticks` nếu nó mang label `component` phù hợp. Phòng thủ theo chiều sâu đòi hỏi filter ở chính service, không chỉ ở Ingress.

### 2.4. CORS

```java
// Có ở CẢ BỐN controller
@CrossOrigin("*")
```

⚠️ Cho phép **bất kỳ website nào** gọi API bằng trình duyệt của người dùng. Với `*`, trình duyệt không cho gửi credential (cookie, `Authorization`), nên rủi ro thấp hơn — nhưng API key đi qua header tuỳ chỉnh `X-API-KEY` thì **vẫn gửi được**, nghĩa là một trang web độc hại có thể gọi API nếu nó biết key.

Production nên là:

```java
@CrossOrigin(origins = "${app.allowed-origins}")   // ví dụ: https://trading.example.com
```

💡 Lưu ý: `@CrossOrigin("*")` **không** giải quyết được trường hợp mở UI bằng `file://` (origin là `null`) — đó là lý do `ui/serve.js` làm proxy same-origin (xem [chương 09](./09-trien-khai-local-va-integration-testing.md) mục 4).

### 2.5. Actuator — bề mặt tấn công bị bỏ quên

```yaml
# Cả 4 application.yml
management:
  endpoints:
    web:
      exposure:
        include: health,info,prometheus,metrics
```

✅ **Liệt kê tường minh, không dùng `*`** — đây là lựa chọn đúng. Nếu mở `*`:

| Endpoint | Nguy hiểm |
|---|---|
| `/actuator/env` | 🔴 Lộ **toàn bộ** biến môi trường, kể cả `DB_PASSWORD` và `ORDER_API_KEY` |
| `/actuator/heapdump` | 🔴 Tải về toàn bộ heap ⇒ trích xuất mọi thứ trong bộ nhớ |
| `/actuator/shutdown` | 🔴 Tắt ứng dụng |
| `/actuator/configprops` | 🟡 Lộ cấu hình chi tiết |
| `/actuator/threaddump` | 🟡 Lộ cấu trúc nội bộ |
| `/actuator/loggers` | 🟡 Đổi log level (có thể dùng để gây DoS bằng log flood) |

⚠️ Nhưng ngay cả với danh sách hiện tại, `/actuator/**` **không được `ApiKeyFilter` bảo vệ** (filter chỉ chặn `/api/`). Bảo vệ duy nhất là NetworkPolicy. Trong namespace, bất kỳ pod nào cũng đọc được `/actuator/metrics` của mọi service.

💡 Tại sao vẫn chấp nhận được: `health`, `info`, `prometheus`, `metrics` không lộ bí mật. Nhưng nếu ai đó thêm `env` vào danh sách "để debug" rồi quên gỡ, thì NetworkPolicy là thứ duy nhất đứng giữa kẻ tấn công và `DB_PASSWORD`.

---

## 3. 🟢 Lớp mạng — Zero-trust NetworkPolicy

Đây là lớp được làm tốt nhất, và nó được mô tả đúng là *"biện pháp an ninh giá trị nhất của cả bộ manifest"*.

### 3.1. Nền default-deny

```yaml
# k8s/base/networkpolicy-default-deny.yaml
# Nền zero-trust: trong namespace này không gì nói chuyện được với gì, trừ khi
# có policy cho phép tường minh. Đây là biện pháp an ninh giá trị nhất của cả
# bộ manifest - một pod bị chiếm quyền không tự ý gọi ra ngoài được.
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: default-deny-all
  namespace: trading
spec:
  podSelector: {}
  policyTypes: [Ingress, Egress]
---
# DNS phải mở cho tất cả, nếu không mọi tên service đều không phân giải được.
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: allow-dns-egress
  namespace: trading
spec:
  podSelector: {}
  policyTypes: [Egress]
  egress:
    - to:
        - namespaceSelector:
            matchLabels: { kubernetes.io/metadata.name: kube-system }
          podSelector:
            matchLabels: { k8s-app: kube-dns }
      ports:
        - { protocol: UDP, port: 53 }
        - { protocol: TCP, port: 53 }
```

🔑 **`podSelector: {}` + `policyTypes: [Ingress, Egress]` không có rule = chặn hết.** NetworkPolicy là **additive**: nhiều policy cùng áp lên một pod thì kết quả là **hợp** của các rule cho phép. Nên default-deny làm nền, các policy khác mở từng cặp cụ thể.

🔑 **DNS phải mở trước tiên.** Đây là lỗi số một khi mới dùng default-deny: quên DNS ⇒ mọi tên service không phân giải được ⇒ `UnknownHostException` và người ta đi tìm nguyên nhân ở Kafka, Postgres, mọi nơi trừ DNS.

💡 Lưu ý cấu trúc YAML: `namespaceSelector` và `podSelector` nằm trong **cùng một phần tử** của mảng `to` ⇒ ngữ nghĩa là **AND** (pod kube-dns *trong* namespace kube-system). Nếu tách thành hai phần tử (hai dấu `-`), ngữ nghĩa thành **OR** — mở cho *mọi* pod trong kube-system *hoặc* mọi pod có label `k8s-app: kube-dns` ở bất kỳ namespace nào. Đây là một trong những lỗi tinh vi nhất của NetworkPolicy, và `k8s/labs/calico/30-cross-ns-AND.yaml` / `31-cross-ns-OR.yaml` tồn tại để luyện đúng điểm này.

### 3.2. Egress của nhóm app — allowlist chặt

```yaml
# k8s/base/apps/apps-networkpolicy.yaml
# Quy tắc cho 4 service Java. Chọn pod theo label `component` thay vì liệt kê
# từng tên: thêm service mới cùng component là tự động được áp dụng.
#
# LƯU Ý: probe của kubelet xuất phát từ NODE, không đi qua NetworkPolicy, nên
# liveness/readiness vẫn chạy dù không có rule ingress nào cho chúng.
spec:
  podSelector:
    matchExpressions:
      - key: app.kubernetes.io/component
        operator: In
        values: [gateway, pricing, order, matching]
  policyTypes: [Egress]
  egress:
    - to:
        - podSelector:
            matchLabels: { app.kubernetes.io/name: kafka }
      ports: [{ protocol: TCP, port: 9092 }]
    - to:
        - podSelector:
            matchLabels: { app.kubernetes.io/name: postgres }
      ports: [{ protocol: TCP, port: 5432 }]
    - to:
        - podSelector:
            matchLabels: { app.kubernetes.io/name: redis }
      ports: [{ protocol: TCP, port: 6379 }]
    - to:
        - podSelector:
            matchLabels: { app.kubernetes.io/name: schema-registry }
      ports: [{ protocol: TCP, port: 8081 }]
    - to:
        - podSelector:
            matchLabels: { app.kubernetes.io/name: zipkin }
      ports: [{ protocol: TCP, port: 9411 }]
```

🔑 **Năm đích, không có đích thứ sáu.** Cụ thể:
- ❌ **Không** ra được Internet — một pod bị chiếm quyền **không thể** tải payload về hay gửi dữ liệu ra ngoài.
- ❌ **Không** gọi được service khác trong nhóm — kiến trúc quy định 4 service chỉ nói chuyện qua Kafka, và NetworkPolicy **cưỡng chế** điều đó.

```yaml
# Traffic vào: chỉ Ingress controller (và chỉ tới order-service) + hệ monitoring.
# Không có rule nào cho service-gọi-service vì thiết kế không có: 4 service
# chỉ nói chuyện qua Kafka.
```

💡 Đây là ví dụ đẹp của **kiến trúc được cưỡng chế bằng hạ tầng, không bằng kỷ luật**. Nếu ai đó lén thêm `RestTemplate` gọi sang service khác, nó bị chặn ở tầng mạng — không phụ thuộc vào việc có ai review code hay không.

### 3.3. Ingress policy và một lỗ hổng tinh vi

```yaml
  policyTypes: [Ingress]
  ingress:
    - from:
        - namespaceSelector:
            matchLabels: { kubernetes.io/metadata.name: ingress-nginx }
      ports:
        - { protocol: TCP, port: 8081 }
        - { protocol: TCP, port: 8082 }
        - { protocol: TCP, port: 8083 }
        - { protocol: TCP, port: 8084 }
```

⚠️ Rule này mở **cả bốn cổng** cho ingress-nginx, trong khi Ingress chỉ route tới `order-service` (và `pricing-risk-service`, `matching-engine` ở một số môi trường). Nghĩa là nếu ai đó tạo được một Ingress mới trỏ tới `market-gateway:8081`, NetworkPolicy **không chặn**.

Bản Helm làm chặt hơn — mỗi `route` sinh ra một NetworkPolicy rule cho **đúng** service đó (xem [chương 11](./11-trien-khai-helm-va-ha-tang-production.md) mục 3.4). Đây là một chỗ bản Helm tốt hơn bản Kustomize.

### 3.4. Policy cho hạ tầng — nguyên tắc đặc quyền tối thiểu

```yaml
# k8s/base/infra/infra-networkpolicy.yaml
# postgres
  ingress:
    - from:
        - podSelector:
            matchExpressions:
              - key: app.kubernetes.io/component
                operator: In
                values: [pricing, order, matching]     # ← KHÔNG có gateway
      ports: [{ protocol: TCP, port: 5432 }]
  egress: []        # database không cần gọi ra ngoài (DNS đã mở ở policy chung)
```

```yaml
# redis
  ingress:
    - from:
        - podSelector:
            matchLabels: { app.kubernetes.io/component: order }   # ← CHỈ order
      ports: [{ protocol: TCP, port: 6379 }]
  egress: []
```

🔑 **Đặc quyền tối thiểu được áp dụng thật, không phải trên giấy:**

| Hạ tầng | Ai được vào | Ghi chú |
|---|---|---|
| Postgres | `pricing`, `order`, `matching` | ❌ **Không** `gateway` — nó không có database |
| Redis | **Chỉ** `order` | Chỉ order-service dùng Redis |
| Schema Registry | cả 4 | gateway produce Avro, pricing consume |
| Zipkin | cả 4 + ingress-nginx | UI xem qua Ingress |
| Kafka | cả 4 + schema-registry + `kafka-topics-init` | SR lưu schema trong topic `_schemas` |

💡 **`egress: []` trên Postgres và Redis** — chúng không cần gọi ra ngoài gì cả. Nếu một trong hai bị khai thác (ví dụ qua một lỗ hổng RCE), kẻ tấn công **không thể gửi dữ liệu ra ngoài** từ chính pod đó.

💡 Rule cho Kafka controller:

```yaml
    - from:
        - podSelector:
            matchLabels: { app.kubernetes.io/name: kafka }
      ports:
        - { protocol: TCP, port: 9092 }
        - { protocol: TCP, port: 9093 }     # controller quorum KRaft
```

Cổng 9093 chỉ mở cho **chính pod Kafka** — quorum KRaft cần nó, không ai khác được chạm.

### 3.5. Phòng lab Calico

```
k8s/labs/calico/
├── 00-namespaces.yaml  01-workloads.yaml
├── 10-default-deny.yaml  11-allow-dns.yaml
├── 20-backend-ingress.yaml
├── 21-frontend-egress-WRONG.yaml    ← cố ý SAI
├── 22-frontend-egress-ok.yaml       ← bản đúng
├── 30-cross-ns-AND.yaml             ← namespaceSelector + podSelector cùng phần tử
├── 31-cross-ns-OR.yaml              ← tách hai phần tử
├── 40-egress-internet.yaml
├── 50-globalnetworkpolicy-OPTIONAL.yaml
└── bugs/bug-1..4.yaml               ← bài tập chẩn đoán
```

💡 Cặp `21-WRONG` / `22-ok` và cặp `30-AND` / `31-OR` dạy đúng hai lỗi tinh vi nhất của NetworkPolicy. `bugs/` là bài tập chẩn đoán — một cách dạy rất hiệu quả.

---

## 4. 🟢 Lớp container — hardening nhiều tầng

```yaml
# k8s/base/apps/order-service.yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: order-service
automountServiceAccountToken: false      # ← lần 1
---
    spec:
      serviceAccountName: order-service
      automountServiceAccountToken: false # ← lần 2 (thừa có chủ đích)
      securityContext:                    # cấp Pod
        runAsNonRoot: true
        runAsUser: 10001
        runAsGroup: 10001
        fsGroup: 10001
        seccompProfile: { type: RuntimeDefault }
      containers:
        - name: app
          securityContext:                # cấp Container
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            capabilities: { drop: ["ALL"] }
          volumeMounts:
            - { name: tmp, mountPath: /tmp }
      volumes:
        - name: tmp
          emptyDir: { medium: Memory, sizeLimit: 128Mi }
```

| Thiết lập | Chặn được gì | Cấp |
|---|---|:--:|
| `runAsNonRoot: true` + UID 10001 | Container escape thành root trên node | Pod |
| `runAsGroup` / `fsGroup` | Ghi file với group đặc quyền | Pod |
| `seccompProfile: RuntimeDefault` | ~300 syscall nguy hiểm (`ptrace`, `mount`, `kexec`, …) | Pod |
| `allowPrivilegeEscalation: false` | `setuid` binary leo thang quyền | Container |
| `readOnlyRootFilesystem: true` | 🔑 Ghi webshell, sửa binary, cài persistence | Container |
| `capabilities: drop ALL` | `NET_RAW` (packet sniffing), `SYS_PTRACE`, `CHOWN`, … | Container |
| `automountServiceAccountToken: false` | 🔑 Dùng token gọi Kubernetes API | Pod + SA |

🔑 **`automountServiceAccountToken: false` là thiết lập bị bỏ quên nhiều nhất.** Mặc định Kubernetes mount token của ServiceAccount vào `/var/run/secrets/kubernetes.io/serviceaccount/token` trong **mọi** pod. Kẻ tấn công vào được pod sẽ dùng token đó gọi Kubernetes API — và nếu RBAC lỏng, liệt kê được Secret của cả namespace.

Bốn service này **không gọi Kubernetes API**, nên token chỉ là bề mặt tấn công thuần tuý. Đặt ở **cả hai nơi** (ServiceAccount và Pod spec) là thừa có chủ đích, phòng trường hợp một patch sau này ghi đè một trong hai.

🔑 **`readOnlyRootFilesystem: true` buộc phải mount `/tmp`** — JVM cần ghi hsperfdata và temp file. `emptyDir: { medium: Memory }` làm nó thành tmpfs (RAM, không chạm đĩa), `sizeLimit: 128Mi` để không ăn hết bộ nhớ node.

💡 **initContainer cũng được hardening riêng:**

```yaml
- name: wait-for-db
  image: postgres:16.4-alpine
  securityContext:
    allowPrivilegeEscalation: false
    readOnlyRootFilesystem: true
    runAsNonRoot: true
    runAsUser: 999                  # UID của user postgres trong image này
    capabilities: { drop: ["ALL"] }
```

UID 999 thay vì 10001 vì đó là UID của user `postgres` trong image `postgres:16.4-alpine`. Chi tiết nhỏ nhưng cho thấy hardening được làm cẩn thận chứ không copy-paste.

### 4.1. Chặn ở tầng namespace

```yaml
# k8s/base/resourcequota.yaml
    services.nodeports: "0"      # không expose NodePort trong namespace này
    services.loadbalancers: "1"
```

💡 NodePort mở một cổng trên **mọi node** của cluster, bỏ qua Ingress và bỏ qua mọi chính sách ở đó. Chặn hẳn bằng quota là cách triệt để — không ai "vô tình" tạo được.

### 4.2. Ingress hardening

```yaml
# k8s/base/apps/ingress.yaml
# KHÔNG dùng annotation `configuration-snippet`: ingress-nginx >= 1.9 chặn mặc
# định vì nó cho phép bất kỳ ai tạo được Ingress chèn config nginx tuỳ ý.
# Security header do ứng dụng tự đặt.
  annotations:
    nginx.ingress.kubernetes.io/proxy-body-size: "1m"
    nginx.ingress.kubernetes.io/proxy-connect-timeout: "5"
    nginx.ingress.kubernetes.io/proxy-read-timeout: "30"
    nginx.ingress.kubernetes.io/proxy-send-timeout: "30"
```

🔑 **Ghi chú về `configuration-snippet` là một hiểu biết bảo mật thật.** Annotation đó cho phép chèn config nginx tuỳ ý; trong một cluster đa người dùng, ai tạo được Ingress thì chèn được directive nginx — bao gồm cả việc đọc file từ container nginx hoặc proxy tới nội bộ. ingress-nginx ≥ 1.9 chặn mặc định (CVE-2021-25742 và các CVE liên quan). Repo chọn đặt security header ở ứng dụng thay vì mượn annotation.

```yaml
# helm values-prod.yaml
  annotations:
    nginx.ingress.kubernetes.io/ssl-redirect: "true"
    nginx.ingress.kubernetes.io/limit-rps: "20"
    nginx.ingress.kubernetes.io/limit-burst-multiplier: "2"
```

💡 `limit-rps: 20` ở Ingress là **lớp thứ hai** của rate limiting, bổ sung cho RateLimiter 200/s/pod trong ứng dụng. Lớp Ingress là trần **toàn cụm** (giải quyết hạn chế per-pod của Resilience4j), lớp ứng dụng bảo vệ pod cụ thể.

⚠️ `proxy-body-size: 1m` — hợp lý cho đơn lệnh, nhưng `POST /api/v1/ticks/batch` nhận `List<TickEvent>` có thể cần lớn hơn. Và `1m` vẫn đủ để gửi ~10.000 tick trong một request.

---

## 5. 🟡 Kafka — có mẫu, chưa bật

### 5.1. Ba lớp, ba câu hỏi khác nhau

```yaml
# security/application-secure-example.yml
# Kích hoạt: SPRING_PROFILES_ACTIVE=secure
# Đặt file này thành application-secure.yml trong từng service.
spring:
  kafka:
    properties:
      security.protocol: SASL_SSL
      sasl.mechanism: SCRAM-SHA-512
      # Mật khẩu KHÔNG viết thẳng vào file - nạp từ K8s Secret qua biến môi trường
      sasl.jaas.config: >
        org.apache.kafka.common.security.scram.ScramLoginModule required
        username="${KAFKA_USER}" password="${KAFKA_PASSWORD}";
      ssl.truststore.location: ${KAFKA_TRUSTSTORE:/etc/kafka/secrets/truststore.jks}
      ssl.truststore.password: ${KAFKA_TRUSTSTORE_PASSWORD}
      ssl.endpoint.identification.algorithm: https   # chống man-in-the-middle

# Schema Registry cũng phải bảo vệ - nếu không, ai cũng đăng ký được schema bậy
schema:
  registry:
    url: https://schema-registry:8081
```

🔑 **Ba lớp trả lời ba câu hỏi khác nhau** — đây là cách phân tích đúng:

| Lớp | Câu hỏi | Cơ chế |
|---|---|---|
| **Authentication** | *Bạn là ai?* | SASL/SCRAM-SHA-512 |
| **Encryption** | *Ai nghe lén được?* | TLS (`SASL_SSL`) |
| **Authorization** | *Bạn được làm gì?* | ACL (mục 5.2) |

Thiếu bất kỳ lớp nào đều để lại lỗ hổng nghiêm trọng:
- Có auth + authz nhưng **không** TLS ⇒ mật khẩu SCRAM và toàn bộ lệnh đi qua mạng dưới dạng rõ.
- Có TLS + auth nhưng **không** ACL ⇒ bất kỳ service nào xác thực được cũng đọc/ghi **mọi** topic.
- Có TLS + authz nhưng **không** auth ⇒ ACL không biết áp cho ai.

🔑 **`ssl.endpoint.identification.algorithm: https`** là dòng dễ bỏ sót nhất. Không có nó, client **không kiểm tra hostname** trong chứng chỉ của broker — nghĩa là một kẻ tấn công có **bất kỳ** chứng chỉ hợp lệ nào (kể cả cho domain khác) cũng mạo danh được broker. TLS mà không verify hostname chỉ chống nghe lén thụ động, **không** chống man-in-the-middle.

💡 **Vì sao SCRAM-SHA-512 chứ không PLAIN:** PLAIN gửi mật khẩu dưới dạng rõ (dựa hoàn toàn vào TLS để bảo vệ) và broker phải lưu mật khẩu để so sánh. SCRAM dùng challenge-response — mật khẩu **không bao giờ đi qua mạng**, và broker lưu salted hash. Kafka hỗ trợ SCRAM credential trong chính Kafka (KRaft) hoặc Zookeeper, xoay được không cần restart broker.

⚠️ `ssl.truststore.password` trong biến môi trường là chấp nhận được, nhưng JKS truststore phải được mount từ Secret — không được build vào image.

### 5.2. ACL đặc quyền tối thiểu

```bash
# security/create-acls.sh
#!/usr/bin/env bash
# Đặc quyền tối thiểu: mỗi service chỉ được đúng thứ nó cần.
set -euo pipefail
BS=${BOOTSTRAP:-kafka:9092}
CMD="kafka-acls.sh --bootstrap-server $BS --command-config /etc/kafka/admin.properties"

# --- market-gateway: chỉ ghi tick ---
$CMD --add --allow-principal User:market-gateway --operation Write --operation Describe --topic stock.ticks

# --- pricing-risk-service ---
$CMD --add --allow-principal User:pricing-risk-service --operation Read --operation Describe --topic stock.ticks
$CMD --add --allow-principal User:pricing-risk-service --operation Read --topic stock.ticks --group pricing-risk-group
$CMD --add --allow-principal User:pricing-risk-service --operation Write --operation Describe --topic stock.enriched
$CMD --add --allow-principal User:pricing-risk-service --operation Write --operation Describe --topic stock.ticks.DLT

# --- order-service ---
$CMD --add --allow-principal User:order-service --operation Read --operation Describe --topic stock.enriched
$CMD --add --allow-principal User:order-service --operation Read --topic stock.enriched --group order-service-group
$CMD --add --allow-principal User:order-service --operation Read --operation Describe --topic stock.executions
$CMD --add --allow-principal User:order-service --operation Read --topic stock.executions --group order-service-exec-group
$CMD --add --allow-principal User:order-service --operation Write --operation Describe --topic stock.orders

# --- matching-engine ---
$CMD --add --allow-principal User:matching-engine --operation Read --operation Describe --topic stock.orders
$CMD --add --allow-principal User:matching-engine --operation Read --topic stock.orders --group matching-engine-group
$CMD --add --allow-principal User:matching-engine --operation Write --operation Describe --topic stock.executions

echo "--- ACL hien tai ---"
$CMD --list
```

**Ma trận quyền:**

| Principal | `stock.ticks` | `stock.enriched` | `stock.orders` | `stock.executions` | `*.DLT` |
|---|:--:|:--:|:--:|:--:|:--:|
| `market-gateway` | **W** | — | — | — | — |
| `pricing-risk-service` | **R** | **W** | — | — | **W** (ticks.DLT) |
| `order-service` | — | **R** | **W** | **R** | — |
| `matching-engine` | — | — | **R** | **W** | — |

🔑 **Giá trị thực của ma trận này:** nếu `market-gateway` bị chiếm quyền, kẻ tấn công **chỉ** ghi được vào `stock.ticks`. Nó **không thể** đọc lệnh của khách (`stock.orders`), không thể giả mạo execution report (`stock.executions`), không thể đọc kết quả khớp. Thiệt hại bị giới hạn ở một topic.

🔑 **Chú ý ACL trên `--group`** — dòng dễ bỏ sót nhất:

```bash
$CMD --add --allow-principal User:pricing-risk-service --operation Read --topic stock.ticks --group pricing-risk-group
```

Kafka yêu cầu quyền `Read` trên **cả topic lẫn consumer group**. Chỉ cấp trên topic thì consumer vẫn fail với `GroupAuthorizationException`. Và việc cấp quyền trên group cụ thể (không phải `--group '*'`) ngăn một service join nhầm group của service khác — điều sẽ gây **cướp partition** và làm service kia ngừng nhận message.

⚠️ Và ACL này phải khớp chính xác với `GROUP_ID_CONFIG` trong mã:

| Group ID trong mã | Khai ở |
|---|---|
| `pricing-risk-group` | `pricing/config/KafkaConfig.java:40` |
| `order-service-group` | `order/config/KafkaConfig.java:31` |
| `order-service-exec-group` | `order/config/KafkaConfig.java:82` |
| `matching-engine-group` | `matching/config/KafkaConfig.java:34` |

Lại là một **ràng buộc giữa hai nơi không ai cưỡng chế** — đổi group ID trong Java mà quên sửa ACL ⇒ service không consume được, và lỗi `GroupAuthorizationException` không nói rõ nguyên nhân.

### 5.3. ⚠️ Khoảng cách: mẫu có, chưa được bật

| | Hiện tại | Cần làm |
|---|---|---|
| Protocol | `PLAINTEXT` ở mọi môi trường | `SASL_SSL` |
| Xác thực | Không | SCRAM-SHA-512 |
| ACL | Script có sẵn, **chưa chạy** | Chạy + đưa vào quy trình deploy |
| Schema Registry | HTTP | HTTPS + basic auth / mTLS |
| Truststore | Không có | Mount từ Secret |

🔴 **Hệ quả hiện tại:** bất kỳ pod nào có đường tới Kafka (tức 4 service + schema-registry + Job) đều đọc/ghi được **mọi** topic. Nếu một service bị chiếm quyền, kẻ tấn công:
- Đọc toàn bộ lệnh của khách hàng từ `stock.orders`.
- **Giả mạo execution report** vào `stock.executions` ⇒ đánh dấu lệnh là FILLED ở giá bất kỳ.
- Bơm lệnh giả vào `stock.orders` — tuy `matching-engine` vẫn kiểm `TradingRules`, nhưng lệnh trong biên độ sẽ được khớp.

Phòng thủ duy nhất hiện tại là **NetworkPolicy** — lớp 3. Đó là lý do lớp đó quan trọng đến vậy trong kiến trúc này.

### 5.4. ⚠️ Schema Registry — `auto.register.schemas`

```java
// market-gateway/config/KafkaProducerConfig.java:48-50
// demo: tự đăng ký schema; PRODUCTION nên false - schema đăng ký qua CI/CD
// có review, producer chỉ được dùng schema đã duyệt
props.put("auto.register.schemas", true);
```

Với `true` và Registry không xác thực, **bất kỳ ai** cũng đăng ký được schema mới. Với `compatibility: NONE` (nếu ai đó đặt), một schema phá vỡ sẽ được chấp nhận và làm chết toàn bộ consumer.

Quy trình đúng cho production:
1. `.avsc` trong Git, thay đổi qua pull request.
2. CI chạy `mvn schema-registry:test-compatibility` trước khi merge.
3. CI đăng ký schema sau khi merge.
4. Producer: `auto.register.schemas=false`, `use.latest.version=true`.
5. Registry: HTTPS + xác thực, `compatibility` ít nhất `BACKWARD`.

---

## 6. 🟡 Quản lý Secret

| Môi trường | Cơ chế | Đánh giá |
|---|---|---|
| Local | Hardcode trong `docker-compose.yml` (`trading123`) | 🟡 Chấp nhận được cho local, nhưng là mật khẩu công khai trong Git |
| dev / staging | `secretGenerator` từ `secrets.env` (gitignored) | ✅ Không nằm trong Git; thiếu thì build **FAIL** |
| **prod** | **External Secrets Operator** từ AWS Secrets Manager | ✅ Đúng chuẩn |

```yaml
# k8s/overlays/prod/kustomization.yaml
# CỐ Ý không có secretGenerator. Mật khẩu production không nằm trong git, kể cả
# dạng mã hoá mà ai đó giải được ở máy cá nhân. Dùng External Secrets Operator
# (externalsecret.yaml) hoặc Sealed Secrets.
```

🔑 *"kể cả dạng mã hoá mà ai đó giải được ở máy cá nhân"* — đây là lý lẽ đúng để **không** dùng SOPS/git-crypt cho production: khoá giải mã phải nằm ở đâu đó, và nếu nó nằm trên máy dev thì secret về bản chất đã ở trên máy dev.

🔑 *"thiếu thì build FAIL ngay, tốt hơn là deploy âm thầm với một mật khẩu placeholder"* — nguyên tắc rất đáng áp dụng rộng. Phương án tệ hơn là có giá trị mặc định: khi đó môi trường deploy thành công với `changeme` và không ai biết.

⚠️ **Hai vấn đề đã biết về secret:**

- **Gap #19** — Secret do ESO dựng không có hash ⇒ cập nhật xong không pod nào restart ⇒ ứng dụng dùng mật khẩu cũ tới lần restart ngẫu nhiên sau đó ([chương 11](./11-trien-khai-helm-va-ha-tang-production.md) mục 5.3).
- **Bug #18** — `POSTGRES_PASSWORD` chỉ tác dụng lúc `initdb`; xoay mật khẩu ở dev/staging làm sập cả môi trường vì DB giữ mật khẩu cũ ([chương 10](./10-trien-khai-kubernetes-kustomize-va-keda.md) mục 7.3).

💡 Và một điểm ít được nhắc: Kubernetes Secret mặc định chỉ được **base64-encode, không mã hoá** trong etcd. Production cần bật `EncryptionConfiguration` ở API server (encryption at rest), nếu không ai đọc được etcd là đọc được mọi secret.

---

## 7. Mô hình đe doạ — ai tấn công được gì

| Kẻ tấn công | Có gì | Làm được gì hiện tại | Chặn bởi |
|---|---|---|---|
| **Internet, không có API key** | — | Gần như không gì | TLS, API key, Ingress |
| **Internet, có API key rò rỉ** | Key | 🔴 Đặt lệnh cho **bất kỳ** account; xem lệnh của **mọi** account | Chỉ rate limit |
| **Pod bị chiếm trong namespace** | Network access | 🔴 Đọc/ghi **mọi** topic Kafka; đọc `/actuator/*` của mọi service; bơm tick giả | NetworkPolicy (một phần) |
| **Pod ngoài namespace** | — | Không gì | NetworkPolicy |
| **Người có `kubectl` trong namespace** | RBAC | 🔴 Đọc mọi Secret | RBAC cluster (ngoài phạm vi repo) |
| **Người có quyền tạo Ingress** | — | 🟡 Route tới bất kỳ service nào trên 4 cổng | NetworkPolicy (lỏng, mục 3.3) |
| **Insider có Git access** | Repo | 🟡 Thấy mật khẩu local; **không** thấy mật khẩu prod | `.gitignore` + ESO |

🔑 **Hai lỗ hổng nghiêm trọng nhất:**
1. **API key rò rỉ ⇒ toàn quyền trên mọi account** (không có phân quyền).
2. **Pod bị chiếm ⇒ toàn quyền trên Kafka** (không có SASL/ACL).

Cả hai đều có lời giải đã biết và có mẫu sẵn trong repo — chỉ chưa được bật.

---

## 8. Lộ trình ưu tiên

| Ưu tiên | Việc | Công sức | Giảm rủi ro |
|:--:|---|---|---|
| 🔴 **1** | **Bật SASL_SSL + SCRAM + ACL cho Kafka** (mẫu đã có ở `security/`) | Trung bình | Rất cao — đóng lỗ hổng #2 |
| 🔴 **2** | **JWT + phân quyền theo `accountId`** thay API key dùng chung | Trung bình | Rất cao — đóng lỗ hổng #1 |
| 🔴 **3** | **Thêm filter cho `market-gateway`** (hoặc tách `/api/v1/ticks` sang profile dev) | Thấp | Cao — chặn thao túng giá |
| 🟠 4 | `auto.register.schemas=false` + đăng ký schema qua CI | Thấp | Trung bình |
| 🟠 5 | Constant-time compare cho API key | 1 dòng | Thấp–TB |
| 🟠 6 | Từ chối `demo-key` khi profile là prod | 3 dòng | Trung bình |
| 🟠 7 | Giới hạn `@CrossOrigin` theo origin cụ thể | Thấp | Trung bình |
| 🟠 8 | Siết Ingress NetworkPolicy theo từng service (như bản Helm) | Thấp | Trung bình |
| 🟡 9 | Audit log cho mọi thao tác đặt lệnh | Trung bình | Tuân thủ |
| 🟡 10 | Rate limit per-account | Trung bình | Trung bình |
| 🟡 11 | mTLS giữa các service (Istio/Linkerd) | Cao | Trung bình (NetworkPolicy đã phủ phần lớn) |
| 🟡 12 | Bật `EncryptionConfiguration` cho etcd | Trung bình | Trung bình |
| 🟡 13 | OPA/Kyverno cưỡng chế chính sách pod | Trung bình | Phòng ngừa |
| 🟡 14 | Quét image (Trivy/Grype) trong CI | Thấp | Trung bình |

---

## 9. Cách kiểm chứng

```bash
URL='http://localhost:8083/api/v1/orders'
BODY='{"clientOrderId":"SEC-1","symbol":"HPG","side":"BUY","price":28500,"quantity":100,"accountId":"ACC001"}'

# === Lớp ứng dụng ===
# Thiếu key -> 401
curl -s -o /dev/null -w '%{http_code}\n' -X POST $URL -H 'Content-Type: application/json' -d "$BODY"

# Sai key -> 401
curl -s -o /dev/null -w '%{http_code}\n' -X POST $URL \
  -H 'Content-Type: application/json' -H 'X-API-KEY: sai' -d "$BODY"

# Đúng key -> 201/422
curl -s -o /dev/null -w '%{http_code}\n' -X POST $URL \
  -H 'Content-Type: application/json' -H 'X-API-KEY: demo-key' -d "$BODY"

# ⚠️ Actuator KHÔNG bị chặn
curl -s -o /dev/null -w '%{http_code}\n' http://localhost:8083/actuator/health     # -> 200
curl -s -o /dev/null -w '%{http_code}\n' http://localhost:8083/actuator/prometheus # -> 200

# ✅ Endpoint nguy hiểm KHÔNG được expose
for e in env heapdump shutdown configprops threaddump loggers beans; do
  echo -n "$e: "; curl -s -o /dev/null -w '%{http_code}\n' http://localhost:8083/actuator/$e
done
# -> 404 cho tất cả

# 🔴 Lỗ hổng phân quyền: xem lệnh của account khác
curl -s "$URL?accountId=ACC999" -H 'X-API-KEY: demo-key' | jq '.totalElements'

# 🔴 Ba service kia KHÔNG có filter
curl -s -o /dev/null -w '%{http_code}\n' http://localhost:8082/api/v1/marketdata     # -> 200
curl -s -o /dev/null -w '%{http_code}\n' http://localhost:8084/api/v1/executions/x   # -> 200

# 🔴 Bơm tick giả — đường thao túng giá
curl -s -o /dev/null -w '%{http_code}\n' -X POST http://localhost:8081/api/v1/ticks \
  -H 'Content-Type: application/json' \
  -d "{\"symbol\":\"HPG\",\"price\":999999,\"volume\":1,\"eventTime\":$(date +%s%3N)}"
sleep 2
curl -s http://localhost:8082/api/v1/marketdata/HPG | jq '.lastPrice'
#  -> 999999 (!!)  — refPrice của lệnh kế tiếp sẽ dùng con số này

# === Lớp mạng (cluster) ===
kubectl -n trading get networkpolicy
kubectl -n trading describe networkpolicy default-deny-all

# Pod không có label hợp lệ -> bị chặn
kubectl -n trading run attacker --rm -it --image=nicolaka/netshoot --restart=Never -- \
  sh -c 'wget -T 3 -O- http://order-service:8083/actuator/health; echo "exit=$?"'
#  -> timeout

# Pod có label component -> được phép
kubectl -n trading run probe --rm -it --restart=Never \
  --labels='app.kubernetes.io/component=order' --image=nicolaka/netshoot -- \
  sh -c 'wget -T 3 -O- http://kafka:9092; echo "exit=$?"'

# Chứng minh KHÔNG ra được Internet
kubectl -n trading exec deploy/order-service -- \
  sh -c 'timeout 5 wget -O- https://example.com 2>&1 | head -3; echo "exit=$?"'
#  -> timeout

# Chứng minh KHÔNG gọi được service khác
kubectl -n trading exec deploy/order-service -- \
  sh -c 'timeout 5 wget -O- http://pricing-risk-service:8082/api/v1/marketdata 2>&1 | head -3'
#  -> timeout (kiến trúc được cưỡng chế bằng hạ tầng)

# === Lớp container ===
kubectl -n trading exec deploy/order-service -- id
#  -> uid=10001 gid=10001

kubectl -n trading exec deploy/order-service -- sh -c 'touch /test 2>&1'
#  -> Read-only file system

kubectl -n trading exec deploy/order-service -- \
  sh -c 'ls /var/run/secrets/kubernetes.io/ 2>&1'
#  -> No such file or directory  (token không được mount)

kubectl -n trading exec deploy/order-service -- sh -c 'cat /proc/1/status | grep -i cap'
#  -> CapEff: 0000000000000000

kubectl -n trading get pod -l app.kubernetes.io/name=order-service \
  -o jsonpath='{.items[0].spec.securityContext}' | jq
kubectl -n trading get pod -l app.kubernetes.io/name=order-service \
  -o jsonpath='{.items[0].spec.containers[0].securityContext}' | jq

# === Kafka (hiện là PLAINTEXT) ===
# 🔴 Bất kỳ ai có network access đều đọc được lệnh của khách
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:19092 --topic stock.orders --from-beginning --max-messages 3

# 🔴 Và giả mạo được execution report
echo '{"orderId":"fake","clientOrderId":"SEC-1","symbol":"HPG","side":"BUY",
       "status":"FILLED","reason":null,"fillPrice":1.0,"filledQty":100,"execTime":0}' | \
docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:19092 --topic stock.executions

# Nếu đã bật SASL: liệt kê ACL
# kafka-acls.sh --bootstrap-server kafka:9093 --command-config admin.properties --list

# === Secret ===
kubectl -n trading get secret trading-secrets -o jsonpath='{.data}' | jq 'keys'
# ⚠️ Giải mã được bởi bất kỳ ai có quyền get secret:
kubectl -n trading get secret trading-secrets -o jsonpath='{.data.DB_PASSWORD}' | base64 -d

# Kiểm tra secrets.env KHÔNG nằm trong Git
git check-ignore -v k8s/overlays/dev/secrets.env
git log --all --full-history -- '**/secrets.env'      # -> phải rỗng

# === Quét image ===
trivy image ghcr.io/trading-system/order-service:main --severity HIGH,CRITICAL
```

---

## 10. Hạn chế đã biết

| Mức | Vị trí | Vấn đề | Hướng sửa |
|:--:|---|---|---|
| 🔴 | Kafka toàn hệ | `PLAINTEXT`, không SASL, không ACL ⇒ pod bị chiếm = toàn quyền trên mọi topic, kể cả giả mạo execution report | Bật `security/application-secure-example.yml` + chạy `create-acls.sh` |
| 🔴 | `OrderController.java:55-88` | `accountId` là bộ lọc, **không** phải ràng buộc quyền ⇒ xem được lệnh của mọi account | JWT mang `accountId`, lọc ở service |
| 🔴 | `market-gateway` | `POST /api/v1/ticks` không có filter ⇒ bơm giá giả ⇒ thao túng `refPrice` | Thêm filter, hoặc tách sang profile dev |
| 🟠 | `ApiKeyFilter.java:38` | `.equals()` không constant-time | `MessageDigest.isEqual` |
| 🟠 | `ApiKeyFilter.java:28` | Mặc định `demo-key`, không cảnh báo | Từ chối khởi động khi profile prod mà key là mặc định |
| 🟠 | 4 controller | `@CrossOrigin("*")` | Giới hạn origin cụ thể |
| 🟠 | `KafkaProducerConfig.java:50` | `auto.register.schemas=true` | `false` + đăng ký qua CI |
| 🟠 | `apps-networkpolicy.yaml` | Ingress rule mở cả 4 cổng cho nginx dù chỉ route tới 1-3 service | Siết theo từng service (như bản Helm) |
| 🟠 | `pricing-risk-service`, `matching-engine` | Không có filter | Thêm `ApiKeyFilter` |
| 🟡 | Toàn hệ | `/actuator/**` không được filter bảo vệ | Thêm `ApiKeyFilter` cho actuator (trừ `health`), hoặc port riêng |
| 🟡 | Toàn hệ | Không có audit log | Log mọi thao tác đặt lệnh kèm principal |
| 🟡 | Schema Registry | HTTP, không xác thực | HTTPS + basic auth |
| 🟡 | Kubernetes | Secret chỉ base64 trong etcd | `EncryptionConfiguration` |
| 🟡 | CI | Không quét image | Trivy/Grype |
| 🟡 | CI | Không kiểm chính sách pod | OPA Conftest / Kyverno |
| 19 | prod | ESO cập nhật secret không trigger restart | Reloader / mount volume |
| 18 | dev/staging | Xoay mật khẩu DB làm sập môi trường (`initdb`) | `ALTER ROLE` trước, rồi mới sửa secret |

---

## 11. Câu hỏi phỏng vấn

**Q: Đánh giá bảo mật của hệ thống này.**

A: Hai lớp hạ tầng làm rất tốt, lớp ứng dụng mới ở mức demo. **Mạng** là điểm sáng nhất: default-deny làm nền, allowlist tường minh cho từng cặp, Postgres và Redis có `egress: []` nên không gửi được dữ liệu ra ngoài kể cả khi bị khai thác, và quan trọng là nó **cưỡng chế kiến trúc** — không service nào gọi REST sang service khác được, vì đơn giản là không có rule cho phép. **Container** cũng tốt: nonroot, readonly rootfs, drop ALL capability, seccomp, và `automountServiceAccountToken: false` — thiết lập cuối cùng này bị bỏ quên nhiều nhất trong thực tế.

Hai lỗ hổng nghiêm trọng, cả hai đều ở lớp ứng dụng và hạ tầng dữ liệu. Thứ nhất: Kafka chạy `PLAINTEXT` không ACL, nên một pod bị chiếm quyền đọc được toàn bộ lệnh của khách và **giả mạo được execution report** đánh dấu lệnh FILLED ở giá bất kỳ. Thứ hai: API key dùng chung không phân quyền, nên `accountId` trong query chỉ là bộ lọc chứ không phải ràng buộc — ai có key đều xem được lệnh của mọi tài khoản. Cả hai đều có lời giải đã biết và mẫu sẵn trong `security/`, chỉ chưa được bật.

**Q: Nếu chỉ được sửa một thứ, bạn sửa gì?**

A: Bật SASL_SSL + SCRAM + ACL cho Kafka. Vì nó đóng lỗ hổng có **sức sát thương lớn nhất**: khả năng giả mạo execution report. Một kẻ tấn công ghi được vào `stock.executions` sẽ làm `order-service` đánh dấu lệnh là FILLED với `fillPrice` bất kỳ — mà `order-service` **không có cách nào** phát hiện, vì nó tin topic đó theo thiết kế. Mẫu cấu hình đã có đầy đủ trong `security/application-secure-example.yml` và `create-acls.sh`, nên công sức chủ yếu là vận hành: dựng CA, phát SCRAM credential, mount truststore. Và ma trận ACL đã được thiết kế đúng nguyên tắc đặc quyền tối thiểu — `market-gateway` bị chiếm thì chỉ ghi được vào `stock.ticks`, không đọc được lệnh nào.

**Q: Vì sao `automountServiceAccountToken: false` quan trọng?**

A: Mặc định Kubernetes mount token của ServiceAccount vào mọi pod tại `/var/run/secrets/kubernetes.io/serviceaccount/token`. Kẻ tấn công vào được pod — qua RCE, qua một dependency có lỗ hổng, qua bất cứ đâu — sẽ dùng token đó gọi Kubernetes API. Nếu RBAC lỏng (và RBAC mặc định của nhiều cluster khá lỏng), họ liệt kê được Secret của cả namespace, tức là lấy được `DB_PASSWORD` và `ORDER_API_KEY`. Bốn service này **không gọi Kubernetes API** nên token chỉ là bề mặt tấn công thuần tuý. Repo đặt nó ở cả ServiceAccount lẫn Pod spec — thừa có chủ đích, phòng trường hợp một patch sau này ghi đè một trong hai.

**Q: Giải thích vì sao `ssl.endpoint.identification.algorithm: https` cần thiết.**

A: Không có nó, client Kafka thiết lập TLS nhưng **không kiểm tra hostname** trong chứng chỉ của broker. Nghĩa là một kẻ tấn công có **bất kỳ** chứng chỉ nào do một CA mà client tin cậy ký — kể cả chứng chỉ cho một domain hoàn toàn khác — cũng mạo danh được broker. TLS lúc đó chỉ chống nghe lén thụ động, **không** chống man-in-the-middle: kẻ tấn công ngồi giữa, giải mã, đọc lệnh, sửa, mã hoá lại và chuyển tiếp. Đây là một trong những dòng cấu hình dễ bỏ sót nhất, và hậu quả của việc bỏ sót thì không nhìn thấy được — mọi thứ vẫn chạy bình thường.

**Q: Bảo vệ `POST /api/v1/ticks` thế nào?**

A: Hiện nó không được bảo vệ ở tầng ứng dụng, và đó là endpoint nguy hiểm nhất trong hệ: ai bơm được tick giả thì điều khiển được `refPrice`, mà `refPrice` chính là giá `matching-engine` dùng để khớp lệnh. Đó là một đường thao túng giá hoàn chỉnh. Hiện nó được chắn bằng **hai lớp hạ tầng**: overlay prod gỡ route `/api/v1/ticks` khỏi Ingress, và NetworkPolicy chỉ cho ingress-nginx vào. Nhưng trong namespace, bất kỳ pod nào có label `component` phù hợp đều gọi được. Phòng thủ theo chiều sâu đòi hỏi filter ở chính service — hoặc tốt hơn, tách endpoint đó sang một profile chỉ bật ở dev, vì ở production feed đến từ adapter FIX chứ không qua REST.

**Q: Vì sao không dùng SOPS hay git-crypt để lưu secret trong Git?**

A: Comment trong `kustomization.yaml` của prod trả lời rất gọn: *"kể cả dạng mã hoá mà ai đó giải được ở máy cá nhân"*. Khoá giải mã phải nằm ở đâu đó. Nếu nó nằm trên máy dev, thì secret production về bản chất đã ở trên máy dev — và một máy dev bị chiếm là mất toàn bộ. Nếu khoá nằm trong KMS, thì ta đã có một hệ quản lý secret rồi, và lúc đó dùng thẳng External Secrets Operator sạch hơn: secret không bao giờ chạm vào Git, xoay được ở một nơi, có audit trail của nhà cung cấp, và IAM kiểm soát ai đọc được gì. SOPS hợp cho cấu hình **không nhạy cảm cao** mà bạn muốn versioning — không hợp cho mật khẩu production.

---

## 12. Bảng tổng kết

| Lớp | Cơ chế | Mức độ | Lỗ hổng còn lại |
|---|---|:--:|---|
| **Biên ngoài** | TLS (cert-manager), `limit-rps: 20`, `proxy-body-size: 1m`, không `configuration-snippet` | 🟡 | Không WAF, không mTLS client, không IP allowlist |
| **Ứng dụng** | API key tĩnh, Bean Validation, RateLimiter 200/s | 🔴 | Không phân quyền theo account, không audit, 3 service không có filter, CORS `*` |
| **Mạng** | default-deny + allowlist từng cặp, `egress: []` cho DB/Redis | 🟢 | Ingress rule mở cả 4 cổng |
| **Container** | nonroot 10001, readonly rootfs, drop ALL, seccomp, no SA token | 🟢 | — |
| **Namespace** | LimitRange, ResourceQuota, `nodeports: 0` | 🟢 | Chưa có OPA/Kyverno |
| **Kafka** | SASL_SSL + SCRAM-512 + ACL đặc quyền tối thiểu | 🟡 | **Mẫu có, chưa bật** |
| **Schema Registry** | — | 🔴 | HTTP, không xác thực, `auto.register=true` |
| **Secret** | ESO (prod), gitignored (dev/staging) | 🟡 | ESO không trigger restart; etcd chưa mã hoá |

| Nguyên tắc rút ra | Phát biểu |
|---|---|
| 🔑 1 | **Phòng thủ theo chiều sâu**: mạng + container + ứng dụng + dữ liệu, không dựa vào một lớp |
| 🔑 2 | **Đặc quyền tối thiểu** áp dụng thật: `egress: []` cho DB, Redis chỉ cho order, ACL theo ma trận |
| 🔑 3 | **Kiến trúc được cưỡng chế bằng hạ tầng**, không bằng kỷ luật — NetworkPolicy chặn REST liên service |
| 🔑 4 | **Nói thẳng chỗ chưa đủ** — bảo mật nửa vời mà tưởng là đủ còn nguy hiểm hơn không có |
| 🔑 5 | **Thiếu secret thì fail ngay**, đừng bao giờ có giá trị mặc định cho mật khẩu |
| 🔑 6 | Secret production **không nằm trong Git**, kể cả dạng mã hoá |

> **Đọc tiếp:** [Chương 13 — Persistence, Flyway & Mô hình dữ liệu](./13-persistence-flyway-va-mo-hinh-du-lieu.md).
