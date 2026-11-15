<#
.SYNOPSIS
    Deploy trading-system lên môi trường STAGING bằng Helm, từ build tới pod Ready.

.DESCRIPTION
    Chạy tuần tự: kiểm tra prereq -> build 4 image -> đưa image vào cluster ->
    chuẩn bị secret -> chuẩn bị namespace -> helm upgrade --install -> chờ
    rollout -> in cách truy cập.

    Bốn ràng buộc dưới đây là lý do script này không chỉ là một dòng
    `helm upgrade --install`:

    1. NAMESPACE: chart có sẵn object Namespace (để gắn nhãn Pod Security
       Standards `restricted`). Nhưng Helm phải ghi được release record VÀO
       namespace đó TRƯỚC khi apply manifest — chicken-and-egg. Mà nếu bạn
       `--create-namespace`, Helm tạo namespace "trần" rồi ngay sau đó manifest
       Namespace của chart đụng phải nó và chết:
           "Namespace ... exists and cannot be imported into the current release"
       Lối ra duy nhất sạch sẽ: tạo namespace trước RỒI gắn đúng bộ nhãn/chú
       thích sở hữu của Helm để chart nhận nuôi nó. Hàm Initialize-Namespace
       dưới đây làm việc đó. ĐỪNG thêm --create-namespace vào lệnh helm.

    2. MẬT KHẨU DB: postgres ghi mật khẩu vào PVC ở lần khởi tạo ĐẦU TIÊN và
       không bao giờ đọc lại POSTGRES_PASSWORD nữa. Sinh mật khẩu ngẫu nhiên
       mỗi lần deploy = lần hai app không đăng nhập được vào chính DB của nó,
       lỗi hiện ra là "password authentication failed" ở app chứ không phải ở
       postgres. Vì vậy mật khẩu được sinh MỘT LẦN rồi giữ trong
       helm\.secrets.staging.env (đã gitignore theo mẫu secrets.env).

    3. TAG KHÔNG ĐỔI + IfNotPresent: y hệt deploy-dev.ps1. Nạp image mới đè lên
       tag cũ thì kubelet thấy "đã có image này rồi" và giữ container đang chạy.
       Vì vậy sau mỗi lần nạp image phải ép `rollout restart`.

    4. JOB TẠO TOPIC: chạy bằng Helm hook post-install/post-upgrade. Nó là thứ
       quyết định `helm upgrade --wait` thành hay bại, và nó cần NetworkPolicy
       init-kafka-topics-egress (đã có trong chart) để gọi được broker.

.PARAMETER Mode
    'local' — build image tại máy, `minikube image load` vào cluster, phủ thêm
              values-staging-local.yaml. Dùng để chạy staging trên minikube.
    'registry' — image đã được CI build & push lên registry; script chỉ deploy.
              Dùng cho cluster staging thật.

.PARAMETER Recreate
    Gỡ hẳn release và XOÁ namespace (mất toàn bộ PVC: postgres, kafka) rồi cài
    lại từ đầu. Dùng khi StatefulSet kẹt ở trạng thái không sửa được.

.EXAMPLE
    .\scripts\helm\Deploy-Staging.ps1
    .\scripts\helm\Deploy-Staging.ps1 -Mode registry -ImageTag main
    .\scripts\helm\Deploy-Staging.ps1 -SkipBuild -Restart
    .\scripts\helm\Deploy-Staging.ps1 -DryRun
#>
#Requires -Version 5.1
[CmdletBinding()]
param(
    [ValidateSet('local','registry')]
    [string]   $Mode           = 'local',
    [string]   $Release        = 'trading',
    [string]   $Namespace      = 'trading-staging',
    [string]   $ClusterProfile = 'minikube',
    [string]   $ImageTag       = 'staging-local',
    [string[]] $Modules        = @('market-gateway','pricing-risk-service','order-service','matching-engine'),
    [switch]   $SkipBuild,
    [switch]   $Restart,
    [switch]   $Recreate,
    [switch]   $DryRun,
    [int]      $TimeoutSec     = 900
)
. (Join-Path $PSScriptRoot '_Common.ps1')

# scripts\helm\ -> lui hai cap moi toi repo root (noi co Dockerfile, pom.xml).
$RepoRoot  = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$ChartDir  = Join-Path $RepoRoot 'helm\trading-system'
$SecretFile= Join-Path $RepoRoot 'helm\.secrets.staging.env'

if (-not (Test-Path (Join-Path $RepoRoot 'Dockerfile'))) { throw "Khong thay Dockerfile tai $RepoRoot." }
if (-not (Test-Path $ChartDir)) { throw "Khong thay chart tai $ChartDir." }

$StartedAt = Get-Date
Write-Host ''
Write-Host "  trading-system -> STAGING   release=$Release  ns=$Namespace  mode=$Mode" -ForegroundColor White

function Get-OrCreate-Secrets {
    param([string]$Path)

    if (Test-Path $Path) {
        Write-Info 'Da co helm\.secrets.staging.env (gitignored) - dung lai.'
    } else {
        Write-Info 'Chua co - sinh mat khau ngau nhien MOT LAN va luu lai.'
        Write-Warn 'Sinh lai file nay sau khi postgres da khoi tao PVC = app khong'
        Write-Warn 'dang nhap duoc vao DB nua. Muon doi that su phai ALTER USER trong DB.'

        # [guid] thay vì Get-Random: đủ entropy cho staging và không cần .NET
        # crypto API (vốn khác nhau giữa PowerShell 5.1 và 7).
        $dbPass  = ([guid]::NewGuid().ToString('N') + [guid]::NewGuid().ToString('N')).Substring(0,32)
        $apiKey  = 'stg-' + [guid]::NewGuid().ToString('N').Substring(0,24)
        @(
            '# Sinh tu dong boi scripts\helm\Deploy-Staging.ps1 - KHONG commit.',
            "DB_PASSWORD=$dbPass",
            "ORDER_API_KEY=$apiKey"
        ) | Set-Content -Path $Path -Encoding ascii -ErrorAction Stop
        Write-Ok "Da tao $Path"
    }

    $map = @{}
    Get-Content $Path | Where-Object { $_ -match '^\s*([A-Z_]+)=(.*)$' } | ForEach-Object {
        if ($_ -match '^\s*([A-Z_]+)=(.*)$') { $map[$Matches[1]] = $Matches[2] }
    }
    foreach ($k in 'DB_PASSWORD','ORDER_API_KEY') {
        if (-not $map[$k]) { throw "$Path thieu $k" }
    }
    return $map
}

# ===========================================================================
Write-Step 'Kiem tra cong cu'

$needed = @('kubectl','helm')
if ($Mode -eq 'local' -and -not $SkipBuild) { $needed += @('docker','minikube') }
foreach ($tool in $needed) {
    $cmd = Get-Command $tool -ErrorAction SilentlyContinue
    if (-not $cmd) { throw "Thieu '$tool' trong PATH." }
    Write-Info ("{0,-9} {1}" -f $tool, $cmd.Source)
}

# ---------------------------------------------------------------------------
Write-Step 'Cluster + prereq cluster-wide'

# BAT BUOC lam truoc moi lenh kubectl khac. Neu cluster tat, cac lenh tham do
# duoi day KHONG chet ma tra ve RONG, va script se ket luan nham la "thieu KEDA,
# hay chay Install-Prereqs" trong khi that ra chi la cluster dang tat.
Assert-ClusterReachable -Target $(if ($Mode -eq 'local') { 'minikube' } else { 'cloud' }) `
                        -ClusterProfile $ClusterProfile

$ctx = kubectl config current-context
Assert-LastExit 'khong doc duoc kubecontext'

# Danh sach file values - dung o CA hai cho: preflight CRD ben duoi va lenh
# helm o buoc 6. Dinh nghia MOT lan de hai cho khong bao gio lech nhau.
$valueFiles = @((Join-Path $ChartDir 'values-staging.yaml'))
if ($Mode -eq 'local') { $valueFiles += (Join-Path $ChartDir 'values-staging-local.yaml') }


# Cac CRD duoi day quyet dinh `helm upgrade` song hay chet. Kiem tra TRUOC khi
# build 4 image mat 5 phut roi moi phat hien thieu.
$missing = @()
if (-not (kubectl get crd scaledobjects.keda.sh --ignore-not-found -o name)) {
    $missing += 'KEDA (keda.enabled=true trong values-staging.yaml)'
}
if (-not (kubectl get crd podmonitors.monitoring.coreos.com --ignore-not-found -o name)) {
    $missing += 'Prometheus Operator (monitoring.enabled=true trong values-staging.yaml)'
}
# cert-manager chi can khi values BAT selfSignedIssuer. Doc thang tu file values
# thay vi gia dinh, de script van dung khi ban tat TLS o local.
$tlsAuto = Select-String -Path $valueFiles -Pattern '^\s*selfSignedIssuer:' -Quiet
if ($tlsAuto -and -not (kubectl get crd certificates.cert-manager.io --ignore-not-found -o name)) {
    $missing += 'cert-manager (ingress.tls.selfSignedIssuer duoc bat trong values)'
}
if ($missing.Count -gt 0) {
    Write-Warn 'Thieu CRD - helm se chet ngay o buoc render:'
    $missing | ForEach-Object { Write-Warn "  - $_" }
    throw "Chay truoc: .\scripts\helm\Install-Prereqs.ps1 -Target $(if($Mode -eq 'local'){'minikube'}else{'cloud'})"
}
Write-Ok 'KEDA + Prometheus Operator: co du.'

if (-not (kubectl get ingressclass nginx --ignore-not-found -o name)) {
    Write-Warn 'KHONG co IngressClass nginx. Helm van cai duoc nhung Ingress se'
    Write-Warn 'khong co controller nao nhan -> goi API timeout (khong phai 404).'
}

# ---------------------------------------------------------------------------
if ($Recreate) {
    Write-Step "Go release + xoa namespace $Namespace"
    Write-Warn 'MAT TOAN BO du lieu trong PVC (postgres, kafka).'
    $ans = Read-Host "    Go dung '$Namespace' de xac nhan"
    if ($ans -ne $Namespace) { throw 'Khong khop - dung lai.' }

    helm uninstall $Release -n $Namespace --wait 2>$null | Out-Null
    kubectl delete namespace $Namespace --ignore-not-found --timeout=300s
    Assert-LastExit 'xoa namespace that bai'

    # `kubectl delete` tra ve NGAY khi API server nhan lenh, namespace con song
    # hang chuc giay de go PVC + finalizer. Wait-NamespaceGone chan cho het.
    Wait-NamespaceGone -Ns $Namespace
}

# ---------------------------------------------------------------------------
if ($SkipBuild -or $Mode -eq 'registry') {
    Write-Step ("Build + nap image  [BO QUA: {0}]" -f $(if ($SkipBuild) { '-SkipBuild' } else { 'Mode=registry' }))
    if ($Mode -eq 'registry') { Write-Info "Dung image co san tag '$ImageTag' tu registry." }
} else {
    Write-Step "Build $($Modules.Count) image  (TUAN TU - Docker Desktop treo khi build song song)"

    docker info --format '{{.ServerVersion}}' > $null 2>$null
    if ($LASTEXITCODE -ne 0) { throw 'Docker daemon khong chay. Bat Docker Desktop roi chay lai.' }

    Push-Location $RepoRoot
    try {
        $i = 0
        foreach ($m in $Modules) {
            $i++
            Write-Host ''
            Write-Info "[$i/$($Modules.Count)] docker build MODULE=$m -> trading/${m}:$ImageTag"
            docker build --build-arg "MODULE=$m" -t "trading/${m}:$ImageTag" .
            Assert-LastExit "build $m that bai"
        }
    } finally { Pop-Location }
    Write-Ok 'Build xong.'

    Write-Step 'Nap image vao containerd cua cluster'
    Write-Info 'minikube image load - KHONG dung docker-env duoc vi runtime la containerd.'
    foreach ($m in $Modules) {
        Write-Info "  -> trading/${m}:$ImageTag"
        minikube image load "trading/${m}:$ImageTag" -p $ClusterProfile
        Assert-LastExit "nap image $m that bai"
    }
    Write-Ok 'Nap xong.'
}

# ---------------------------------------------------------------------------
Write-Step 'Secret cho staging'
$sec = Get-OrCreate-Secrets -Path $SecretFile

# ---------------------------------------------------------------------------
Write-Step "Namespace $Namespace (tao + gan quyen so huu Helm)"
if ($DryRun) {
    Write-Info '[DryRun] bo qua - nhung lenh that se tao namespace truoc.'
} else {
    Initialize-Namespace -Ns $Namespace -Rel $Release
    Write-Ok 'Namespace san sang cho Helm nhan nuoi.'
}

# ---------------------------------------------------------------------------
Write-Step 'helm upgrade --install'

# Release ket o pending-install / pending-upgrade / uninstalling.
#
# Xay ra khi mot lan helm truoc bi NGAT giua chung (Ctrl+C, terminal dong, may
# ngu, hoac --atomic dang rollback thi bi giet). Helm ghi trang thai "pending"
# vao release record roi khong bao gio don. Lan chay sau no TU CHOI lam gi tiep:
#
#   Error: another operation (install/upgrade/rollback) is in progress
#
# Thong bao do di ra stderr, rat de bi nuot khi pipe output -> nguoi dung chi
# thay "helm that bai (exit 1)" ma khong hieu vi sao. Bat som va noi ro.
$relState = helm -n $Namespace list --all -o json 2>$null | ConvertFrom-Json |
            Where-Object { $_.name -eq $Release } | Select-Object -First 1
if ($relState -and $relState.status -like 'pending-*') {
    Write-Warn "Release '$Release' dang ket o trang thai '$($relState.status)' (revision $($relState.revision))."
    Write-Warn 'Mot lan helm truoc bi ngat giua chung. Helm se tu choi moi thao tac tiep theo.'
    if ($relState.revision -le 1) {
        Write-Warn 'Day la lan cai DAU TIEN -> go han release roi cai lai:'
        Write-Warn "  helm uninstall $Release -n $Namespace"
    } else {
        Write-Warn 'Da tung cai thanh cong truoc do -> quay ve revision gan nhat con tot:'
        Write-Warn "  helm -n $Namespace history $Release"
        Write-Warn "  helm -n $Namespace rollback $Release <revision>"
    }
    throw "Release ket o '$($relState.status)' - xu ly theo huong dan tren roi chay lai."
}


$helmArgs = @(
    'upgrade','--install', $Release, $ChartDir,
    '--namespace', $Namespace
)
foreach ($f in $valueFiles) { $helmArgs += @('-f', $f) }

$helmArgs += @(
    # --set ghi de len file values. Mat khau KHONG nam trong git, chi di qua
    # dong lenh nay roi vao Secret cua release trong cluster.
    '--set', ("secrets.DB_PASSWORD=" + $sec['DB_PASSWORD']),
    '--set', ("secrets.ORDER_API_KEY=" + $sec['ORDER_API_KEY'])
)

if ($Mode -eq 'registry') {
    foreach ($pair in @(
        @('marketGateway','market-gateway'), @('pricingRiskService','pricing-risk-service'),
        @('orderService','order-service'),   @('matchingEngine','matching-engine'))) {
        $helmArgs += @('--set', ("{0}.image.tag={1}" -f $pair[0], $ImageTag))
    }
}

if ($DryRun) {
    $helmArgs += @('--dry-run','--debug')
    Write-Info 'DryRun: chi render, khong dong vao cluster.'
} else {
    # --wait: cho tat ca Deployment/StatefulSet Ready VA Job hook hoan thanh.
    # --atomic: that bai thi TU DONG rollback ve revision truoc. Dung thieu no
    #   o staging — khong thi ban se co mot release nua song nua chet.
    # --timeout: 15 phut. Kafka + postgres khoi dong lan dau mat vai phut, va
    #   Job tao topic con doi broker san sang nua.
    $helmArgs += @('--wait', '--atomic', '--timeout', "${TimeoutSec}s")
}

Write-Info ("helm " + (($helmArgs | ForEach-Object { if ($_ -match 'DB_PASSWORD|API_KEY') { ($_ -replace '=.*','=***') } else { $_ } }) -join ' '))
Write-Host ''
# `& helm @args` goi thang thi stderr cua helm di ra ngoai stream loi cua
# PowerShell va CO THE bi mat khi nguoi dung pipe output di cho khac
# (vd `| Select-Object -Last 40`). Thong bao quan trong nhat cua Helm —
# "another operation is in progress", "cannot re-use a name" — nam dung o do.
# 2>&1 gop no vao stdout de no LUON hien ra va luon vao log.
& helm @helmArgs 2>&1 | ForEach-Object { "$_" }
$helmExit = $LASTEXITCODE

if ($DryRun) {
    Write-Host ''
    Write-Ok 'DryRun xong - khong co gi duoc apply.'
    exit 0
}

if ($helmExit -ne 0) {
    Write-Host ''
    Write-Warn "helm that bai (exit $helmExit) - --atomic da rollback."
    Write-Warn 'Ba cho dang xem dau tien:'
    Write-Warn "  kubectl -n $Namespace get pods"
    Write-Warn "  kubectl -n $Namespace describe pod <ten-pod>"
    Write-Warn "  kubectl -n $Namespace logs job/init-kafka-topics --tail=50"
    Write-Warn "  helm -n $Namespace history $Release"
    throw "Deploy that bai."
}
Write-Ok 'helm upgrade thanh cong.'

# ---------------------------------------------------------------------------
# Tag khong doi + IfNotPresent = kubelet giu container cu. Phai ep restart,
# neu khong ban soi log cua binary cu ma tuong la code vua build.
if ($Mode -eq 'local' -and ((-not $SkipBuild) -or $Restart)) {
    Write-Step 'Rollout restart cac service vua nap image moi'
    Write-Info "Bat buoc: tag :$ImageTag khong doi nen kubelet khong tu thay image moi."
    foreach ($m in $Modules) {
        kubectl -n $Namespace rollout restart "deployment/$m" | Out-Null
        if ($LASTEXITCODE -eq 0) { Write-Info "  restarted $m" }
    }
    foreach ($m in $Modules) {
        kubectl -n $Namespace rollout status "deployment/$m" --timeout=300s
        if ($LASTEXITCODE -ne 0) { Write-Warn "  $m chua Ready sau restart" }
    }
}

# ---------------------------------------------------------------------------
Write-Step 'Trang thai'

kubectl -n $Namespace get pods -o wide

# Pod o ContainerCreating / Init van mang phase=Pending, nen loc theo phase bao
# dong gia. Chi PodScheduled=False moi la that su khong xep duoc node.
$unsched = kubectl -n $Namespace get pods `
    -o jsonpath='{range .items[?(@.status.conditions[?(@.type=="PodScheduled")].status=="False")]}{.metadata.name}{"\n"}{end}' 2>$null
Write-Host ''
if ($unsched) {
    Write-Warn 'Pod KHONG xep duoc node (PodScheduled=False) - thuong la het CPU/RAM:'
    $unsched -split "`n" | Where-Object { $_ } | ForEach-Object { Write-Warn "  $_" }
    Write-Warn '  Cap them cho minikube: minikube delete; minikube start --cpus=4 --memory=8192 --cni=calico'
} else {
    Write-Ok 'Moi pod deu da duoc xep node.'
}

Write-Host ''
Write-Info 'KEDA ScaledObject:'
kubectl -n $Namespace get scaledobject 2>$null
Write-Info 'Job tao topic:'
kubectl -n $Namespace get job init-kafka-topics --ignore-not-found

# ---------------------------------------------------------------------------
Write-Step 'Truy cap'

$elapsed = [int]((Get-Date) - $StartedAt).TotalSeconds
$host_   = kubectl -n $Namespace get ingress trading-ingress -o jsonpath='{.spec.rules[0].host}' 2>$null
$apiKey  = $sec['ORDER_API_KEY']

Write-Host @"
    API KEY staging (nam trong helm\.secrets.staging.env):
        $apiKey

    INGRESS TREN WINDOWS/MINIKUBE - node IP KHONG toi duoc tu Windows. Mang
    docker bridge nam TRONG VM WSL2 va Windows khong co route toi do. Sua hosts
    tro vao node IP se TIMEOUT (khong phai 404). Cach chay duoc:

        kubectl -n ingress-nginx port-forward svc/ingress-nginx-controller 80:80

    roi them vao C:\Windows\System32\drivers\etc\hosts (CAN quyen Administrator):

        127.0.0.1  $host_

    BAN DO API - moi service mot prefix RIENG:

        POST /api/v1/orders              -> order-service        :8083  (qua Ingress)
        GET  /api/v1/orders              -> order-service        :8083  (qua Ingress) liet ke, phan trang
        GET  /api/v1/orders/{clientOrderId} -> order-service     :8083  (qua Ingress) tra 1 lenh
        GET  /api/v1/marketdata          -> pricing-risk-service :8082  (qua Ingress) snapshot tat ca
        GET  /api/v1/marketdata/{symbol} -> pricing-risk-service :8082  (qua Ingress)
        GET  /api/v1/ticks               -> market-gateway        :8081  (port-forward)
        GET  /api/v1/executions/{cid}    -> matching-engine       :8084  (port-forward)

    Ingress route theo danh sach ingress.routes trong values (pathType: Prefix).
    Them route: sua ingress.routes - no sinh CA path lan NetworkPolicy tuong ung.
    Duong KHONG co trong routes se ra 404 - khong phai loi, chi la khong co route.

    Goi thu qua Ingress (feed gia lap sinh 4 symbol: VN30F2607, HPG, VCB, FPT):

        curl.exe -X POST http://$host_/api/v1/orders ``
          -H "X-API-KEY: $apiKey" -H "Content-Type: application/json" ``
          -d "{\"clientOrderId\":\"test-1\",\"symbol\":\"FPT\",\"side\":\"BUY\",\"price\":105000,\"quantity\":100,\"accountId\":\"ACC1\"}"

    Doc ket qua - moi ma mot y nghia khac han:
        timeout -> chua toi duoc nginx  (port-forward chet, hoac hosts sai)
        404     -> toi nginx roi nhung Host header / duong dan khong khop Ingress
        401     -> da vao toi app, sai X-API-KEY
        429     -> da vao toi app, dinh RateLimiter (dung, khong phai loi)
        422     -> da vao toi app, risk engine TU CHOI lenh. Doc truong "reason".
                   Feed gia lap bien dong manh nen ca 4 symbol thuong o
                   RISK_LEVEL_HIGH va MOI lenh deu bi tu choi -> day van la
                   duong di dung, he thong dang lam viec.
        201     -> lenh duoc nhan, da day sang matching-engine.

    Xem lai lenh da dat:
        curl.exe -H "X-API-KEY: $apiKey" "https://$host_/api/v1/orders?size=20"
        curl.exe -H "X-API-KEY: $apiKey" "https://$host_/api/v1/orders?status=REJECTED"
        curl.exe -H "X-API-KEY: $apiKey" "https://$host_/api/v1/orders/<clientOrderId>"

    Xem gia + rui ro (qua Ingress, khong can port-forward nua):
        curl.exe -H "X-API-KEY: $apiKey" "https://$host_/api/v1/marketdata"
        curl.exe -H "X-API-KEY: $apiKey" "https://$host_/api/v1/marketdata/FPT"
    (size bi chan cung o 200; mac dinh 20, sap xep moi nhat truoc)

    Xem gia va muc rui ro dang tinh duoc:
        kubectl -n $Namespace port-forward svc/pricing-risk-service 8082:8082
        curl.exe -H "X-API-KEY: $apiKey" http://127.0.0.1:8082/api/v1/marketdata/FPT

    Khong can Ingress thi port-forward thang tung service:
        kubectl -n $Namespace port-forward svc/order-service        8083:8083
        kubectl -n $Namespace port-forward svc/pricing-risk-service 8082:8082
        kubectl -n $Namespace port-forward svc/matching-engine      8084:8084
        kubectl -n $Namespace port-forward svc/market-gateway       8081:8081
        kubectl -n $Namespace port-forward svc/zipkin               9411:9411   # chi khi zipkin.enabled=true

    Grafana cua kube-prometheus-stack:
        kubectl -n monitoring port-forward svc/kube-prometheus-stack-grafana 3000:80
        user admin / pass: kubectl -n monitoring get secret kube-prometheus-stack-grafana ``
          -o jsonpath='{.data.admin-password}' | base64 -d

    Kiem tra KEDA co thuc su doc duoc lag khong (READY phai la True):
        kubectl -n $Namespace get scaledobject
        kubectl -n $Namespace describe scaledobject pricing-risk-service

    Theo doi:
        kubectl -n $Namespace get pods -w
        kubectl -n $Namespace logs -l app.kubernetes.io/name=order-service -f
        helm -n $Namespace history $Release
"@

Write-Host ''
if (-not $unsched) {
    Write-Host "  Xong sau ${elapsed}s - staging da len." -ForegroundColor Green
} else {
    Write-Host "  Xong sau ${elapsed}s - CO POD CHUA XEP DUOC NODE (xem tren)." -ForegroundColor Yellow
    exit 1
}
