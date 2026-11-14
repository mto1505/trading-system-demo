<#
.SYNOPSIS
    Deploy trading-system lên minikube (overlay dev), từ máy trắng tới pod Ready.

.DESCRIPTION
    Chạy tuần tự: bật Docker Desktop -> bật minikube -> build 4 image ->
    nạp vào cluster -> apply kustomize -> chờ rollout -> in cách truy cập.

    Script này viết cho ĐÚNG cluster trên máy này; ba ràng buộc dưới đây là lý
    do nó không chỉ là một chuỗi lệnh docker build:

    1. Runtime của minikube là containerd, KHÔNG phải docker. Vì vậy
       `minikube docker-env` vô dụng (nó còn hỏng hẳn trên Windows vì minikube
       không khởi động được ssh-agent). Đường duy nhất đưa image vào cluster là
       `minikube image load`.

    2. Deployment đặt `imagePullPolicy: IfNotPresent` và tag cố định `:dev`.
       Nạp image mới ĐÈ LÊN tag cũ thì kubelet thấy "đã có image này rồi" và
       giữ nguyên container đang chạy. Không `rollout restart` thì bạn deploy
       xong vẫn đang chạy code cũ mà không có dấu hiệu gì báo.

    3. Docker Desktop trên máy này treo khi build song song. Vòng build cố ý
       tuần tự; đừng "tối ưu" bằng ForEach-Object -Parallel.

.PARAMETER SkipBuild
    Bỏ qua build + nạp image. Dùng khi chỉ sửa manifest YAML.
    LƯU Ý: nó cũng bỏ qua `rollout restart`. Nếu bạn đã tự `minikube image load`
    rồi mới chạy với -SkipBuild, pod sẽ VẪN chạy container cũ — thêm -Restart.

.PARAMETER Restart
    Ép `rollout restart` kể cả khi -SkipBuild. Dùng khi image đã được nạp sẵn
    bằng đường khác.

.PARAMETER Modules
    Chỉ build/nạp/restart một phần. VD: -Modules order-service,matching-engine

.PARAMETER Recreate
    Xoá namespace rồi apply lại từ đầu. Mất dữ liệu trong PVC.

.EXAMPLE
    .\scripts\deploy-dev.ps1
    .\scripts\deploy-dev.ps1 -Modules order-service
    .\scripts\deploy-dev.ps1 -SkipBuild
#>
#Requires -Version 5.1
[CmdletBinding()]
param(
    [string]   $ClusterProfile = 'minikube',
    [string]   $Namespace      = 'trading-dev',
    [string]   $Tag            = 'dev',
    [string[]] $Modules        = @('market-gateway','pricing-risk-service','order-service','matching-engine'),
    [switch]   $SkipBuild,
    [switch]   $Restart,
    [switch]   $Recreate,
    [int]      $RolloutTimeoutSec = 600
)

# CO Y de 'Continue', KHONG phai 'Stop'.
#
# Trong PowerShell 5.1, khi mot native exe (kubectl/docker/minikube) ghi ra
# stderr VA ta chuyen huong stderr, PowerShell boc tung dong thanh ErrorRecord
# (NativeCommandError). Voi EAP='Stop' no thanh loi chi mang va giet script,
# NGAY CA KHI exe tra ve exit code 0 hoac khi "khong tim thay" la ket qua binh
# thuong ta dang kiem tra.
#
# Vi du that: `kubectl get job kafka-topics-init` luc Job chua ton tai in
# 'Error from server (NotFound)' ra stderr -> script chet giua chung thay vi
# di tiep nhanh "chua co Job".
#
# Doi lai: moi lenh native QUAN TRONG phai tu kiem tra $LASTEXITCODE bang
# Assert-LastExit, va moi cmdlet quan trong phai co -ErrorAction Stop.
$ErrorActionPreference = 'Continue'
$script:StepNo = 0

function Write-Step {
    param([string]$Text)
    $script:StepNo++
    Write-Host ''
    Write-Host ("==> [{0}] {1}" -f $script:StepNo, $Text) -ForegroundColor Cyan
}
function Write-Info { param([string]$T) Write-Host "    $T" -ForegroundColor DarkGray }
function Write-Ok   { param([string]$T) Write-Host "    $T" -ForegroundColor Green }
function Write-Warn { param([string]$T) Write-Host "    $T" -ForegroundColor Yellow }

function Assert-LastExit {
    param([string]$What)
    if ($LASTEXITCODE -ne 0) { throw "$What  (exit $LASTEXITCODE)" }
}

# Repo root = thư mục cha của scripts/. Dockerfile và pom.xml nằm ở đó, và
# build context phải là nó chứ không phải cwd của người gọi.
$RepoRoot = Split-Path -Parent $PSScriptRoot
$OverlayDir = Join-Path $RepoRoot 'k8s\overlays\dev'

if (-not (Test-Path (Join-Path $RepoRoot 'Dockerfile'))) {
    throw "Khong thay Dockerfile tai $RepoRoot - script dang nam sai cho?"
}

$StartedAt = Get-Date
Write-Host ''
Write-Host "  trading-system -> minikube/$ClusterProfile  (namespace $Namespace)" -ForegroundColor White
Write-Host "  repo: $RepoRoot"

# ---------------------------------------------------------------------------
Write-Step 'Kiem tra cong cu'

foreach ($tool in 'docker','kubectl','minikube') {
    $cmd = Get-Command $tool -ErrorAction SilentlyContinue
    if (-not $cmd) { throw "Thieu '$tool' trong PATH." }
    Write-Info ("{0,-9} {1}" -f $tool, $cmd.Source)
}

# ---------------------------------------------------------------------------
Write-Step 'Docker daemon'

docker info --format '{{.ServerVersion}}' > $null 2>$null
if ($LASTEXITCODE -ne 0) {
    $dd = 'C:\Program Files\Docker\Docker\Docker Desktop.exe'
    if (-not (Test-Path $dd)) { throw "Docker daemon khong chay va khong tim thay Docker Desktop." }
    Write-Warn 'Daemon chua chay - dang bat Docker Desktop...'
    Start-Process -FilePath $dd -ErrorAction Stop | Out-Null

    $deadline = (Get-Date).AddSeconds(180)
    while ((Get-Date) -lt $deadline) {
        Start-Sleep -Seconds 5
        docker info --format '{{.ServerVersion}}' > $null 2>$null
        if ($LASTEXITCODE -eq 0) { break }
    }
    if ($LASTEXITCODE -ne 0) { throw "Docker daemon khong len sau 180s." }
}
$dockerMem = [math]::Round((docker info --format '{{.MemTotal}}') / 1GB, 1)
Write-Ok "Daemon OK - RAM cap cho Docker: $dockerMem GB"
if ($dockerMem -lt 6) {
    Write-Warn "Duoi 6GB: minikube 6144MB se khong len noi. Sua memory= trong %USERPROFILE%\.wslconfig roi 'wsl --shutdown'."
}

# ---------------------------------------------------------------------------
Write-Step "Cluster minikube/$ClusterProfile"

$mkStatus = (minikube status -p $ClusterProfile --format '{{.Host}}' 2>$null)
if ($mkStatus -ne 'Running') {
    Write-Info "Trang thai: $(if ($mkStatus) { $mkStatus } else { 'khong xac dinh' }) - dang khoi dong..."
    # KHONG truyen --cpus/--memory/--cni o day: profile da ton tai thi minikube
    # bo qua chung trong im lang. Muon doi phai `minikube delete` roi tao lai.
    minikube start -p $ClusterProfile --wait=apiserver,system_pods
    Assert-LastExit 'minikube start that bai'
} else {
    Write-Info 'Da chay.'
}

minikube profile $ClusterProfile | Out-Null
kubectl config use-context $ClusterProfile | Out-Null
Assert-LastExit 'khong doi duoc kubecontext'

# Dat luon namespace mac dinh cho context - KHONG chi de do phai go
# '-n trading-dev' moi lan. Ly do that: IntelliJ va cac k8s client khac lay
# namespace TU KUBECONFIG. De context o 'default' thi cay Kubernetes trong IDE
# chi thay moi svc/kubernetes va trong y het nhu chua deploy gi ca.
kubectl config set-context --current --namespace=$Namespace | Out-Null
Assert-LastExit 'khong dat duoc namespace mac dinh'

$nodeMem = kubectl get node $ClusterProfile -o jsonpath='{.status.allocatable.memory}'
Write-Ok "Context: $ClusterProfile   ns mac dinh: $Namespace   node allocatable: $nodeMem"

# NetworkPolicy chi co hieu luc khi CNI thuc thi no. kindnet thi KHONG.
$cniPods = kubectl get pods -n kube-system -o name 2>$null
if ($cniPods -match 'calico') {
    Write-Info 'CNI: Calico - NetworkPolicy CO hieu luc.'
} elseif ($cniPods -match 'kindnet') {
    Write-Warn 'CNI: kindnet - NetworkPolicy apply duoc nhung KHONG duoc thuc thi. Dung nham voi "da an toan".'
}

# ingress-nginx cua minikube nam o namespace RIENG `ingress-nginx`, khong phai
# kube-system - tim nham cho thi luc nao cung bao "chua bat".
$ingressReady = kubectl get deployment ingress-nginx-controller -n ingress-nginx --ignore-not-found `
    -o jsonpath='{.status.readyReplicas}' 2>$null
if (-not $ingressReady) {
    $nsExists = kubectl get namespace ingress-nginx --ignore-not-found -o name
    if ($nsExists) {
        Write-Warn 'Addon ingress da bat nhung controller CHUA Ready - trading.dev.local se 503.'
        Write-Warn '  kubectl -n ingress-nginx get pods'
    } else {
        Write-Warn 'Addon ingress chua bat - host trading.dev.local se khong vao duoc.'
        Write-Warn "  minikube addons enable ingress -p $ClusterProfile"
    }
} else {
    Write-Info "Ingress: ingress-nginx-controller Ready ($ingressReady/1)."
}

# ---------------------------------------------------------------------------
Write-Step 'secrets.env'

$secrets = Join-Path $OverlayDir 'secrets.env'
if (-not (Test-Path $secrets)) {
    Copy-Item (Join-Path $OverlayDir 'secrets.env.example') $secrets -ErrorAction Stop
    Write-Ok 'Da tao tu secrets.env.example (gia tri dev mac dinh).'
} else {
    Write-Info 'Da co (gitignored).'
}
$apiKey = (Select-String -Path $secrets -Pattern '^ORDER_API_KEY=(.*)$').Matches.Groups[1].Value

# ---------------------------------------------------------------------------
if ($SkipBuild) {
    Write-Step 'Build + nap image  [BO QUA: -SkipBuild]'
} else {
    Write-Step ("Build {0} image  (TUAN TU - Docker Desktop treo khi build song song)" -f $Modules.Count)

    Push-Location $RepoRoot
    try {
        $i = 0
        foreach ($m in $Modules) {
            $i++
            Write-Host ''
            Write-Info "[$i/$($Modules.Count)] docker build MODULE=$m -> trading/${m}:$Tag"
            docker build --build-arg "MODULE=$m" -t "trading/${m}:$Tag" .
            Assert-LastExit "build $m that bai"
        }
    } finally {
        Pop-Location
    }
    Write-Ok 'Build xong.'

    Write-Step 'Nap image vao containerd cua cluster'
    Write-Info 'minikube image load - KHONG dung docker-env duoc vi runtime la containerd.'
    foreach ($m in $Modules) {
        Write-Info "  -> trading/${m}:$Tag"
        minikube image load "trading/${m}:$Tag" -p $ClusterProfile
        Assert-LastExit "nap image $m that bai"
    }
    Write-Ok 'Nap xong.'
}

# ---------------------------------------------------------------------------
if ($Recreate) {
    Write-Step "Xoa namespace $Namespace"
    Write-Warn 'Mat toan bo du lieu trong PVC (postgres, kafka).'
    kubectl delete namespace $Namespace --ignore-not-found --timeout=180s
    Assert-LastExit 'xoa namespace that bai'
}

Write-Step 'Apply kustomize overlay dev'

# Job gan nhu bat bien: doi bat ky gia tri nao trong trading-config (vd
# KAFKA_PARTITIONS) -> ConfigMap doi hash -> pod template cua Job doi ->
# `kubectl apply` tra loi "field is immutable". Xoa truoc cho gon; Job nay
# idempotent (kafka-topics --create --if-not-exists) nen chay lai vo hai.
# --ignore-not-found: tra ve rong thay vi ghi 'Error from server (NotFound)'
# ra stderr. Sach hon han viec nuot stderr bang 2>$null.
$jobExists = kubectl -n $Namespace get job kafka-topics-init --ignore-not-found -o name
if ($jobExists) {
    Write-Info 'Xoa job/kafka-topics-init cu (bat bien) truoc khi apply.'
    kubectl -n $Namespace delete job kafka-topics-init --ignore-not-found --wait=true | Out-Null
}

kubectl apply -k (Join-Path $RepoRoot 'k8s\overlays\dev')
Assert-LastExit 'kubectl apply that bai'

# ---------------------------------------------------------------------------
# Tag khong doi + IfNotPresent = kubelet giu container cu. Phai ep restart,
# neu khong ban se soi log cua binary cu ma tuong la code vua build.
if ((-not $SkipBuild) -or $Restart) {
    Write-Step 'Rollout restart cac service vua nap image moi'
    Write-Info 'Bat buoc: tag :dev khong doi nen kubelet khong tu thay image moi.'
    foreach ($m in $Modules) {
        kubectl -n $Namespace rollout restart "deployment/$m" | Out-Null
        if ($LASTEXITCODE -eq 0) { Write-Info "  restarted $m" }
    }
}

# ---------------------------------------------------------------------------
Write-Step 'Cho rollout'

$failed = @()

Write-Info 'Job kafka-topics-init...'
kubectl -n $Namespace wait --for=condition=complete job/kafka-topics-init --timeout=300s 2>$null
if ($LASTEXITCODE -ne 0) { $failed += 'job/kafka-topics-init' }

foreach ($sts in 'postgres','kafka') {
    Write-Info "statefulset/$sts..."
    kubectl -n $Namespace rollout status "statefulset/$sts" --timeout="${RolloutTimeoutSec}s"
    if ($LASTEXITCODE -ne 0) { $failed += "statefulset/$sts" }
}

$deploys = @('redis','schema-registry','zipkin','prometheus') + $Modules
foreach ($d in $deploys) {
    Write-Info "deployment/$d..."
    kubectl -n $Namespace rollout status "deployment/$d" --timeout="${RolloutTimeoutSec}s"
    if ($LASTEXITCODE -ne 0) { $failed += "deployment/$d" }
}

# ---------------------------------------------------------------------------
Write-Step 'Ket qua'

kubectl -n $Namespace get pods

# Pod o ContainerCreating / Init: van mang phase=Pending, nen `--field-selector
# status.phase=Pending` bao dong gia. Chi PodScheduled=False moi la that su
# khong xep duoc node (het RAM / het CPU / khong khop nodeSelector).
$unsched = kubectl -n $Namespace get pods `
    -o jsonpath='{range .items[?(@.status.conditions[?(@.type=="PodScheduled")].status=="False")]}{.metadata.name}{"\n"}{end}' 2>$null

Write-Host ''
if ($unsched) {
    Write-Warn 'Pod KHONG xep duoc node (PodScheduled=False):'
    $unsched -split "`n" | Where-Object { $_ } | ForEach-Object { Write-Warn "  $_" }
    Write-Warn "Thuong la het RAM. Xem: kubectl describe node $ClusterProfile"
} else {
    Write-Ok 'Moi pod deu da duoc xep node (khong co PodScheduled=False).'
}

Write-Host ''
Write-Host '  Node da cap phat:' -ForegroundColor White
kubectl describe node $ClusterProfile |
    Select-String -Pattern '^\s+(cpu|memory)\s' |
    Select-Object -First 2 |
    ForEach-Object { Write-Host "   $($_.Line.Trim())" }

if ($failed.Count -gt 0) {
    Write-Host ''
    Write-Warn "Chua Ready: $($failed -join ', ')"
    Write-Warn "  kubectl -n $Namespace describe pod <ten-pod>"
    Write-Warn "  kubectl -n $Namespace logs <ten-pod> --tail=80"
}

# ---------------------------------------------------------------------------
Write-Step 'Truy cap'

$ip = (minikube ip -p $ClusterProfile).Trim()
$elapsed = [int]((Get-Date) - $StartedAt).TotalSeconds

Write-Host @"
    INGRESS TREN WINDOWS - node IP $ip KHONG toi duoc tu Windows.
    Mang 192.168.49.0/24 la docker bridge nam TRONG VM WSL2; Windows khong co
    route toi do. Sua hosts tro vao node IP se TIMEOUT (khong phai 404).
    Cach chay duoc - port-forward thang controller, giu terminal nay mo:

        kubectl -n ingress-nginx port-forward svc/ingress-nginx-controller 80:80

    Windows cho user thuong bind port 80 nen khong can admin o buoc nay.
    Roi them dong duoi vao C:\Windows\System32\drivers\etc\hosts
    (chi buoc NAY moi can quyen Administrator):

        127.0.0.1  trading.dev.local

    Goi thu (feed gia lap chi sinh 4 symbol: VN30F2607, HPG, VCB, FPT -
    dung symbol khac se 404 dung nghia "khong co du lieu", khong phai loi route):
        curl.exe -H "X-API-KEY: $apiKey" http://trading.dev.local/api/v1/marketdata/FPT
        curl.exe -H "X-API-KEY: $apiKey" http://trading.dev.local/api/v1/marketdata

    Khong can Ingress thi port-forward thang tung service:
        kubectl -n $Namespace port-forward svc/order-service        8083:8083
        kubectl -n $Namespace port-forward svc/pricing-risk-service 8082:8082
        kubectl -n $Namespace port-forward svc/zipkin               9411:9411
        kubectl -n $Namespace port-forward svc/prometheus           9090:9090

    Doc ket qua goi thu - ba loi khac han nhau:
        timeout -> chua toi duoc nginx  (port-forward chet, hoac hosts tro node IP)
        404     -> toi nginx roi nhung Host header khong khop rule Ingress
        401     -> da vao toi app, sai X-API-KEY

    Theo doi:
        kubectl -n $Namespace get pods -w
        kubectl -n $Namespace logs -l app.kubernetes.io/name=order-service -f
"@

Write-Host ''
if ($failed.Count -eq 0 -and -not $unsched) {
    Write-Host "  Xong sau ${elapsed}s - tat ca Ready." -ForegroundColor Green
} else {
    Write-Host "  Xong sau ${elapsed}s - CO THANH PHAN CHUA READY (xem tren)." -ForegroundColor Yellow
    exit 1
}
