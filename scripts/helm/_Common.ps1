<#
    Hàm dùng chung cho các script Helm. Dot-source ở đầu mỗi script:

        . (Join-Path $PSScriptRoot '_Common.ps1')

    File này KHÔNG tự chạy việc gì; nó chỉ định nghĩa hàm.
#>

# CỐ Ý để 'Continue', KHÔNG phải 'Stop'.
#
# Trong PowerShell 5.1, khi một native exe (kubectl/docker/minikube) ghi ra
# stderr VÀ ta chuyển hướng stderr, PowerShell bọc từng dòng thành ErrorRecord
# (NativeCommandError). Với EAP='Stop' nó thành lỗi chí mạng và giết script,
# NGAY CẢ KHI exe trả về exit code 0, hoặc khi "không tìm thấy" là kết quả bình
# thường ta đang kiểm tra.
#
# Đổi lại: mọi lệnh native QUAN TRỌNG phải tự kiểm tra $LASTEXITCODE bằng
# Assert-LastExit, và mọi cmdlet quan trọng phải có -ErrorAction Stop.
$ErrorActionPreference = 'Continue'
$script:StepNo = 0

function Write-Step { param([string]$T) $script:StepNo++; Write-Host ''; Write-Host ("==> [{0}] {1}" -f $script:StepNo,$T) -ForegroundColor Cyan }
function Write-Info { param([string]$T) Write-Host "    $T" -ForegroundColor DarkGray }
function Write-Ok   { param([string]$T) Write-Host "    $T" -ForegroundColor Green }
function Write-Warn { param([string]$T) Write-Host "    $T" -ForegroundColor Yellow }
function Write-Bad  { param([string]$T) Write-Host "    $T" -ForegroundColor Red }

function Assert-LastExit {
    param([string]$What)
    if ($LASTEXITCODE -ne 0) { throw "$What  (exit $LASTEXITCODE)" }
}

function Test-ClusterReachable {
    <#
        True nếu API server trả lời. KHÔNG in gì ra màn hình.

        --request-timeout rất quan trọng: mặc định kubectl chờ 32 giây rồi mới
        chịu thua, nhân với số lần thử trong script là người dùng ngồi nhìn màn
        hình đen cả phút mà không biết chuyện gì.

        Mọi output đều bị nuốt vì khi API server chết, kubectl ói ra 5-6 dòng
        "memcache.go:265 Unhandled Error ... couldn't get current server API
        group list" — vô dụng với người đọc và che mất thông báo thật.
    #>
    kubectl cluster-info --request-timeout=5s *> $null
    return ($LASTEXITCODE -eq 0)
}

function Start-DockerDesktop {
    <#
        Bật Docker Desktop và chờ daemon lên. Trả về $true nếu daemon sẵn sàng.
    #>
    param([int]$TimeoutSec = 180)

    docker info --format '{{.ServerVersion}}' *> $null
    if ($LASTEXITCODE -eq 0) { return $true }

    $dd = 'C:\Program Files\Docker\Docker\Docker Desktop.exe'
    if (-not (Test-Path $dd)) {
        Write-Bad 'Docker daemon khong chay va khong tim thay Docker Desktop.'
        return $false
    }

    Write-Warn 'Docker daemon chua chay - dang bat Docker Desktop...'
    Start-Process -FilePath $dd -ErrorAction Stop | Out-Null

    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    while ((Get-Date) -lt $deadline) {
        Start-Sleep -Seconds 5
        docker info --format '{{.ServerVersion}}' *> $null
        if ($LASTEXITCODE -eq 0) {
            $mem = [math]::Round((docker info --format '{{.MemTotal}}') / 1GB, 1)
            Write-Ok "Docker daemon OK - RAM cap cho Docker: $mem GB"
            if ($mem -lt 6) {
                Write-Warn "Duoi 6GB: minikube se khong len noi. Sua memory= trong"
                Write-Warn "  %USERPROFILE%\.wslconfig  roi chay 'wsl --shutdown'."
            }
            return $true
        }
    }
    Write-Bad "Docker daemon khong len sau ${TimeoutSec}s."
    return $false
}

function Assert-ClusterReachable {
    <#
        Bảo đảm API server trả lời được, rồi mới cho script đi tiếp.

        Vì sao cần hàm riêng thay vì để `kubectl get nodes` tự chết: khi cluster
        tắt, kubectl ói ra 5-6 dòng memcache.go "Unhandled Error ... couldn't get
        current server API group list" rồi mới tới dòng thật. Người đọc thấy một
        bức tường lỗi Go và không rõ phải làm gì. Tệ hơn, các lệnh thăm dò kiểu
        `kubectl get crd ... --ignore-not-found` KHÔNG chết — chúng trả về RỖNG,
        và script sẽ kết luận nhầm là "thiếu CRD, hãy cài KEDA" trong khi thật ra
        chỉ là cluster đang tắt.

        Với -Target minikube, hàm tự bật Docker Desktop và minikube nếu cần.
    #>
    param(
        [ValidateSet('minikube','cloud')] [string] $Target         = 'minikube',
        [string] $ClusterProfile = 'minikube',
        [switch] $NoAutoStart
    )

    if (Test-ClusterReachable) {
        Write-Ok "API server tra loi (context: $(kubectl config current-context))."
        return
    }

    $ctx = kubectl config current-context 2>$null

    if ($Target -eq 'cloud') {
        Write-Bad "Khong goi duoc API server cua context '$ctx'."
        Write-Info 'Cluster cloud: kiem tra VPN, SSO token con han khong, va kubeconfig:'
        Write-Info '  kubectl config get-contexts'
        Write-Info '  aws eks update-kubeconfig --name <cluster> --region <region>'
        throw 'Cluster khong voi toi duoc.'
    }

    Write-Warn "Khong goi duoc API server (context: $ctx)."

    if ($NoAutoStart) {
        Write-Info "Chay tay:  minikube start -p $ClusterProfile"
        throw 'Cluster khong voi toi duoc.'
    }

    # minikube chạy TRONG một container Docker. Daemon chết thì `minikube status`
    # cũng chỉ in ra chính lỗi docker đó - phải sửa từ gốc.
    if (-not (Start-DockerDesktop)) {
        Write-Info 'Bat Docker Desktop bang tay roi chay lai script.'
        throw 'Docker daemon khong san sang.'
    }

    $mkHost = (minikube status -p $ClusterProfile --format '{{.Host}}' 2>$null)
    if ($mkHost -ne 'Running') {
        Write-Info ("minikube: {0} - dang khoi dong..." -f $(if ($mkHost) { $mkHost } else { 'chua chay' }))

        # KHÔNG truyền --cpus/--memory/--cni ở đây: profile đã tồn tại thì
        # minikube BỎ QUA chúng trong im lặng. Muốn đổi phải `minikube delete`
        # rồi tạo lại - xem HELM-DEPLOY.md muc 3.2.
        minikube start -p $ClusterProfile --wait=apiserver,system_pods
        Assert-LastExit 'minikube start that bai'
    } else {
        Write-Info 'minikube bao Running nhung API server khong tra loi - thu lai...'
    }

    kubectl config use-context $ClusterProfile *> $null

    if (-not (Test-ClusterReachable)) {
        Write-Bad 'Van khong goi duoc API server sau khi khoi dong minikube.'
        Write-Info "  minikube status -p $ClusterProfile"
        Write-Info "  minikube logs -p $ClusterProfile --length=50"
        throw 'Cluster khong voi toi duoc.'
    }
    Write-Ok "API server tra loi (context: $(kubectl config current-context))."
}

function Wait-NamespaceGone {
    <#
        Chờ một namespace đang Terminating biến mất hẳn.

        Vì sao phải có: `kubectl delete namespace` trả về NGAY khi API server
        nhận lệnh, nhưng namespace còn ở trạng thái Terminating hàng chục giây
        (gỡ PVC, gỡ finalizer của từng object). Trong khoảng đó namespace VẪN
        tồn tại, nên `kubectl create/apply/label` đều báo thành công — rồi Helm
        mới chết ở bước ghi release record:

            Error: create: failed to create: secrets "sh.helm.release.v1.trading.v1"
            is forbidden: unable to create new content in namespace trading-staging
            because it is being terminated

        Thông báo đó nằm MÃI SAU khi đã build xong 4 image, nên không bắt sớm
        là mất cả chục phút.
    #>
    param(
        [Parameter(Mandatory)] [string] $Ns,
        [int] $TimeoutSec = 300
    )

    $phase = kubectl get namespace $Ns -o jsonpath='{.status.phase}' 2>$null
    if ($phase -ne 'Terminating') { return }

    Write-Warn "Namespace '$Ns' dang Terminating - cho no bien mat han..."
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    while ((Get-Date) -lt $deadline) {
        Start-Sleep -Seconds 3
        if (-not (kubectl get namespace $Ns --ignore-not-found -o name 2>$null)) {
            Write-Ok 'Da xoa xong.'
            return
        }
    }

    # Quá hạn gần như luôn là finalizer treo. Chỉ ra ĐÚNG object còn kẹt thay vì
    # bảo người dùng tự mò.
    Write-Bad "Namespace '$Ns' van Terminating sau ${TimeoutSec}s."
    Write-Info 'Gan nhu luon la finalizer treo. Tim xem con object nao:'
    Write-Info "  kubectl -n $Ns get all"
    Write-Info "  kubectl get namespace $Ns -o jsonpath='{.spec.finalizers}'"
    Write-Info "  kubectl -n $Ns get pvc"
    $stuck = kubectl api-resources --verbs=list --namespaced -o name 2>$null |
             ForEach-Object { kubectl -n $Ns get $_ --ignore-not-found -o name 2>$null } |
             Where-Object { $_ }
    if ($stuck) {
        Write-Warn 'Object con sot lai:'
        $stuck | Select-Object -First 15 | ForEach-Object { Write-Warn "  $_" }
    }
    throw "Namespace '$Ns' khong xoa xong."
}

function Initialize-Namespace {
    <#
        Tạo namespace và gắn đúng bộ nhãn/chú thích sở hữu để chart NHẬN NUÔI nó.

        Chart có sẵn object Namespace (để gắn nhãn Pod Security Standards
        `restricted`), nên KHÔNG được dùng --create-namespace: Helm sẽ tạo
        namespace "trần" rồi manifest Namespace của chart đụng phải nó và chết với
            "Namespace ... exists and cannot be imported into the current release"
        Nhưng bỏ --create-namespace cũng không xong: Helm phải ghi release record
        VÀO namespace đó TRƯỚC khi apply manifest.

        Lối ra là cơ chế adoption chính thức của Helm 3: tạo trước, rồi gắn đủ
        BA thứ dưới đây. Thiếu bất kỳ cái nào là Helm vẫn từ chối.
    #>
    param(
        [Parameter(Mandatory)] [string] $Ns,
        [Parameter(Mandatory)] [string] $Rel
    )

    # PHẢI đứng trước mọi thứ khác: trên namespace đang Terminating thì
    # create/apply/label đều "thành công" một cách vô nghĩa (xem Wait-NamespaceGone).
    Wait-NamespaceGone -Ns $Ns

    # --dry-run=client | apply để lệnh idempotent: `kubectl create ns` lần hai
    # in AlreadyExists ra stderr.
    kubectl create namespace $Ns --dry-run=client -o yaml | kubectl apply -f - | Out-Null
    Assert-LastExit "tao namespace $Ns that bai"

    kubectl label    namespace $Ns app.kubernetes.io/managed-by=Helm --overwrite | Out-Null
    kubectl annotate namespace $Ns `
        meta.helm.sh/release-name=$Rel `
        meta.helm.sh/release-namespace=$Ns --overwrite | Out-Null
    Assert-LastExit "gan nhan so huu Helm cho namespace $Ns that bai"
}
