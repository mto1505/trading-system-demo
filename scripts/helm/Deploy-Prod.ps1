<#
.SYNOPSIS
    Deploy trading-system lên PRODUCTION bằng Helm, ghim image theo digest.

.DESCRIPTION
    Script này CỐ Ý không giống Deploy-Staging.ps1. Prod khác ở bốn điểm căn bản:

    1. KHÔNG build gì cả. Image phải đã được CI build, test, push và ký. Script
       chỉ PHÂN GIẢI tag -> digest rồi deploy theo digest.

       Vì sao digest chứ không phải tag: tag là con trỏ có thể ghi đè. Deploy
       `:v1.4.2` hôm nay và `:v1.4.2` tuần sau có thể ra hai binary khác nhau
       nếu ai đó push đè. Với digest, `helm rollback` đưa bạn về đúng BIT đã
       chạy, không phải "cái mà tag đó đang trỏ tới lúc này".

    2. KHÔNG tạo Secret. values-prod.yaml đặt secrets.create=false và dùng
       External Secrets Operator kéo từ AWS Secrets Manager. Script chỉ KIỂM TRA
       rằng ExternalSecret đã sync xong — vì nếu chưa, pod sẽ kẹt ở
       CreateContainerConfigError và thông báo lỗi không hề nhắc tới ESO.

    3. KHÔNG --atomic theo mặc định. --atomic tự rollback khi --wait hết giờ.
       Ở prod điều đó NGUY HIỂM: một rollout chậm (kéo image lớn, node scale
       chậm) bị hiểu nhầm là hỏng, và Helm rollback ngay giữa lúc bạn đang có
       hai phiên bản chạy song song. Mặc định ở đây là deploy rồi QUAN SÁT, và
       người vận hành tự quyết `helm rollback`. Bật -Atomic nếu bạn thật sự muốn.

    4. BẮT buộc xác nhận context. Nhầm cluster là lỗi đắt nhất trong nghề.

.PARAMETER ImageTag
    Tag trên registry để phân giải ra digest (vd 'v1.4.2' hoặc SHA commit).
    Bỏ qua nếu bạn truyền -Digests trực tiếp.

.PARAMETER Digests
    Hashtable digest dựng sẵn, dùng trong CI khi bước build đã biết digest:
      -Digests @{ 'order-service'='sha256:...'; 'matching-engine'='sha256:...' }

.PARAMETER DryRun
    Render + diff, không đụng cluster. LUÔN chạy cái này trước khi deploy thật.

.PARAMETER Atomic
    Bật tự động rollback khi --wait hết giờ. Đọc mục 3 ở trên trước khi dùng.

.EXAMPLE
    .\scripts\helm\Deploy-Prod.ps1 -ImageTag v1.4.2 -DryRun
    .\scripts\helm\Deploy-Prod.ps1 -ImageTag v1.4.2
    .\scripts\helm\Deploy-Prod.ps1 -Rollback
#>
#Requires -Version 5.1
[CmdletBinding()]
param(
    [string]    $Release      = 'trading',
    [string]    $Namespace    = 'trading-prod',
    [string]    $Registry     = 'ghcr.io/trading-system',
    [string]    $ImageTag     = '',
    [hashtable] $Digests      = @{},
    [string[]]  $Modules      = @('market-gateway','pricing-risk-service','order-service','matching-engine'),
    [switch]    $DryRun,
    [switch]    $Atomic,
    [switch]    $Rollback,
    [int]       $RollbackTo   = 0,
    [switch]    $Yes,
    [int]       $TimeoutSec   = 900
)
. (Join-Path $PSScriptRoot '_Common.ps1')

$RepoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$ChartDir = Join-Path $RepoRoot 'helm\trading-system'
$ValuesProd = Join-Path $ChartDir 'values-prod.yaml'

if (-not (Test-Path $ValuesProd)) { throw "Khong thay $ValuesProd" }

# Tên biến -> tên module, dùng ở cả phân giải digest lẫn --set.
$ValueKey = @{
    'market-gateway'       = 'marketGateway'
    'pricing-risk-service' = 'pricingRiskService'
    'order-service'        = 'orderService'
    'matching-engine'      = 'matchingEngine'
}

Write-Host ''
Write-Host "  trading-system -> PRODUCTION   release=$Release  ns=$Namespace" -ForegroundColor White

# ===========================================================================
Write-Step 'Xac nhan cluster'

# Prod luon la cluster that (EKS/GKE/AKS) - KHONG tu dong khoi dong gi ca.
# Cluster khong voi toi duoc thi dung han va noi ro, thay vi de cac lenh tham do
# ben duoi tra ve RONG roi bao nham la "thieu KEDA".
Assert-ClusterReachable -Target cloud

$ctx = kubectl config current-context
Assert-LastExit 'khong doc duoc kubecontext'
Write-Host ''
Write-Host "    CONTEXT: $ctx" -ForegroundColor Yellow
Write-Host "    NAMESPACE: $Namespace" -ForegroundColor Yellow
kubectl get nodes -o wide
Assert-LastExit 'khong goi duoc API server'

if (-not $Yes -and -not $DryRun) {
    Write-Host ''
    $ans = Read-Host "    Go dung ten context de xac nhan day la PROD"
    if ($ans -ne $ctx) { throw "Khong khop ('$ans' != '$ctx') - dung lai." }
}

# ===========================================================================
if ($Rollback) {
    Write-Step 'Rollback'

    helm -n $Namespace history $Release
    Assert-LastExit 'khong doc duoc lich su release'

    if ($RollbackTo -eq 0) {
        Write-Info 'Khong truyen -RollbackTo: quay ve revision LIEN TRUOC.'
    }
    if (-not $Yes) {
        $ans = Read-Host "    Go 'rollback' de xac nhan"
        if ($ans -ne 'rollback') { throw 'Dung lai.' }
    }

    $rbArgs = @('rollback', $Release)
    if ($RollbackTo -gt 0) { $rbArgs += "$RollbackTo" }
    $rbArgs += @('-n', $Namespace, '--wait', '--timeout', "${TimeoutSec}s")
    & helm @rbArgs
    Assert-LastExit 'rollback that bai'

    Write-Ok 'Rollback xong.'
    kubectl -n $Namespace get pods
    exit 0
}

# ===========================================================================
Write-Step 'Preflight: CRD va operator bat buoc'

$hard = @()
if (-not (kubectl get crd scaledobjects.keda.sh --ignore-not-found -o name)) {
    $hard += 'KEDA chua cai - values-prod.yaml co keda.enabled=true'
}
if (-not (kubectl get crd podmonitors.monitoring.coreos.com --ignore-not-found -o name)) {
    $hard += 'Prometheus Operator chua cai - values-prod.yaml co monitoring.enabled=true'
}
if (-not (kubectl get crd externalsecrets.external-secrets.io --ignore-not-found -o name)) {
    $hard += 'External Secrets Operator chua cai - values-prod.yaml co externalSecrets.enabled=true'
}
if ($hard.Count -gt 0) {
    $hard | ForEach-Object { Write-Bad "  - $_" }
    throw 'Thieu thanh phan cluster-wide. helm se chet ngay o buoc render.'
}
Write-Ok 'KEDA + Prometheus Operator + ESO: co du.'

# ClusterSecretStore la cau noi giua ESO va AWS Secrets Manager. Thieu no thi
# ExternalSecret duoc tao nhung khong bao gio sync, pod ket o
# CreateContainerConfigError va thong bao loi KHONG he nhac toi ESO.
$storeName = (Select-String -Path $ValuesProd -Pattern '^\s+name:\s*(\S+)\s*$' -Context 1,0 |
              Where-Object { $_.Context.PreContext[0] -match 'secretStoreRef' } |
              ForEach-Object { $_.Matches.Groups[1].Value }) | Select-Object -First 1
if (-not $storeName) { $storeName = 'aws-secrets-manager' }

$store = kubectl get clustersecretstore $storeName --ignore-not-found -o name
if (-not $store) {
    Write-Bad "ClusterSecretStore '$storeName' KHONG ton tai."
    Write-Bad '  ExternalSecret se khong bao gio sync -> pod ket o CreateContainerConfigError.'
    Write-Bad '  Tao no truoc (xem HELM-DEPLOY.md muc 5.2), hoac sua'
    Write-Bad '  externalSecrets.secretStoreRef.name trong values-prod.yaml.'
    if (-not $DryRun) { throw 'Dung lai.' }
} else {
    $ready = kubectl get clustersecretstore $storeName -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>$null
    if ($ready -eq 'True') { Write-Ok "ClusterSecretStore '$storeName': Ready." }
    else { Write-Warn "ClusterSecretStore '$storeName' ton tai nhung Ready=$ready - kiem tra IAM role." }
}

# ---------------------------------------------------------------------------
Write-Step 'Preflight: gia tri placeholder trong values-prod.yaml'

# values-prod.yaml duoc ship kem cac gia tri gia. Deploy nguyen xi la hong, va
# hong theo kieu kho doan: DB_HOST gia -> init container wait-for-db quay mai;
# digest toan so 0 -> ImagePullBackOff.
$prodText = Get-Content $ValuesProd -Raw
$placeholders = @()
if ($prodText -match 'abcdefg\.ap-southeast-1\.rds\.amazonaws\.com') {
    $placeholders += 'config.DB_HOST van la endpoint RDS gia (abcdefg...)'
}
if ($prodText -match 'trading\.example\.com') {
    $placeholders += 'ingress.host van la trading.example.com'
}
if ($prodText -match 'sha256:0{64}') {
    $placeholders += 'image.digest van la sha256:000...0 (se duoc ghi de boi script nay)'
}
if ($placeholders.Count -gt 0) {
    Write-Warn 'values-prod.yaml con gia tri mau:'
    $placeholders | ForEach-Object { Write-Warn "  - $_" }
    Write-Warn 'Digest se duoc script ghi de. DB_HOST va ingress.host thi KHONG -'
    Write-Warn 'phai sua trong file hoac truyen --set.'
}

# ---------------------------------------------------------------------------
Write-Step 'Phan giai tag -> digest'

# `docker buildx imagetools inspect` doc manifest tu registry ma KHONG pull
# image ve. Dung duoc voi moi registry chuan OCI (GHCR, ECR, Docker Hub).
if ($Digests.Count -eq 0) {
    if (-not $ImageTag) { throw 'Phai truyen -ImageTag hoac -Digests.' }
    if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
        throw 'Khong co docker de phan giai digest. Truyen thang -Digests tu CI.'
    }

    foreach ($m in $Modules) {
        $ref = "$Registry/${m}:$ImageTag"
        Write-Info "inspect $ref"
        $out = docker buildx imagetools inspect $ref 2>&1 | Out-String
        if ($LASTEXITCODE -ne 0) {
            Write-Bad $out.Trim()
            throw "Khong doc duoc manifest cua $ref. Image da push chua? Da docker login chua?"
        }
        if ($out -match 'Digest:\s*(sha256:[0-9a-f]{64})') {
            $Digests[$m] = $Matches[1]
            Write-Ok ("  {0,-22} {1}" -f $m, $Matches[1])
        } else {
            throw "Khong tim thay Digest trong output cua $ref"
        }
    }
} else {
    foreach ($m in $Modules) {
        if (-not $Digests[$m]) { throw "-Digests thieu module '$m'." }
        if ($Digests[$m] -notmatch '^sha256:[0-9a-f]{64}$') { throw "Digest cua '$m' sai dinh dang." }
        Write-Ok ("  {0,-22} {1}" -f $m, $Digests[$m])
    }
}

# ---------------------------------------------------------------------------
Write-Step 'Dung lenh helm'

$helmArgs = @('upgrade','--install', $Release, $ChartDir, '--namespace', $Namespace, '-f', $ValuesProd)
foreach ($m in $Modules) {
    $helmArgs += @('--set', ("{0}.image.digest={1}" -f $ValueKey[$m], $Digests[$m]))
}

# Nhan lai phien ban ung dung vao release de `helm list` doc duoc, va de
# `helm history` noi duoc tung revision ung voi tag nao.
if ($ImageTag) { $helmArgs += @('--description', "deploy $ImageTag") }

if ($DryRun) {
    Write-Info 'DryRun: render + so sanh voi release dang chay.'
    & helm @($helmArgs + @('--dry-run'))
    $rc = $LASTEXITCODE

    # helm diff la plugin, khong phai lenh built-in. Co thi dung, khong co thi
    # bao cach cai — no la cong cu dang gia nhat truoc moi lan deploy prod.
    if (helm plugin list 2>$null | Select-String -Quiet 'diff') {
        Write-Host ''
        Write-Info '=== helm diff upgrade (thay doi thuc su so voi dang chay) ==='
        & helm @(@('diff','upgrade', $Release, $ChartDir, '--namespace', $Namespace, '-f', $ValuesProd) +
                 ($Modules | ForEach-Object { @('--set', ("{0}.image.digest={1}" -f $ValueKey[$_], $Digests[$_])) }))
    } else {
        Write-Warn 'Chua co plugin helm-diff. Rat nen cai truoc khi deploy prod:'
        Write-Warn '  helm plugin install https://github.com/databus23/helm-diff'
    }

    Write-Host ''
    if ($rc -eq 0) { Write-Ok 'DryRun xong - khong co gi duoc apply.' } else { Write-Bad "Render that bai (exit $rc)." }
    exit $rc
}

$helmArgs += @('--wait','--timeout', "${TimeoutSec}s")
if ($Atomic) {
    Write-Warn '-Atomic: helm se TU DONG rollback neu --wait het gio.'
    Write-Warn '  Mot rollout cham (keo image lon, node scale cham) cung bi coi la hong.'
    $helmArgs += '--atomic'
}

Write-Host ''
Write-Info ("helm " + ($helmArgs -join ' '))

if (-not $Yes) {
    Write-Host ''
    $ans = Read-Host "    Go 'deploy' de thuc thi"
    if ($ans -ne 'deploy') { throw 'Dung lai.' }
}

# ---------------------------------------------------------------------------
Write-Step 'Deploy'

$prevRev = (helm -n $Namespace list -o json 2>$null | ConvertFrom-Json |
            Where-Object { $_.name -eq $Release } | Select-Object -First 1).revision
if ($prevRev) { Write-Info "Revision hien tai: $prevRev (dung so nay neu can rollback)." }

$t0 = Get-Date
& helm @helmArgs
$helmExit = $LASTEXITCODE
$elapsed = [int]((Get-Date) - $t0).TotalSeconds

if ($helmExit -ne 0) {
    Write-Host ''
    Write-Bad "helm that bai sau ${elapsed}s (exit $helmExit)."
    Write-Warn 'Release co the dang o trang thai pending/failed. Kiem tra TRUOC khi lam gi tiep:'
    Write-Warn "  helm -n $Namespace status $Release"
    Write-Warn "  helm -n $Namespace history $Release"
    Write-Warn "  kubectl -n $Namespace get pods"
    Write-Warn "  kubectl -n $Namespace get externalsecret trading-secrets -o yaml"
    Write-Warn ''
    Write-Warn 'Quay lai ban cu:'
    Write-Warn "  .\scripts\helm\Deploy-Prod.ps1 -Rollback$(if($prevRev){" -RollbackTo $prevRev"})"
    exit $helmExit
}
Write-Ok "helm upgrade thanh cong sau ${elapsed}s."

# ---------------------------------------------------------------------------
Write-Step 'Kiem tra sau deploy'

kubectl -n $Namespace get pods -o wide
Write-Host ''

# Xac nhan image DANG CHAY dung bang digest vua deploy. Day la buoc bat duoc
# truong hop pod cu chua bi thay the (vi PDB chan, vi quota, vi node het cho).
Write-Info 'Image thuc su dang chay:'
foreach ($m in $Modules) {
    $running = kubectl -n $Namespace get pods -l "app.kubernetes.io/name=$m" `
        -o jsonpath='{range .items[*]}{.spec.containers[0].image}{"\n"}{end}' 2>$null
    $uniq = ($running -split "`n" | Where-Object { $_ } | Select-Object -Unique)
    foreach ($img in $uniq) {
        if ($img -match [regex]::Escape($Digests[$m])) { Write-Ok  ("  {0,-22} {1}" -f $m, $img) }
        else                                           { Write-Bad ("  {0,-22} {1}  <- KHONG khop digest vua deploy" -f $m, $img) }
    }
}

Write-Host ''
Write-Info 'ExternalSecret:'
kubectl -n $Namespace get externalsecret 2>$null
Write-Info 'KEDA:'
kubectl -n $Namespace get scaledobject 2>$null
Write-Info 'PodDisruptionBudget (ALLOWED DISRUPTIONS phai > 0, neu = 0 thi drain node se treo):'
kubectl -n $Namespace get pdb 2>$null

Write-Host ''
Write-Host "    Theo doi 15 phut toi truoc khi coi la xong:" -ForegroundColor White
Write-Host "      kubectl -n $Namespace get pods -w"
Write-Host "      kubectl -n $Namespace logs -l app.kubernetes.io/name=order-service -f --tail=50"
Write-Host "      kubectl -n $Namespace get scaledobject -w"
Write-Host ""
Write-Host "    Quay lai ban cu neu co su co:" -ForegroundColor White
Write-Host "      .\scripts\helm\Deploy-Prod.ps1 -Rollback$(if($prevRev){" -RollbackTo $prevRev"})"
Write-Host ''
Write-Ok 'Deploy prod hoan tat.'
