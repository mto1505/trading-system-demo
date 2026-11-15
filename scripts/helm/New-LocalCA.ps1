<#
.SYNOPSIS
    Tự tạo CA nội bộ (ngoài cert-manager) và nạp nó vào cluster làm Issuer.

.DESCRIPTION
    Vì sao nên tự tạo CA thay vì để cert-manager sinh:

    CA do cert-manager sinh (chế độ `useCA: true`) gắn với vòng đời Helm release.
    `helm uninstall` rồi cài lại, hoặc xoá namespace, là ra một CA MỚI với
    thumbprint MỚI — và cái bạn đã cài vào kho tin cậy của Windows thành vô dụng.
    Mỗi lần dựng lại cluster lại phải cài CA một lần nữa.

    CA tự tạo sống trên ĐĨA, độc lập hoàn toàn với Helm và với cluster:
      - cài vào Windows MỘT lần, dùng mãi
      - dựng lại cluster, đổi namespace, đổi cluster khác -> vẫn CA đó
      - sao lưu được, chia cho máy khác trong team được

    Script sinh ra ba thứ trong helm\.ca\ (đã gitignore):
      ca.key  khoá riêng  — BÍ MẬT, đừng commit, đừng gửi đi đâu
      ca.crt  chứng chỉ CA — công khai, đây là file cài vào Windows
      ca.srl  bộ đếm serial của openssl

    Rồi tạo Secret kubernetes.io/tls trong namespace đích để cert-manager dùng.
    Secret đó KHÔNG do Helm quản nên `helm uninstall` không xoá nó.

.PARAMETER Force
    Ghi đè CA đang có. Mặc định script TỪ CHỐI ghi đè — sinh CA mới là làm hỏng
    mọi chứng chỉ đã cấp và mọi kho tin cậy đã cài.

.PARAMETER CommonName
    Tên hiển thị của CA. Đây là chuỗi bạn sẽ thấy ở "Issued By" trong Chrome.

.PARAMETER Days
    Hạn của CA. Mặc định 10 năm — CA nội bộ dùng cho dev thì hạn dài là hợp lý,
    vì hết hạn nghĩa là phải cài lại vào mọi máy.

.EXAMPLE
    .\scripts\helm\New-LocalCA.ps1
    .\scripts\helm\New-LocalCA.ps1 -CommonName "TX Tech Dev CA" -Days 3650
    .\scripts\helm\New-LocalCA.ps1 -Force          # tao CA moi, bo cai cu
#>
#Requires -Version 5.1
[CmdletBinding()]
param(
    [string] $Namespace  = 'trading-staging',
    [string] $SecretName = 'trading-ca',
    [string] $CommonName = 'trading-local-ca',
    [int]    $Days       = 3650,
    [ValidateSet('ecdsa','rsa')]
    [string] $Algorithm  = 'ecdsa',
    [string] $CaDir      = '',
    [switch] $Force
)

. (Join-Path $PSScriptRoot '_Common.ps1')

$RepoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
if (-not $CaDir) { $CaDir = Join-Path $RepoRoot 'helm\.ca' }
$KeyFile = Join-Path $CaDir 'ca.key'
$CrtFile = Join-Path $CaDir 'ca.crt'

# ---------------------------------------------------------------------------
Write-Step 'Tim openssl'

# openssl KHONG nam trong PATH cua PowerShell tren Windows, nhung Git for Windows
# luon ship mot ban. Tim theo thu tu: PATH -> cac vi tri chuan cua Git.
$openssl = (Get-Command openssl -ErrorAction SilentlyContinue).Source
if (-not $openssl) {
    $openssl = @(
        "$env:ProgramFiles\Git\usr\bin\openssl.exe",
        "$env:ProgramFiles\Git\mingw64\bin\openssl.exe",
        "${env:ProgramFiles(x86)}\Git\usr\bin\openssl.exe"
    ) | Where-Object { Test-Path $_ } | Select-Object -First 1
}
if (-not $openssl) { throw 'Khong tim thay openssl. Cai Git for Windows hoac them openssl vao PATH.' }
Write-Info $openssl
Write-Info (& $openssl version)

# ---------------------------------------------------------------------------
Write-Step 'Sinh CA'

if (-not (Test-Path $CaDir)) { New-Item -ItemType Directory -Path $CaDir -Force | Out-Null }

if ((Test-Path $CrtFile) -and -not $Force) {
    Write-Info 'Da co CA tren dia - DUNG LAI (khong sinh moi).'
    Write-Warn 'Sinh CA moi se lam MOI chung chi da cap tro nen khong hop le, va'
    Write-Warn 'moi may da cai CA cu phai cai lai. Dung -Force neu that su muon.'
} else {
    if (Test-Path $CrtFile) {
        $old = New-Object System.Security.Cryptography.X509Certificates.X509Certificate2 ((Resolve-Path $CrtFile).Path)
        Write-Warn "-Force: dang GHI DE CA cu  $($old.Subject)  $($old.Thumbprint)"
        Write-Warn 'Moi chung chi do CA cu ky se khong con hop le.'
    }

    # DUNG `genpkey`, KHONG dung `ecparam -genkey`.
    #
    # `openssl ecparam -genkey` ghi RA HAI khoi PEM: "EC PARAMETERS" roi moi toi
    # "EC PRIVATE KEY". cert-manager doc khoi DAU TIEN va chet voi:
    #     ErrGetKeyPair: unknown private key type: EC PARAMETERS
    # Issuer dung mai o Ready=False con Certificate thi khong bao gio duoc ky.
    #
    # `genpkey` luon xuat dung MOT khoi PKCS#8 ("BEGIN PRIVATE KEY") - dinh dang
    # ma cert-manager doc duoc cho ca EC lan RSA.
    if ($Algorithm -eq 'ecdsa') {
        & $openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out $KeyFile 2>$null
    } else {
        & $openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:4096 -out $KeyFile 2>$null
    }
    Assert-LastExit 'sinh khoa rieng that bai'

    # -x509 + -days => chung chi tu ky.
    # basicConstraints=critical,CA:true  <- TRUONG QUYET DINH: chi chung chi co
    #   co nay moi ky duoc cho chung chi khac. Thieu no thi cert-manager se tao
    #   Issuer thanh cong nhung MOI lan ky deu that bai.
    # pathlen:0 => CA nay chi ky duoc cho cert la (khong ky duoc CA con).
    # keyUsage keyCertSign,cRLSign <- quyen ky chung chi va ky danh sach thu hoi.
    $cnf = Join-Path $CaDir 'ca.cnf'
    @(
        '[req]',
        'distinguished_name = dn',
        'prompt = no',
        'x509_extensions = v3_ca',
        '',
        '[dn]',
        "CN = $CommonName",
        '',
        '[v3_ca]',
        'basicConstraints = critical,CA:true,pathlen:0',
        'keyUsage = critical,keyCertSign,cRLSign',
        'subjectKeyIdentifier = hash'
    ) | Set-Content -Path $cnf -Encoding ascii

    & $openssl req -x509 -new -key $KeyFile -sha256 -days $Days -out $CrtFile -config $cnf 2>$null
    Assert-LastExit 'sinh chung chi CA that bai'
    Remove-Item $cnf -ErrorAction SilentlyContinue

    Write-Ok "Da sinh CA moi ($Algorithm, $Days ngay)."
}

$ca = New-Object System.Security.Cryptography.X509Certificates.X509Certificate2 ((Resolve-Path $CrtFile).Path)
Write-Info "Subject   : $($ca.Subject)"
Write-Info "Thumbprint: $($ca.Thumbprint)"
Write-Info "Het han   : $($ca.NotAfter.ToString('yyyy-MM-dd')) ($([int]($ca.NotAfter - (Get-Date)).TotalDays) ngay nua)"
$bc = $ca.Extensions | Where-Object { $_.Oid.Value -eq '2.5.29.19' }
if (-not ($bc -and $bc.CertificateAuthority)) { throw 'Chung chi sinh ra KHONG co CA:TRUE - sai cau hinh openssl.' }
Write-Info "CA:TRUE   : co"

Write-Host ''
Write-Warn "KHOA RIENG nam o: $KeyFile"
Write-Warn 'Dung commit, dung gui qua chat/mail. Ai co file nay ky duoc cert cho'
Write-Warn 'MOI ten mien va moi may da cai CA nay se tin.'

# ---------------------------------------------------------------------------
Write-Step "Nap vao cluster (namespace $Namespace)"

Assert-ClusterReachable -Target minikube

# Issuer la namespaced nen cert-manager CHI tim Secret trong CUNG namespace voi
# no. Tao Secret o namespace khac thi Issuer se bao khong thay.
kubectl get namespace $Namespace *> $null
if ($LASTEXITCODE -ne 0) {
    Write-Warn "Namespace '$Namespace' chua ton tai - tao truoc."
    kubectl create namespace $Namespace | Out-Null
    Assert-LastExit "tao namespace $Namespace that bai"
}

# --dry-run=client | apply: idempotent, va cap nhat duoc Secret da ton tai.
kubectl create secret tls $SecretName --cert=$CrtFile --key=$KeyFile `
    -n $Namespace --dry-run=client -o yaml | kubectl apply -f - | Out-Null
Assert-LastExit "tao secret $SecretName that bai"
Write-Ok "secret/$SecretName (kubernetes.io/tls) da san sang trong $Namespace."

Write-Info 'Secret nay KHONG do Helm quan ly -> `helm uninstall` se khong xoa no.'

# ---------------------------------------------------------------------------
Write-Step 'Buoc tiep theo'

Write-Host @"
    1. Tro chart vao CA nay — sua helm\trading-system\values-staging-local.yaml:

           ingress:
             tls:
               enabled: true
               secretName: trading-tls
               selfSignedIssuer:
                 enabled: true
                 name: trading-local-issuer
                 existingCaSecret: $SecretName      # <-- them dong nay

       Dat existingCaSecret thi chart BO QUA useCA: no khong sinh CA nua, chi
       tao Issuer kind: ca tro vao secret/$SecretName.

    2. Deploy lai:

           .\scripts\helm\Deploy-Staging.ps1 -SkipBuild

    3. Cai CA vao Windows (lam MOT lan, dung mai):

           certutil -addstore -f -user Root "$CrtFile"

       Hoac dung script cho tien (no tu lay CA dang duoc Issuer dung):

           .\scripts\helm\Trust-LocalCA.ps1

    4. Kiem tra:

           .\scripts\helm\Verify-Release.ps1 -Namespace $Namespace

       Trong ket qua, "issuer" cua cert phai la: CN=$CommonName

    SAO LUU: giu ca helm\.ca\ca.key va ca.crt o cho an toan. Mat ca.key thi
    khong ky tiep duoc va phai lam lai tu dau (ke ca cai lai vao moi may).
"@
Write-Host ''
Write-Ok 'Xong.'
