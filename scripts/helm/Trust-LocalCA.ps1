<#
.SYNOPSIS
    Nạp CA nội bộ (do cert-manager sinh) vào kho tin cậy của Windows, rồi mở
    Chrome vào Ingress của staging với ổ khoá xanh.

.DESCRIPTION
    Chrome trên Windows KHÔNG có kho chứng chỉ riêng — nó đọc thẳng kho của hệ
    điều hành. Vì vậy chỉ cần đưa CA vào "Trusted Root Certification Authorities"
    là Chrome, Edge, .NET và PowerShell đều tin theo.

    Script dùng kho của NGƯỜI DÙNG HIỆN TẠI (Cert:\CurrentUser\Root), KHÔNG phải
    kho máy. Hai lý do:
      - không cần quyền Administrator
      - phạm vi ảnh hưởng gói trong đúng tài khoản này, gỡ ra cũng sạch hơn

    VỀ MẶT AN TOÀN — đọc trước khi chạy:
    Một CA trong kho Root ký được chứng chỉ cho BẤT KỲ tên miền nào, và máy này
    sẽ tin. Khoá riêng của CA nằm trong Secret `<issuer>-ca-key-pair` của cluster;
    ai đọc được Secret đó thì giả mạo được mọi site đối với máy này. Chấp nhận
    được cho một cluster local dùng một mình — KHÔNG nên làm trên máy dùng chung,
    và nên gỡ khi không còn cần:

        .\scripts\helm\Trust-LocalCA.ps1 -Uninstall

.PARAMETER Uninstall
    Gỡ CA khỏi kho tin cậy và xoá file ca-local.crt.

.PARAMETER OpenChrome
    Sau khi cài, mở luôn port-forward 443 và Chrome vào host của Ingress.

    KHÔNG cần sửa file hosts: Chrome được bật kèm --host-resolver-rules để tự
    ánh xạ host về 127.0.0.1. Đổi lại, cách này chỉ áp dụng cho CỬA SỔ Chrome do
    script mở. Muốn mọi trình duyệt và mọi công cụ dùng được thì thêm dòng vào
    hosts (cần Administrator) — script in sẵn lệnh ở cuối.

.PARAMETER HostsLine
    Chỉ in ra lệnh thêm hosts rồi thoát, không đụng gì khác.

.EXAMPLE
    .\scripts\helm\Trust-LocalCA.ps1
    .\scripts\helm\Trust-LocalCA.ps1 -OpenChrome
    .\scripts\helm\Trust-LocalCA.ps1 -Uninstall
#>
#Requires -Version 5.1
[CmdletBinding()]
param(
    [string] $Namespace = 'trading-staging',
    [string] $Release   = 'trading',
    [switch] $OpenChrome,
    [switch] $Uninstall,
    [switch] $HostsLine,
    [int]    $Port      = 443
)

. (Join-Path $PSScriptRoot '_Common.ps1')

$RepoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$CaFile   = Join-Path $RepoRoot 'ca-local.crt'
$StoreUri = 'Cert:\CurrentUser\Root'

function Get-InstalledCa {
    param([string]$Subject = '*trading-local-ca*')
    Get-ChildItem $StoreUri -ErrorAction SilentlyContinue |
        Where-Object { $_.Subject -like $Subject }
}

# ===========================================================================
if ($Uninstall) {
    Write-Step 'Go CA khoi kho tin cay'

    $found = Get-InstalledCa
    if (-not $found) {
        Write-Info 'Khong co CA nao ten trading-local-ca trong kho - khong phai go.'
    } else {
        foreach ($c in $found) {
            Write-Info "Go $($c.Subject)  $($c.Thumbprint)"
            # certutil -delstore chap nhan thumbprint; Remove-Item tren provider
            # Cert: cung duoc nhung hay vuong UI prompt y nhu luc cai.
            certutil -delstore -user Root $c.Thumbprint *> $null
            if ($LASTEXITCODE -ne 0) { Write-Warn "  certutil tra ve $LASTEXITCODE" }
        }
        if (Get-InstalledCa) { Write-Bad 'VAN CON trong kho - go tay bang certmgr.msc' }
        else { Write-Ok 'Da go khoi kho tin cay.' }
    }

    if (Test-Path $CaFile) { Remove-Item $CaFile -Force; Write-Ok "Da xoa $CaFile" }
    Write-Host ''
    Write-Info 'Chrome giu cache quyet dinh TLS - dong het cua so Chrome roi mo lai'
    Write-Info 'thi moi thay no quay lai bao "khong tin cay".'
    exit 0
}

# ===========================================================================
Write-Step 'Lay CA tu cluster'

Assert-ClusterReachable -Target minikube

# Ten Secret CA = <ten issuer>-ca-key-pair. Doc ten issuer thang tu Ingress thay
# vi doan, de script van dung khi ban doi selfSignedIssuer.name trong values.
$issuerName = kubectl -n $Namespace get ingress trading-ingress `
    -o jsonpath='{.metadata.annotations.cert-manager\.io/issuer}' 2>$null
if (-not $issuerName) {
    Write-Bad "Ingress trong namespace '$Namespace' khong co annotation cert-manager.io/issuer."
    Write-Info 'TLS tu dong chua duoc bat. Trong values-staging-local.yaml can:'
    Write-Info '  ingress.tls.enabled=true  va  ingress.tls.selfSignedIssuer.enabled=true'
    throw 'Chua co Issuer de lay CA.'
}
Write-Info "Issuer: $issuerName"

# Ten Secret CA doc THANG tu spec cua Issuer, KHONG doan theo quy uoc dat ten.
# Ly do: co hai che do va ten khac han nhau —
#   cert-manager tu sinh  -> <issuer>-ca-key-pair
#   CA tu mang            -> ten bat ky do ban dat (vd trading-ca)
# Doan theo quy uoc thi script chet ngay khi doi sang CA tu mang.
$caSecret = kubectl -n $Namespace get issuer $issuerName -o jsonpath='{.spec.ca.secretName}' 2>$null
if (-not $caSecret) {
    Write-Bad "Issuer '$issuerName' khong phai kieu ca (khong co spec.ca.secretName)."
    Write-Info 'Dang chay o che do tu ky thang (useCA=false)? Khi do KHONG co CA chung'
    Write-Info 'de cai - moi cert la mot goc rieng. Dat useCA: true roi deploy lai.'
    throw 'Issuer khong co CA de lay.'
}
Write-Info "Secret CA: $caSecret"

$b64 = kubectl -n $Namespace get secret $caSecret -o jsonpath='{.data.tls\.crt}' 2>$null
if (-not $b64) { throw "Khong doc duoc secret/$caSecret trong namespace $Namespace." }

Set-Content -Path $CaFile -Value ([Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($b64))) -Encoding ascii -NoNewline
$cert = New-Object System.Security.Cryptography.X509Certificates.X509Certificate2 ((Resolve-Path $CaFile).Path)
Write-Ok "Da ghi $CaFile"
Write-Info "Subject   : $($cert.Subject)"
Write-Info "Thumbprint: $($cert.Thumbprint)"
Write-Info "Het han   : $($cert.NotAfter.ToString('yyyy-MM-dd')) ($([int]($cert.NotAfter - (Get-Date)).TotalDays) ngay nua)"

$bc = $cert.Extensions | Where-Object { $_.Oid.Value -eq '2.5.29.19' }
if (-not ($bc -and $bc.CertificateAuthority)) {
    Write-Bad 'Chung chi nay KHONG phai CA (basicConstraints CA:FALSE).'
    throw 'Cai mot cert khong phai CA vao kho Root la vo nghia.'
}

# ---------------------------------------------------------------------------
Write-Step 'Nap vao kho tin cay cua Windows'

$already = Get-ChildItem $StoreUri -ErrorAction SilentlyContinue |
           Where-Object { $_.Thumbprint -eq $cert.Thumbprint }
if ($already) {
    Write-Ok 'Da co san trong kho (dung thumbprint) - khong cai lai.'
} else {
    # Import-Certificate / Remove-Item tren provider Cert: deu goi API co hien
    # hop thoai canh bao bao mat. Trong phien KHONG tuong tac no that bai voi
    # "UI is not allowed in this operation". certutil -f bo qua hop thoai do.
    certutil -addstore -f -user Root $CaFile *> $null
    if ($LASTEXITCODE -ne 0) { throw "certutil -addstore that bai (exit $LASTEXITCODE)" }

    if (Get-ChildItem $StoreUri -ErrorAction SilentlyContinue |
        Where-Object { $_.Thumbprint -eq $cert.Thumbprint }) {
        Write-Ok 'Da nap vao Trusted Root Certification Authorities (CurrentUser).'
    } else {
        throw 'certutil bao thanh cong nhung khong thay cert trong kho.'
    }
}

Write-Warn 'CA nay ky duoc cert cho BAT KY ten mien nao va may nay se tin.'
Write-Warn 'Go khi khong con can:  .\scripts\helm\Trust-LocalCA.ps1 -Uninstall'

# ---------------------------------------------------------------------------
$ingHost = kubectl -n $Namespace get ingress trading-ingress -o jsonpath='{.spec.rules[0].host}' 2>$null
$url = "https://$ingHost" + $(if ($Port -ne 443) { ":$Port" } else { '' })

Write-Step 'Kiem chung bang kho tin cay (khong truyen --cacert)'
# Cong co the DA bi mot port-forward truoc do giu. Khi do Start-Process se thoat
# ngay va thong bao "Khong bind duoc cong" lai che mat nguyen nhan that.
# Phat hien truoc, va DUNG LAI cai dang chay thay vi bao loi.
$owned = $false
$listening = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue |
             Select-Object -First 1
if ($listening) {
    $who = (Get-Process -Id $listening.OwningProcess -ErrorAction SilentlyContinue).ProcessName
    Write-Info "Cong $Port da co '$who' (PID $($listening.OwningProcess)) nghe - dung lai, khong mo them."
    $pf = $null
} else {
    $pf = Start-Process kubectl -PassThru -WindowStyle Hidden `
          -ArgumentList '-n','ingress-nginx','port-forward','svc/ingress-nginx-controller',"${Port}:443"
    $owned = $true
    Start-Sleep -Seconds 6
}
try {
    if ($owned -and $pf.HasExited) {
        Write-Bad "kubectl port-forward thoat ngay - khong bind duoc cong $Port."
        Write-Info "  Get-NetTCPConnection -LocalPort $Port -State Listen   # xem ai dang giu"
        throw "Khong bind duoc cong $Port."
    }
    if ($pf.HasExited) { throw "Khong bind duoc cong $Port." }

    # --ssl-no-revoke: curl cua Windows dung Schannel, voi doi kiem tra thu hoi
    # (CRL/OCSP). CA tu ky khong co diem phan phoi CRL nen Schannel tu choi voi
    # "the revocation status is unknown" - KHONG phai loi chung chi.
    # Chrome khong hard-fail nhu vay nen no van hien o khoa xanh.
    $code = curl.exe -s --ssl-no-revoke --resolve "${ingHost}:${Port}:127.0.0.1" `
                "$url/api/v1/orders" -o NUL -w '%{http_code}' 2>$null
    if ($code -eq '000') {
        Write-Bad 'TLS van bi tu choi - kho tin cay chua an.'
        Write-Info '  certmgr.msc -> Trusted Root Certification Authorities -> Certificates'
    } else {
        Write-Ok "TLS xac thuc QUA bang kho Windows (HTTP $code tu ung dung)."
        Write-Info '  401 = da vao toi app, thieu X-API-KEY. Dung nghia la TLS da dat.'
    }

    if ($OpenChrome) {
        Write-Step 'Mo Chrome'
        $chrome = @(
            "$env:ProgramFiles\Google\Chrome\Application\chrome.exe",
            "${env:ProgramFiles(x86)}\Google\Chrome\Application\chrome.exe",
            "$env:LOCALAPPDATA\Google\Chrome\Application\chrome.exe"
        ) | Where-Object { Test-Path $_ } | Select-Object -First 1

        if (-not $chrome) { Write-Bad 'Khong tim thay chrome.exe.' }
        else {
            # --host-resolver-rules thay cho viec sua file hosts (can admin).
            #
            # CU PHAP: "MAP <host> <ip>" - KHONG them ":<port>" vao ve phai.
            # Dang "MAP host 127.0.0.1:443" KHONG hoat dong: Chrome bao
            # "This site can't be reached" du port-forward van song va curl
            # qua cung duong do van tra 200/401. Da kiem chung tren may nay.
            # Vi vay port-forward PHAI o dung 443.
            # --user-data-dir BAT BUOC: khong co no, Chrome dang chay se chi mo
            # them mot tab trong tien trinh cu va BO QUA moi co dong lenh.
            $profileDir = Join-Path $env:TEMP 'chrome-trading-staging'
            Write-Info "Ho so rieng: $profileDir"
            Write-Info "Anh xa: $ingHost -> 127.0.0.1"
            Start-Process $chrome -ArgumentList `
                "--user-data-dir=`"$profileDir`"", `
                "--host-resolver-rules=MAP $ingHost 127.0.0.1", `
                "$url/api/v1/orders"
            Write-Ok 'Da mo Chrome.'
            Write-Host ''
            Write-Host '    Can thay gi de coi la DAT:' -ForegroundColor White
            Write-Host '      - O KHOA, khong co canh bao "Not secure"'
            Write-Host '      - Bam o khoa -> Connection is secure -> Certificate is valid'
            Write-Host '      - Trong cert: Issued By = trading-local-ca'
            Write-Host '      - Noi dung trang: 401 JSON (thieu X-API-KEY) - DUNG, vi trinh'
            Write-Host '        duyet khong gui header do. TLS da dat la muc tieu cua buoc nay.'
            Write-Host ''
            Write-Host '    GIU CUA SO POWERSHELL NAY MO - dong la mat port-forward.' -ForegroundColor Yellow
            Write-Host '    Nhan Enter khi xem xong de don dep.' -ForegroundColor Yellow
            [void](Read-Host)
        }
    }
} finally {
    # CHI dong cai do CHINH script nay mo. Dong nham port-forward cua nguoi khac
    # se lam ho mat ket noi ma khong hieu vi sao.
    if ($owned -and $pf) {
        Stop-Process -Id $pf.Id -Force -ErrorAction SilentlyContinue
        Write-Info 'Da dong port-forward (do script nay mo).'
    } else {
        Write-Info 'Giu nguyen port-forward dang co san.'
    }
}

# ---------------------------------------------------------------------------
Write-Step 'Dung vinh vien cho moi trinh duyet'

Write-Host @"
    Cach tren chi hieu luc cho cua so Chrome do script mo. Muon MOI trinh duyet
    va moi cong cu (curl, Postman, IDE) dung duoc ten $ingHost thi them mot dong
    vao file hosts - buoc nay CAN quyen Administrator:

        Start-Process powershell -Verb RunAs -ArgumentList '-NoProfile','-Command',
          'Add-Content -Path "`$env:SystemRoot\System32\drivers\etc\hosts" -Value "127.0.0.1 $ingHost"'

    Hoac mo Notepad bang quyen Administrator va sua tay:
        %SystemRoot%\System32\drivers\etc\hosts
        127.0.0.1  $ingHost

    Sau do chi can giu port-forward roi vao thang $url :
        kubectl -n ingress-nginx port-forward svc/ingress-nginx-controller 443:443
"@
Write-Host ''
Write-Ok 'Xong.'
