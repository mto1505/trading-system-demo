<#
.SYNOPSIS
    Kiểm tra sức khoẻ một release trading-system và chỉ đúng chỗ hỏng.

.DESCRIPTION
    `kubectl get pods` chỉ nói CÓ hỏng. Script này nói hỏng Ở ĐÂU, bằng cách
    kiểm riêng từng thứ hay hỏng mà triệu chứng lại giống hệt nhau:

      - Pod chưa Ready  -> phân biệt "chưa xếp được node" / "ImagePullBackOff"
                           / "CrashLoopBackOff" / "đang chờ init container".
      - Job tạo topic   -> nếu nó kẹt, gần như luôn là NetworkPolicy chặn egress
                           tới broker chứ không phải Kafka hỏng.
      - ScaledObject    -> READY=False nghĩa là KEDA KHÔNG đọc được lag. Deployment
                           vẫn chạy bình thường nên không ai để ý, nhưng autoscale
                           đã chết im lặng.
      - PodMonitor      -> có object không có nghĩa là Prometheus đọc tới. Script
                           so label `release` với selector thật của Prometheus.
      - PDB             -> ALLOWED DISRUPTIONS = 0 làm `kubectl drain` treo vĩnh
                           viễn khi nâng cấp node. Không có cảnh báo nào từ k8s.
      - ExternalSecret  -> chưa sync thì pod kẹt CreateContainerConfigError, và
                           thông báo lỗi không hề nhắc tới ESO.

.EXAMPLE
    .\scripts\helm\Verify-Release.ps1 -Namespace trading-staging
    .\scripts\helm\Verify-Release.ps1 -Namespace trading-prod -Release trading
#>
#Requires -Version 5.1
[CmdletBinding()]
param(
    [string]   $Namespace = 'trading-staging',
    [string]   $Release   = 'trading',
    [string[]] $Modules   = @('market-gateway','pricing-risk-service','order-service','matching-engine')
)
# Dot-source TRUOC khi dinh nghia cac ham Write-* rieng ben duoi: script nay
# dung dinh dang rieng ([OK] / [LOI]) va Write-Bad con cong don vao $Problems,
# nen cac ban dinh nghia o duoi CO Y de len cua _Common.ps1. Thu can tu file
# chung chi la Test-ClusterReachable va Assert-LastExit.
. (Join-Path $PSScriptRoot '_Common.ps1')

$ErrorActionPreference = 'Continue'
$script:Problems = @()

function Write-Head { param([string]$T) Write-Host ''; Write-Host "== $T" -ForegroundColor Cyan }
function Write-Ok   { param([string]$T) Write-Host "   [OK]   $T" -ForegroundColor Green }
function Write-Bad  { param([string]$T) Write-Host "   [LOI]  $T" -ForegroundColor Red;    $script:Problems += $T }
function Write-Warn { param([string]$T) Write-Host "   [CHU Y] $T" -ForegroundColor Yellow }
function Write-Info { param([string]$T) Write-Host "          $T" -ForegroundColor DarkGray }

# Cluster tat thi MOI kiem tra ben duoi deu tra ve rong va script se bao cao
# mot danh sach loi gia tu dau den cuoi. Dung han o day.
if (-not (Test-ClusterReachable)) {
    Write-Host ''
    Write-Host "   [LOI]  Khong goi duoc API server (context: $(kubectl config current-context 2>$null))." -ForegroundColor Red
    Write-Host '          Cluster dang tat? Voi minikube:' -ForegroundColor DarkGray
    Write-Host '            minikube status' -ForegroundColor DarkGray
    Write-Host '            minikube start' -ForegroundColor DarkGray
    Write-Host ''
    exit 1
}

Write-Host ''
Write-Host "  Kiem tra release '$Release' trong namespace '$Namespace'" -ForegroundColor White
Write-Host "  context: $(kubectl config current-context)" -ForegroundColor DarkGray

# ---------------------------------------------------------------------------
Write-Head 'Release Helm'

$rel = helm -n $Namespace list -o json 2>$null | ConvertFrom-Json | Where-Object { $_.name -eq $Release }
if (-not $rel) {
    Write-Bad "Khong co release '$Release' trong namespace '$Namespace'."
    Write-Info "helm -n $Namespace list --all   # xem ca release that bai"
    exit 1
}
if ($rel.status -eq 'deployed') { Write-Ok "status=deployed  revision=$($rel.revision)  chart=$($rel.chart)" }
else {
    Write-Bad "status=$($rel.status) (khong phai 'deployed')"
    Write-Info "helm -n $Namespace history $Release"
}

# ---------------------------------------------------------------------------
Write-Head 'Pod'

$podsJson = kubectl -n $Namespace get pods -o json 2>$null | ConvertFrom-Json
if (-not $podsJson -or $podsJson.items.Count -eq 0) { Write-Bad 'Khong co pod nao.'; exit 1 }

foreach ($p in $podsJson.items) {
    $name  = $p.metadata.name
    $phase = $p.status.phase

    # Pod da hoan thanh (Job) khong can Ready.
    if ($phase -eq 'Succeeded') { Write-Ok "$name  (Succeeded)"; continue }

    $readyCond = $p.status.conditions | Where-Object { $_.type -eq 'Ready' } | Select-Object -First 1
    if ($readyCond -and $readyCond.status -eq 'True') { Write-Ok "$name  Ready"; continue }

    # Chua Ready - tim LY DO cu the thay vi chi bao "chua Ready".
    $sched = $p.status.conditions | Where-Object { $_.type -eq 'PodScheduled' } | Select-Object -First 1
    if ($sched -and $sched.status -eq 'False') {
        Write-Bad "$name  KHONG xep duoc node: $($sched.message)"
        Write-Info 'Gan nhu luon la het CPU/RAM tren node, hoac vuong ResourceQuota.'
        Write-Info "  kubectl -n $Namespace describe quota"
        continue
    }

    $waitingReasons = @()
    foreach ($cs in @($p.status.initContainerStatuses) + @($p.status.containerStatuses)) {
        if ($cs -and $cs.state.waiting) { $waitingReasons += "$($cs.name): $($cs.state.waiting.reason)" }
    }
    if ($waitingReasons.Count -gt 0) {
        Write-Bad "$name  $($waitingReasons -join ' | ')"
        switch -Regex ($waitingReasons -join ' ') {
            'ImagePullBackOff|ErrImagePull' {
                Write-Info 'Image khong keo duoc. Tren minikube: da `minikube image load` chua?'
                Write-Info 'Image nap kieu do KHONG co tien to registry - repository phai la trading/<module>.'
            }
            'CreateContainerConfigError' {
                Write-Info 'Thuong la thieu Secret/ConfigMap. O prod: ExternalSecret chua sync.'
                Write-Info "  kubectl -n $Namespace get externalsecret trading-secrets -o yaml"
            }
            'CrashLoopBackOff' {
                Write-Info "  kubectl -n $Namespace logs $name --previous --tail=60"
            }
            'PodInitializing' {
                Write-Info 'Init container wait-for-db dang quay. DB_HOST tro dung cho chua?'
                Write-Info "  kubectl -n $Namespace logs $name -c wait-for-db --tail=20"
            }
        }
        continue
    }
    Write-Bad "$name  phase=$phase, chua Ready (probe chua qua?)"
    Write-Info "  kubectl -n $Namespace describe pod $name"
}

# ---------------------------------------------------------------------------
Write-Head 'Job tao topic Kafka'

$job = kubectl -n $Namespace get job init-kafka-topics --ignore-not-found -o json 2>$null | ConvertFrom-Json
if (-not $job) {
    Write-Warn 'Khong co Job init-kafka-topics (binh thuong neu kafka.enabled=false, vd prod dung managed Kafka).'
} elseif ($job.status.succeeded -ge 1) {
    Write-Ok 'Job da hoan thanh - topic da duoc tao/chinh.'
} else {
    Write-Bad "Job CHUA hoan thanh (active=$($job.status.active) failed=$($job.status.failed))."
    Write-Info 'Neu log lap mai "broker chua san sang" thi KHONG phai Kafka hong, ma la'
    Write-Info 'egress cua pod Job bi default-deny chan. Kiem tra policy:'
    Write-Info "  kubectl -n $Namespace get netpol init-kafka-topics-egress"
    Write-Info "  kubectl -n $Namespace logs job/init-kafka-topics --tail=30"
}

# ---------------------------------------------------------------------------
Write-Head 'KEDA ScaledObject'

$sos = kubectl -n $Namespace get scaledobject -o json 2>$null | ConvertFrom-Json
if (-not $sos -or $sos.items.Count -eq 0) {
    Write-Warn 'Khong co ScaledObject (keda.enabled=false?).'
} else {
    foreach ($so in $sos.items) {
        $ready = ($so.status.conditions | Where-Object { $_.type -eq 'Ready' } | Select-Object -First 1)
        $trig  = $so.spec.triggers[0].metadata
        if ($ready -and $ready.status -eq 'True') {
            Write-Ok "$($so.metadata.name)  Ready  topic=$($trig.topic) group=$($trig.consumerGroup) threshold=$($trig.lagThreshold)"
        } else {
            Write-Bad "$($so.metadata.name)  Ready=$($ready.status): $($ready.message)"
            Write-Info 'KEDA khong doc duoc lag -> autoscale CHET IM LANG, deployment van chay.'
            Write-Info 'Hay doi chieu topic/consumerGroup voi @KafkaListener trong code:'
            Write-Info '  pricing: stock.ticks  / pricing-risk-group'
            Write-Info '  matching: stock.orders / matching-engine-group'
            Write-Info "  kubectl -n keda logs -l app=keda-operator --tail=40"
        }
    }
}

# ---------------------------------------------------------------------------
Write-Head 'Monitoring (PodMonitor co thuc su duoc Prometheus doc khong)'

$pm = kubectl -n $Namespace get podmonitor trading-apps --ignore-not-found -o json 2>$null | ConvertFrom-Json
if (-not $pm) {
    Write-Warn 'Khong co PodMonitor (monitoring.enabled=false?).'
} else {
    Write-Ok 'PodMonitor trading-apps ton tai.'

    # Co object KHONG co nghia la Prometheus doc toi. kube-prometheus-stack mac
    # dinh chi nhan PodMonitor mang dung label `release` cua chinh no.
    $proms = kubectl get prometheus -A -o json 2>$null | ConvertFrom-Json
    if ($proms -and $proms.items.Count -gt 0) {
        $prom = $proms.items[0]
        $sel  = $prom.spec.podMonitorSelector
        if (-not $sel -or -not $sel.matchLabels) {
            Write-Ok 'Prometheus nhan MOI PodMonitor (selector rong).'
        } else {
            $need = $sel.matchLabels
            $have = $pm.metadata.labels
            $miss = @()
            foreach ($k in $need.PSObject.Properties.Name) {
                if ($have.$k -ne $need.$k) { $miss += "$k=$($need.$k) (dang co: '$($have.$k)')" }
            }
            if ($miss.Count -eq 0) { Write-Ok 'Label khop podMonitorSelector cua Prometheus.' }
            else {
                Write-Bad "PodMonitor THIEU label ma Prometheus doi: $($miss -join ', ')"
                Write-Info 'Prometheus se bo qua no TRONG IM LANG - dashboard trong tron.'
                Write-Info 'Sua monitoring.extraLabels trong values-staging.yaml / values-prod.yaml.'
            }
        }
    } else {
        Write-Warn 'Khong tim thay object Prometheus nao - stack chua cai?'
    }

    # Netpol cho scrape. Thieu no thi target co trong Prometheus nhung moi lan
    # scrape deu timeout.
    if (kubectl -n $Namespace get netpol trading-apps-ingress --ignore-not-found -o name) {
        Write-Ok 'NetworkPolicy trading-apps-ingress co - scrape khong bi default-deny chan.'
    } else {
        Write-Bad 'THIEU NetworkPolicy trading-apps-ingress - moi scrape se timeout.'
    }
}

# ---------------------------------------------------------------------------
Write-Head 'PodDisruptionBudget'

$pdbs = kubectl -n $Namespace get pdb -o json 2>$null | ConvertFrom-Json
if ($pdbs -and $pdbs.items.Count -gt 0) {
    foreach ($b in $pdbs.items) {
        $allowed = $b.status.disruptionsAllowed
        if ($allowed -ge 1) { Write-Ok "$($b.metadata.name)  ALLOWED DISRUPTIONS=$allowed" }
        else {
            Write-Bad "$($b.metadata.name)  ALLOWED DISRUPTIONS=0 -> kubectl drain se TREO VINH VIEN."
            Write-Info 'Hoac pod chua du Ready, hoac PDB dat qua chat (maxUnavailable: 0).'
        }
    }
} else { Write-Warn 'Khong co PDB nao.' }

# ---------------------------------------------------------------------------
Write-Head 'TLS tu dong hoa (cert-manager)'

$issuers = kubectl -n $Namespace get issuer -o json 2>$null | ConvertFrom-Json
if (-not $issuers -or $issuers.items.Count -eq 0) {
    Write-Warn 'Khong co Issuer (ingress.tls.selfSignedIssuer.enabled=false?).'
} else {
    foreach ($i in $issuers.items) {
        $c = $i.status.conditions | Where-Object { $_.type -eq 'Ready' } | Select-Object -First 1
        if ($c -and $c.status -eq 'True') { Write-Ok "Issuer $($i.metadata.name)  Ready ($($c.reason))" }
        else {
            Write-Bad "Issuer $($i.metadata.name)  Ready=$($c.status): $($c.message)"
            Write-Info 'Issuer kieu ca can Secret CA da ton tai. Kiem Certificate ...-ca truoc.'
        }
    }

    $certs = kubectl -n $Namespace get certificate -o json 2>$null | ConvertFrom-Json
    foreach ($ct in $certs.items) {
        $c = $ct.status.conditions | Where-Object { $_.type -eq 'Ready' } | Select-Object -First 1
        if ($c -and $c.status -eq 'True') {
            Write-Ok "Certificate $($ct.metadata.name)  Ready  -> secret/$($ct.spec.secretName)  het han $($ct.status.notAfter)"
        } else {
            Write-Bad "Certificate $($ct.metadata.name)  Ready=$($c.status): $($c.message)"
            Write-Info 'Xem chi tiet o CertificateRequest - do moi la noi ghi ly do that:'
            Write-Info "  kubectl -n $Namespace get certificaterequest"
            Write-Info "  kubectl -n $Namespace describe certificaterequest"
        }
    }

    # Mat xich DE SOT nhat: Certificate cho Ingress KHONG nam trong chart.
    # cert-manager phai tu sinh no tu annotation. Neu no khong sinh, Ingress van
    # chay va nginx van phuc vu HTTPS - bang cert MAC DINH tu ky cua chinh nginx.
    # Trinh duyet bao "khong tin cay" y het truong hop cert cua ta, nen rat de
    # tuong la binh thuong.
    $ing = kubectl -n $Namespace get ingress trading-ingress -o json 2>$null | ConvertFrom-Json
    if ($ing) {
        $want = $ing.spec.tls[0].secretName
        if ($want) {
            $auto = $certs.items | Where-Object { $_.spec.secretName -eq $want }
            if ($auto) {
                Write-Ok "Certificate cho Ingress da duoc cert-manager TU sinh (secret/$want)."
                $crt = kubectl -n $Namespace get secret $want -o jsonpath='{.data.tls\.crt}' 2>$null
                if ($crt) {
                    # Dung .NET thay vi goi openssl: openssl KHONG nam trong PATH
                    # cua PowerShell tren Windows (no di kem Git, chi thay trong
                    # Git Bash). X509Certificate2 co san trong moi ban .NET.
                    try {
                        $bytes = [Convert]::FromBase64String($crt)
                        $x = New-Object System.Security.Cryptography.X509Certificates.X509Certificate2 (,$bytes)
                        Write-Info "  issuer : $($x.Issuer)"
                        Write-Info "  het han: $($x.NotAfter.ToString('yyyy-MM-dd HH:mm')) ($([int]($x.NotAfter - (Get-Date)).TotalDays) ngay nua)"
                        $san = $x.Extensions | Where-Object { $_.Oid.Value -eq '2.5.29.17' }
                        if ($san) { Write-Info "  SAN    : $($san.Format($false))" }
                        $x.Dispose()
                    } catch {
                        Write-Warn "  Khong doc duoc cert: $($_.Exception.Message)"
                    }
                } else {
                    Write-Bad "Secret $want chua co du lieu tls.crt."
                }
            } else {
                Write-Bad "Ingress trong cho secret '$want' nhung KHONG co Certificate nao sinh ra no."
                Write-Info 'cert-manager chua doc duoc annotation. Kiem:'
                Write-Info "  kubectl -n $Namespace get ingress trading-ingress -o jsonpath='{.metadata.annotations}'"
                Write-Info '  kubectl -n cert-manager logs deploy/cert-manager --tail=40'
                Write-Info 'Canh bao: nginx VAN phuc vu HTTPS bang cert mac dinh cua no -> de tuong la OK.'
            }
        }
    }
}

# ---------------------------------------------------------------------------
Write-Head 'Secret'

$es = kubectl -n $Namespace get externalsecret trading-secrets --ignore-not-found -o json 2>$null | ConvertFrom-Json
if ($es) {
    $c = $es.status.conditions | Where-Object { $_.type -eq 'Ready' } | Select-Object -First 1
    if ($c -and $c.status -eq 'True') { Write-Ok 'ExternalSecret da sync.' }
    else {
        Write-Bad "ExternalSecret CHUA sync: $($c.reason) - $($c.message)"
        Write-Info 'Pod se ket o CreateContainerConfigError ma khong nhac gi toi ESO.'
        Write-Info '  kubectl -n external-secrets logs -l app.kubernetes.io/name=external-secrets --tail=40'
    }
} else {
    $s = kubectl -n $Namespace get secret trading-secrets --ignore-not-found -o name 2>$null
    if ($s) { Write-Ok 'Secret trading-secrets co (tao truc tiep tu values - dung cho dev/staging).' }
    else    { Write-Bad 'KHONG co trading-secrets va cung khong co ExternalSecret.' }
}

# ---------------------------------------------------------------------------
Write-Head 'Ket luan'

if ($script:Problems.Count -eq 0) {
    Write-Host ''
    Write-Host '   Khong phat hien van de.' -ForegroundColor Green
    Write-Host ''
    exit 0
} else {
    Write-Host ''
    Write-Host "   $($script:Problems.Count) van de:" -ForegroundColor Red
    $script:Problems | ForEach-Object { Write-Host "     - $_" -ForegroundColor Red }
    Write-Host ''
    exit 1
}
