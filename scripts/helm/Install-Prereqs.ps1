<#
.SYNOPSIS
    Cài các thành phần cluster-wide mà chart trading-system TRÔNG CHỜ ĐÃ CÓ SẴN.

.DESCRIPTION
    Chart không tự cài những thứ này, và đó là cố ý: chúng là hạ tầng dùng chung
    cho cả cluster, không thuộc vòng đời của một release ứng dụng. Nhưng nếu
    thiếu, triệu chứng lại KHÔNG giống nhau chút nào — đó là lý do script này
    kiểm tra từng cái một và nói rõ cái gì hỏng:

      KEDA thiếu          -> `helm upgrade` CHẾT NGAY ở bước render với
                             "no matches for kind ScaledObject". Dễ thấy nhất.

      Prometheus Operator -> cũng chết ở render với "no matches for kind
      thiếu                 PodMonitor". Dễ thấy.

      ingress-nginx thiếu -> helm CÀI THÀNH CÔNG, pod Ready hết, nhưng Ingress
                             không có controller nào nhận -> gọi API timeout.
                             Đây là cái im lặng, tốn thời gian nhất.

      CNI không thực thi  -> helm cài thành công, mọi NetworkPolicy apply được,
      NetworkPolicy         nhưng KHÔNG CÁI NÀO có hiệu lực. Bạn tưởng mình đã
                             cô lập mạng trong khi thật ra chưa. kindnet (mặc
                             định của minikube) nằm đúng nhóm này.

    Script idempotent: chạy lại nhiều lần vô hại, các bước đã xong sẽ bị bỏ qua.

.PARAMETER Target
    'minikube' — thêm bước bật addon ingress của minikube và cảnh báo CNI.
    'cloud'    — EKS/GKE/AKS: cài ingress-nginx bằng Helm, không đụng addon.

.PARAMETER MonitoringRelease
    Tên release của kube-prometheus-stack. PHẢI khớp `monitoring.extraLabels.release`
    trong values-staging.yaml / values-prod.yaml, nếu không Prometheus sẽ bỏ qua
    PodMonitor của chart TRONG IM LẶNG (xem ghi chú trong values.yaml).

.PARAMETER SkipMonitoring
    Bỏ kube-prometheus-stack (nó ngốn ~1.5GB RAM). Khi dùng cờ này PHẢI deploy
    kèm --set monitoring.enabled=false, nếu không helm chết ở kind PodMonitor.

.EXAMPLE
    .\scripts\helm\Install-Prereqs.ps1 -Target minikube
    .\scripts\helm\Install-Prereqs.ps1 -Target cloud -MonitoringRelease kps
#>
#Requires -Version 5.1
[CmdletBinding()]
param(
    [ValidateSet('minikube','cloud')]
    [string] $Target            = 'minikube',
    [string] $ClusterProfile    = 'minikube',
    [string] $MonitoringRelease = 'kube-prometheus-stack',
    [string] $MonitoringNs      = 'monitoring',
    [string] $KedaNs            = 'keda',
    [switch] $SkipMonitoring,
    [string] $CertManagerNs      = 'cert-manager',
    [switch] $SkipIngress,
    [switch] $SkipCertManager
)

. (Join-Path $PSScriptRoot '_Common.ps1')

# ---------------------------------------------------------------------------
Write-Step 'Kiem tra cong cu'

$tools = @('kubectl','helm')
if ($Target -eq 'minikube') { $tools += @('docker','minikube') }
foreach ($tool in $tools) {
    $cmd = Get-Command $tool -ErrorAction SilentlyContinue
    if (-not $cmd) { throw "Thieu '$tool' trong PATH." }
    Write-Info ("{0,-9} {1}" -f $tool, $cmd.Source)
}

$helmMajor = (helm version --template '{{.Version}}') -replace '^v(\d+)\..*','$1'
if ([int]$helmMajor -lt 3) { throw "Can Helm 3 tro len." }

# ---------------------------------------------------------------------------
Write-Step 'Cluster dang tro toi'

# BAT BUOC lam truoc moi lenh kubectl khac. Neu cluster tat, cac lenh tham do
# kieu `kubectl get crd ... --ignore-not-found` KHONG chet ma tra ve RONG, va
# script se ket luan nham la "thieu KEDA" thay vi "cluster dang tat".
Assert-ClusterReachable -Target $Target -ClusterProfile $ClusterProfile

$ctx = kubectl config current-context
Assert-LastExit 'khong doc duoc kubecontext'

kubectl get nodes -o wide
Assert-LastExit 'khong goi duoc API server'

# Dung sai cluster la loi dat gia nhat trong ca quy trinh. Doi voi 'cloud' thi
# bat nguoi chay xac nhan bang tay — khong co cach tu dong nao phan biet duoc
# "staging cua toi" voi "prod cua ca cong ty" tu kubeconfig.
if ($Target -eq 'cloud') {
    Write-Warn "Ban sap cai KEDA + Prometheus Operator + ingress-nginx vao context TREN."
    $ans = Read-Host "    Go dung ten context de xac nhan"
    if ($ans -ne $ctx) { throw "Khong khop ('$ans' != '$ctx') - dung lai." }
}

# ---------------------------------------------------------------------------
Write-Step 'CNI co thuc thi NetworkPolicy khong'

# Chart tao 10-11 NetworkPolicy, trong do co default-deny-all. Nếu CNI không
# thực thi thì chúng chỉ là object trang trí trong etcd.
$cniPods = kubectl get pods -n kube-system -o name 2>$null
if ($cniPods -match 'calico')        { Write-Ok   'CNI: Calico - NetworkPolicy CO hieu luc.' }
elseif ($cniPods -match 'cilium')    { Write-Ok   'CNI: Cilium - NetworkPolicy CO hieu luc.' }
elseif ($cniPods -match 'kindnet')   {
    Write-Warn 'CNI: kindnet - NetworkPolicy apply duoc nhung KHONG duoc thuc thi.'
    Write-Warn '  Dung nham voi "da an toan". Muon test that su phai tao lai cluster:'
    Write-Warn "    minikube delete -p $ClusterProfile"
    Write-Warn "    minikube start  -p $ClusterProfile --cni=calico --cpus=4 --memory=8192"
}
else { Write-Warn 'Khong nhan dien duoc CNI - tu kiem tra xem NetworkPolicy co hieu luc.' }

# ---------------------------------------------------------------------------
Write-Step 'Helm repo'

helm repo add kedacore            https://kedacore.github.io/charts            2>$null | Out-Null
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts 2>$null | Out-Null
helm repo add ingress-nginx       https://kubernetes.github.io/ingress-nginx   2>$null | Out-Null
helm repo update | Out-Null
Assert-LastExit 'helm repo update that bai'
Write-Ok 'Da cap nhat index.'

# ---------------------------------------------------------------------------
Write-Step 'KEDA'

# Bat buoc cho staging va prod: ca hai deu bat keda.enabled=true. Khong co CRD
# ScaledObject thi `helm template` con chua qua noi, noi gi `helm upgrade`.
$kedaCrd = kubectl get crd scaledobjects.keda.sh --ignore-not-found -o name
if ($kedaCrd) {
    $kedaVer = kubectl -n $KedaNs get deployment keda-operator -o jsonpath='{.metadata.labels.app\.kubernetes\.io/version}' 2>$null
    Write-Ok "Da co (version $kedaVer)."
} else {
    Write-Info 'Chua co - dang cai...'
    helm upgrade --install keda kedacore/keda `
        --namespace $KedaNs --create-namespace `
        --wait --timeout 5m
    Assert-LastExit 'cai KEDA that bai'
    Write-Ok 'KEDA san sang.'
}

# ---------------------------------------------------------------------------
if ($SkipMonitoring) {
    Write-Step 'kube-prometheus-stack  [BO QUA: -SkipMonitoring]'
    Write-Warn 'NHO deploy kem: --set monitoring.enabled=false'
    Write-Warn 'Neu khong, helm se chet voi "no matches for kind PodMonitor".'
} else {
    Write-Step 'kube-prometheus-stack (Prometheus Operator + Grafana)'

    $pmCrd = kubectl get crd podmonitors.monitoring.coreos.com --ignore-not-found -o name
    if ($pmCrd) {
        Write-Ok 'CRD PodMonitor da co.'
    } else {
        Write-Info 'Chua co - dang cai (~1.5GB RAM, mat vai phut)...'
        # Hai co quan trong:
        #   podMonitorSelectorNilUsesHelmValues=false
        #   serviceMonitorSelectorNilUsesHelmValues=false
        # Mac dinh chart nay gan selector `release: <ten-release>` cho Prometheus.
        # Ta VAN gan dung label do trong values-staging/prod, nen hai co nay chi
        # la lop bao hiem — nhung chung bien "khong thay metric" tu mot cuoc dieu
        # tra 2 tieng thanh khong bao gio xay ra.
        helm upgrade --install $MonitoringRelease prometheus-community/kube-prometheus-stack `
            --namespace $MonitoringNs --create-namespace `
            --set prometheus.prometheusSpec.podMonitorSelectorNilUsesHelmValues=false `
            --set prometheus.prometheusSpec.serviceMonitorSelectorNilUsesHelmValues=false `
            --set prometheus.prometheusSpec.retention=7d `
            --wait --timeout 10m
        Assert-LastExit 'cai kube-prometheus-stack that bai'
        Write-Ok 'Prometheus Operator san sang.'
    }

    # Namespace cua stack PHAI khop monitoring.namespace trong values, vi
    # NetworkPolicy trading-apps-ingress cho phep scrape theo nhan
    # kubernetes.io/metadata.name cua namespace do.
    Write-Info "NetworkPolicy se mo cho namespace '$MonitoringNs'."
    Write-Info "values-staging.yaml / values-prod.yaml phai co: monitoring.namespace=$MonitoringNs"
    Write-Info "va monitoring.extraLabels.release=$MonitoringRelease"
}

# ---------------------------------------------------------------------------
if ($SkipIngress) {
    Write-Step 'Ingress controller  [BO QUA: -SkipIngress]'
} else {
    Write-Step 'Ingress controller'

    $ingClass = kubectl get ingressclass nginx --ignore-not-found -o name
    if ($ingClass) {
        Write-Ok 'IngressClass nginx da co.'
    } elseif ($Target -eq 'minikube') {
        Write-Info 'Bat addon ingress cua minikube...'
        minikube addons enable ingress -p $ClusterProfile
        Assert-LastExit 'bat addon ingress that bai'
        kubectl -n ingress-nginx wait --for=condition=available deployment/ingress-nginx-controller --timeout=180s
        Write-Ok 'ingress-nginx san sang.'
    } else {
        Write-Info 'Cai ingress-nginx bang Helm...'
        helm upgrade --install ingress-nginx ingress-nginx/ingress-nginx `
            --namespace ingress-nginx --create-namespace `
            --wait --timeout 10m
        Assert-LastExit 'cai ingress-nginx that bai'
        Write-Ok 'ingress-nginx san sang.'
    }

    # NetworkPolicy allow-ingress-to-order-service chon namespace nguon bang
    # nhan kubernetes.io/metadata.name=ingress-nginx. Nhan nay duoc kube-apiserver
    # tu gan tu k8s 1.21, nhung cluster nang cap tu ban cu hon co the thieu —
    # khi do traffic tu nginx bi default-deny chan va API tra 503.
    $nsLabel = kubectl get ns ingress-nginx -o jsonpath='{.metadata.labels.kubernetes\.io/metadata\.name}' 2>$null
    if (-not $nsLabel) {
        Write-Warn 'Namespace ingress-nginx THIEU nhan kubernetes.io/metadata.name.'
        Write-Warn '  NetworkPolicy se chan nginx -> API tra 503. Vá bang:'
        Write-Warn '    kubectl label ns ingress-nginx kubernetes.io/metadata.name=ingress-nginx'
    }
}

# ---------------------------------------------------------------------------
if ($SkipCertManager) {
    Write-Step 'cert-manager  [BO QUA: -SkipCertManager]'
    Write-Warn 'NHO dat ingress.tls.selfSignedIssuer.enabled=false, neu khong helm'
    Write-Warn 'se chet o render voi "no matches for kind Issuer".'
} else {
    Write-Step 'cert-manager (tu dong cap phat TLS bang YAML)'

    # Chart dung Issuer tu ky (local CA) chu khong phai ACME: minikube va cac
    # cluster noi bo khong co domain cong khai nen Let's Encrypt khong the qua
    # duoc thu thach HTTP-01. Issuer tu ky van test duoc DUNG luong tu dong hoa:
    # Ingress annotation -> cert-manager tu tao Certificate -> ghi ra Secret.
    $cmCrd = kubectl get crd certificates.cert-manager.io --ignore-not-found -o name
    if ($cmCrd) {
        $cmVer = kubectl -n $CertManagerNs get deployment cert-manager `
            -o jsonpath='{.metadata.labels.app\.kubernetes\.io/version}' 2>$null
        Write-Ok "Da co (version $cmVer)."
    } else {
        Write-Info 'Chua co - dang cai...'
        helm repo add jetstack https://charts.jetstack.io 2>$null | Out-Null
        helm repo update jetstack 2>$null | Out-Null

        # crds.enabled=true: KHONG tach CRD ra buoc rieng. Tach ra thi `helm
        # uninstall cert-manager` de lai CRD mo coi, va lan cai sau bao
        # "CustomResourceDefinition already exists".
        helm upgrade --install cert-manager jetstack/cert-manager `
            --namespace $CertManagerNs --create-namespace `
            --set crds.enabled=true `
            --set resources.requests.cpu=10m `
            --set resources.requests.memory=48Mi `
            --set resources.limits.memory=128Mi `
            --set webhook.resources.requests.cpu=10m `
            --set webhook.resources.requests.memory=32Mi `
            --set webhook.resources.limits.memory=96Mi `
            --set cainjector.resources.requests.cpu=10m `
            --set cainjector.resources.requests.memory=64Mi `
            --set cainjector.resources.limits.memory=160Mi `
            --wait --timeout 8m
        Assert-LastExit 'cai cert-manager that bai'
        Write-Ok 'cert-manager san sang (3 pod: controller, webhook, cainjector).'
    }

    # Webhook la thanh phan hay gay bat ngo nhat: no phai Ready TRUOC khi co ai
    # tao Issuer/Certificate, neu khong API server tra loi
    #   "failed calling webhook ... connection refused"
    # va `helm install` chet giua chung du cert-manager "da cai xong".
    $whReady = kubectl -n $CertManagerNs get deployment cert-manager-webhook `
        -o jsonpath='{.status.readyReplicas}' 2>$null
    if (-not $whReady) {
        Write-Warn 'cert-manager-webhook CHUA Ready - tao Issuer luc nay se that bai.'
        Write-Warn "  kubectl -n $CertManagerNs get pods"
    } else {
        Write-Info "webhook Ready ($whReady/1) - co the tao Issuer/Certificate."
    }
}

# ---------------------------------------------------------------------------
Write-Step 'Tom tat'

Write-Host ''
Write-Host '    THANH PHAN           TRANG THAI' -ForegroundColor White
$checks = @(
    @{ n = 'KEDA (ScaledObject)';   q = { kubectl get crd scaledobjects.keda.sh --ignore-not-found -o name } },
    @{ n = 'PodMonitor CRD';        q = { kubectl get crd podmonitors.monitoring.coreos.com --ignore-not-found -o name } },
    @{ n = 'ServiceMonitor CRD';    q = { kubectl get crd servicemonitors.monitoring.coreos.com --ignore-not-found -o name } },
    @{ n = 'IngressClass nginx';    q = { kubectl get ingressclass nginx --ignore-not-found -o name } },
    @{ n = 'cert-manager';          q = { kubectl get crd certificates.cert-manager.io --ignore-not-found -o name } },
    @{ n = 'ExternalSecret (ESO)';  q = { kubectl get crd externalsecrets.external-secrets.io --ignore-not-found -o name } }
)
foreach ($c in $checks) {
    $have = & $c.q
    if ($have) { Write-Host ("    {0,-22} CO"      -f $c.n) -ForegroundColor Green }
    else       { Write-Host ("    {0,-22} CHUA CO" -f $c.n) -ForegroundColor Yellow }
}

Write-Host ''
Write-Info 'ExternalSecret CHI can cho PROD (externalSecrets.enabled=true).'
Write-Info 'Cai rieng khi len prod:'
Write-Info '  helm repo add external-secrets https://charts.external-secrets.io'
Write-Info '  helm upgrade --install external-secrets external-secrets/external-secrets \'
Write-Info '    -n external-secrets --create-namespace --set installCRDs=true --wait'
Write-Host ''
Write-Ok 'Xong. Buoc tiep theo: .\scripts\helm\Deploy-Staging.ps1'
