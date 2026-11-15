<#
.SYNOPSIS
    Monitor-Trading.ps1 — Giám sát & Tổng hợp số lượng, dung lượng, tài nguyên cụm Trading System
.EXAMPLE
    .\Monitor-Trading.ps1 -Namespace trading-staging
    .\Monitor-Trading.ps1 -Namespace trading-staging -Watch
#>
[CmdletBinding()]
param(
    [string]$Namespace = "trading-staging",
    [switch]$Watch,
    [int]$IntervalSeconds = 5
)

function Show-TradingStatus {
    $line = "=" * 75
    Write-Host "`n$line" -ForegroundColor Cyan
    Write-Host "🔍 GIÁM SÁT & TỔNG HỢP TÀI NGUYÊN: $Namespace" -ForegroundColor Green
    Write-Host "⏰ THỜI GIAN: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" -ForegroundColor Gray
    Write-Host "$line" -ForegroundColor Cyan

    # =========================================================================
    # 1. TỔNG HỢP PODS & WORKLOADS (REPLICAS / AVAILABILITY)
    # =========================================================================
    Write-Host "`n📊 1. TỔNG HỢP WORKLOADS & REPLICAS:" -ForegroundColor Yellow
    Write-Host ("-" * 75)
    
    $podsJson = kubectl get pods -n $Namespace -o json 2>$null | ConvertFrom-Json
    $deployJson = kubectl get deploy -n $Namespace -o json 2>$null | ConvertFrom-Json
    $stsJson = kubectl get sts -n $Namespace -o json 2>$null | ConvertFrom-Json

    $totalDesired = 0
    $totalReady = 0
    $totalAvailable = 0

    if ($deployJson -and $deployJson.items) {
        foreach ($d in $deployJson.items) {
            $totalDesired += [int]($d.spec.replicas)
            $totalReady += [int]($d.status.readyReplicas)
            $totalAvailable += [int]($d.status.availableReplicas)
        }
    }
    if ($stsJson -and $stsJson.items) {
        foreach ($s in $stsJson.items) {
            $totalDesired += [int]($s.spec.replicas)
            $totalReady += [int]($s.status.readyReplicas)
            $totalAvailable += [int]($s.status.availableReplicas)
        }
    }

    $totalPodsRunning = 0
    $totalPodsRestart = 0
    if ($podsJson -and $podsJson.items) {
        $totalPodsCount = $podsJson.items.Count
        foreach ($p in $podsJson.items) {
            if ($p.status.phase -eq "Running") { $totalPodsRunning++ }
            foreach ($cs in $p.status.containerStatuses) {
                $totalPodsRestart += [int]$cs.restartCount
            }
        }
    } else {
        $totalPodsCount = 0
    }

    Write-Host "  • Tổng Replica mong muốn (Desired):   $totalDesired" -ForegroundColor White
    Write-Host "  • Tổng Replica sẵn sàng (Ready):       $totalReady / $totalDesired" -ForegroundColor $(if ($totalReady -eq $totalDesired -and $totalDesired -gt 0) { "Green" } else { "Red" })
    Write-Host "  • Tổng Pods đang chạy (Running):      $totalPodsRunning / $totalPodsCount" -ForegroundColor White
    Write-Host "  • Tổng số lần Pod bị Restart:          $totalPodsRestart" -ForegroundColor $(if ($totalPodsRestart -eq 0) { "Green" } else { "Yellow" })

    Write-Host "`nChi tiết từng Service:" -ForegroundColor DarkGray
    $workloadCols = "NAME:.metadata.name,DESIRED:.spec.replicas,CURRENT:.status.replicas,READY:.status.readyReplicas,AVAILABLE:.status.availableReplicas"
    kubectl get deploy,sts -n $Namespace -o "custom-columns=$workloadCols"

    # =========================================================================
    # 2. TỔNG HỢP DUNG LƯỢNG Ổ ĐĨA (PVC / STORAGE)
    # =========================================================================
    Write-Host "`n💾 2. TỔNG HỢP DUNG LƯỢNG LƯU TRỮ (STORAGE / PVC):" -ForegroundColor Yellow
    Write-Host ("-" * 75)
    $pvcJson = kubectl get pvc -n $Namespace -o json 2>$null | ConvertFrom-Json

    $totalStorageBytes = 0
    $pvcCount = 0
    if ($pvcJson -and $pvcJson.items) {
        $pvcCount = $pvcJson.items.Count
        foreach ($pvc in $pvcJson.items) {
            $capStr = $pvc.status.capacity.storage
            if ($capStr -match "([0-9]+)Gi") {
                $totalStorageBytes += ([int]$matches[1]) * 1GB
            } elseif ($capStr -match "([0-9]+)Mi") {
                $totalStorageBytes += ([int]$matches[1]) * 1MB
            }
        }
    }
    $totalStorageGB = [math]::Round($totalStorageBytes / 1GB, 2)
    Write-Host "  • Tổng số ổ đĩa PVC:             $pvcCount ổ đĩa" -ForegroundColor White
    Write-Host "  • Tổng dung lượng đã cấp phát:    $totalStorageGB GiB" -ForegroundColor Green

    Write-Host "`nChi tiết PVC:" -ForegroundColor DarkGray
    $pvcCols = "NAME:.metadata.name,STATUS:.status.phase,CAPACITY:.status.capacity.storage,CLASS:.spec.storageClassName"
    kubectl get pvc -n $Namespace -o "custom-columns=$pvcCols"

    # =========================================================================
    # 3. TỔNG TÀI NGUYÊN QUOTA (HẠN MỨC TỐI ĐA VS ĐÃ CẤP PHÁT)
    # =========================================================================
    Write-Host "`n📈 3. TỔNG TÀI NGUYÊN THEO RESOURCE QUOTA (ĐÃ DÙNG / TRẦN CHO PHÉP):" -ForegroundColor Yellow
    Write-Host ("-" * 75)
    $quotaJson = kubectl get resourcequota -n $Namespace -o json 2>$null | ConvertFrom-Json

    if ($quotaJson -and $quotaJson.items -and $quotaJson.items.Count -gt 0) {
        $q = $quotaJson.items[0]
        $hard = $q.status.hard
        $used = $q.status.used

        function Format-QuotaItem($name, $usedVal, $hardVal) {
            if (-not $hardVal) { return }
            Write-Host ("  • {0,-25}: {1,8} / {2,-8}" -f $name, $usedVal, $hardVal) -NoNewline
            # Tính phần trăm nếu là số nguyên hoặc có cùng đơn vị
            try {
                $uNum = ($usedVal -replace '[^0-9.]','')
                $hNum = ($hardVal -replace '[^0-9.]','')
                if ([double]$hNum -gt 0) {
                    $pct = [math]::Round(([double]$uNum / [double]$hNum) * 100, 1)
                    Write-Host " ($pct%)" -ForegroundColor $(if ($pct -gt 85) { "Red" } elseif ($pct -gt 60) { "Yellow" } else { "Green" })
                } else {
                    Write-Host ""
                }
            } catch {
                Write-Host ""
            }
        }

        Format-QuotaItem "CPU Request" $used."requests.cpu" $hard."requests.cpu"
        Format-QuotaItem "CPU Limit" $used."limits.cpu" $hard."limits.cpu"
        Format-QuotaItem "Memory Request" $used."requests.memory" $hard."requests.memory"
        Format-QuotaItem "Memory Limit" $used."limits.memory" $hard."limits.memory"
        Format-QuotaItem "Số lượng Pods" $used."pods" $hard."pods"
        Format-QuotaItem "Số lượng PVCs" $used."persistentvolumeclaims" $hard."persistentvolumeclaims"
        Format-QuotaItem "Storage Requests" $used."requests.storage" $hard."requests.storage"
    } else {
        Write-Host "  ⚠️ Không tìm thấy ResourceQuota trong namespace $Namespace" -ForegroundColor DarkGray
    }

    # =========================================================================
    # 4. MỨC ĂN TÀI NGUYÊN THỰC TẾ (REAL-TIME ACTUAL METRICS)
    # =========================================================================
    Write-Host "`n⚡ 4. TIÊU THỤ CPU / RAM THỰC TẾ TỨC THỜI (TỪ METRICS SERVER):" -ForegroundColor Yellow
    Write-Host ("-" * 75)
    $topOutput = kubectl top pods -n $Namespace 2>$null
    if ($LASTEXITCODE -eq 0 -and $topOutput) {
        $totalCpuMilli = 0
        $totalMemMiB = 0
        # Bỏ dòng header
        $lines = $topOutput | Select-Object -Skip 1
        foreach ($l in $lines) {
            $parts = ($l -split '\s+')
            if ($parts.Count -ge 3) {
                $cpuStr = $parts[1]
                $memStr = $parts[2]
                if ($cpuStr -match "([0-9]+)m") { $totalCpuMilli += [int]$matches[1] }
                if ($memStr -match "([0-9]+)Mi") { $totalMemMiB += [int]$matches[1] }
            }
        }
        $totalCpuCore = [math]::Round($totalCpuMilli / 1000, 2)
        $totalMemGiB = [math]::Round($totalMemMiB / 1024, 2)

        Write-Host "  • Tổng CPU thực tế đang ăn:  $totalCpuMilli m ($totalCpuCore Cores)" -ForegroundColor Green
        Write-Host "  • Tổng RAM thực tế đang ăn:  $totalMemMiB MiB ($totalMemGiB GiB)" -ForegroundColor Green
        Write-Host "`nChi tiết từng Pod:" -ForegroundColor DarkGray
        $topOutput | ForEach-Object { Write-Host "    $_" }
    } else {
        Write-Host "  ⚠️ Metrics Server chưa cài đặt hoặc chưa sẵn sàng để đọc CPU/RAM tức thời." -ForegroundColor DarkGray
    }

    # =========================================================================
    # 5. MẠNG, INGRESS & CHỨNG CHỈ TLS
    # =========================================================================
    Write-Host "`n🌐 5. MẠNG, INGRESS & TLS:" -ForegroundColor Yellow
    Write-Host ("-" * 75)
    $ingCols = "NAME:.metadata.name,HOST:.spec.rules[*].host,ADDRESS:.status.loadBalancer.ingress[0].ip,TLS-SECRET:.spec.tls[*].secretName"
    kubectl get ingress -n $Namespace -o "custom-columns=$ingCols" 2>$null

    $cert = kubectl get certificate,issuer -n $Namespace 2>$null
    if ($cert) {
        Write-Host "`nCertificates & Issuers:" -ForegroundColor DarkGray
        $cert
    }

    # =========================================================================
    # 6. KEDA AUTOSCALING (NẾU CÓ)
    # =========================================================================
    $keda = kubectl get scaledobject -n $Namespace 2>$null
    if ($keda) {
        Write-Host "`n📈 6. KEDA AUTOSCALING (Kafka Consumer Lag):" -ForegroundColor Yellow
        Write-Host ("-" * 75)
        $keda
    }

    Write-Host "`n$line" -ForegroundColor Cyan
    Write-Host "✅ TỔNG HỢP HOÀN TẤT CHO: $Namespace" -ForegroundColor Green
    Write-Host "$line`n" -ForegroundColor Cyan
}

if ($Watch) {
    Write-Host "Đang bật Live Dashboard (cập nhật mỗi $IntervalSeconds giây, nhấn Ctrl+C để dừng)..." -ForegroundColor Magenta
    while ($true) {
        Clear-Host
        Show-TradingStatus
        Start-Sleep -Seconds $IntervalSeconds
    }
} else {
    Show-TradingStatus
}
