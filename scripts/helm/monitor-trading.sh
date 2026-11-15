#!/usr/bin/env bash
# =============================================================================
# monitor-trading.sh — Giám sát toàn diện trạng thái cụm Trading System
# Cách dùng: ./monitor-trading.sh [trading-staging | trading-dev | trading-prod]
# =============================================================================

set -e

NAMESPACE="${1:-trading-staging}"
LINE="======================================================================"

echo -e "\n$LINE"
echo "🔍 ĐANG GIÁM SÁT NAMESPACE: $NAMESPACE"
echo "⏰ THỜI GIAN: $(date '+%Y-%m-%d %H:%M:%S')"
echo "$LINE"

# 1. TRẠNG THÁI POD, REPLICA & SỐ LẦN RESTART
echo -e "\n📦 1. PODS & WORKLOAD HEALTH (Replicas, Available, Restarts):"
echo "----------------------------------------------------------------------"
kubectl get pods -n "$NAMESPACE" -o custom-columns=\
"TÊN POD":.metadata.name,\
"READY":.status.containerStatuses[0].ready,\
"TRẠNG THÁI":.status.phase,\
"RESTARTS":.status.containerStatuses[0].restartCount,\
"TUỔI":.metadata.creationTimestamp,\
"NODE":.spec.nodeName

echo -e "\n📊 DEPLOYMENTS & STATEFULSETS:"
kubectl get deploy,sts -n "$NAMESPACE" -o custom-columns=\
"TÊN":.metadata.name,\
"MONG MUỐN":.spec.replicas,\
"HIỆN TẠI":.status.replicas,\
"SẴN SÀNG":.status.readyReplicas,\
"AVAILABLE":.status.availableReplicas

# 2. DUNG LƯỢNG LƯU TRỮ (PVC) VÀ RESOURCE QUOTA
echo -e "\n💾 2. DUNG LƯỢNG & TÀI NGUYÊN (Storage, Quota, Limits):"
echo "----------------------------------------------------------------------"
echo ">>> Persistent Volume Claims (PVC):"
kubectl get pvc -n "$NAMESPACE" -o custom-columns=\
"PVC NAME":.metadata.name,\
"STATUS":.status.phase,\
"VOLUME":.spec.volumeName,\
"DUNG LƯỢNG":.status.capacity.storage,\
"STORAGE CLASS":.spec.storageClassName

echo -e "\n>>> Mức tiêu thụ Resource Quota (Hard vs Used):"
kubectl get resourcequota -n "$NAMESPACE" || echo "Không có ResourceQuota"

if kubectl top pods -n "$NAMESPACE" &>/dev/null; then
  echo -e "\n>>> Mức tiêu thụ CPU/RAM thực tế (Metrics-server):"
  kubectl top pods -n "$NAMESPACE"
else
  echo -e "\n⚠️ Metrics Server chưa cài, không thể đo CPU/RAM tức thời."
fi

# 3. MẠNG & ROUTING (Ingress, TLS, Service, NetworkPolicy)
echo -e "\n🌐 3. MẠNG, ROUTING & TLS CHỨNG CHỈ:"
echo "----------------------------------------------------------------------"
echo ">>> Ingress Endpoint:"
kubectl get ingress -n "$NAMESPACE" -o custom-columns=\
"INGRESS":.metadata.name,\
"HOSTS":.spec.rules[*].host,\
"ADDRESS":.status.loadBalancer.ingress[0].ip,\
"PORTS":.spec.tls[*].secretName

echo -e "\n>>> Trạng thái TLS Certificate (cert-manager):"
kubectl get certificate,issuer -n "$NAMESPACE" 2>/dev/null || echo "Không tìm thấy Certificate CRD của cert-manager"

echo -e "\n>>> Network Policies đang kích hoạt:"
kubectl get networkpolicy -n "$NAMESPACE"

# 4. KEDA AUTOSCALING & METRICS MONITORING
echo -e "\n📈 4. AUTOSCALING (KEDA) & PROMETHEUS SCRAPE:"
echo "----------------------------------------------------------------------"
echo ">>> KEDA ScaledObjects (Consumer Lag):"
kubectl get scaledobject -n "$NAMESPACE" 2>/dev/null || echo "Chưa có ScaledObject hoặc KEDA chưa cài"

echo -e "\n>>> Prometheus Monitors (PodMonitor / ServiceMonitor):"
kubectl get podmonitor,servicemonitor -n "$NAMESPACE" 2>/dev/null || echo "Chưa bật CRD Monitoring"

echo -e "\n$LINE"
echo "✅ HOÀN TẤT BÁO CÁO GIÁM SÁT CHO: $NAMESPACE"
echo "$LINE"
