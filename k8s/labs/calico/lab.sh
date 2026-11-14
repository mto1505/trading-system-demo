#!/usr/bin/env bash
# =============================================================================
#  lab.sh — bộ lệnh phụ trợ cho lab Calico.  Dùng bằng cách NẠP vào shell:
#
#      cd trading-system/k8s/labs/calico
#      source ./lab.sh
#
#  Rồi gọi:
#      up                    dựng namespace + pod, chờ Ready
#      matrix                đo toàn bộ ma trận kết nối (lệnh dùng nhiều nhất)
#      probe <pod> <host> [port] [ns]     đo một đường
#      raw   <pod> <host> [port] [ns]     như trên nhưng in output gốc
#      pol                   liệt kê policy đang áp
#      reset                 xoá sạch policy, giữ nguyên pod
#      iface <pod>           tên veth cali* của pod (hỏi Calico, không đoán)
#      chains <pod>          chuỗi iptables Calico của pod đó
#      down                  xoá hẳn hai namespace
#
#  Script CHỈ ĐỌC trừ up/reset/down.
# =============================================================================

# Git Bash tự đổi /dev/null thành C:/Program Files/Git/dev/null. Hai dòng này tắt nó.
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL='*'

NS=${NS:-calico-lab}
EXT_NS=${EXT_NS:-calico-lab-ext}
LAB_DIR=${LAB_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}

up() {
  kubectl apply -f "$LAB_DIR/00-namespaces.yaml"
  kubectl apply -f "$LAB_DIR/01-workloads.yaml"
  echo "Đang chờ pod Ready (lần đầu phải kéo image, có thể 1-2 phút)..."
  kubectl -n "$NS"     wait --for=condition=Ready pod --all --timeout=180s
  kubectl -n "$EXT_NS" wait --for=condition=Ready pod --all --timeout=180s
  kubectl -n "$NS"     get pods -o wide
  kubectl -n "$EXT_NS" get pods -o wide
}

down() { kubectl delete ns "$NS" "$EXT_NS" --ignore-not-found; }

reset() {
  kubectl -n "$NS"     delete networkpolicy --all --ignore-not-found >/dev/null
  kubectl -n "$EXT_NS" delete networkpolicy --all --ignore-not-found >/dev/null
  echo "Đã xoá hết NetworkPolicy. Namespace trở lại trạng thái 'thông mọi hướng'."
}

pol() {
  kubectl get netpol -A -o custom-columns=\
'NS:.metadata.namespace,TEN:.metadata.name,CHON-POD:.spec.podSelector.matchLabels,CHIEU:.spec.policyTypes' \
    | grep -E "NS|calico-lab"
}

# probe <pod> <host> [port] [ns]
# Phân loại kết quả, và quan trọng nhất là ĐO THỜI GIAN — đó là thứ phân biệt
# "bị policy vứt gói" (treo ~4s rồi timeout) với "không ai nghe cổng" (trả lời ngay).
probe() {
  local pod=$1 host=$2 port=${3:-80} ns=${4:-$NS}
  local t0 t1 ms out verdict
  t0=$(date +%s%N)
  out=$(kubectl -n "$ns" exec "$pod" -- sh -c \
    "if command -v curl >/dev/null 2>&1; then \
       curl -sS -m 4 -o /dev/null -w 'HTTP %{http_code}' http://$host:$port/ ; \
     else \
       wget -q -T 4 -O /dev/null http://$host:$port/ && echo 'HTTP 200' ; \
     fi" 2>&1)
  t1=$(date +%s%N); ms=$(( (t1 - t0) / 1000000 ))
  case "$out" in
    *"HTTP 2"*|*"HTTP 3"*|*"HTTP 4"*) verdict="ALLOW   " ;;
    *resolve*|*"bad address"*|*"Temporary failure"*|*"Try again"*) verdict="DNS-FAIL" ;;
    *refused*)                        verdict="REFUSED " ;;
    *) [ "$ms" -ge 3500 ] && verdict="DROP    " || verdict="FAIL    " ;;
  esac
  printf '  %-12s -> %-26s %s %6sms   %s\n' \
    "$pod" "$host:$port" "$verdict" "$ms" "$(echo "$out" | tr '\n' ' ' | cut -c1-54)"
}

raw() {
  local pod=$1 host=$2 port=${3:-80} ns=${4:-$NS}
  kubectl -n "$ns" exec "$pod" -- sh -c \
    "if command -v curl >/dev/null 2>&1; then curl -v -m 4 -o /dev/null http://$host:$port/ ; \
     else wget -T 4 -O /dev/null http://$host:$port/ ; fi"
}

matrix() {
  echo "--- qua Service (có DNS + DNAT) ----------------------------------------"
  probe frontend   backend  8080
  probe frontend   database 80
  probe attacker   backend  8080
  probe attacker   database 80
  probe backend    database 80
  probe ext-client   backend.calico-lab 8080 "$EXT_NS"
  probe ext-attacker backend.calico-lab 8080 "$EXT_NS"
  echo "--- thẳng podIP (không DNS, không DNAT) --------------------------------"
  local bip; bip=$(kubectl -n "$NS" get pod backend -o jsonpath='{.status.podIP}')
  probe frontend "$bip" 80
  probe attacker "$bip" 80
  echo "--- ra Internet ---------------------------------------------------------"
  probe frontend example.com 443
}

# iface <pod> — Calico tự ghi ánh xạ pod -> veth vào CRD WorkloadEndpoint.
# Không cần tra route rồi đoán như hồi kindnet.
iface() {
  kubectl -n "$NS" get workloadendpoints \
    -o custom-columns='POD:.spec.pod,IFACE:.spec.interfaceName,IP:.spec.ipNetworks[0]' \
    | { [ -n "$1" ] && grep -E "POD|^$1 " || cat; }
}

# chains <pod> — hai chuỗi iptables của pod:
#   cali-fw-<iface> = from workload = EGRESS
#   cali-tw-<iface> = to   workload = INGRESS
chains() {
  local prof=${MINIKUBE_PROFILE:-minikube}
  local ifc
  ifc=$(kubectl -n "$NS" get workloadendpoints \
        -o jsonpath="{.items[?(@.spec.pod=='$1')].spec.interfaceName}")
  [ -z "$ifc" ] && { echo "Không tìm thấy workloadendpoint cho pod '$1'"; return 1; }
  echo "pod $1  ->  $ifc"
  docker exec "$prof" sh -c "iptables-save -t filter | grep -E 'cali-(fw|tw)-$ifc'"
}
