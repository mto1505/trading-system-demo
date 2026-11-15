{{/*
Expand the name of the chart.
*/}}
{{- define "trading.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "trading.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Common labels — Tương đương labels trong base kustomization (includeSelectors: false).
KHÔNG bao giờ đưa vào selector của Deployment để tránh lỗi immutable selector.
*/}}
{{- define "trading.commonLabels" -}}
app.kubernetes.io/part-of: {{ .Values.global.partOf }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end -}}

{{/*
Selector labels — Chỉ chứa tên service để bảo đảm tính bất biến (immutable).
*/}}
{{- define "trading.selectorLabels" -}}
app.kubernetes.io/name: {{ .name }}
{{- end -}}

{{/*
Labels đầy đủ cho một app.
*/}}
{{- define "trading.appLabels" -}}
{{ include "trading.commonLabels" .context }}
{{ include "trading.selectorLabels" . }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}

{{/*
Image string: nếu digest có giá trị thì dùng @digest (prod), ngược lại dùng :tag (dev/staging).
*/}}
{{- define "trading.image" -}}
{{- if .digest -}}
{{ .repository }}@{{ .digest }}
{{- else -}}
{{ .repository }}:{{ .tag | default "latest" }}
{{- end -}}
{{- end -}}

{{/*
Prometheus scrape annotations cho Spring Boot Actuator.
*/}}
{{- define "trading.prometheusAnnotations" -}}
prometheus.io/scrape: "true"
prometheus.io/port: "{{ .port }}"
prometheus.io/path: "/actuator/prometheus"
{{- end -}}

{{/*
Pod Security Context chuẩn restricted cho các app.
*/}}
{{- define "trading.appPodSecurityContext" -}}
runAsNonRoot: true
runAsUser: 10001
runAsGroup: 10001
fsGroup: 10001
seccompProfile:
  type: RuntimeDefault
{{- end -}}

{{/*
Container Security Context chuẩn restricted cho các app.
*/}}
{{- define "trading.appContainerSecurityContext" -}}
allowPrivilegeEscalation: false
readOnlyRootFilesystem: true
capabilities:
  drop: ["ALL"]
{{- end -}}

{{/*
TopologySpreadConstraints trải đều pod ra zone và host.
Prod dùng DoNotSchedule, dev/staging dùng ScheduleAnyway.
*/}}
{{- define "trading.topologySpread" -}}
{{- if .spread.enabled }}
topologySpreadConstraints:
  - maxSkew: 1
    topologyKey: topology.kubernetes.io/zone
    whenUnsatisfiable: {{ .spread.whenUnsatisfiable | default "ScheduleAnyway" }}
    labelSelector:
      matchLabels:
        app.kubernetes.io/name: {{ .name }}
  - maxSkew: 1
    topologyKey: kubernetes.io/hostname
    whenUnsatisfiable: {{ .spread.whenUnsatisfiable | default "ScheduleAnyway" }}
    labelSelector:
      matchLabels:
        app.kubernetes.io/name: {{ .name }}
{{- end }}
{{- end -}}

{{/*
ConfigMap checksum annotation: thay đổi config tự động kích hoạt rolling restart.
Tương đương cơ chế name suffix hash của Kustomize.
*/}}
{{- define "trading.configChecksum" -}}
checksum/config: {{ include (print $.Template.BasePath "/configmap.yaml") . | sha256sum }}
{{- end -}}

{{/*
Bảng tra: tên Service -> cổng, dựng từ chính values của 4 service.

Dùng ở CẢ ingress.yaml lẫn networkpolicy-apps.yaml. Lý do phải có:
thêm một route vào Ingress mà quên mở NetworkPolicy tương ứng thì nginx nhận
request rồi treo tới timeout -> HTTP 504, và log của app KHÔNG có gì cả vì gói
tin chưa bao giờ tới nơi. Tra chung một bảng thì hai chỗ không thể lệch nhau.
*/}}
{{- define "trading.servicePorts" -}}
{{- dict
      "market-gateway"       .Values.marketGateway.port
      "pricing-risk-service" .Values.pricingRiskService.port
      "order-service"        .Values.orderService.port
      "matching-engine"      .Values.matchingEngine.port
    | toYaml -}}
{{- end -}}
