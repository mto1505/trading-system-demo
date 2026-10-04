# Cluster prerequisites

The NetworkPolicies select peer namespaces by the `kubernetes.io/metadata.name`
label, which the API server sets automatically on every namespace (k8s >= 1.22).
No manual labelling is required for `ingress-nginx` or `monitoring`, but those
namespaces must exist with exactly those names - adjust the policies if your
ingress controller or Prometheus lives elsewhere.

Required cluster add-ons:

| Add-on                     | Needed for                                  | Hard requirement? |
|----------------------------|---------------------------------------------|-------------------|
| CNI with NetworkPolicy     | every NetworkPolicy in `base/`              | yes - without it the policies are silently ignored |
| metrics-server             | HorizontalPodAutoscaler                     | yes |
| ingress-nginx              | Ingress                                     | yes |
| cert-manager               | TLS certificates                            | prod/staging |
| kube-prometheus-stack      | `components/monitoring`                     | recommended |
| external-secrets-operator  | production credentials                      | prod |
| A StorageClass with `reclaimPolicy: Retain` | prod Postgres volume        | prod |

> minikube's default CNI does NOT enforce NetworkPolicy. Start with
> `minikube start --cni=calico` if you want the policies to actually take effect.

## Minimum Kubernetes version: 1.30

| Feature used | Available since |
|---|---|
| `lifecycle.preStop.sleep` | 1.30 (beta, on by default), GA 1.32 |
| `PodDisruptionBudget.spec.unhealthyPodEvictionPolicy` | 1.27 |
| `CronJob.spec.timeZone` | 1.27 |
| Pod Security Admission (`pod-security.kubernetes.io/*`) | 1.25 |
| `autoscaling/v2` HPA `behavior` | 1.23 |

On an older cluster, replace the `preStop.sleep` action with
`exec: { command: ["sleep", "5"] }` - note that requires a shell/coreutils in the
image, which the distroless-style runtime stage does not have.
