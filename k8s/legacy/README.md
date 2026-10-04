# Manifest cũ (phẳng, một namespace)

Những file ở đây là bộ manifest `kubectl apply -f k8s/` ban đầu: một namespace
`trading` duy nhất, không tách môi trường, secret ghi thẳng trong YAML.

Chúng được **thay thế** bằng `k8s/base` + `k8s/overlays/{dev,staging,prod}`.
Giữ lại để đối chiếu khi review, và vì một vài quyết định thiết kế (bình luận
về KEDA, về partition/concurrency, về PDB) đã được chuyển sang cấu trúc mới.

Đừng apply thư mục này nữa — nó sẽ tạo namespace `trading` song song với
`trading-dev` / `trading-staging` / `trading-prod`.
