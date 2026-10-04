# Bảo mật Kafka: SASL/SCRAM + TLS + ACL

Dev chạy PLAINTEXT cho nhanh. Production **bắt buộc** phải bật, vì mặc định
bất kỳ ai vào được mạng cluster đều đọc/ghi được mọi topic — nghĩa là đọc được
toàn bộ dòng lệnh của khách hàng.

## Ba lớp, ba mục đích khác nhau
| Lớp | Chống điều gì |
|---|---|
| TLS | Nghe lén trên đường truyền |
| SASL/SCRAM | Không biết client là AI (authentication) |
| ACL | Biết là ai nhưng làm quá quyền (authorization) |

Thiếu ACL mà có SASL: mọi service dùng chung một identity → một service bị chiếm
là đọc được hết. Vì vậy **mỗi service một principal riêng**.

## Tạo user SCRAM
```bash
kafka-configs.sh --bootstrap-server kafka:9092 --alter \
  --add-config 'SCRAM-SHA-512=[password=<secret>]' \
  --entity-type users --entity-name order-service
```

## Cấp ACL theo nguyên tắc đặc quyền tối thiểu
Xem `create-acls.sh`. Nguyên tắc áp dụng:
- `market-gateway`: **chỉ Write** `stock.ticks` — gateway bị chiếm cũng không đọc được lệnh
- `pricing-risk-service`: Read `stock.ticks`, Write `stock.enriched` + DLT
- `order-service`: Read `stock.enriched`, `stock.executions`; Write `stock.orders`
- `matching-engine`: Read `stock.orders`; Write `stock.executions`

Không service nào được `--operation All` hay `--topic '*'`.

## Cấu hình Spring
Thêm vào application.yml (profile `secure`) — xem `application-secure-example.yml`.
Mật khẩu **không** để trong yml: dùng biến môi trường nạp từ K8s Secret /
Vault. Trong K8s, truststore mount dạng volume từ Secret.

## Cái giá phải trả
- TLS tốn CPU (bắt tay + mã hoá) và **mất zero-copy** ở broker → throughput giảm
  đáng kể với topic tick lưu lượng cao. Đo trước khi cam kết SLA.
- Xoay vòng chứng chỉ trở thành việc vận hành định kỳ; hết hạn cert = cả hệ dừng.
- Thêm một tầng phải debug khi kết nối lỗi (lỗi SSL thường khó đọc).
