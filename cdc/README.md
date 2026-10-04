# Debezium CDC thay cho polling relay

## Vì sao đổi
Polling relay đọc bảng outbox mỗi 500ms. CDC đọc **WAL** (write-ahead log) của Postgres:
sự kiện được đẩy đi gần như tức thì, và **không có truy vấn nào chạm vào bảng**.

## Điểm mấu chốt: KHÔNG phải đổi schema
`EventRouter` của Debezium đọc đúng bảng `outbox_event` hiện có (`id`, `msg_key`,
`payload`, `topic`). Message ra Kafka giống hệt message do relay tạo — consumer
không cần sửa một dòng nào. Đây là lý do outbox pattern đáng giá: nó tách
"ghi sự kiện" khỏi "cách chuyển sự kiện đi".

## Cách chạy
```bash
docker compose -f docker-compose.yml -f docker-compose.cdc.yml up -d --build
sleep 30
./cdc/register-connectors.sh
```

## Ưu / nhược

| | Polling relay | Debezium CDC |
|---|---|---|
| Độ trễ | ~500ms (chu kỳ poll) | ~ms (đọc WAL) |
| Tải lên DB | Query định kỳ + UPDATE mark published | Chỉ đọc WAL, không query, không UPDATE |
| Hạ tầng | Không thêm gì | Kafka Connect + connector phải vận hành |
| Vận hành | Đơn giản, debug bằng SQL | Phải canh **replication slot**: connector chết lâu → WAL không được dọn → **đầy đĩa Postgres** |
| Dọn dẹp outbox | Row còn lại, phải có job xoá | Có thể cấu hình xoá ngay sau khi ghi (row chỉ cần tồn tại trong WAL) |
| Thứ tự | Theo id, do relay gửi tuần tự | Theo thứ tự commit trong WAL |

**Rủi ro lớn nhất của CDC** chính là replication slot: nó là con dao hai lưỡi —
đảm bảo không mất event, nhưng nếu connector down mà không ai để ý, Postgres giữ
lại toàn bộ WAL từ điểm đó và đĩa đầy dần. Bắt buộc phải có alert trên
`pg_replication_slots.confirmed_flush_lsn` tụt lại so với `pg_current_wal_lsn()`.

Với hệ này, polling 500ms là đủ (lệnh không cần độ trễ micro-giây ở chặng
publish). CDC đáng chuyển khi outbox lớn tới mức query polling gây tải, hoặc
khi cần độ trễ dưới 100ms.
