-- Vòng đời lệnh: ACCEPTED -> FILLED | REJ_MATCH (do matching-engine phản hồi)
ALTER TABLE orders ADD COLUMN version        BIGINT        NOT NULL DEFAULT 0;
ALTER TABLE orders ADD COLUMN updated_at     TIMESTAMPTZ;
ALTER TABLE orders ADD COLUMN filled_qty     BIGINT;
ALTER TABLE orders ADD COLUMN avg_fill_price NUMERIC(18,4);

-- Truy vấn vận hành hay dùng: "lệnh nào đang treo chưa có phản hồi khớp?"
CREATE INDEX idx_orders_pending ON orders (created_at) WHERE status = 'ACCEPTED';
