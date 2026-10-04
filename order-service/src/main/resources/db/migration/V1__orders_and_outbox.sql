-- Schema riêng "ordersvc": nhiều service dùng chung 1 Postgres instance (demo)
-- nhưng KHÔNG đụng bảng của nhau, mỗi service 1 flyway_schema_history riêng.
CREATE TABLE orders (
    id              BIGSERIAL PRIMARY KEY,
    order_id        VARCHAR(36)  NOT NULL UNIQUE,
    client_order_id VARCHAR(64)  NOT NULL,
    status          VARCHAR(10)  NOT NULL,
    reason          VARCHAR(200),
    symbol          VARCHAR(20)  NOT NULL,
    side            VARCHAR(4)   NOT NULL,
    price           NUMERIC(18,4) NOT NULL,
    quantity        BIGINT       NOT NULL,
    ref_price       NUMERIC(18,4),
    risk_level      VARCHAR(10),
    account_id      VARCHAR(32)  NOT NULL,
    created_at      TIMESTAMPTZ  NOT NULL,
    -- DEDUPE TẬP TRUNG cấp DB: chốt chặn cuối cùng chống duplicate lệnh
    CONSTRAINT uq_client_order UNIQUE (client_order_id)
);

CREATE TABLE outbox_event (
    id             BIGSERIAL PRIMARY KEY,
    aggregate_type VARCHAR(30) NOT NULL,
    aggregate_id   VARCHAR(64) NOT NULL,
    event_type     VARCHAR(30) NOT NULL,
    topic          VARCHAR(60) NOT NULL,
    msg_key        VARCHAR(64) NOT NULL,
    payload        TEXT        NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL,
    published_at   TIMESTAMPTZ
);

-- Partial index: relay chỉ quét event chưa publish -> index nhỏ, quét nhanh
CREATE INDEX idx_outbox_unpublished ON outbox_event (id) WHERE published_at IS NULL;
