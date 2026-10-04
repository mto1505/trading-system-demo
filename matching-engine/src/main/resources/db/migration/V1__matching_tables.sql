CREATE TABLE processed_event (
    client_order_id VARCHAR(64) PRIMARY KEY,   -- PK = cơ chế dedupe
    processed_at    TIMESTAMPTZ NOT NULL
);

CREATE TABLE execution (
    id              BIGSERIAL PRIMARY KEY,
    order_id        VARCHAR(36)   NOT NULL,
    client_order_id VARCHAR(64)   NOT NULL,
    symbol          VARCHAR(20)   NOT NULL,
    side            VARCHAR(4)    NOT NULL,
    fill_price      NUMERIC(18,4) NOT NULL,
    quantity        BIGINT        NOT NULL,
    exec_time       TIMESTAMPTZ   NOT NULL
);

CREATE INDEX idx_exec_client_order ON execution (client_order_id);
