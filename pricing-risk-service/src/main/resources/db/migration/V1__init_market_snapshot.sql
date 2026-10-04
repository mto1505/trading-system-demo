-- Flyway migration V1: schema được version-control, KHÔNG để Hibernate tự sửa.
-- Bài học từ mọi cuộc migration DB (DB2->MySQL...): schema drift là kẻ thù số 1.
CREATE TABLE market_snapshot (
    id          BIGSERIAL PRIMARY KEY,
    symbol      VARCHAR(20)    NOT NULL,
    last_price  NUMERIC(18,4),
    ema_price   NUMERIC(18,4),
    volatility  NUMERIC(10,6),
    var95       NUMERIC(10,6),
    risk_level  VARCHAR(10),
    snap_time   TIMESTAMPTZ    NOT NULL
);

CREATE INDEX idx_symbol_time ON market_snapshot (symbol, snap_time);
