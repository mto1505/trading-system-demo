CREATE TABLE outbox_event (
    id           BIGSERIAL PRIMARY KEY,
    aggregate_id VARCHAR(64) NOT NULL,
    event_type   VARCHAR(30) NOT NULL,
    topic        VARCHAR(60) NOT NULL,
    msg_key      VARCHAR(64) NOT NULL,
    payload      TEXT        NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL,
    published_at TIMESTAMPTZ
);

CREATE INDEX idx_match_outbox_unpublished ON outbox_event (id) WHERE published_at IS NULL;
