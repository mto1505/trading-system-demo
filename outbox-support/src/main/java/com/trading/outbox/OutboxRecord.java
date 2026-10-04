package com.trading.outbox;

import java.time.Instant;

/**
 * PORT: relay chỉ cần biết ngần này về một outbox event.
 *
 * Vì sao KHÔNG dùng chung một @Entity JPA giữa các service?
 * Shared entity kéo theo ràng buộc schema, JPA config, và vòng đời migration
 * chung giữa các service - đúng loại coupling mà microservices muốn tránh.
 * Mỗi service tự sở hữu bảng outbox của mình (schema ordersvc / matchsvc),
 * chỉ chia sẻ HÀNH VI (relay) chứ không chia sẻ CẤU TRÚC DỮ LIỆU.
 */
public interface OutboxRecord {
    Long getId();
    String getTopic();
    String getMsgKey();
    String getPayload();
    void setPublishedAt(Instant publishedAt);
}
