package com.trading.matching.entity;

import com.trading.outbox.OutboxRecord;
import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;

/**
 * Outbox của matching-engine: execution report cũng phải ra Kafka NGUYÊN TỬ
 * cùng với việc ghi execution. Không có outbox ở đây thì saga sẽ hỏng ở
 * chiều về: khớp lệnh xong, publish fail -> order-service treo ACCEPTED mãi mãi.
 */
@Entity
@Table(name = "outbox_event", schema = "matchsvc")
@Getter @Setter @NoArgsConstructor
public class OutboxEventEntity implements OutboxRecord {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "aggregate_id", nullable = false, length = 64)
    private String aggregateId;

    @Column(name = "event_type", nullable = false, length = 30)
    private String eventType;

    @Column(nullable = false, length = 60)
    private String topic;

    @Column(name = "msg_key", nullable = false, length = 64)
    private String msgKey;

    @Column(nullable = false, columnDefinition = "text")
    private String payload;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "published_at")
    private Instant publishedAt;
}
