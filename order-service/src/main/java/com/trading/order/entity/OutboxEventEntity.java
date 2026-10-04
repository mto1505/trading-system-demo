package com.trading.order.entity;

import com.trading.outbox.OutboxRecord;
import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;

/**
 * TRANSACTIONAL OUTBOX - lời giải cho distributed transaction giữa DB và Kafka.
 *
 * Vấn đề dual-write: ghi DB xong publish Kafka fail (hoặc ngược lại)
 * -> hai hệ thống lệch nhau vĩnh viễn. XA/2PC không dùng được:
 * Kafka không hỗ trợ XA, và 2PC blocking giết throughput.
 *
 * Giải pháp: KHÔNG ghi 2 hệ thống. Bảng orders + bảng outbox_event được insert
 * trong CÙNG MỘT local ACID transaction của Postgres (nguyên tử tuyệt đối).
 * OutboxRelay sau đó đọc outbox -> publish Kafka -> đánh dấu published_at.
 * Crash ở bất kỳ điểm nào: event chưa mark thì relay gửi lại -> AT-LEAST-ONCE,
 * và idempotent consumer ở matching-engine biến nó thành effectively-once.
 */
@Entity
@Table(name = "outbox_event", schema = "ordersvc")
@Getter @Setter @NoArgsConstructor
public class OutboxEventEntity implements OutboxRecord {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "aggregate_type", nullable = false, length = 30)
    private String aggregateType;

    @Column(name = "aggregate_id", nullable = false, length = 64)
    private String aggregateId;

    @Column(name = "event_type", nullable = false, length = 30)
    private String eventType;

    @Column(nullable = false, length = 60)
    private String topic;

    @Column(name = "msg_key", nullable = false, length = 64)
    private String msgKey;

    @Column(nullable = false, columnDefinition = "text")
    private String payload;      // JSON của OrderResult

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "published_at")
    private Instant publishedAt; // NULL = chưa publish
}
