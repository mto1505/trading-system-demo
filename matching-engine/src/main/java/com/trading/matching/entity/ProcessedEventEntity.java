package com.trading.matching.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;

/**
 * Sổ ghi nhớ "đã xử lý": PRIMARY KEY client_order_id chính là cơ chế dedupe.
 * INSERT ... ON CONFLICT DO NOTHING trả về 0 row = đã xử lý rồi -> bỏ qua.
 */
@Entity
@Table(name = "processed_event", schema = "matchsvc")
@Getter @Setter @NoArgsConstructor
public class ProcessedEventEntity {

    @Id
    @Column(name = "client_order_id", length = 64)
    private String clientOrderId;

    @Column(name = "processed_at", nullable = false)
    private Instant processedAt;
}
