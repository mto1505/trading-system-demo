package com.trading.matching.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.math.BigDecimal;
import java.time.Instant;

/** Kết quả khớp lệnh (giả lập fill toàn bộ tại giá tham chiếu). */
@Entity
@Table(name = "execution", schema = "matchsvc")
@Getter @Setter @NoArgsConstructor
public class ExecutionEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "order_id", nullable = false, length = 36)
    private String orderId;

    @Column(name = "client_order_id", nullable = false, length = 64)
    private String clientOrderId;

    @Column(nullable = false, length = 20)
    private String symbol;

    @Column(nullable = false, length = 4)
    private String side;

    @Column(name = "fill_price", nullable = false, precision = 18, scale = 4)
    private BigDecimal fillPrice;

    @Column(nullable = false)
    private Long quantity;

    @Column(name = "exec_time", nullable = false)
    private Instant execTime;
}
