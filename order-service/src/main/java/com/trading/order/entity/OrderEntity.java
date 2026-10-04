package com.trading.order.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Bảng orders - hồ sơ mọi lệnh (kể cả REJECTED, phục vụ audit).
 * UNIQUE(client_order_id) = DEDUPE TẬP TRUNG cấp DB: dù Redis chết,
 * dù retry rơi vào pod khác, DB constraint vẫn chặn lệnh trùng tuyệt đối.
 */
@Entity
@Table(name = "orders", schema = "ordersvc")
@Getter @Setter @NoArgsConstructor
public class OrderEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "order_id", nullable = false, unique = true, length = 36)
    private String orderId;

    @Column(name = "client_order_id", nullable = false, unique = true, length = 64)
    private String clientOrderId;

    @Column(nullable = false, length = 10)
    private String status;

    @Column(length = 200)
    private String reason;

    @Column(nullable = false, length = 20)
    private String symbol;

    @Column(nullable = false, length = 4)
    private String side;

    @Column(nullable = false, precision = 18, scale = 4)
    private BigDecimal price;

    @Column(nullable = false)
    private Long quantity;

    @Column(name = "ref_price", precision = 18, scale = 4)
    private BigDecimal refPrice;

    @Column(name = "risk_level", length = 10)
    private String riskLevel;

    @Column(name = "account_id", nullable = false, length = 32)
    private String accountId;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at")
    private Instant updatedAt;

    @Column(name = "filled_qty")
    private Long filledQty;

    @Column(name = "avg_fill_price", precision = 18, scale = 4)
    private BigDecimal avgFillPrice;

    /**
     * OPTIMISTIC LOCKING: hai execution report tới song song trên 2 pod
     * cùng UPDATE một lệnh -> pod thua cuộc nhận OptimisticLockingFailureException
     * và retry trên dữ liệu mới, thay vì âm thầm ghi đè (lost update).
     * Chọn optimistic thay vì pessimistic vì xung đột là HIẾM - khoá bi quan
     * sẽ trả giá lock trên mọi update chỉ để phòng trường hợp hiếm.
     */
    @Version
    @Column(nullable = false)
    private Long version;
}
