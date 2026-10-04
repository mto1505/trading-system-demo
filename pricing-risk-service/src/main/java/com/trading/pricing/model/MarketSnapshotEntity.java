package com.trading.pricing.model;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Bảng lưu snapshot giá & rủi ro (ghi định kỳ, không ghi từng tick).
 * Lưu ý migration DB2->MySQL/Postgres: tránh cột tên trùng reserved keyword.
 */
@Entity
@Table(name = "market_snapshot", indexes = @Index(name = "idx_symbol_time", columnList = "symbol, snap_time"))
@Getter @Setter @NoArgsConstructor
public class MarketSnapshotEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, length = 20)
    private String symbol;

    @Column(name = "last_price", precision = 18, scale = 4)
    private BigDecimal lastPrice;

    @Column(name = "ema_price", precision = 18, scale = 4)
    private BigDecimal emaPrice;

    @Column(precision = 10, scale = 6)
    private BigDecimal volatility;

    @Column(name = "var95", precision = 10, scale = 6)
    private BigDecimal var95;

    @Column(name = "risk_level", length = 10)
    private String riskLevel;

    @Column(name = "snap_time", nullable = false)
    private Instant snapTime;
}
