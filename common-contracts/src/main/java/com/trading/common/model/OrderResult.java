package com.trading.common.model;

import java.math.BigDecimal;

/** Kết quả lệnh: order-service -> topic stock.orders -> hệ thống khớp lệnh. */
public record OrderResult(
        String orderId,
        String clientOrderId,   // id phía client -> chìa khoá idempotency/dedupe downstream
        String status,          // ACCEPTED / REJECTED
        String reason,
        String symbol,
        String side,
        BigDecimal price,
        long quantity,
        BigDecimal refPrice,
        String riskLevel,
        long timestamp
) {}
