package com.trading.common.model;

import java.math.BigDecimal;

/**
 * Kết quả xử lý lệnh do matching-engine phát ngược về (topic stock.executions).
 * Đây là "reply leg" của SAGA CHOREOGRAPHY:
 *   order-service --stock.orders--> matching-engine --stock.executions--> order-service
 *
 * status: FILLED | REJECTED  (REJECTED = matching từ chối vì luật giao dịch,
 * khác với REJECTED lúc pre-trade check ở order-service).
 */
public record ExecutionReport(
        String orderId,
        String clientOrderId,
        String symbol,
        String side,
        String status,
        String reason,
        BigDecimal fillPrice,
        Long filledQty,
        long execTime
) {}
