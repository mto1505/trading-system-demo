package com.trading.common.model;

import java.math.BigDecimal;

/** Dữ liệu đã làm giàu: pricing-risk-service -> topic stock.enriched -> order-service. */
public record EnrichedMarketData(
        String symbol,
        BigDecimal lastPrice,
        BigDecimal emaPrice,
        BigDecimal volatility,
        BigDecimal var95,
        String riskLevel,        // LOW / MEDIUM / HIGH
        long calcTime,
        long sourceEventTime
) {}
