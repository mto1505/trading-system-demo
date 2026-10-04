package com.trading.order.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.trading.common.model.EnrichedMarketData;
import com.trading.common.model.OrderResult;
import com.trading.order.consumer.MarketDataCache;
import com.trading.order.model.OrderRequest;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.util.UUID;

/**
 * Luồng đặt lệnh v3 - PHÒNG THỦ NHIỀU LỚP chống duplicate + dual-write:
 *
 *  1. Redis GET idem:{clientOrderId}   -> hit: trả kết quả cũ (nhanh, TẬP TRUNG mọi pod)
 *  2. Pre-trade check in-memory        -> staleness / risk / fat-finger
 *  3. TRANSACTIONAL OUTBOX             -> INSERT orders + outbox_event trong 1 local tx
 *     UNIQUE(client_order_id)          -> tầng dedupe CUỐI CÙNG: Redis chết vẫn chặn trùng
 *  4. Redis SET kết quả (TTL 10')      -> tăng tốc lần retry sau
 *  (5. OutboxRelay publish Kafka; matching-engine idempotent consumer)
 *
 * Redis ở đây là CACHE TĂNG TỐC, không phải source of truth của dedupe -
 * DB constraint mới là. Vì vậy Redis chết -> hệ vẫn ĐÚNG, chỉ chậm hơn 1 chút.
 */
@Slf4j
@Service
public class OrderService {

    private static final Duration IDEM_TTL = Duration.ofMinutes(10);

    private final MarketDataCache cache;
    private final OrderPersistence persistence;
    private final StringRedisTemplate redis;
    private final ObjectMapper objectMapper;
    private final Counter accepted;
    private final Counter rejected;
    private final Counter duplicates;

    @Value("${order.max-price-deviation-pct:5}")
    private BigDecimal maxDeviationPct;

    @Value("${order.max-data-age-ms:10000}")
    private long maxDataAgeMs;

    public OrderService(MarketDataCache cache,
                        OrderPersistence persistence,
                        StringRedisTemplate redis,
                        ObjectMapper objectMapper,
                        MeterRegistry registry) {
        this.cache = cache;
        this.persistence = persistence;
        this.redis = redis;
        this.objectMapper = objectMapper;
        this.accepted = Counter.builder("orders.accepted").register(registry);
        this.rejected = Counter.builder("orders.rejected").register(registry);
        this.duplicates = Counter.builder("orders.duplicate").register(registry);
    }

    /**
     * CircuitBreaker "orders-db": DB chết -> sau ngưỡng lỗi, breaker MỞ,
     * request fail-fast ngay (không xếp hàng chờ timeout DB, không cạn thread pool).
     * Fallback trả REJECTED SYSTEM_UNAVAILABLE - fail-safe: không chắc chắn thì không nhận lệnh.
     */
    @CircuitBreaker(name = "orders-db", fallbackMethod = "systemUnavailable")
    public OrderResult placeOrder(OrderRequest req) {
        // ===== Lớp 1: Redis fast-path (degrade gracefully nếu Redis chết) =====
        String idemKey = "idem:" + req.clientOrderId();
        try {
            String cached = redis.opsForValue().get(idemKey);
            if (cached != null) {
                duplicates.increment();
                return objectMapper.readValue(cached, OrderResult.class);
            }
        } catch (Exception e) {
            log.warn("Redis khong kha dung, dung DB constraint lam dedupe: {}", e.getMessage());
        }

        // ===== Lớp 2: pre-trade check =====
        OrderResult result = check(req);

        // ===== Lớp 3: persist + outbox, DB constraint chặn trùng tuyệt đối =====
        try {
            persistence.saveWithOutbox(req, result);
        } catch (DataIntegrityViolationException dup) {
            // Trùng client_order_id (retry rơi vào pod khác / Redis miss)
            duplicates.increment();
            OrderResult existing = persistence.findExisting(req.clientOrderId()).orElseThrow();
            cacheResult(idemKey, existing);
            return existing;
        }

        if ("ACCEPTED".equals(result.status())) accepted.increment(); else rejected.increment();

        // ===== Lớp 4: cache kết quả =====
        cacheResult(idemKey, result);
        return result;
    }

    private OrderResult check(OrderRequest req) {
        EnrichedMarketData md = cache.get(req.symbol()).orElse(null);

        if (md == null || System.currentTimeMillis() - md.calcTime() > maxDataAgeMs) {
            return reject(req, "NO_FRESH_MARKET_DATA");
        }
        if ("HIGH".equals(md.riskLevel())) {
            return reject(req, "RISK_LEVEL_HIGH volatility=" + md.volatility());
        }
        BigDecimal deviation = req.price().subtract(md.lastPrice()).abs()
                .divide(md.lastPrice(), 6, RoundingMode.HALF_UP)
                .multiply(BigDecimal.valueOf(100));
        if (deviation.compareTo(maxDeviationPct) > 0) {
            return reject(req, "PRICE_DEVIATION " + deviation.setScale(2, RoundingMode.HALF_UP)
                    + "% > " + maxDeviationPct + "%");
        }
        return new OrderResult(UUID.randomUUID().toString(), req.clientOrderId(), "ACCEPTED", null,
                req.symbol(), req.side(), req.price(), req.quantity(),
                md.lastPrice(), md.riskLevel(), System.currentTimeMillis());
    }

    private OrderResult reject(OrderRequest req, String reason) {
        log.warn("Tu choi lenh clientOrderId={} - {}", req.clientOrderId(), reason);
        return new OrderResult(UUID.randomUUID().toString(), req.clientOrderId(), "REJECTED", reason,
                req.symbol(), req.side(), req.price(), req.quantity(),
                null, null, System.currentTimeMillis());
    }

    private void cacheResult(String key, OrderResult result) {
        try {
            redis.opsForValue().set(key, objectMapper.writeValueAsString(result), IDEM_TTL);
        } catch (Exception e) {
            log.warn("Khong cache duoc ket qua vao Redis: {}", e.getMessage());
        }
    }

    /** Fallback khi breaker mở hoặc lỗi hệ thống: fail-fast, KHÔNG nhận lệnh mù. */
    private OrderResult systemUnavailable(OrderRequest req, Throwable t) {
        rejected.increment();
        log.error("He thong khong san sang, tu choi lenh clientOrderId={}", req.clientOrderId(), t);
        return new OrderResult(UUID.randomUUID().toString(), req.clientOrderId(), "REJECTED",
                "SYSTEM_UNAVAILABLE", req.symbol(), req.side(), req.price(), req.quantity(),
                null, null, System.currentTimeMillis());
    }
}
