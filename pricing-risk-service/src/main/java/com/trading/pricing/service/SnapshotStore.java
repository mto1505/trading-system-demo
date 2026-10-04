package com.trading.pricing.service;

import com.trading.common.model.EnrichedMarketData;
import com.trading.pricing.model.MarketSnapshotEntity;
import com.trading.pricing.repository.MarketSnapshotRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * WRITE-BEHIND CACHE:
 * - Hot path: chỉ update ConcurrentHashMap (nanoseconds)
 * - Cold path: @Scheduled flush snapshot mới nhất xuống Postgres mỗi 5s (batch insert)
 *
 * Ưu điểm: DB chậm/chết KHÔNG ảnh hưởng luồng tính toán realtime.
 * Nhược điểm: mất tối đa 5s dữ liệu snapshot nếu pod chết (chấp nhận được
 * vì Kafka vẫn giữ raw tick, có thể replay để rebuild).
 */
@Slf4j
@Service
public class SnapshotStore {

    private final Map<String, EnrichedMarketData> latest = new ConcurrentHashMap<>();
    private final MarketSnapshotRepository repository;

    public SnapshotStore(MarketSnapshotRepository repository) {
        this.repository = repository;
    }

    public void update(EnrichedMarketData data) {
        latest.put(data.symbol(), data);   // hot path - O(1), lock-free read
    }

    public Optional<EnrichedMarketData> get(String symbol) {
        return Optional.ofNullable(latest.get(symbol));
    }

    public Map<String, EnrichedMarketData> all() {
        return Map.copyOf(latest);
    }

    @Scheduled(fixedDelay = 5000)
    public void flushToDatabase() {
        if (latest.isEmpty()) return;
        Instant now = Instant.now();
        List<MarketSnapshotEntity> batch = latest.values().stream().map(d -> {
            MarketSnapshotEntity e = new MarketSnapshotEntity();
            e.setSymbol(d.symbol());
            e.setLastPrice(d.lastPrice());
            e.setEmaPrice(d.emaPrice());
            e.setVolatility(d.volatility());
            e.setVar95(d.var95());
            e.setRiskLevel(d.riskLevel());
            e.setSnapTime(now);
            return e;
        }).toList();
        try {
            repository.saveAll(batch);   // Hibernate batch insert (xem application.yml)
        } catch (Exception ex) {
            // Không ném exception: DB lỗi không được làm chết scheduler / hot path
            log.error("Flush DB that bai, se thu lai lan sau", ex);
        }
    }
}
