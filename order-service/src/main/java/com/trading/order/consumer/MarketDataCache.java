package com.trading.order.consumer;

import com.trading.common.model.EnrichedMarketData;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Local read-model: luôn giữ bản ghi enriched MỚI NHẤT của mỗi symbol.
 * Ghi đè liên tục -> memory O(số symbol), không phình.
 */
@Component
public class MarketDataCache {

    private final Map<String, EnrichedMarketData> cache = new ConcurrentHashMap<>();

    @KafkaListener(topics = "stock.enriched",
                   groupId = "order-service-group",
                   containerFactory = "enrichedListenerFactory")
    public void onEnriched(EnrichedMarketData data) {
        cache.put(data.symbol(), data);
    }

    public Optional<EnrichedMarketData> get(String symbol) {
        return Optional.ofNullable(cache.get(symbol));
    }
}
