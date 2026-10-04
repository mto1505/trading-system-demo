package com.trading.pricing.controller;

import com.trading.common.model.EnrichedMarketData;
import com.trading.pricing.model.MarketSnapshotEntity;
import com.trading.pricing.repository.MarketSnapshotRepository;
import com.trading.pricing.service.SnapshotStore;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * REST API cung cấp data cho order-service (hoặc UI):
 * - Realtime: đọc từ in-memory snapshot (<1ms)
 * - Lịch sử: đọc từ Postgres
 */
@RestController
@RequestMapping("/api/v1/marketdata")
public class MarketDataController {

    private final SnapshotStore store;
    private final MarketSnapshotRepository repository;

    public MarketDataController(SnapshotStore store, MarketSnapshotRepository repository) {
        this.store = store;
        this.repository = repository;
    }

    @GetMapping("/{symbol}")
    public ResponseEntity<EnrichedMarketData> latest(@PathVariable String symbol) {
        return store.get(symbol)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping
    public Map<String, EnrichedMarketData> all() {
        return store.all();
    }

    @GetMapping("/{symbol}/history")
    public List<MarketSnapshotEntity> history(@PathVariable String symbol,
                                              @RequestParam(defaultValue = "50") int limit) {
        return repository.findRecent(symbol, limit);
    }
}
