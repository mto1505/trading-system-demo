package com.trading.pricing.consumer;

import com.trading.common.model.EnrichedMarketData;
import com.trading.common.avro.TickEventAvro;
import com.trading.common.model.TickEvent;
import com.trading.pricing.service.RiskCalculator;
import com.trading.pricing.service.SnapshotStore;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
public class TickConsumer {

    private final RiskCalculator riskCalculator;
    private final SnapshotStore snapshotStore;
    private final KafkaTemplate<String, EnrichedMarketData> enrichedTemplate;
    private final Timer processingTimer;
    private final Timer e2eLatencyTimer;

    public TickConsumer(RiskCalculator riskCalculator,
                        SnapshotStore snapshotStore,
                        KafkaTemplate<String, EnrichedMarketData> enrichedTemplate,
                        MeterRegistry registry) {
        this.riskCalculator = riskCalculator;
        this.snapshotStore = snapshotStore;
        this.enrichedTemplate = enrichedTemplate;
        // Metric quan trọng nhất của hệ thống trading:
        this.processingTimer = Timer.builder("pricing.batch.process")
                .description("Thoi gian xu ly 1 batch tick")
                .publishPercentiles(0.5, 0.95, 0.99)   // p50/p95/p99 hien tren Grafana
                .register(registry);
        this.e2eLatencyTimer = Timer.builder("pricing.e2e.latency")
                .description("Latency tu nguon toi khi tinh xong")
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry);
    }

    /**
     * BATCH LISTENER: xử lý cả lô tick trong 1 lần gọi.
     * Toàn bộ hot path chỉ gồm: tính toán in-memory + put map + send async.
     * KHÔNG có blocking I/O nào ở đây.
     */
    @KafkaListener(topics = "stock.ticks", containerFactory = "tickListenerFactory")
    public void onTicks(List<TickEventAvro> avroTicks) {
        long start = System.nanoTime();
        long now = System.currentTimeMillis();

        for (TickEventAvro avro : avroTicks) {
            // Map wire format (Avro) -> domain model (record) ngay tại biên;
            // toàn bộ logic phía trong không biết gì về Avro (dễ test, dễ đổi wire format)
            TickEvent tick = new TickEvent(avro.getSymbol(), avro.getPrice(),
                    avro.getVolume(), avro.getEventTime());
            EnrichedMarketData enriched = riskCalculator.onTick(tick);   // O(1)
            snapshotStore.update(enriched);                              // O(1), in-memory
            enrichedTemplate.send("stock.enriched", enriched.symbol(), enriched); // async
            e2eLatencyTimer.record(Math.max(0, now - tick.eventTime()), TimeUnit.MILLISECONDS);
        }
        processingTimer.record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
    }
}
