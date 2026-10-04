package com.trading.gateway.feed;

import com.trading.gateway.kafka.TickProducer;
import com.trading.common.model.TickEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Feed giả lập (random walk) để demo. Tắt bằng feed.simulated.enabled=false
 * khi có nguồn dữ liệu thật.
 */
@Component
public class SimulatedFeed {

    private final TickProducer producer;
    private final Map<String, BigDecimal> lastPrices = new ConcurrentHashMap<>(Map.of(
            "VN30F2607", new BigDecimal("1350.5"),
            "HPG", new BigDecimal("28500"),
            "VCB", new BigDecimal("92000"),
            "FPT", new BigDecimal("135000")
    ));

    @Value("${feed.simulated.enabled:true}")
    private boolean enabled;

    public SimulatedFeed(TickProducer producer) {
        this.producer = producer;
    }

    @Scheduled(fixedRate = 200) // 4 mã x 5 lần/giây = 20 tick/s (demo)
    public void generate() {
        if (!enabled) return;
        long now = System.currentTimeMillis();
        lastPrices.replaceAll((symbol, price) -> {
            // Random walk +-0.2%
            double drift = 1 + (ThreadLocalRandom.current().nextDouble() - 0.5) * 0.004;
            BigDecimal next = price.multiply(BigDecimal.valueOf(drift))
                    .setScale(2, RoundingMode.HALF_UP);
            long vol = ThreadLocalRandom.current().nextLong(1, 500);
            producer.send(new TickEvent(symbol, next, vol, now));
            return next;
        });
    }
}
