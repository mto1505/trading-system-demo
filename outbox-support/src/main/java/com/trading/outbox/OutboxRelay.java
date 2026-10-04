package com.trading.outbox;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * POLLING PUBLISHER dùng chung cho mọi service có outbox.
 *
 * @ConditionalOnProperty outbox.relay.enabled (mặc định true):
 * đặt false khi chuyển sang Debezium CDC - CDC đọc thẳng WAL của Postgres,
 * lúc đó relay polling phải TẮT nếu không hai đường sẽ publish trùng.
 * Cùng một bảng outbox phục vụ được cả hai cơ chế, đổi bằng 1 property.
 */
@Component
@ConditionalOnProperty(name = "outbox.relay.enabled", havingValue = "true", matchIfMissing = true)
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private final OutboxStore store;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final Counter published;
    private final Counter failed;

    @Value("${outbox.relay.batch-size:100}")
    private int batchSize;

    public OutboxRelay(OutboxStore store,
                       @Qualifier("outboxKafkaTemplate") KafkaTemplate<String, String> kafkaTemplate,
                       MeterRegistry registry) {
        this.store = store;
        this.kafkaTemplate = kafkaTemplate;
        this.published = Counter.builder("outbox.published").register(registry);
        this.failed = Counter.builder("outbox.publish.failed").register(registry);
    }

    @Scheduled(fixedDelayString = "${outbox.relay.interval-ms:500}")
    @Transactional
    public void relay() {
        List<? extends OutboxRecord> batch = store.lockUnpublishedBatch(batchSize);
        for (OutboxRecord event : batch) {
            try {
                // Gửi ĐỒNG BỘ có chủ đích: chắc chắn Kafka nhận (acks=all) trước khi
                // mark published, và giữ đúng thứ tự id tăng dần.
                kafkaTemplate.send(event.getTopic(), event.getMsgKey(), event.getPayload())
                        .get(5, TimeUnit.SECONDS);
                event.setPublishedAt(Instant.now());
                published.increment();
            } catch (Exception ex) {
                failed.increment();
                log.error("Publish outbox id={} that bai, rollback ca batch de giu thu tu",
                        event.getId(), ex);
                // Rollback: không mark published cho event nào trong batch.
                // Event đã lên Kafka có thể gửi lại = at-least-once, consumer dedupe lo.
                throw new IllegalStateException("Outbox publish failed", ex);
            }
        }
    }
}
