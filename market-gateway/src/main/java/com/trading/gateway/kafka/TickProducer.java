package com.trading.gateway.kafka;

import com.trading.common.avro.TickEventAvro;
import com.trading.common.model.TickEvent;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.math.RoundingMode;

@Slf4j
@Service
public class TickProducer {

    private final KafkaTemplate<String, TickEventAvro> kafkaTemplate;
    private final Counter sentCounter;
    private final Counter errorCounter;

    public TickProducer(KafkaTemplate<String, TickEventAvro> kafkaTemplate, MeterRegistry registry) {
        this.kafkaTemplate = kafkaTemplate;
        this.sentCounter = Counter.builder("gateway.ticks.sent").register(registry);
        this.errorCounter = Counter.builder("gateway.ticks.error").register(registry);
    }

    public void send(TickEvent tick) {
        // Map domain model (record) -> wire format (Avro). Tách 2 tầng có chủ đích:
        // REST API / code nội bộ dùng record gọn gàng, wire format do schema quản.
        // Decimal logical type yêu cầu ĐÚNG scale=4 như khai báo trong .avsc.
        TickEventAvro avro = TickEventAvro.newBuilder()
                .setSymbol(tick.symbol())
                .setPrice(tick.price().setScale(4, RoundingMode.HALF_UP))
                .setVolume(tick.volume())
                .setEventTime(tick.eventTime())
                .build();

        kafkaTemplate.send("stock.ticks", tick.symbol(), avro)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        errorCounter.increment();
                        log.error("Gui tick that bai: {}", tick, ex);
                    } else {
                        sentCounter.increment();
                    }
                });
    }
}
