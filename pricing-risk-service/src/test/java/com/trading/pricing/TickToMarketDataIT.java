package com.trading.pricing;

import com.trading.common.avro.TickEventAvro;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.web.client.RestTemplate;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * INTEGRATION TEST với Testcontainers: Kafka THẬT + Postgres THẬT trong Docker,
 * không mock. Verify luồng: produce tick Avro -> consumer tính risk -> REST trả enriched.
 *
 * Schema Registry: dùng MockSchemaRegistry (url "mock://scope") - producer test
 * và consumer app cùng JVM chia sẻ cùng registry in-memory, không cần container SR.
 *
 * Yêu cầu: máy chạy test phải có Docker (local dev, CI runner đều có).
 */
@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class TickToMarketDataIT {

    @Container
    static KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("trading").withUsername("trading").withPassword("trading123");

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("schema.registry.url", () -> "mock://pricing-it");
    }

    @LocalServerPort
    int port;

    @Test
    void tickStreamProducesEnrichedMarketData() {
        // Bơm 50 tick random-walk cho mã TEST qua Kafka thật
        try (KafkaProducer<String, TickEventAvro> producer = buildAvroProducer()) {
            BigDecimal price = new BigDecimal("100.0000");
            for (int i = 0; i < 50; i++) {
                double drift = 1 + (ThreadLocalRandom.current().nextDouble() - 0.5) * 0.01;
                price = price.multiply(BigDecimal.valueOf(drift)).setScale(4, RoundingMode.HALF_UP);
                TickEventAvro tick = TickEventAvro.newBuilder()
                        .setSymbol("TEST").setPrice(price)
                        .setVolume(10).setEventTime(System.currentTimeMillis()).build();
                producer.send(new ProducerRecord<>("stock.ticks", "TEST", tick));
            }
            producer.flush();
        }

        // Chờ pipeline xử lý xong rồi assert qua REST API thật
        RestTemplate rest = new RestTemplate();
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> {
                    ResponseEntity<Map> resp = rest.getForEntity(
                            "http://localhost:" + port + "/api/v1/marketdata/TEST", Map.class);
                    assertThat(resp.getStatusCode().is2xxSuccessful()).isTrue();
                    Map<?, ?> body = resp.getBody();
                    assertThat(body).isNotNull();
                    assertThat(body.get("symbol")).isEqualTo("TEST");
                    assertThat(body.get("riskLevel")).isIn("LOW", "MEDIUM", "HIGH");
                    // volatility phải được tính ra (>= 0) sau 50 tick
                    assertThat(new BigDecimal(body.get("volatility").toString()))
                            .isGreaterThanOrEqualTo(BigDecimal.ZERO);
                });
    }

    private KafkaProducer<String, TickEventAvro> buildAvroProducer() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put("schema.registry.url", "mock://pricing-it");   // cùng scope với app
        return new KafkaProducer<>(props);
    }
}
