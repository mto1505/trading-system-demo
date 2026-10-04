package com.trading.matching;

import com.trading.matching.repository.ExecutionRepository;
import com.trading.matching.repository.OutboxEventRepository;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * SAGA - chiều về: lệnh qua được pre-trade check của order-service nhưng
 * vi phạm luật sàn (giá lệch 40% so với tham chiếu, quá biên độ 7%).
 * Kỳ vọng: KHÔNG có execution, và có một execution report REJECTED nằm
 * trong outbox để trả ngược về order-service.
 */
@Testcontainers
@SpringBootTest
class SagaRejectionIT {

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
    }

    @Autowired ExecutionRepository executionRepository;
    @Autowired OutboxEventRepository outboxRepository;

    @Test
    void orderOutsidePriceBandIsRejectedAndReported() {
        String clientOrderId = "saga-reject-1";
        // refPrice 28450 nhưng đặt 40000 -> lệch ~40% > biên độ 7%
        String orderJson = """
                {"orderId":"o-9","clientOrderId":"%s","status":"ACCEPTED","reason":null,
                 "symbol":"HPG","side":"BUY","price":40000.0,"quantity":100,
                 "refPrice":28450.0,"riskLevel":"LOW","timestamp":%d}
                """.formatted(clientOrderId, System.currentTimeMillis());

        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>("stock.orders", "HPG", orderJson));
            producer.flush();
        }

        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> {
                    assertThat(outboxRepository.findAll())
                            .anyMatch(e -> e.getAggregateId().equals(clientOrderId)
                                    && e.getTopic().equals("stock.executions")
                                    && e.getPayload().contains("REJECTED")
                                    && e.getPayload().contains("PRICE_OUT_OF_BAND"));
                    // và tuyệt đối không được có execution nào
                    assertThat(executionRepository.findByClientOrderId(clientOrderId)).isEmpty();
                });
    }
}
