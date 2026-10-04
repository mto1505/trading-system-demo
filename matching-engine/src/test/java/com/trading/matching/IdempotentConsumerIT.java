package com.trading.matching;

import com.trading.matching.repository.ExecutionRepository;
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
 * TEST QUAN TRỌNG NHẤT của hệ thống: chứng minh EFFECTIVELY-ONCE.
 * Gửi cùng 1 order 3 lần (giả lập outbox relay re-send / Kafka redelivery)
 * -> bảng execution phải có ĐÚNG 1 dòng.
 */
@Testcontainers
@SpringBootTest
class IdempotentConsumerIT {

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

    @Autowired
    ExecutionRepository executionRepository;

    @Test
    void duplicateMessagesProduceExactlyOneExecution() {
        String orderJson = """
                {"orderId":"o-1","clientOrderId":"dup-test-1","status":"ACCEPTED","reason":null,
                 "symbol":"HPG","side":"BUY","price":28500.0,"quantity":100,
                 "refPrice":28450.0,"riskLevel":"LOW","timestamp":%d}
                """.formatted(System.currentTimeMillis());

        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < 3; i++) {   // GỬI TRÙNG 3 LẦN
                producer.send(new ProducerRecord<>("stock.orders", "HPG", orderJson));
            }
            producer.flush();
        }

        // Chờ consumer xử lý cả 3, rồi khẳng định chỉ có 1 execution
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(500))
                .untilAsserted(() ->
                        assertThat(executionRepository.findByClientOrderId("dup-test-1")).hasSize(1));

        // Giữ nguyên khẳng định thêm 3s nữa (đề phòng message trễ tạo thêm dòng)
        await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(10))
                .untilAsserted(() ->
                        assertThat(executionRepository.findByClientOrderId("dup-test-1")).hasSize(1));
    }
}
