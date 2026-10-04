package com.trading.gateway.config;

import com.trading.common.avro.TickEventAvro;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Producer AVRO + SCHEMA REGISTRY thay cho JSON:
 * - Message nhỏ hơn 3-5x (binary, không lặp tên field mỗi message)
 * - Schema evolution ĐƯỢC CƯỠNG CHẾ: producer đăng ký schema mới với Registry,
 *   Registry kiểm tra tương thích (BACKWARD mặc định) - deploy phá vỡ consumer
 *   sẽ FAIL NGAY LÚC PRODUCE đầu tiên chứ không âm thầm làm consumer chết.
 * - Message chỉ mang schema ID (4 byte), consumer tự fetch schema từ Registry (có cache).
 */
@Configuration
public class KafkaProducerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${schema.registry.url}")
    private String schemaRegistryUrl;

    @Bean
    public ProducerFactory<String, TickEventAvro> tickProducerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put("schema.registry.url", schemaRegistryUrl);
        // demo: tự đăng ký schema; PRODUCTION nên false - schema đăng ký qua CI/CD
        // có review, producer chỉ được dùng schema đã duyệt
        props.put("auto.register.schemas", true);

        // === TUNING TỐC ĐỘ (như cũ, xem DESIGN-NOTES) ===
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67108864);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, TickEventAvro> tickKafkaTemplate() {
        KafkaTemplate<String, TickEventAvro> template = new KafkaTemplate<>(tickProducerFactory());
        template.setObservationEnabled(true);
        return template;
    }

    /**
     * Topic 12 partition: trần cứng của scale-out consumer, chỉ tăng được
     * không giảm được -> provision dư từ đầu.
     */
    @Bean
    public NewTopic ticksTopic() {
        return TopicBuilder.name("stock.ticks").partitions(12).replicas(1).build();
    }
}
