package com.trading.pricing.config;

import com.trading.common.avro.TickEventAvro;
import com.trading.common.model.EnrichedMarketData;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${schema.registry.url}")
    private String schemaRegistryUrl;

    // ================= CONSUMER (đọc stock.ticks) =================
    @Bean
    public ConsumerFactory<String, TickEventAvro> tickConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "pricing-risk-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        // AVRO: consumer đọc schema ID trong message, fetch schema từ Registry (cache local)
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put("schema.registry.url", schemaRegistryUrl);
        props.put("specific.avro.reader", true);   // trả về TickEventAvro thay vì GenericRecord

        // CooperativeStickyAssignor - KHẮC PHỤC nhược điểm rebalance:
        // Assignor mặc định (eager) khi rolling update: MỌI consumer nhả HẾT partition
        // rồi chia lại -> stop-the-world vài giây, tick dồn lag.
        // Cooperative: chỉ partition thật sự phải chuyển chủ mới bị thu hồi,
        // các consumer còn lại XỬ LÝ TIẾP không gián đoạn (incremental rebalance).
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                CooperativeStickyAssignor.class.getName());

        // === TUNING TỐC ĐỘ (throughput vs latency, xem DESIGN-NOTES) ===
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 65536);
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 50);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1000);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, TickEventAvro> tickListenerFactory(
            KafkaTemplate<String, Object> dltTemplate) {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, TickEventAvro>();
        factory.setConsumerFactory(tickConsumerFactory());
        factory.setBatchListener(true);
        // 12 partition / concurrency 3 = tối đa 4 pod consumer hiệu quả (khớp KEDA max)
        factory.setConcurrency(3);

        // DLQ - KHẮC PHỤC nhược điểm "retry 3 lần rồi skip mất message":
        // Hết retry -> message được publish sang "stock.ticks.DLT" kèm header
        // (exception, stacktrace, offset gốc) -> alert trên lag của DLT,
        // xem xét/sửa rồi replay lại. KHÔNG BAO GIỜ mất dữ liệu âm thầm.
        var recoverer = new DeadLetterPublishingRecoverer(dltTemplate);
        factory.setCommonErrorHandler(new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 3)));

        // Propagate traceId từ Kafka header vào span của listener
        factory.getContainerProperties().setObservationEnabled(true);
        return factory;
    }

    // ================= PRODUCER cho DLT (Object để chở mọi loại message lỗi) ====
    @Bean
    public KafkaTemplate<String, Object> dltTemplate() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        // Avro serializer để chở nguyên vẹn record lỗi sang DLT
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put("schema.registry.url", schemaRegistryUrl);
        // DLT là dữ liệu chẩn đoán quan trọng -> acks=all cho chắc
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props));
    }

    @Bean
    public NewTopic ticksDltTopic() {
        // DLT ít message -> 1 partition đủ, retention nên dài hơn topic chính
        return TopicBuilder.name("stock.ticks.DLT").partitions(1).replicas(1).build();
    }

    // ================= PRODUCER (ghi stock.enriched) =================
    @Bean
    public ProducerFactory<String, EnrichedMarketData> enrichedProducerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, EnrichedMarketData> enrichedKafkaTemplate() {
        var template = new KafkaTemplate<>(enrichedProducerFactory());
        template.setObservationEnabled(true);   // nối tiếp trace sang order-service
        return template;
    }

    @Bean
    public NewTopic enrichedTopic() {
        return TopicBuilder.name("stock.enriched").partitions(12).replicas(1).build();
    }
}
