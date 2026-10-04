package com.trading.order.config;

import com.trading.common.model.EnrichedMarketData;
import com.trading.common.model.ExecutionReport;
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
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    // ============ CONSUMER stock.enriched (local read model) ============
    @Bean
    public ConsumerFactory<String, EnrichedMarketData> enrichedConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "order-service-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, EnrichedMarketData.class);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.trading.*");
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                CooperativeStickyAssignor.class.getName());
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, EnrichedMarketData> enrichedListenerFactory() {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, EnrichedMarketData>();
        factory.setConsumerFactory(enrichedConsumerFactory());
        factory.setConcurrency(3);
        factory.getContainerProperties().setObservationEnabled(true);
        return factory;
    }

    // ============ PRODUCER cho OutboxRelay ============
    // Payload đã là JSON string (serialize lúc ghi outbox) -> StringSerializer.
    // Lệnh không được mất/trùng do producer retry: acks=all + idempotence.
    @Bean
    public KafkaTemplate<String, String> outboxKafkaTemplate() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        var template = new KafkaTemplate<>(new DefaultKafkaProducerFactory<String, String>(props));
        template.setObservationEnabled(true);
        return template;
    }

    // ============ CONSUMER stock.executions (reply leg cua saga) ============
    @Bean
    public ConsumerFactory<String, ExecutionReport> executionConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "order-service-exec-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, ExecutionReport.class);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.trading.*");
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
        // KHAC voi enriched (latest): ket qua khop lenh KHONG duoc bo sot -> earliest
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                CooperativeStickyAssignor.class.getName());
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, ExecutionReport> executionListenerFactory() {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, ExecutionReport>();
        factory.setConsumerFactory(executionConsumerFactory());
        factory.setConcurrency(3);
        factory.getContainerProperties().setObservationEnabled(true);
        return factory;
    }

    @Bean
    public NewTopic executionsTopic() {
        return TopicBuilder.name("stock.executions").partitions(12).replicas(1).build();
    }

    @Bean
    public NewTopic ordersTopic() {
        return TopicBuilder.name("stock.orders").partitions(12).replicas(1).build();
    }
}
