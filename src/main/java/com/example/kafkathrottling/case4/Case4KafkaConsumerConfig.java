package com.example.kafkathrottling.case4;

import com.example.kafkathrottling.common.DelayTimeCalculator;
import com.example.kafkathrottling.common.KafkaThrottlingConfig;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.listener.ListenerContainerPauseService;

@Import(KafkaThrottlingConfig.class)
@Configuration
public class Case4KafkaConsumerConfig {
    public static final String CASE4_CONTAINER_FACTORY = "case4ContainerFactory";

    private final KafkaListenerEndpointRegistry registry;
    private final ListenerContainerPauseService pauser;
    private final DelayTimeCalculator delayTimeCalculator;

    public Case4KafkaConsumerConfig(
        KafkaListenerEndpointRegistry registry,
        ListenerContainerPauseService pauser,
        DelayTimeCalculator delayTimeCalculator
    ) {
        this.registry = registry;
        this.pauser = pauser;
        this.delayTimeCalculator = delayTimeCalculator;
    }

    @Bean(name = CASE4_CONTAINER_FACTORY)
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> kafkaListenerContainerFactory() {
        final var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
        factory.setConsumerFactory(consumerFactory());
        factory.getContainerProperties().setAckMode(AckMode.MANUAL);
        return factory;
    }

    private ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 5);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        // interceptor
        props.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, KafkaThrottlingInterceptor.class.getName());
        props.put(KafkaThrottlingInterceptor.KAFKA_LISTENER_ENDPOINT_REGISTRY_CONFIG_KEY, this.registry);
        props.put(KafkaThrottlingInterceptor.PAUSE_SERVICE_CONFIG_KEY, this.pauser);
        props.put(KafkaThrottlingInterceptor.PAUSE_TIME_CALCULATOR_CONFIG_KEY, this.delayTimeCalculator);

        // manual ackmode
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        return new DefaultKafkaConsumerFactory<>(props);
    }


}
