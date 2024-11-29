package com.example.kafkathrottling.case4;

import static com.example.kafkathrottling.case4.Case4KafkaConsumerConfig.CASE4_CONTAINER_FACTORY;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class Case4KafkaThrottledListener {
    public static final String TOPIC = "throttled-topic4";
    public static final String LISTENER_ID = TOPIC + "-listener";

    private final KafkaListenerEndpointRegistry registry;

    @KafkaListener(
        id = LISTENER_ID,
        topics = TOPIC,
        concurrency = "3",
        containerFactory = CASE4_CONTAINER_FACTORY
    )
    public void listen(
        ConsumerRecord<String, String> record,
        @Payload String message,
        Acknowledgment ack
    ) throws InterruptedException {
        final var container = getContainer(record);
        log.info("Processing Message: {}, partition: {}, container: {}", message, topicPartition(record), container.getListenerId());

        // takes 50ms
        Thread.sleep(50);

        // acknowledge
        ack.acknowledge();
        log.info("Ack Message: {}, partition: {}, container: {}", message, topicPartition(record), container.getListenerId());
    }

    private MessageListenerContainer getContainer(ConsumerRecord<String, String> record) {
        final var concurrentContainer = (ConcurrentMessageListenerContainer<String, String>) registry.getListenerContainer(LISTENER_ID);
        return concurrentContainer.getContainerFor(record.topic(), record.partition());
    }

    private String topicPartition(ConsumerRecord<String, String> record) {
        return record.topic() + "-" + record.partition();
    }
}
