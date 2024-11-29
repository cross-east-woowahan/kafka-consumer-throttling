package com.example.kafkathrottling.case3;

import static com.example.kafkathrottling.case3.Case3KafkaConsumerConfig.CASE3_CONTAINER_FACTORY;

import com.example.kafkathrottling.common.DelayTimeCalculator;
import java.time.Duration;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ListenerContainerPauseService;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class Case3KafkaThrottledListener {
    public static final String TOPIC = "throttled-topic3";
    public static final String LISTENER_ID = TOPIC + "-listener";

    private final KafkaListenerEndpointRegistry registry;
    private final DelayTimeCalculator delayTimeCalculator;
    private final ListenerContainerPauseService pauser;

    @KafkaListener(
        id = LISTENER_ID,
        topics = TOPIC,
        concurrency = "3",
        containerFactory = CASE3_CONTAINER_FACTORY
    )
    public void listen(
        ConsumerRecords<String, String> records,
        Acknowledgment ack
    ) {
        final var container = getContainer(records);
        records.forEach(record -> {
            log.info("Processing Message: {}, partition: {}, container: {}", record.value(), topicPartition(record), container.getListenerId());

            // takes 50ms
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        // throttling
        final long delayTime = delayTimeCalculator.calculateDelayTime();
        log.info("Delay Time: {}, partition: {}", delayTime, records.partitions());
        if(delayTime > 0) {
            pauser.pause(container, Duration.ofMillis(delayTime));
        }

        // acknowledge
        log.info("Ack partitions: {}, Messages Count: {}", records.partitions(), records.count());
        ack.acknowledge();
    }

    private MessageListenerContainer getContainer(ConsumerRecords<String, String> records) {
        final var anyPartition = records.partitions().stream().findAny().get();
        final var concurrentContainer = (ConcurrentMessageListenerContainer<String, String>) registry.getListenerContainer(LISTENER_ID);
        return concurrentContainer.getContainerFor(anyPartition.topic(), anyPartition.partition());
    }

    private String topicPartition(ConsumerRecord<String, String> record) {
        return record.topic() + "-" + record.partition();
    }
}
