package com.example.kafkathrottling.case2;

import static com.example.kafkathrottling.case2.Case2KafkaConsumerConfig.CASE2_CONTAINER_FACTORY;

import com.example.kafkathrottling.common.DelayTimeCalculator;
import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ListenerContainerPauseService;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class Case2KafkaThrottledListener {
    public static final String TOPIC = "throttled-topic2";
    public static final String LISTENER_ID = TOPIC + "-listener";

    private final KafkaListenerEndpointRegistry registry;
    private final DelayTimeCalculator delayTimeCalculator;
    private final ListenerContainerPauseService pauser;

    @KafkaListener(
        id = LISTENER_ID,
        topics = TOPIC,
        concurrency = "3",
        containerFactory = CASE2_CONTAINER_FACTORY
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

        // throttling
        final long delayTime = delayTimeCalculator.calculateDelayTime();
        log.info("Delay Time: {}, message: {}, partition: {}, container: {}", message, delayTime, topicPartition(record), container.getListenerId());
        if(delayTime > 0) {
            pauser.pause(container, Duration.ofMillis(delayTime));
        }

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
