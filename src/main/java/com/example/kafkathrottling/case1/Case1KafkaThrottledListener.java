package com.example.kafkathrottling.case1;

import static com.example.kafkathrottling.case1.Case1KafkaConsumerConfig.CASE1_CONTAINER_FACTORY;

import com.example.kafkathrottling.common.DelayTimeCalculator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
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
public class Case1KafkaThrottledListener {
    public static final String TOPIC = "throttled-topic1";
    public static final String LISTENER_ID = TOPIC + "-listener";

    private final DelayTimeCalculator delayTimeCalculator;

    @KafkaListener(
        id = LISTENER_ID,
        topics = TOPIC,
        concurrency = "3",
        containerFactory = CASE1_CONTAINER_FACTORY
    )
    public void listen(
        ConsumerRecord<String, String> record,
        @Payload String message,
        Acknowledgment ack
    ) throws InterruptedException {
        log.info("Processing Message: {}, partition: {}", message, topicPartition(record));

        // takes 50ms
        Thread.sleep(50);
        ack.acknowledge();
        log.info("Ack Message: {}, partition: {}", message, topicPartition(record));

        // throttling
        final long delayTime = delayTimeCalculator.calculateDelayTime();
        log.info("Delay Time: {}, message: {}, partition: {}", delayTime, message, topicPartition(record));
        if(delayTime > 0) {
            Thread.sleep(delayTime);
        }
        log.info("Delay End: {}, message: {}, partition: {}", delayTime, message, topicPartition(record));
    }

    private String topicPartition(ConsumerRecord<String, String> record) {
        return record.topic() + "-" + record.partition();
    }
}
