package com.example.kafkathrottling;

import com.example.kafkathrottling.case4.Case4Import;
import com.example.kafkathrottling.case4.Case4KafkaThrottledListener;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;

@EnableAutoConfiguration
@SpringBootTest(classes = {Case4Import.class})
@EmbeddedKafka(
    partitions = 6,
    topics = {Case4KafkaThrottledListener.TOPIC},
    brokerProperties = {
        "listeners=PLAINTEXT://localhost:9092",
        "port=9092",
        "max.poll.records=5",
        "auto.create.topics.enable=true",
    }
)
@Slf4j
public class Case4KafkaThrottlingTest {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Test
    @DisplayName("Consumer throttling test with pause() and resume()")
    void consumerThrottlingTest() throws InterruptedException {
        IntStream.range(0, 100).forEach(this::sendMessage);

        Thread.sleep(10000L);
    }

    private void sendMessage(Integer i) {
        try {
            final var result = kafkaTemplate.send(Case4KafkaThrottledListener.TOPIC, String.valueOf(i), "message" + i).get();
            final var partition = result.getRecordMetadata().partition();
            log.info("Sent message: {}, partition: {}", i, partition);
        } catch (Exception e) {
            log.error("Error sending message: {}", e.getMessage());
        }
    }
}
