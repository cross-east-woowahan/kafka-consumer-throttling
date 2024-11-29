package com.example.kafkathrottling.case4;

import com.example.kafkathrottling.common.DelayTimeCalculator;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.ListenerContainerPauseService;
import org.springframework.kafka.listener.MessageListenerContainer;

@Slf4j
public class KafkaThrottlingInterceptor implements ConsumerInterceptor<String, String> {
    public static final String KAFKA_LISTENER_ENDPOINT_REGISTRY_CONFIG_KEY = "listenerRegistry";
    public static final String PAUSE_SERVICE_CONFIG_KEY = "pauseService";
    public static final String PAUSE_TIME_CALCULATOR_CONFIG_KEY = "pauseTimeCalculator";

    private KafkaListenerEndpointRegistry registry;
    private ListenerContainerPauseService pauser;
    private DelayTimeCalculator delayTimeCalculator;

    @Override
    public void configure(Map<String, ?> configs) {
        this.registry = (KafkaListenerEndpointRegistry) configs.get(KAFKA_LISTENER_ENDPOINT_REGISTRY_CONFIG_KEY);
        this.pauser = (ListenerContainerPauseService) configs.get(PAUSE_SERVICE_CONFIG_KEY);
        this.delayTimeCalculator = (DelayTimeCalculator) configs.get(PAUSE_TIME_CALCULATOR_CONFIG_KEY);;
    }

    @Override
    public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> records) {
        return records;
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        final var targetContainers = findTargetContainers(offsets.keySet(), findConcurrentContainers());
        log.info("onCommit. targetContainers: {}, offsets: {}", targetContainers, offsets);

        targetContainers.forEach(container -> {
            final long pauseTime = delayTimeCalculator.calculateDelayTime();
            log.info("Delay Time: {}, container: {}", pauseTime, container.getListenerId());
            if(pauseTime > 0) {
                pauser.pause(container, Duration.ofMillis(pauseTime));
            }
        });
    }

    @Override
    public void close() {

    }

    private List<ConcurrentMessageListenerContainer<String, String>> findConcurrentContainers() {
        return registry.getListenerContainers().stream()
            .filter(container -> container instanceof ConcurrentMessageListenerContainer)
            .map(container -> (ConcurrentMessageListenerContainer<String, String>) container)
            .collect(Collectors.toList());
    }

    private Set<MessageListenerContainer> findTargetContainers(
        Set<TopicPartition> topicPartitions,
        List<ConcurrentMessageListenerContainer<String, String>> concurrentContainers
    ) {
        return concurrentContainers.stream()
            .flatMap(concurrentContainer -> concurrentContainer.getContainers().stream())
            .filter(container -> containsAnyTopicPartition(topicPartitions, container))
            .collect(Collectors.toSet());
    }

    private boolean containsAnyTopicPartition(Set<TopicPartition> topicPartitions, KafkaMessageListenerContainer<String, String> container) {
        return topicPartitions.stream()
            .anyMatch(partition -> containsTopicPartition(partition, container));
    }

    private boolean containsTopicPartition(TopicPartition topicPartition, KafkaMessageListenerContainer<String, String> container) {
        final var assignedPartitions = container.getAssignedPartitions();
        return assignedPartitions != null && assignedPartitions.contains(topicPartition);
    }
}
