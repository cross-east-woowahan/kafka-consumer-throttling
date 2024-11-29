package com.example.kafkathrottling.common;

import org.springframework.boot.task.ThreadPoolTaskSchedulerBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.ListenerContainerPauseService;
import org.springframework.scheduling.TaskScheduler;

@Configuration
public class KafkaThrottlingConfig {
    // 쓰로틀링 대상이 되는 MessageListenerContainer의 개수(ConcurrentMessageListenerContainer 내부 포함)를
    // 넘어서는 쓰레드가 사용되지 않으므로 고려해서 크기를 결정한다.
    private static final int THROTTLING_POOL_SIZE = 3;

    private final KafkaListenerEndpointRegistry registry;

    public KafkaThrottlingConfig(KafkaListenerEndpointRegistry registry) {
        this.registry = registry;
    }

    @Bean
    public ListenerContainerPauseService listenerContainerPauseService() {
        return new ListenerContainerPauseService(registry, throttlingTaskScheduler());
    }

    @Bean
    public TaskScheduler throttlingTaskScheduler() {
        return new ThreadPoolTaskSchedulerBuilder()
            .poolSize(THROTTLING_POOL_SIZE)
            .build();
    }

    @Bean
    public DelayTimeCalculator CpuMonitoringDelayTimeCalculator() {
        return new CpuMonitoringDelayTimeCalculator(new MockCpuUsageMonitorClient());
    }

}
