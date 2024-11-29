package com.example.kafkathrottling.common;

import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class CpuMonitoringDelayTimeCalculator implements DelayTimeCalculator {
    private static final int ALLOWABLE_CPU_USAGE_LIMIT = 50; // 쓰로틀링을 최소로 유지하는 CPU 사용률(%)
    private static final int CPU_USAGE_LIMIT = 80; // 쓰로틀링을 최대로 하는 CPU 사용률(%)
    private static final long MIN_DELAY_TIME = 0L; // 최소 쓰로틀링일 때 지연 시간(ms)
    private static final long MAX_DELAY_TIME = 500L; // 최대 쓰로틀링일 때 지연 시간(ms

    private final CpuUsageMonitorClient cpuUsageMonitorClient;

    @Override
    public long calculateDelayTime() {
        final int cpuUsage = cpuUsageMonitorClient.getCpuUsage();
        log.info("CPU Usage: {}", cpuUsage);

        if (cpuUsage > CPU_USAGE_LIMIT) {
            return MAX_DELAY_TIME;
        }

        if (cpuUsage < ALLOWABLE_CPU_USAGE_LIMIT) {
            return MIN_DELAY_TIME;
        }

        final var calculatorFunction =
            quadratic(
                ALLOWABLE_CPU_USAGE_LIMIT,
                MIN_DELAY_TIME,
                CPU_USAGE_LIMIT,
                MAX_DELAY_TIME
            );

        return calculatorFunction.apply(cpuUsage);
    }

    /**
     * (x0, y0), (x1, y1) 좌표를 지나는 2차함수
     * y = r(x-x0)^2 + y0
     */
    private Function<Integer, Long> quadratic(
        int x0,
        long y0,
        int x1,
        long y1
    ) {
        // y = r(x-x0)^2 + y0
        final var r = (y1 - y0) / Math.pow(x1 - x0, 2);
        return x -> (long)(r * Math.pow(x - x0, 2) + y0);
    }
}
