package com.example.kafkathrottling.common;

import java.util.concurrent.atomic.AtomicInteger;

// 호출시마다 CPU 사용량을 5%씩 80%까지 증가시키는 Mock Client
public class MockCpuUsageMonitorClient implements CpuUsageMonitorClient {
    private static final int CPU_USAGE_LIMIT = 90;

    private final AtomicInteger cpuUsage = new AtomicInteger(0);

    @Override
    public int getCpuUsage() {
        return cpuUsage.updateAndGet(current -> Math.min(current + 5, CPU_USAGE_LIMIT));
    }
}
