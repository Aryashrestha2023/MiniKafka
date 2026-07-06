package com.minikafka.broker;

import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32;

@Component
public class DefaultPartitioner {

    private final ConcurrentHashMap<String, AtomicInteger> roundRobinCounters = new ConcurrentHashMap<>();

    public int partition(String topic, String key, int partitionCount) {
        if (partitionCount <= 0) {
            throw new IllegalArgumentException("partitionCount must be positive");
        }
        if (key != null && !key.isBlank()) {
            CRC32 crc = new CRC32();
            crc.update(key.getBytes(StandardCharsets.UTF_8));
            return (int) (crc.getValue() % partitionCount);
        }
        AtomicInteger counter = roundRobinCounters.computeIfAbsent(topic, ignored -> new AtomicInteger());
        return Math.floorMod(counter.getAndIncrement(), partitionCount);
    }
}
