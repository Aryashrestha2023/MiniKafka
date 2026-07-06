package com.minikafka.broker.dto;

import java.time.Instant;
import java.util.Map;

public record MessageResponse(
        String topic,
        int partition,
        long offset,
        String key,
        String value,
        Map<String, String> headers,
        Instant timestamp
) {
}
