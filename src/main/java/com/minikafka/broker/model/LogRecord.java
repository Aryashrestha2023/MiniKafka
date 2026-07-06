package com.minikafka.broker.model;

import java.time.Instant;
import java.util.Map;

public record LogRecord(
        long offset,
        String key,
        String value,
        Map<String, String> headers,
        Instant timestamp
) {
    public LogRecord {
        headers = headers == null ? Map.of() : Map.copyOf(headers);
    }
}
