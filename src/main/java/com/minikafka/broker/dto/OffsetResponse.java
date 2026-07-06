package com.minikafka.broker.dto;

import java.time.Instant;

public record OffsetResponse(
        String groupId,
        String topic,
        int partition,
        long offset,
        Instant updatedAt
) {
}
