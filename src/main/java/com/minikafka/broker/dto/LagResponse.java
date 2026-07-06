package com.minikafka.broker.dto;

public record LagResponse(
        String groupId,
        String topic,
        int partition,
        long endOffset,
        long committedOffset,
        long lag,
        String status
) {
}
