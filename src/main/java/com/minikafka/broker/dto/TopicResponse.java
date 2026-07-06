package com.minikafka.broker.dto;

import java.time.Instant;

public record TopicResponse(String name, int partitions, Instant createdAt) {
}
