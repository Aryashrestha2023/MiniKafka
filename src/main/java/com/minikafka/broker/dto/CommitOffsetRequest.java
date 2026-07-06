package com.minikafka.broker.dto;

import jakarta.validation.constraints.Min;

public record CommitOffsetRequest(
        @Min(0) int partition,
        @Min(0) long offset
) {
}
