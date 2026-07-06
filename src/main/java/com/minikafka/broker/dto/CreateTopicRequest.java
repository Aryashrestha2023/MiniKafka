package com.minikafka.broker.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

public record CreateTopicRequest(
        @NotBlank String name,
        @Min(1) @Max(128) int partitions
) {
}
