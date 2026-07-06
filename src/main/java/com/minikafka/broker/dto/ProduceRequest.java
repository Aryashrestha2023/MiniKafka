package com.minikafka.broker.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

import java.util.Map;

public record ProduceRequest(
        String key,
        @NotBlank String value,
        @Min(0) Integer partition,
        Map<String, String> headers
) {
}
