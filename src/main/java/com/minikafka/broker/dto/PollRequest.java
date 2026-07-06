package com.minikafka.broker.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;

public record PollRequest(
        @Min(1) @Max(500) Integer maxMessages,
        Boolean autoCommit
) {
}
