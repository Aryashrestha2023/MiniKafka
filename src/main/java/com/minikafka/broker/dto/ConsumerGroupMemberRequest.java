package com.minikafka.broker.dto;

import jakarta.validation.constraints.NotBlank;

public record ConsumerGroupMemberRequest(
        @NotBlank String memberId
) {
}
