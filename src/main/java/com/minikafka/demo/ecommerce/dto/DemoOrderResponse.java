package com.minikafka.demo.ecommerce.dto;

import com.minikafka.demo.ecommerce.EcommerceOrderStatus;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record DemoOrderResponse(
        UUID id,
        String customerName,
        String itemName,
        BigDecimal amount,
        EcommerceOrderStatus status,
        Instant createdAt,
        Instant updatedAt
) {
}
