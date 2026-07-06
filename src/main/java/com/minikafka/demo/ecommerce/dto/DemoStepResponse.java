package com.minikafka.demo.ecommerce.dto;

import java.util.List;

public record DemoStepResponse(
        String worker,
        int recordsRead,
        int ordersUpdated,
        List<DemoOrderResponse> orders
) {
}
