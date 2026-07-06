package com.minikafka.demo.ecommerce.dto;

import com.minikafka.broker.dto.LagResponse;
import com.minikafka.broker.dto.MessageResponse;

import java.util.List;
import java.util.Map;

public record DemoStateResponse(
        List<DemoOrderResponse> orders,
        Map<String, List<MessageResponse>> events,
        List<LagResponse> paymentWorkerLag,
        List<LagResponse> notificationWorkerLag
) {
}
