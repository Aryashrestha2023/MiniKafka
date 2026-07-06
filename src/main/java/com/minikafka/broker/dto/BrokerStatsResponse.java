package com.minikafka.broker.dto;

import java.util.Map;

public record BrokerStatsResponse(
        long topics,
        long consumerGroups,
        long storedMessages,
        long storageBytes,
        Map<String, Object> tcp
) {
}
