package com.minikafka.broker.dto;

import java.util.List;
import java.util.Map;

public record ConsumerGroupAssignmentResponse(
        String groupId,
        String topic,
        Map<String, List<Integer>> assignments
) {
}
