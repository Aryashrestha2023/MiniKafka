package com.minikafka.broker.dto;

import java.util.List;
import java.util.Map;

public record ConsumerGroupMemberResponse(
        String groupId,
        String topic,
        String memberId,
        List<Integer> partitions,
        Map<String, List<Integer>> assignments
) {
}
