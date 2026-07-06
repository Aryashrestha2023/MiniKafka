package com.minikafka.broker.dto;

import java.util.List;
import java.util.Map;

public record ConsumeResponse(List<MessageResponse> records, Map<Integer, Long> nextOffsets) {
}
