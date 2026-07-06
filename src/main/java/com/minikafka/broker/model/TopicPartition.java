package com.minikafka.broker.model;

public record TopicPartition(String topic, int partition) {

    public TopicPartition {
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("topic is required");
        }
        if (partition < 0) {
            throw new IllegalArgumentException("partition must be zero or greater");
        }
    }

    public String storageKey() {
        return topic + "-" + partition;
    }
}
