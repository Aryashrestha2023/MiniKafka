package com.minikafka.broker;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DefaultPartitionerTest {

    private final DefaultPartitioner partitioner = new DefaultPartitioner();

    @Test
    void keyBasedPartitioningIsStable() {
        int first = partitioner.partition("orders", "order-123", 8);
        int second = partitioner.partition("orders", "order-123", 8);

        assertThat(second).isEqualTo(first);
    }

    @Test
    void keyBasedPartitioningStaysWithinBounds() {
        int partition = partitioner.partition("orders", "order-123", 3);

        assertThat(partition).isBetween(0, 2);
    }

    @Test
    void nullKeysUseRoundRobin() {
        assertThat(partitioner.partition("orders", null, 3)).isEqualTo(0);
        assertThat(partitioner.partition("orders", null, 3)).isEqualTo(1);
        assertThat(partitioner.partition("orders", null, 3)).isEqualTo(2);
        assertThat(partitioner.partition("orders", null, 3)).isEqualTo(0);
    }

    @Test
    void blankKeysUseRoundRobin() {
        assertThat(partitioner.partition("payments", " ", 2)).isEqualTo(0);
        assertThat(partitioner.partition("payments", "", 2)).isEqualTo(1);
    }

    @Test
    void roundRobinCountersArePerTopic() {
        partitioner.partition("orders", null, 3);

        assertThat(partitioner.partition("payments", null, 3)).isEqualTo(0);
    }

    @Test
    void rejectsInvalidPartitionCount() {
        assertThatThrownBy(() -> partitioner.partition("orders", null, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void highPartitionCountsStillReturnValidPartition() {
        int partition = partitioner.partition("orders", "key", 128);

        assertThat(partition).isBetween(0, 127);
    }
}
