package com.minikafka.broker;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ConsumerGroupCoordinatorTest {

    private final ConsumerGroupCoordinator coordinator = new ConsumerGroupCoordinator();

    @Test
    void singleMemberOwnsAllPartitions() {
        var assignments = coordinator.join("checkout", "orders", "member-a", 3);

        assertThat(assignments).containsEntry("member-a", List.of(0, 1, 2));
    }

    @Test
    void secondMemberTriggersRoundRobinRebalance() {
        coordinator.join("checkout", "orders", "member-a", 4);

        var assignments = coordinator.join("checkout", "orders", "member-b", 4);

        assertThat(assignments).containsEntry("member-a", List.of(0, 2));
        assertThat(assignments).containsEntry("member-b", List.of(1, 3));
    }

    @Test
    void leavingMemberReassignsPartitionsToRemainingMembers() {
        coordinator.join("checkout", "orders", "member-a", 3);
        coordinator.join("checkout", "orders", "member-b", 3);

        var assignments = coordinator.leave("checkout", "orders", "member-b", 3);

        assertThat(assignments).containsEntry("member-a", List.of(0, 1, 2));
        assertThat(assignments).doesNotContainKey("member-b");
    }

    @Test
    void assignmentsReflectPartitionCountChanges() {
        coordinator.join("checkout", "orders", "member-a", 2);
        coordinator.join("checkout", "orders", "member-b", 2);

        var assignments = coordinator.assignments("checkout", "orders", 5);

        assertThat(assignments).containsEntry("member-a", List.of(0, 2, 4));
        assertThat(assignments).containsEntry("member-b", List.of(1, 3));
    }
}
