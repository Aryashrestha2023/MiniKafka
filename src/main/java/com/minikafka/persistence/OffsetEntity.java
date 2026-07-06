package com.minikafka.persistence;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(
        name = "consumer_offsets",
        uniqueConstraints = @UniqueConstraint(
                name = "uk_offsets_group_topic_partition",
                columnNames = {"group_id", "topic_name", "partition_id"}
        )
)
public class OffsetEntity {

    @Id
    @GeneratedValue
    private UUID id;

    @Column(name = "group_id", nullable = false, length = 128)
    private String groupId;

    @Column(name = "topic_name", nullable = false, length = 128)
    private String topicName;

    @Column(name = "partition_id", nullable = false)
    private int partitionId;

    @Column(name = "next_offset", nullable = false)
    private long nextOffset;

    @Column(nullable = false)
    private Instant updatedAt;

    protected OffsetEntity() {
    }

    public OffsetEntity(String groupId, String topicName, int partitionId, long nextOffset) {
        this.groupId = groupId;
        this.topicName = topicName;
        this.partitionId = partitionId;
        this.nextOffset = nextOffset;
    }

    @PrePersist
    @PreUpdate
    void touch() {
        updatedAt = Instant.now();
    }

    public UUID getId() {
        return id;
    }

    public String getGroupId() {
        return groupId;
    }

    public String getTopicName() {
        return topicName;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public long getNextOffset() {
        return nextOffset;
    }

    public void setNextOffset(long nextOffset) {
        this.nextOffset = nextOffset;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }
}
