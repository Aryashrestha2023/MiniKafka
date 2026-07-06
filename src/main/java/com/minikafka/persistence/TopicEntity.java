package com.minikafka.persistence;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "topics", uniqueConstraints = @UniqueConstraint(name = "uk_topics_name", columnNames = "name"))
public class TopicEntity {

    @Id
    @GeneratedValue
    private UUID id;

    @Column(nullable = false, unique = true, length = 128)
    private String name;

    @Column(nullable = false)
    private int partitionCount;

    @Column(nullable = false, updatable = false)
    private Instant createdAt;

    protected TopicEntity() {
    }

    public TopicEntity(String name, int partitionCount) {
        this.name = name;
        this.partitionCount = partitionCount;
    }

    @PrePersist
    void prePersist() {
        if (createdAt == null) {
            createdAt = Instant.now();
        }
    }

    public UUID getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public int getPartitionCount() {
        return partitionCount;
    }

    public void setPartitionCount(int partitionCount) {
        this.partitionCount = partitionCount;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }
}
