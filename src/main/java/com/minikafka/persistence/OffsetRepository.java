package com.minikafka.persistence;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface OffsetRepository extends JpaRepository<OffsetEntity, UUID> {

    Optional<OffsetEntity> findByGroupIdAndTopicNameAndPartitionId(String groupId, String topicName, int partitionId);

    List<OffsetEntity> findByGroupIdAndTopicNameOrderByPartitionId(String groupId, String topicName);

    List<OffsetEntity> findByGroupIdOrderByTopicNameAscPartitionIdAsc(String groupId);

    void deleteByTopicName(String topicName);

    @Query("select distinct o.groupId from OffsetEntity o order by o.groupId")
    List<String> findConsumerGroups();
}
