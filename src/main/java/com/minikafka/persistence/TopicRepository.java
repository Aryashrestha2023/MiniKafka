package com.minikafka.persistence;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;
import java.util.UUID;

public interface TopicRepository extends JpaRepository<TopicEntity, UUID> {

    Optional<TopicEntity> findByName(String name);

    boolean existsByName(String name);

    void deleteByName(String name);
}
