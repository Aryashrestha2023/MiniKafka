package com.minikafka.demo.ecommerce;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface EcommerceOrderRepository extends JpaRepository<EcommerceOrderEntity, UUID> {

    List<EcommerceOrderEntity> findAllByOrderByCreatedAtDesc();
}
