package com.minikafka.demo.ecommerce;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.minikafka.api.ConflictException;
import com.minikafka.broker.BrokerService;
import com.minikafka.broker.dto.ConsumeResponse;
import com.minikafka.broker.dto.CreateTopicRequest;
import com.minikafka.broker.dto.LagResponse;
import com.minikafka.broker.dto.MessageResponse;
import com.minikafka.broker.dto.PollRequest;
import com.minikafka.broker.dto.ProduceRequest;
import com.minikafka.demo.ecommerce.dto.CreateDemoOrderRequest;
import com.minikafka.demo.ecommerce.dto.DemoOrderResponse;
import com.minikafka.demo.ecommerce.dto.DemoStateResponse;
import com.minikafka.demo.ecommerce.dto.DemoStepResponse;
import com.minikafka.persistence.TopicRepository;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
public class EcommerceDemoService {

    public static final String ORDERS_TOPIC = "ecommerce.orders";
    public static final String PAYMENTS_TOPIC = "ecommerce.payments";
    public static final String NOTIFICATIONS_TOPIC = "ecommerce.notifications";
    public static final String PAYMENT_WORKER_GROUP = "payment-worker";
    public static final String NOTIFICATION_WORKER_GROUP = "notification-worker";

    private static final int DEMO_PARTITIONS = 3;

    private final EcommerceOrderRepository orderRepository;
    private final TopicRepository topicRepository;
    private final BrokerService brokerService;
    private final ObjectMapper objectMapper;

    public EcommerceDemoService(
            EcommerceOrderRepository orderRepository,
            TopicRepository topicRepository,
            BrokerService brokerService,
            ObjectMapper objectMapper
    ) {
        this.orderRepository = orderRepository;
        this.topicRepository = topicRepository;
        this.brokerService = brokerService;
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    void initializeTopics() {
        ensureTopic(ORDERS_TOPIC);
        ensureTopic(PAYMENTS_TOPIC);
        ensureTopic(NOTIFICATIONS_TOPIC);
    }

    @Transactional
    public DemoOrderResponse createOrder(CreateDemoOrderRequest request) {
        initializeTopics();
        EcommerceOrderEntity order = orderRepository.save(new EcommerceOrderEntity(
                request.customerName(),
                request.itemName(),
                request.amount()
        ));
        produceEvent(ORDERS_TOPIC, order.getId().toString(), Map.of(
                "eventType", "ORDER_CREATED",
                "orderId", order.getId().toString(),
                "customerName", order.getCustomerName(),
                "itemName", order.getItemName(),
                "amount", order.getAmount(),
                "status", order.getStatus().name()
        ));
        return toResponse(order);
    }

    @Transactional
    public DemoStepResponse processPayments() {
        initializeTopics();
        ConsumeResponse records = brokerService.poll(PAYMENT_WORKER_GROUP, ORDERS_TOPIC, new PollRequest(25, true));
        List<DemoOrderResponse> updated = new ArrayList<>();
        for (MessageResponse record : records.records()) {
            Map<String, Object> event = readEvent(record.value());
            if (!"ORDER_CREATED".equals(event.get("eventType"))) {
                continue;
            }
            UUID orderId = UUID.fromString(String.valueOf(event.get("orderId")));
            orderRepository.findById(orderId)
                    .filter(order -> order.getStatus() == EcommerceOrderStatus.ORDER_CREATED)
                    .ifPresent(order -> {
                        order.setStatus(EcommerceOrderStatus.PAYMENT_COMPLETED);
                        EcommerceOrderEntity saved = orderRepository.save(order);
                        produceEvent(PAYMENTS_TOPIC, saved.getId().toString(), Map.of(
                                "eventType", "PAYMENT_COMPLETED",
                                "orderId", saved.getId().toString(),
                                "amount", saved.getAmount(),
                                "status", saved.getStatus().name()
                        ));
                        updated.add(toResponse(saved));
                    });
        }
        return new DemoStepResponse("payment-worker", records.records().size(), updated.size(), updated);
    }

    @Transactional
    public DemoStepResponse processNotifications() {
        initializeTopics();
        ConsumeResponse records = brokerService.poll(NOTIFICATION_WORKER_GROUP, PAYMENTS_TOPIC, new PollRequest(25, true));
        List<DemoOrderResponse> updated = new ArrayList<>();
        for (MessageResponse record : records.records()) {
            Map<String, Object> event = readEvent(record.value());
            if (!"PAYMENT_COMPLETED".equals(event.get("eventType"))) {
                continue;
            }
            UUID orderId = UUID.fromString(String.valueOf(event.get("orderId")));
            orderRepository.findById(orderId)
                    .filter(order -> order.getStatus() == EcommerceOrderStatus.PAYMENT_COMPLETED)
                    .ifPresent(order -> {
                        order.setStatus(EcommerceOrderStatus.NOTIFICATION_SENT);
                        EcommerceOrderEntity saved = orderRepository.save(order);
                        produceEvent(NOTIFICATIONS_TOPIC, saved.getId().toString(), Map.of(
                                "eventType", "NOTIFICATION_SENT",
                                "orderId", saved.getId().toString(),
                                "message", "Order update sent to " + saved.getCustomerName(),
                                "status", saved.getStatus().name()
                        ));
                        updated.add(toResponse(saved));
                    });
        }
        return new DemoStepResponse("notification-worker", records.records().size(), updated.size(), updated);
    }

    @Transactional(readOnly = true)
    public DemoStateResponse state() {
        Map<String, List<MessageResponse>> events = new LinkedHashMap<>();
        events.put(ORDERS_TOPIC, brokerService.consume(ORDERS_TOPIC, null, 0L, 100).records());
        events.put(PAYMENTS_TOPIC, brokerService.consume(PAYMENTS_TOPIC, null, 0L, 100).records());
        events.put(NOTIFICATIONS_TOPIC, brokerService.consume(NOTIFICATIONS_TOPIC, null, 0L, 100).records());
        List<LagResponse> paymentLag = brokerService.lag(PAYMENT_WORKER_GROUP, ORDERS_TOPIC);
        List<LagResponse> notificationLag = brokerService.lag(NOTIFICATION_WORKER_GROUP, PAYMENTS_TOPIC);
        return new DemoStateResponse(orders(), events, paymentLag, notificationLag);
    }

    @Transactional(readOnly = true)
    public List<DemoOrderResponse> orders() {
        return orderRepository.findAllByOrderByCreatedAtDesc()
                .stream()
                .map(this::toResponse)
                .toList();
    }

    public DemoStateResponse reset() {
        orderRepository.deleteAllInBatch();
        deleteTopicIfExists(ORDERS_TOPIC);
        deleteTopicIfExists(PAYMENTS_TOPIC);
        deleteTopicIfExists(NOTIFICATIONS_TOPIC);
        initializeTopics();
        return state();
    }

    private void deleteTopicIfExists(String topic) {
        if (topicRepository.existsByName(topic)) {
            brokerService.deleteTopic(topic);
        }
    }

    private void ensureTopic(String topic) {
        if (topicRepository.existsByName(topic)) {
            return;
        }
        try {
            brokerService.createTopic(new CreateTopicRequest(topic, DEMO_PARTITIONS));
        } catch (ConflictException ignored) {
            // The demo topics are created once and then reused.
        }
    }

    private void produceEvent(String topic, String key, Map<String, Object> event) {
        try {
            brokerService.produce(topic, new ProduceRequest(key, objectMapper.writeValueAsString(event), null, Map.of("demo", "ecommerce")));
        } catch (Exception ex) {
            throw new IllegalStateException("Could not produce ecommerce demo event", ex);
        }
    }

    private Map<String, Object> readEvent(String value) {
        try {
            return objectMapper.readValue(value, new TypeReference<>() {
            });
        } catch (Exception ex) {
            throw new IllegalStateException("Could not parse ecommerce demo event", ex);
        }
    }

    private DemoOrderResponse toResponse(EcommerceOrderEntity order) {
        return new DemoOrderResponse(
                order.getId(),
                order.getCustomerName(),
                order.getItemName(),
                order.getAmount(),
                order.getStatus(),
                order.getCreatedAt(),
                order.getUpdatedAt()
        );
    }
}
