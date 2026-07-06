package com.minikafka.broker;

import com.minikafka.api.NotFoundException;
import com.minikafka.broker.dto.BrokerStatsResponse;
import com.minikafka.broker.storage.AppendOnlyLogStore;
import com.minikafka.config.MiniKafkaProperties;
import com.minikafka.persistence.OffsetRepository;
import com.minikafka.persistence.TopicEntity;
import com.minikafka.persistence.TopicRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.LinkedHashMap;
import java.util.Map;

@Service
public class BrokerAdminService {

    private final TopicRepository topicRepository;
    private final OffsetRepository offsetRepository;
    private final AppendOnlyLogStore logStore;
    private final MiniKafkaProperties properties;

    public BrokerAdminService(
            TopicRepository topicRepository,
            OffsetRepository offsetRepository,
            AppendOnlyLogStore logStore,
            MiniKafkaProperties properties
    ) {
        this.topicRepository = topicRepository;
        this.offsetRepository = offsetRepository;
        this.logStore = logStore;
        this.properties = properties;
    }

    @Transactional(readOnly = true)
    public BrokerStatsResponse brokerStats() {
        long storedMessages = topicRepository.findAll().stream()
                .mapToLong(topic -> logStore.topicMessageCount(topic.getName(), topic.getPartitionCount()))
                .sum();
        Map<String, Object> tcp = new LinkedHashMap<>();
        tcp.put("enabled", properties.getTcp().isEnabled());
        tcp.put("host", properties.getTcp().getHost());
        tcp.put("port", properties.getTcp().getPort());
        tcp.put("workerThreads", properties.getTcp().getWorkerThreads());
        return new BrokerStatsResponse(
                topicRepository.count(),
                offsetRepository.findConsumerGroups().size(),
                storedMessages,
                logStore.storageBytes(),
                tcp
        );
    }

    @Transactional(readOnly = true)
    public Map<String, Object> topicStats(String topicName) {
        TopicEntity topic = topicRepository.findByName(topicName)
                .orElseThrow(() -> new NotFoundException("Topic not found: " + topicName));
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("name", topic.getName());
        stats.put("partitions", topic.getPartitionCount());
        stats.put("endOffsets", logStore.endOffsets(topic.getName(), topic.getPartitionCount()));
        stats.put("messages", logStore.topicMessageCount(topic.getName(), topic.getPartitionCount()));
        stats.put("createdAt", topic.getCreatedAt());
        return stats;
    }

    public Map<String, Object> storageInfo() {
        Map<String, Object> info = new LinkedHashMap<>();
        info.put("root", logStore.root().toAbsolutePath().toString());
        info.put("bytes", logStore.storageBytes());
        return info;
    }

    public Map<String, Object> runtimeConfig() {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("storageRoot", properties.getStorage().getRoot());
        config.put("tcpEnabled", properties.getTcp().isEnabled());
        config.put("tcpPort", properties.getTcp().getPort());
        config.put("tcpWorkers", properties.getTcp().getWorkerThreads());
        return config;
    }
}
