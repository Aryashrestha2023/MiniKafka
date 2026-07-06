package com.minikafka.broker;

import com.minikafka.api.BadRequestException;
import com.minikafka.api.ConflictException;
import com.minikafka.api.NotFoundException;
import com.minikafka.broker.dto.CommitOffsetRequest;
import com.minikafka.broker.dto.ConsumeResponse;
import com.minikafka.broker.dto.CreateTopicRequest;
import com.minikafka.broker.dto.LagResponse;
import com.minikafka.broker.dto.MessageResponse;
import com.minikafka.broker.dto.OffsetResponse;
import com.minikafka.broker.dto.PollRequest;
import com.minikafka.broker.dto.ProduceRequest;
import com.minikafka.broker.dto.TopicResponse;
import com.minikafka.broker.model.LogRecord;
import com.minikafka.broker.storage.AppendOnlyLogStore;
import com.minikafka.persistence.OffsetEntity;
import com.minikafka.persistence.OffsetRepository;
import com.minikafka.persistence.TopicEntity;
import com.minikafka.persistence.TopicRepository;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

@Service
public class BrokerService {

    private static final Logger log = LoggerFactory.getLogger(BrokerService.class);
    private static final Pattern TOPIC_NAME = Pattern.compile("^[a-zA-Z0-9._-]{1,128}$");
    private static final int DEFAULT_MAX_MESSAGES = 50;

    private final TopicRepository topicRepository;
    private final OffsetRepository offsetRepository;
    private final AppendOnlyLogStore logStore;
    private final DefaultPartitioner partitioner;

    public BrokerService(
            TopicRepository topicRepository,
            OffsetRepository offsetRepository,
            AppendOnlyLogStore logStore,
            DefaultPartitioner partitioner
    ) {
        this.topicRepository = topicRepository;
        this.offsetRepository = offsetRepository;
        this.logStore = logStore;
        this.partitioner = partitioner;
    }

    @PostConstruct
    void initializeKnownTopics() {
        topicRepository.findAll().forEach(topic -> logStore.ensureTopic(topic.getName(), topic.getPartitionCount()));
    }

    @Transactional
    public TopicResponse createTopic(CreateTopicRequest request) {
        validateTopicName(request.name());
        validatePartitionCount(request.partitions());
        if (topicRepository.existsByName(request.name())) {
            throw new ConflictException("Topic already exists: " + request.name());
        }
        TopicEntity topic = topicRepository.save(new TopicEntity(request.name(), request.partitions()));
        logStore.ensureTopic(topic.getName(), topic.getPartitionCount());
        log.info("event=topic_created topic={} partitions={}", topic.getName(), topic.getPartitionCount());
        return toTopicResponse(topic);
    }

    @Transactional(readOnly = true)
    public List<TopicResponse> listTopics() {
        return topicRepository.findAll().stream()
                .sorted(Comparator.comparing(TopicEntity::getName))
                .map(this::toTopicResponse)
                .toList();
    }

    @Transactional(readOnly = true)
    public TopicResponse getTopic(String name) {
        return toTopicResponse(requireTopic(name));
    }

    @Transactional
    public TopicResponse increasePartitions(String name, int targetPartitions) {
        TopicEntity topic = requireTopic(name);
        validatePartitionCount(targetPartitions);
        if (targetPartitions <= topic.getPartitionCount()) {
            throw new BadRequestException("targetPartitions must be greater than current partition count");
        }
        topic.setPartitionCount(targetPartitions);
        TopicEntity saved = topicRepository.save(topic);
        logStore.ensureTopic(saved.getName(), saved.getPartitionCount());
        log.info("event=partitions_increased topic={} partitions={}", saved.getName(), saved.getPartitionCount());
        return toTopicResponse(saved);
    }

    @Transactional
    public void deleteTopic(String name) {
        requireTopic(name);
        offsetRepository.deleteByTopicName(name);
        topicRepository.deleteByName(name);
        logStore.deleteTopic(name);
        log.info("event=topic_deleted topic={}", name);
    }

    @Transactional(readOnly = true)
    public List<Integer> partitions(String topicName) {
        TopicEntity topic = requireTopic(topicName);
        List<Integer> partitions = new ArrayList<>();
        for (int partition = 0; partition < topic.getPartitionCount(); partition++) {
            partitions.add(partition);
        }
        return partitions;
    }

    @Transactional(readOnly = true)
    public Map<Integer, Long> endOffsets(String topicName) {
        TopicEntity topic = requireTopic(topicName);
        return logStore.endOffsets(topic.getName(), topic.getPartitionCount());
    }

    @Transactional
    public MessageResponse produce(String topicName, ProduceRequest request) {
        TopicEntity topic = requireTopic(topicName);
        int partition = request.partition() == null
                ? partitioner.partition(topic.getName(), request.key(), topic.getPartitionCount())
                : request.partition();
        validatePartition(topic, partition);
        LogRecord record = logStore.append(topic.getName(), partition, request.key(), request.value(), request.headers());
        log.info("event=message_produced topic={} partition={} offset={} keyPresent={}",
                topic.getName(), partition, record.offset(), request.key() != null);
        return toMessageResponse(topic.getName(), partition, record);
    }

    @Transactional(readOnly = true)
    public ConsumeResponse consume(String topicName, Integer partition, Long offset, Integer maxMessages) {
        TopicEntity topic = requireTopic(topicName);
        int limit = limit(maxMessages);
        long startOffset = offset == null ? 0 : offset;
        Map<Integer, Long> nextOffsets = new LinkedHashMap<>();
        List<MessageResponse> responses = new ArrayList<>();

        if (partition != null) {
            validatePartition(topic, partition);
            List<LogRecord> records = logStore.read(topic.getName(), partition, startOffset, limit);
            records.forEach(record -> responses.add(toMessageResponse(topic.getName(), partition, record)));
            nextOffsets.put(partition, nextOffset(startOffset, records));
            return new ConsumeResponse(responses, nextOffsets);
        }

        for (int currentPartition = 0; currentPartition < topic.getPartitionCount() && responses.size() < limit; currentPartition++) {
            int remaining = limit - responses.size();
            List<LogRecord> records = logStore.read(topic.getName(), currentPartition, startOffset, remaining);
            int responsePartition = currentPartition;
            records.forEach(record -> responses.add(toMessageResponse(topic.getName(), responsePartition, record)));
            nextOffsets.put(currentPartition, nextOffset(startOffset, records));
        }
        return new ConsumeResponse(responses, nextOffsets);
    }

    @Transactional
    public ConsumeResponse poll(String groupId, String topicName, PollRequest request) {
        TopicEntity topic = requireTopic(topicName);
        int limit = limit(request.maxMessages());
        boolean autoCommit = Boolean.TRUE.equals(request.autoCommit());
        List<MessageResponse> responses = new ArrayList<>();
        Map<Integer, Long> nextOffsets = new LinkedHashMap<>();

        for (int partition = 0; partition < topic.getPartitionCount() && responses.size() < limit; partition++) {
            long startOffset = currentOffset(groupId, topic.getName(), partition);
            int remaining = limit - responses.size();
            List<LogRecord> records = logStore.read(topic.getName(), partition, startOffset, remaining);
            int responsePartition = partition;
            records.forEach(record -> responses.add(toMessageResponse(topic.getName(), responsePartition, record)));
            long nextOffset = nextOffset(startOffset, records);
            nextOffsets.put(partition, nextOffset);
            if (autoCommit && nextOffset > startOffset) {
                commitOffset(groupId, topic.getName(), new CommitOffsetRequest(partition, nextOffset));
            }
        }
        log.info("event=consumer_poll group={} topic={} records={} autoCommit={}",
                groupId, topicName, responses.size(), autoCommit);
        return new ConsumeResponse(responses, nextOffsets);
    }

    @Transactional
    public OffsetResponse commitOffset(String groupId, String topicName, CommitOffsetRequest request) {
        TopicEntity topic = requireTopic(topicName);
        validatePartition(topic, request.partition());
        OffsetEntity offset = offsetRepository.findByGroupIdAndTopicNameAndPartitionId(
                        groupId, topic.getName(), request.partition())
                .orElseGet(() -> new OffsetEntity(groupId, topic.getName(), request.partition(), 0));
        offset.setNextOffset(request.offset());
        OffsetEntity saved = offsetRepository.save(offset);
        log.info("event=offset_committed group={} topic={} partition={} offset={}",
                groupId, topic.getName(), request.partition(), request.offset());
        return toOffsetResponse(saved);
    }

    @Transactional(readOnly = true)
    public List<OffsetResponse> offsets(String groupId, String topicName) {
        requireTopic(topicName);
        return offsetRepository.findByGroupIdAndTopicNameOrderByPartitionId(groupId, topicName)
                .stream()
                .map(this::toOffsetResponse)
                .toList();
    }

    @Transactional(readOnly = true)
    public List<LagResponse> lag(String groupId, String topicName) {
        TopicEntity topic = requireTopic(topicName);
        List<LagResponse> responses = new ArrayList<>();
        for (int partition = 0; partition < topic.getPartitionCount(); partition++) {
            long endOffset = logStore.endOffset(topic.getName(), partition);
            long committedOffset = currentOffset(groupId, topic.getName(), partition);
            long lag = Math.max(0, endOffset - committedOffset);
            responses.add(new LagResponse(
                    groupId,
                    topic.getName(),
                    partition,
                    endOffset,
                    committedOffset,
                    lag,
                    lag > 10 ? "WARNING" : "OK"
            ));
        }
        return responses;
    }

    @Transactional(readOnly = true)
    public List<OffsetResponse> offsetsForGroup(String groupId) {
        return offsetRepository.findByGroupIdOrderByTopicNameAscPartitionIdAsc(groupId)
                .stream()
                .map(this::toOffsetResponse)
                .toList();
    }

    @Transactional(readOnly = true)
    public List<String> consumerGroups() {
        return offsetRepository.findConsumerGroups();
    }

    @Transactional(readOnly = true)
    public long storedMessages() {
        return topicRepository.findAll().stream()
                .mapToLong(topic -> logStore.topicMessageCount(topic.getName(), topic.getPartitionCount()))
                .sum();
    }

    private TopicEntity requireTopic(String name) {
        return topicRepository.findByName(name)
                .orElseThrow(() -> new NotFoundException("Topic not found: " + name));
    }

    private void validateTopicName(String topicName) {
        if (!TOPIC_NAME.matcher(topicName).matches()) {
            throw new BadRequestException("Topic names may contain letters, numbers, '.', '_' and '-' only");
        }
    }

    private void validatePartition(TopicEntity topic, int partition) {
        if (partition < 0 || partition >= topic.getPartitionCount()) {
            throw new BadRequestException("Partition " + partition + " is outside topic " + topic.getName());
        }
    }

    private void validatePartitionCount(int partitions) {
        if (partitions < 1 || partitions > 128) {
            throw new BadRequestException("partitions must be between 1 and 128");
        }
    }

    private int limit(Integer maxMessages) {
        if (maxMessages == null) {
            return DEFAULT_MAX_MESSAGES;
        }
        if (maxMessages < 1 || maxMessages > 500) {
            throw new BadRequestException("maxMessages must be between 1 and 500");
        }
        return maxMessages;
    }

    private long currentOffset(String groupId, String topicName, int partition) {
        return offsetRepository.findByGroupIdAndTopicNameAndPartitionId(groupId, topicName, partition)
                .map(OffsetEntity::getNextOffset)
                .orElse(0L);
    }

    private long nextOffset(long startOffset, List<LogRecord> records) {
        if (records.isEmpty()) {
            return startOffset;
        }
        return records.get(records.size() - 1).offset() + 1;
    }

    private TopicResponse toTopicResponse(TopicEntity topic) {
        return new TopicResponse(topic.getName(), topic.getPartitionCount(), topic.getCreatedAt());
    }

    private MessageResponse toMessageResponse(String topic, int partition, LogRecord record) {
        return new MessageResponse(topic, partition, record.offset(), record.key(), record.value(), record.headers(), record.timestamp());
    }

    private OffsetResponse toOffsetResponse(OffsetEntity offset) {
        return new OffsetResponse(offset.getGroupId(), offset.getTopicName(), offset.getPartitionId(), offset.getNextOffset(), offset.getUpdatedAt());
    }
}
