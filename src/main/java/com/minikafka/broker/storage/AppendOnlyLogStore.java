package com.minikafka.broker.storage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.minikafka.broker.model.LogRecord;
import com.minikafka.broker.model.TopicPartition;
import com.minikafka.config.MiniKafkaProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

@Component
public class AppendOnlyLogStore {

    private static final Logger log = LoggerFactory.getLogger(AppendOnlyLogStore.class);
    private static final String LOG_FILE_NAME = "messages.jsonl";

    private final Path root;
    private final ObjectMapper objectMapper;
    private final ConcurrentHashMap<TopicPartition, AtomicLong> nextOffsets = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<TopicPartition, ReentrantReadWriteLock> locks = new ConcurrentHashMap<>();

    public AppendOnlyLogStore(MiniKafkaProperties properties, ObjectMapper objectMapper) {
        this.root = Path.of(properties.getStorage().getRoot());
        this.objectMapper = objectMapper;
    }

    public void ensureTopic(String topic, int partitions) {
        for (int partition = 0; partition < partitions; partition++) {
            TopicPartition topicPartition = new TopicPartition(topic, partition);
            Path file = logFile(topicPartition);
            try {
                Files.createDirectories(file.getParent());
                if (Files.notExists(file)) {
                    Files.createFile(file);
                }
                long existingMessages;
                try (Stream<String> lines = Files.lines(file, StandardCharsets.UTF_8)) {
                    existingMessages = lines.count();
                }
                nextOffsets.put(topicPartition, new AtomicLong(existingMessages));
                locks.computeIfAbsent(topicPartition, ignored -> new ReentrantReadWriteLock());
                log.info("event=partition_initialized topic={} partition={} nextOffset={}", topic, partition, existingMessages);
            } catch (IOException ex) {
                throw new LogStorageException("Could not initialize log file for " + topicPartition.storageKey(), ex);
            }
        }
    }

    public LogRecord append(String topic, int partition, String key, String value, Map<String, String> headers) {
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        ensurePartitionKnown(topicPartition);
        ReentrantReadWriteLock.WriteLock writeLock = lock(topicPartition).writeLock();
        writeLock.lock();
        try {
            long offset = nextOffsets.get(topicPartition).getAndIncrement();
            LogRecord record = new LogRecord(offset, key, value, headers, Instant.now());
            Files.writeString(
                    logFile(topicPartition),
                    objectMapper.writeValueAsString(record) + System.lineSeparator(),
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND
            );
            return record;
        } catch (IOException ex) {
            throw new LogStorageException("Could not append record to " + topicPartition.storageKey(), ex);
        } finally {
            writeLock.unlock();
        }
    }

    public List<LogRecord> read(String topic, int partition, long offset, int maxMessages) {
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be zero or greater");
        }
        if (maxMessages <= 0) {
            throw new IllegalArgumentException("maxMessages must be positive");
        }
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        ensurePartitionKnown(topicPartition);
        ReentrantReadWriteLock.ReadLock readLock = lock(topicPartition).readLock();
        readLock.lock();
        try (Stream<String> lines = Files.lines(logFile(topicPartition), StandardCharsets.UTF_8)) {
            List<LogRecord> records = new ArrayList<>();
            lines.map(this::deserialize)
                    .filter(record -> record.offset() >= offset)
                    .limit(maxMessages)
                    .forEach(records::add);
            return records;
        } catch (IOException ex) {
            throw new LogStorageException("Could not read records from " + topicPartition.storageKey(), ex);
        } finally {
            readLock.unlock();
        }
    }

    public long endOffset(String topic, int partition) {
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        ensurePartitionKnown(topicPartition);
        return nextOffsets.get(topicPartition).get();
    }

    public long topicMessageCount(String topic, int partitions) {
        long total = 0;
        for (int partition = 0; partition < partitions; partition++) {
            total += endOffset(topic, partition);
        }
        return total;
    }

    public Map<Integer, Long> endOffsets(String topic, int partitions) {
        java.util.LinkedHashMap<Integer, Long> offsets = new java.util.LinkedHashMap<>();
        for (int partition = 0; partition < partitions; partition++) {
            offsets.put(partition, endOffset(topic, partition));
        }
        return offsets;
    }

    public long storageBytes() {
        if (Files.notExists(root)) {
            return 0;
        }
        try (Stream<Path> paths = Files.walk(root)) {
            return paths.filter(Files::isRegularFile)
                    .mapToLong(this::size)
                    .sum();
        } catch (IOException ex) {
            throw new LogStorageException("Could not calculate storage size", ex);
        }
    }

    public void deleteTopic(String topic) {
        Path topicPath = root.resolve(topic);
        nextOffsets.keySet().removeIf(topicPartition -> topicPartition.topic().equals(topic));
        locks.keySet().removeIf(topicPartition -> topicPartition.topic().equals(topic));
        if (Files.notExists(topicPath)) {
            return;
        }
        try (Stream<Path> paths = Files.walk(topicPath)) {
            paths.sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (IOException ex) {
                    throw new LogStorageException("Could not delete " + path, ex);
                }
            });
        } catch (IOException ex) {
            throw new LogStorageException("Could not delete topic " + topic, ex);
        }
    }

    public Path root() {
        return root;
    }

    private void ensurePartitionKnown(TopicPartition topicPartition) {
        if (!nextOffsets.containsKey(topicPartition)) {
            ensureTopic(topicPartition.topic(), topicPartition.partition() + 1);
        }
    }

    private ReentrantReadWriteLock lock(TopicPartition topicPartition) {
        return locks.computeIfAbsent(topicPartition, ignored -> new ReentrantReadWriteLock());
    }

    private Path logFile(TopicPartition topicPartition) {
        return root.resolve(topicPartition.topic())
                .resolve("partition-" + topicPartition.partition())
                .resolve(LOG_FILE_NAME);
    }

    private LogRecord deserialize(String line) {
        try {
            return objectMapper.readValue(line, LogRecord.class);
        } catch (JsonProcessingException ex) {
            throw new LogStorageException("Corrupt log record", ex);
        }
    }

    private long size(Path path) {
        try {
            return Files.size(path);
        } catch (IOException ex) {
            throw new LogStorageException("Could not stat " + path, ex);
        }
    }
}
