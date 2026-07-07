package com.minikafka.broker.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.minikafka.broker.model.LogRecord;
import com.minikafka.config.MiniKafkaProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AppendOnlyLogStoreTest {

    @TempDir
    Path tempDir;

    private AppendOnlyLogStore store;

    @BeforeEach
    void setUp() {
        MiniKafkaProperties properties = new MiniKafkaProperties();
        properties.getStorage().setRoot(tempDir.toString());
        ObjectMapper objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        store = new AppendOnlyLogStore(properties, objectMapper);
    }

    @Test
    void ensureTopicCreatesPartitionFiles() {
        store.ensureTopic("orders", 2);

        assertThat(tempDir.resolve("orders/partition-0/messages.wal")).exists();
        assertThat(tempDir.resolve("orders/partition-1/messages.wal")).exists();
    }

    @Test
    void appendStartsAtOffsetZero() {
        store.ensureTopic("orders", 1);

        LogRecord record = store.append("orders", 0, "k1", "created", Map.of());

        assertThat(record.offset()).isZero();
    }

    @Test
    void appendIncrementsOffsetsInOrder() {
        store.ensureTopic("orders", 1);

        LogRecord first = store.append("orders", 0, null, "one", Map.of());
        LogRecord second = store.append("orders", 0, null, "two", Map.of());

        assertThat(first.offset()).isZero();
        assertThat(second.offset()).isEqualTo(1);
    }

    @Test
    void appendPreservesKeyAndValue() {
        store.ensureTopic("orders", 1);

        LogRecord record = store.append("orders", 0, "order-1", "paid", Map.of());

        assertThat(record.key()).isEqualTo("order-1");
        assertThat(record.value()).isEqualTo("paid");
    }

    @Test
    void appendCopiesHeaders() {
        store.ensureTopic("orders", 1);

        LogRecord record = store.append("orders", 0, "order-1", "paid", Map.of("source", "checkout"));

        assertThat(record.headers()).containsEntry("source", "checkout");
    }

    @Test
    void readFromOffsetSkipsEarlierRecords() {
        store.ensureTopic("orders", 1);
        store.append("orders", 0, null, "zero", Map.of());
        store.append("orders", 0, null, "one", Map.of());
        store.append("orders", 0, null, "two", Map.of());

        List<LogRecord> records = store.read("orders", 0, 1, 10);

        assertThat(records).extracting(LogRecord::value).containsExactly("one", "two");
    }

    @Test
    void readHonorsMaxMessages() {
        store.ensureTopic("orders", 1);
        store.append("orders", 0, null, "zero", Map.of());
        store.append("orders", 0, null, "one", Map.of());

        List<LogRecord> records = store.read("orders", 0, 0, 1);

        assertThat(records).hasSize(1);
    }

    @Test
    void endOffsetReflectsAppendedMessages() {
        store.ensureTopic("orders", 1);
        store.append("orders", 0, null, "zero", Map.of());
        store.append("orders", 0, null, "one", Map.of());

        assertThat(store.endOffset("orders", 0)).isEqualTo(2);
    }

    @Test
    void endOffsetsReturnsAllPartitions() {
        store.ensureTopic("orders", 2);
        store.append("orders", 0, null, "zero", Map.of());
        store.append("orders", 1, null, "one", Map.of());
        store.append("orders", 1, null, "two", Map.of());

        assertThat(store.endOffsets("orders", 2)).containsEntry(0, 1L).containsEntry(1, 2L);
    }

    @Test
    void topicMessageCountSumsPartitions() {
        store.ensureTopic("orders", 2);
        store.append("orders", 0, null, "zero", Map.of());
        store.append("orders", 1, null, "one", Map.of());

        assertThat(store.topicMessageCount("orders", 2)).isEqualTo(2);
    }

    @Test
    void storageBytesGrowsAfterAppend() {
        store.ensureTopic("orders", 1);

        store.append("orders", 0, null, "zero", Map.of());

        assertThat(store.storageBytes()).isPositive();
    }

    @Test
    void deleteTopicRemovesTopicDirectory() {
        store.ensureTopic("orders", 1);
        store.append("orders", 0, null, "zero", Map.of());

        store.deleteTopic("orders");

        assertThat(tempDir.resolve("orders")).doesNotExist();
    }

    @Test
    void ensureTopicRehydratesExistingOffsetFromDisk() throws Exception {
        store.ensureTopic("orders", 1);
        store.append("orders", 0, null, "zero", Map.of());
        MiniKafkaProperties properties = new MiniKafkaProperties();
        properties.getStorage().setRoot(tempDir.toString());
        AppendOnlyLogStore rehydrated = new AppendOnlyLogStore(properties, new ObjectMapper().registerModule(new JavaTimeModule()));

        rehydrated.ensureTopic("orders", 1);

        assertThat(rehydrated.append("orders", 0, null, "one", Map.of()).offset()).isEqualTo(1);
        assertThat(rehydrated.read("orders", 0, 0, 10)).extracting(LogRecord::value).containsExactly("zero", "one");
    }

    @Test
    void ensureTopicRecoversTruncatedWalTail() throws Exception {
        store.ensureTopic("orders", 1);
        store.append("orders", 0, null, "zero", Map.of());
        store.append("orders", 0, null, "one", Map.of());
        Path wal = tempDir.resolve("orders/partition-0/messages.wal");
        long originalSize = Files.size(wal);
        try (var channel = Files.newByteChannel(wal, StandardOpenOption.WRITE)) {
            channel.truncate(originalSize - 3);
        }
        MiniKafkaProperties properties = new MiniKafkaProperties();
        properties.getStorage().setRoot(tempDir.toString());
        AppendOnlyLogStore rehydrated = new AppendOnlyLogStore(properties, new ObjectMapper().registerModule(new JavaTimeModule()));

        rehydrated.ensureTopic("orders", 1);

        assertThat(rehydrated.read("orders", 0, 0, 10)).extracting(LogRecord::value).containsExactly("zero");
        assertThat(rehydrated.append("orders", 0, null, "recovered", Map.of()).offset()).isEqualTo(1);
    }

    @Test
    void ensureTopicRejectsCorruptWalLength() throws Exception {
        store.ensureTopic("orders", 1);
        Path wal = tempDir.resolve("orders/partition-0/messages.wal");
        Files.write(wal, new byte[]{0, 0, 0, 0});
        MiniKafkaProperties properties = new MiniKafkaProperties();
        properties.getStorage().setRoot(tempDir.toString());
        AppendOnlyLogStore rehydrated = new AppendOnlyLogStore(properties, new ObjectMapper().registerModule(new JavaTimeModule()));

        assertThatThrownBy(() -> rehydrated.ensureTopic("orders", 1))
                .isInstanceOf(LogStorageException.class);
    }

    @Test
    void readRejectsNegativeOffset() {
        store.ensureTopic("orders", 1);

        assertThatThrownBy(() -> store.read("orders", 0, -1, 1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void readRejectsNonPositiveMaxMessages() {
        store.ensureTopic("orders", 1);

        assertThatThrownBy(() -> store.read("orders", 0, 0, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
