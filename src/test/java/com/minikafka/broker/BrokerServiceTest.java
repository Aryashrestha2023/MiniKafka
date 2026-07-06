package com.minikafka.broker;

import com.minikafka.api.BadRequestException;
import com.minikafka.api.ConflictException;
import com.minikafka.broker.dto.CommitOffsetRequest;
import com.minikafka.broker.dto.CreateTopicRequest;
import com.minikafka.broker.dto.PollRequest;
import com.minikafka.broker.dto.ProduceRequest;
import com.minikafka.broker.model.LogRecord;
import com.minikafka.broker.storage.AppendOnlyLogStore;
import com.minikafka.persistence.OffsetEntity;
import com.minikafka.persistence.OffsetRepository;
import com.minikafka.persistence.TopicEntity;
import com.minikafka.persistence.TopicRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class BrokerServiceTest {

    @Mock
    private TopicRepository topicRepository;
    @Mock
    private OffsetRepository offsetRepository;
    @Mock
    private AppendOnlyLogStore logStore;
    @Mock
    private DefaultPartitioner partitioner;

    private BrokerService brokerService;

    @BeforeEach
    void setUp() {
        brokerService = new BrokerService(topicRepository, offsetRepository, logStore, partitioner);
    }

    @Test
    void createTopicRejectsDuplicates() {
        when(topicRepository.existsByName("orders")).thenReturn(true);

        assertThatThrownBy(() -> brokerService.createTopic(new CreateTopicRequest("orders", 3)))
                .isInstanceOf(ConflictException.class);
    }

    @Test
    void createTopicRejectsInvalidNames() {
        assertThatThrownBy(() -> brokerService.createTopic(new CreateTopicRequest("bad topic", 3)))
                .isInstanceOf(BadRequestException.class);
    }

    @Test
    void produceWithExplicitPartitionAppendsRecord() {
        TopicEntity topic = new TopicEntity("orders", 3);
        when(topicRepository.findByName("orders")).thenReturn(Optional.of(topic));
        when(logStore.append("orders", 1, "order-1", "paid", Map.of()))
                .thenReturn(new LogRecord(0, "order-1", "paid", Map.of(), Instant.now()));

        var response = brokerService.produce("orders", new ProduceRequest("order-1", "paid", 1, Map.of()));

        assertThat(response.partition()).isEqualTo(1);
        assertThat(response.offset()).isZero();
        verify(partitioner, never()).partition(any(), any(), any(Integer.class));
    }

    @Test
    void produceWithoutPartitionUsesPartitioner() {
        TopicEntity topic = new TopicEntity("orders", 3);
        when(topicRepository.findByName("orders")).thenReturn(Optional.of(topic));
        when(partitioner.partition("orders", "order-1", 3)).thenReturn(2);
        when(logStore.append("orders", 2, "order-1", "paid", Map.of()))
                .thenReturn(new LogRecord(4, "order-1", "paid", Map.of(), Instant.now()));

        var response = brokerService.produce("orders", new ProduceRequest("order-1", "paid", null, Map.of()));

        assertThat(response.partition()).isEqualTo(2);
        assertThat(response.offset()).isEqualTo(4);
    }

    @Test
    void produceRejectsOutOfRangePartition() {
        when(topicRepository.findByName("orders")).thenReturn(Optional.of(new TopicEntity("orders", 2)));

        assertThatThrownBy(() -> brokerService.produce("orders", new ProduceRequest("k", "v", 2, Map.of())))
                .isInstanceOf(BadRequestException.class);
    }

    @Test
    void consumeReadsRequestedPartition() {
        when(topicRepository.findByName("orders")).thenReturn(Optional.of(new TopicEntity("orders", 2)));
        when(logStore.read("orders", 1, 5, 10))
                .thenReturn(List.of(new LogRecord(5, "k", "v", Map.of(), Instant.now())));

        var response = brokerService.consume("orders", 1, 5L, 10);

        assertThat(response.records()).hasSize(1);
        assertThat(response.nextOffsets()).containsEntry(1, 6L);
    }

    @Test
    void commitCreatesOffsetWhenMissing() {
        when(topicRepository.findByName("orders")).thenReturn(Optional.of(new TopicEntity("orders", 2)));
        when(offsetRepository.findByGroupIdAndTopicNameAndPartitionId("checkout", "orders", 1))
                .thenReturn(Optional.empty());
        when(offsetRepository.save(any(OffsetEntity.class))).thenAnswer(invocation -> invocation.getArgument(0));

        var response = brokerService.commitOffset("checkout", "orders", new CommitOffsetRequest(1, 42));

        assertThat(response.offset()).isEqualTo(42);
    }

    @Test
    void pollUsesCommittedOffsetAndAutoCommits() {
        when(topicRepository.findByName("orders")).thenReturn(Optional.of(new TopicEntity("orders", 1)));
        when(offsetRepository.findByGroupIdAndTopicNameAndPartitionId("checkout", "orders", 0))
                .thenReturn(Optional.of(new OffsetEntity("checkout", "orders", 0, 3)));
        when(logStore.read("orders", 0, 3, 2))
                .thenReturn(List.of(new LogRecord(3, "k", "v", Map.of(), Instant.now())));
        when(offsetRepository.save(any(OffsetEntity.class))).thenAnswer(invocation -> invocation.getArgument(0));

        var response = brokerService.poll("checkout", "orders", new PollRequest(2, true));

        assertThat(response.records()).hasSize(1);
        assertThat(response.nextOffsets()).containsEntry(0, 4L);
        verify(offsetRepository).save(any(OffsetEntity.class));
    }

    @Test
    void lagComparesEndOffsetWithCommittedOffsetPerPartition() {
        when(topicRepository.findByName("orders")).thenReturn(Optional.of(new TopicEntity("orders", 2)));
        when(logStore.endOffset("orders", 0)).thenReturn(10L);
        when(logStore.endOffset("orders", 1)).thenReturn(4L);
        when(offsetRepository.findByGroupIdAndTopicNameAndPartitionId("checkout", "orders", 0))
                .thenReturn(Optional.of(new OffsetEntity("checkout", "orders", 0, 3)));
        when(offsetRepository.findByGroupIdAndTopicNameAndPartitionId("checkout", "orders", 1))
                .thenReturn(Optional.empty());

        var response = brokerService.lag("checkout", "orders");

        assertThat(response).hasSize(2);
        assertThat(response.get(0).lag()).isEqualTo(7);
        assertThat(response.get(0).status()).isEqualTo("OK");
        assertThat(response.get(1).lag()).isEqualTo(4);
    }
}
