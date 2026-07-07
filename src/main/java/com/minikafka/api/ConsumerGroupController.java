package com.minikafka.api;

import com.minikafka.broker.BrokerService;
import com.minikafka.broker.dto.CommitOffsetRequest;
import com.minikafka.broker.dto.ConsumerGroupAssignmentResponse;
import com.minikafka.broker.dto.ConsumerGroupMemberRequest;
import com.minikafka.broker.dto.ConsumerGroupMemberResponse;
import com.minikafka.broker.dto.ConsumeResponse;
import com.minikafka.broker.dto.LagResponse;
import com.minikafka.broker.dto.OffsetResponse;
import com.minikafka.broker.dto.PollRequest;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@Validated
@RequestMapping("/api/v1/consumer-groups")
public class ConsumerGroupController {

    private final BrokerService brokerService;

    public ConsumerGroupController(BrokerService brokerService) {
        this.brokerService = brokerService;
    }

    @GetMapping
    public List<String> groups() {
        return brokerService.consumerGroups();
    }

    @GetMapping("/{groupId}/offsets")
    public List<OffsetResponse> groupOffsets(@PathVariable String groupId) {
        return brokerService.offsetsForGroup(groupId);
    }

    @GetMapping("/{groupId}/topics/{topic}/offsets")
    public List<OffsetResponse> topicOffsets(@PathVariable String groupId, @PathVariable String topic) {
        return brokerService.offsets(groupId, topic);
    }

    @GetMapping("/{groupId}/topics/{topic}/lag")
    public List<LagResponse> lag(@PathVariable String groupId, @PathVariable String topic) {
        return brokerService.lag(groupId, topic);
    }

    @PostMapping("/{groupId}/topics/{topic}/members")
    public ConsumerGroupMemberResponse join(
            @PathVariable String groupId,
            @PathVariable String topic,
            @Valid @RequestBody ConsumerGroupMemberRequest request
    ) {
        return brokerService.joinGroup(groupId, topic, request.memberId());
    }

    @DeleteMapping("/{groupId}/topics/{topic}/members/{memberId}")
    public ConsumerGroupAssignmentResponse leave(
            @PathVariable String groupId,
            @PathVariable String topic,
            @PathVariable String memberId
    ) {
        return brokerService.leaveGroup(groupId, topic, memberId);
    }

    @GetMapping("/{groupId}/topics/{topic}/assignments")
    public ConsumerGroupAssignmentResponse assignments(@PathVariable String groupId, @PathVariable String topic) {
        return brokerService.assignments(groupId, topic);
    }

    @PostMapping("/{groupId}/topics/{topic}/offsets")
    @ResponseStatus(HttpStatus.CREATED)
    public OffsetResponse commit(
            @PathVariable String groupId,
            @PathVariable String topic,
            @Valid @RequestBody CommitOffsetRequest request
    ) {
        return brokerService.commitOffset(groupId, topic, request);
    }

    @PostMapping("/{groupId}/topics/{topic}/poll")
    public ConsumeResponse poll(
            @PathVariable String groupId,
            @PathVariable String topic,
            @Valid @RequestBody PollRequest request
    ) {
        return brokerService.poll(groupId, topic, request);
    }
}
