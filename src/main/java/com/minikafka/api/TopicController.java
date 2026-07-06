package com.minikafka.api;

import com.minikafka.broker.BrokerService;
import com.minikafka.broker.dto.CreateTopicRequest;
import com.minikafka.broker.dto.TopicResponse;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import org.springframework.http.HttpStatus;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@Validated
@RequestMapping("/api/v1/topics")
public class TopicController {

    private final BrokerService brokerService;

    public TopicController(BrokerService brokerService) {
        this.brokerService = brokerService;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public TopicResponse createTopic(@Valid @RequestBody CreateTopicRequest request) {
        return brokerService.createTopic(request);
    }

    @GetMapping
    public List<TopicResponse> listTopics() {
        return brokerService.listTopics();
    }

    @GetMapping("/{topic}")
    public TopicResponse getTopic(@PathVariable String topic) {
        return brokerService.getTopic(topic);
    }

    @DeleteMapping("/{topic}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deleteTopic(@PathVariable String topic) {
        brokerService.deleteTopic(topic);
    }

    @GetMapping("/{topic}/partitions")
    public List<Integer> partitions(@PathVariable String topic) {
        return brokerService.partitions(topic);
    }

    @PutMapping("/{topic}/partitions")
    public TopicResponse increasePartitions(
            @PathVariable String topic,
            @RequestParam @Min(1) @Max(128) int target
    ) {
        return brokerService.increasePartitions(topic, target);
    }

    @GetMapping("/{topic}/offsets/end")
    public Map<Integer, Long> endOffsets(@PathVariable String topic) {
        return brokerService.endOffsets(topic);
    }
}
