package com.minikafka.api;

import com.minikafka.broker.BrokerService;
import com.minikafka.broker.dto.ConsumeResponse;
import com.minikafka.broker.dto.MessageResponse;
import com.minikafka.broker.dto.ProduceRequest;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import org.springframework.http.HttpStatus;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Validated
@RequestMapping("/api/v1/topics/{topic}")
public class MessageController {

    private final BrokerService brokerService;

    public MessageController(BrokerService brokerService) {
        this.brokerService = brokerService;
    }

    @PostMapping("/messages")
    @ResponseStatus(HttpStatus.CREATED)
    public MessageResponse produce(@PathVariable String topic, @Valid @RequestBody ProduceRequest request) {
        return brokerService.produce(topic, request);
    }

    @GetMapping("/messages")
    public ConsumeResponse consume(
            @PathVariable String topic,
            @RequestParam(required = false) @Min(0) Integer partition,
            @RequestParam(defaultValue = "0") @Min(0) Long offset,
            @RequestParam(defaultValue = "50") @Min(1) @Max(500) Integer maxMessages
    ) {
        return brokerService.consume(topic, partition, offset, maxMessages);
    }

    @GetMapping("/partitions/{partition}/messages")
    public ConsumeResponse consumePartition(
            @PathVariable String topic,
            @PathVariable @Min(0) Integer partition,
            @RequestParam(defaultValue = "0") @Min(0) Long offset,
            @RequestParam(defaultValue = "50") @Min(1) @Max(500) Integer maxMessages
    ) {
        return brokerService.consume(topic, partition, offset, maxMessages);
    }

    @GetMapping("/partitions/{partition}/messages/{offset}")
    public MessageResponse getMessage(
            @PathVariable String topic,
            @PathVariable @Min(0) Integer partition,
            @PathVariable @Min(0) Long offset
    ) {
        return brokerService.consume(topic, partition, offset, 1)
                .records()
                .stream()
                .findFirst()
                .filter(message -> message.offset() == offset)
                .orElseThrow(() -> new NotFoundException("Message not found at offset " + offset));
    }
}
