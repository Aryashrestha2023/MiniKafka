package com.minikafka.demo.ecommerce;

import com.minikafka.demo.ecommerce.dto.CreateDemoOrderRequest;
import com.minikafka.demo.ecommerce.dto.DemoOrderResponse;
import com.minikafka.demo.ecommerce.dto.DemoStateResponse;
import com.minikafka.demo.ecommerce.dto.DemoStepResponse;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/v1/demo/ecommerce")
public class EcommerceDemoController {

    private final EcommerceDemoService demoService;

    public EcommerceDemoController(EcommerceDemoService demoService) {
        this.demoService = demoService;
    }

    @PostMapping("/orders")
    @ResponseStatus(HttpStatus.CREATED)
    public DemoOrderResponse createOrder(@Valid @RequestBody CreateDemoOrderRequest request) {
        return demoService.createOrder(request);
    }

    @GetMapping("/orders")
    public List<DemoOrderResponse> orders() {
        return demoService.orders();
    }

    @PostMapping("/payments/process")
    public DemoStepResponse processPayments() {
        return demoService.processPayments();
    }

    @PostMapping("/notifications/process")
    public DemoStepResponse processNotifications() {
        return demoService.processNotifications();
    }

    @PostMapping("/reset")
    public DemoStateResponse reset() {
        return demoService.reset();
    }

    @GetMapping("/state")
    public DemoStateResponse state() {
        return demoService.state();
    }
}
