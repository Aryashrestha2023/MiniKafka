package com.minikafka.api;

import com.minikafka.broker.BrokerAdminService;
import com.minikafka.broker.dto.BrokerStatsResponse;
import com.minikafka.broker.tcp.TcpProtocolHandler;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/v1/admin")
public class AdminController {

    private final BrokerAdminService adminService;
    private final TcpProtocolHandler tcpProtocolHandler;

    public AdminController(BrokerAdminService adminService, TcpProtocolHandler tcpProtocolHandler) {
        this.adminService = adminService;
        this.tcpProtocolHandler = tcpProtocolHandler;
    }

    @GetMapping("/broker")
    public BrokerStatsResponse broker() {
        return adminService.brokerStats();
    }

    @GetMapping("/storage")
    public Map<String, Object> storage() {
        return adminService.storageInfo();
    }

    @GetMapping("/config")
    public Map<String, Object> config() {
        return adminService.runtimeConfig();
    }

    @GetMapping("/topics/{topic}/stats")
    public Map<String, Object> topicStats(@PathVariable String topic) {
        return adminService.topicStats(topic);
    }

    @PostMapping("/tcp/command")
    public String tcpCommand(@Valid @RequestBody TcpCommandRequest request) {
        return tcpProtocolHandler.handleLine(request.command()).trim();
    }

    public record TcpCommandRequest(@NotBlank String command) {
    }
}
