package com.minikafka.benchmark;

import com.minikafka.api.ConflictException;
import com.minikafka.broker.BrokerService;
import com.minikafka.broker.dto.CreateTopicRequest;
import com.minikafka.config.MiniKafkaProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@Component
@Profile("benchmark")
public class TcpBenchmarkRunner implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(TcpBenchmarkRunner.class);

    private final BrokerService brokerService;
    private final MiniKafkaProperties properties;
    private final ConfigurableApplicationContext context;
    private final String topic;
    private final int partitions;
    private final int clients;
    private final int messagesPerClient;
    private final int targetMessagesPerSecond;
    private final boolean exitOnComplete;

    public TcpBenchmarkRunner(
            BrokerService brokerService,
            MiniKafkaProperties properties,
            ConfigurableApplicationContext context,
            @Value("${minikafka.benchmark.topic:benchmark.orders}") String topic,
            @Value("${minikafka.benchmark.partitions:6}") int partitions,
            @Value("${minikafka.benchmark.clients:8}") int clients,
            @Value("${minikafka.benchmark.messages-per-client:1000}") int messagesPerClient,
            @Value("${minikafka.benchmark.target-messages-per-second:5000}") int targetMessagesPerSecond,
            @Value("${minikafka.benchmark.exit-on-complete:true}") boolean exitOnComplete
    ) {
        this.brokerService = brokerService;
        this.properties = properties;
        this.context = context;
        this.topic = topic;
        this.partitions = partitions;
        this.clients = clients;
        this.messagesPerClient = messagesPerClient;
        this.targetMessagesPerSecond = targetMessagesPerSecond;
        this.exitOnComplete = exitOnComplete;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        ensureTopic();
        waitForTcpServer();
        int totalMessages = clients * messagesPerClient;
        var executor = Executors.newFixedThreadPool(clients);
        List<Future<Integer>> results = new ArrayList<>();
        long started = System.nanoTime();
        for (int client = 0; client < clients; client++) {
            int clientId = client;
            results.add(executor.submit(newClient(clientId)));
        }
        int produced = 0;
        for (Future<Integer> result : results) {
            produced += result.get();
        }
        executor.shutdown();
        long elapsedNanos = System.nanoTime() - started;
        double seconds = elapsedNanos / 1_000_000_000.0;
        double messagesPerSecond = produced / seconds;
        boolean targetMet = messagesPerSecond >= targetMessagesPerSecond;
        log.info(
                "event=tcp_benchmark_complete produced={} requested={} clients={} partitions={} seconds={} messagesPerSecond={} target={} targetMet={}",
                produced,
                totalMessages,
                clients,
                partitions,
                String.format(Locale.ROOT, "%.3f", seconds),
                String.format(Locale.ROOT, "%.2f", messagesPerSecond),
                targetMessagesPerSecond,
                targetMet
        );
        if (exitOnComplete) {
            SpringApplication.exit(context, () -> targetMet ? 0 : 2);
        }
    }

    private void ensureTopic() {
        try {
            brokerService.createTopic(new CreateTopicRequest(topic, partitions));
        } catch (ConflictException ignored) {
            // Reusing an existing benchmark topic is fine for repeated local runs.
        }
    }

    private Callable<Integer> newClient(int clientId) {
        return () -> {
            int sent = 0;
            try (
                    Socket socket = new Socket(properties.getTcp().getHost().replace("0.0.0.0", "127.0.0.1"), properties.getTcp().getPort());
                    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
                    BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))
            ) {
                socket.setSoTimeout((int) Duration.ofSeconds(10).toMillis());
                for (int message = 0; message < messagesPerClient; message++) {
                    writer.write("PRODUCE " + topic + " client-" + clientId + "-" + message + " value-" + message);
                    writer.newLine();
                    writer.flush();
                    String response = reader.readLine();
                    if (response == null || !response.contains("\"status\":\"ok\"")) {
                        throw new IllegalStateException("Unexpected TCP benchmark response: " + response);
                    }
                    sent++;
                }
                return sent;
            }
        };
    }

    private void waitForTcpServer() throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (System.nanoTime() < deadline) {
            try (Socket ignored = new Socket(properties.getTcp().getHost().replace("0.0.0.0", "127.0.0.1"), properties.getTcp().getPort())) {
                return;
            } catch (Exception ignored) {
                Thread.sleep(100);
            }
        }
        throw new IllegalStateException("TCP benchmark could not connect to broker port " + properties.getTcp().getPort());
    }
}
