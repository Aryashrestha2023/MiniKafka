package com.minikafka.config;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "minikafka")
public class MiniKafkaProperties {

    private final Storage storage = new Storage();
    private final Tcp tcp = new Tcp();
    private final Jwt jwt = new Jwt();

    public Storage getStorage() {
        return storage;
    }

    public Tcp getTcp() {
        return tcp;
    }

    public Jwt getJwt() {
        return jwt;
    }

    public static class Storage {
        @NotBlank
        private String root = "data/minikafka-logs";

        public String getRoot() {
            return root;
        }

        public void setRoot(String root) {
            this.root = root;
        }
    }

    public static class Tcp {
        private boolean enabled = true;
        @NotBlank
        private String host = "0.0.0.0";
        @Min(1)
        private int port = 9092;
        @Min(1)
        private int workerThreads = 8;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public int getWorkerThreads() {
            return workerThreads;
        }

        public void setWorkerThreads(int workerThreads) {
            this.workerThreads = workerThreads;
        }
    }

    public static class Jwt {
        @NotBlank
        private String secret = "change-me-change-me-change-me-change-me";
        @Min(1)
        private long ttlMinutes = 240;

        public String getSecret() {
            return secret;
        }

        public void setSecret(String secret) {
            this.secret = secret;
        }

        public long getTtlMinutes() {
            return ttlMinutes;
        }

        public void setTtlMinutes(long ttlMinutes) {
            this.ttlMinutes = ttlMinutes;
        }
    }
}
