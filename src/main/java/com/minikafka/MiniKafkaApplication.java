package com.minikafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class MiniKafkaApplication {

    public static void main(String[] args) {
        SpringApplication.run(MiniKafkaApplication.class, args);
    }
}
