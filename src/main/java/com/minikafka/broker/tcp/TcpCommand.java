package com.minikafka.broker.tcp;

import java.util.List;

public record TcpCommand(String name, List<String> args) {

    public static TcpCommand parse(String line) {
        if (line == null || line.isBlank()) {
            throw new IllegalArgumentException("command is required");
        }
        String trimmed = line.trim();
        String[] tokens = trimmed.split("\\s+");
        String name = tokens[0].toUpperCase();
        return switch (name) {
            case "PRODUCE" -> parseProduce(trimmed);
            default -> new TcpCommand(name, java.util.Arrays.stream(tokens).skip(1).toList());
        };
    }

    private static TcpCommand parseProduce(String line) {
        String[] tokens = line.split("\\s+", 4);
        if (tokens.length < 4) {
            throw new IllegalArgumentException("PRODUCE requires: PRODUCE <topic> <key|-> <value>");
        }
        return new TcpCommand("PRODUCE", List.of(tokens[1], tokens[2], tokens[3]));
    }
}
