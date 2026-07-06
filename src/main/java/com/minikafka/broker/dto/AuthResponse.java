package com.minikafka.broker.dto;

import java.time.Instant;
import java.util.Set;

public record AuthResponse(String token, String username, Set<String> roles, Instant expiresAt) {
}
