package com.minikafka.auth;

import com.minikafka.config.MiniKafkaProperties;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.stereotype.Service;

import javax.crypto.SecretKey;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Date;
import java.util.Map;

@Service
public class JwtService {

    private final MiniKafkaProperties properties;

    public JwtService(MiniKafkaProperties properties) {
        this.properties = properties;
    }

    public String generateToken(UserPrincipal principal) {
        Instant expiresAt = expiresAt();
        return Jwts.builder()
                .subject(principal.getUsername())
                .claims(Map.of("roles", principal.roles()))
                .issuedAt(Date.from(Instant.now()))
                .expiration(Date.from(expiresAt))
                .signWith(secretKey())
                .compact();
    }

    public String username(String token) {
        return claims(token).getSubject();
    }

    public boolean isValid(String token, UserDetails userDetails) {
        return userDetails.getUsername().equals(username(token)) && claims(token).getExpiration().after(new Date());
    }

    public Instant expiresAt() {
        return Instant.now().plusSeconds(properties.getJwt().getTtlMinutes() * 60);
    }

    private Claims claims(String token) {
        return Jwts.parser()
                .verifyWith(secretKey())
                .build()
                .parseSignedClaims(token)
                .getPayload();
    }

    private SecretKey secretKey() {
        return Keys.hmacShaKeyFor(properties.getJwt().getSecret().getBytes(StandardCharsets.UTF_8));
    }
}
