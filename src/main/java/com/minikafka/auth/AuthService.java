package com.minikafka.auth;

import com.minikafka.api.ConflictException;
import com.minikafka.broker.dto.AuthResponse;
import com.minikafka.broker.dto.LoginRequest;
import com.minikafka.broker.dto.RegisterRequest;
import com.minikafka.persistence.UserEntity;
import com.minikafka.persistence.UserRepository;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class AuthService {

    private final UserRepository userRepository;
    private final PasswordEncoder passwordEncoder;
    private final AuthenticationManager authenticationManager;
    private final JwtService jwtService;

    public AuthService(
            UserRepository userRepository,
            PasswordEncoder passwordEncoder,
            AuthenticationManager authenticationManager,
            JwtService jwtService
    ) {
        this.userRepository = userRepository;
        this.passwordEncoder = passwordEncoder;
        this.authenticationManager = authenticationManager;
        this.jwtService = jwtService;
    }

    @Transactional
    public AuthResponse register(RegisterRequest request) {
        if (userRepository.existsByUsername(request.username())) {
            throw new ConflictException("Username is already registered");
        }
        if (userRepository.existsByEmail(request.email())) {
            throw new ConflictException("Email is already registered");
        }
        String roles = userRepository.count() == 0 ? "ROLE_ADMIN,ROLE_USER" : "ROLE_USER";
        UserEntity user = userRepository.save(new UserEntity(
                request.username(),
                request.email(),
                passwordEncoder.encode(request.password()),
                roles
        ));
        UserPrincipal principal = new UserPrincipal(user);
        return new AuthResponse(jwtService.generateToken(principal), user.getUsername(), principal.roles(), jwtService.expiresAt());
    }

    @Transactional(readOnly = true)
    public AuthResponse login(LoginRequest request) {
        var authentication = authenticationManager.authenticate(
                new UsernamePasswordAuthenticationToken(request.username(), request.password())
        );
        UserPrincipal principal = (UserPrincipal) authentication.getPrincipal();
        return new AuthResponse(jwtService.generateToken(principal), principal.getUsername(), principal.roles(), jwtService.expiresAt());
    }
}
