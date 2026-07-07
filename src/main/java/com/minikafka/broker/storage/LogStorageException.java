package com.minikafka.broker.storage;

public class LogStorageException extends RuntimeException {

    public LogStorageException(String message) {
        super(message);
    }

    public LogStorageException(String message, Throwable cause) {
        super(message, cause);
    }
}
