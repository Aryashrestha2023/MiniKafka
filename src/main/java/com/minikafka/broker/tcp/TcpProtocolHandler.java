package com.minikafka.broker.tcp;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.minikafka.broker.BrokerAdminService;
import com.minikafka.broker.BrokerService;
import com.minikafka.broker.dto.CommitOffsetRequest;
import com.minikafka.broker.dto.CreateTopicRequest;
import com.minikafka.broker.dto.PollRequest;
import com.minikafka.broker.dto.ProduceRequest;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;

@Component
public class TcpProtocolHandler {

    private final BrokerService brokerService;
    private final BrokerAdminService brokerAdminService;
    private final ObjectMapper objectMapper;

    public TcpProtocolHandler(BrokerService brokerService, BrokerAdminService brokerAdminService, ObjectMapper objectMapper) {
        this.brokerService = brokerService;
        this.brokerAdminService = brokerAdminService;
        this.objectMapper = objectMapper;
    }

    public String handleLine(String line) {
        try {
            TcpCommand command = TcpCommand.parse(line);
            Object payload = handle(command);
            return objectMapper.writeValueAsString(Map.of("status", "ok", "data", payload)) + "\n";
        } catch (Exception ex) {
            return error(ex.getMessage());
        }
    }

    private Object handle(TcpCommand command) {
        return switch (command.name()) {
            case "HELP" -> Map.of(
                    "commands",
                    "CREATE_TOPIC topic partitions | TOPICS | PRODUCE topic key|- value | CONSUME topic partition offset max | POLL group topic max autoCommit | POLL_MEMBER group topic member max autoCommit | JOIN_GROUP group topic member | LEAVE_GROUP group topic member | ASSIGNMENTS group topic | COMMIT group topic partition offset | OFFSETS group topic | STATS"
            );
            case "CREATE_TOPIC" -> {
                require(command, 2);
                yield brokerService.createTopic(new CreateTopicRequest(command.args().get(0), integer(command.args().get(1), "partitions")));
            }
            case "TOPICS" -> brokerService.listTopics();
            case "PRODUCE" -> {
                require(command, 3);
                String key = "-".equals(command.args().get(1)) ? null : command.args().get(1);
                yield brokerService.produce(command.args().get(0), new ProduceRequest(key, command.args().get(2), null, Map.of()));
            }
            case "CONSUME" -> {
                require(command, 4);
                yield brokerService.consume(
                        command.args().get(0),
                        integer(command.args().get(1), "partition"),
                        longValue(command.args().get(2), "offset"),
                        integer(command.args().get(3), "max")
                );
            }
            case "POLL" -> {
                require(command, 4);
                yield brokerService.poll(
                        command.args().get(0),
                        command.args().get(1),
                        new PollRequest(integer(command.args().get(2), "max"), Boolean.parseBoolean(command.args().get(3)))
                );
            }
            case "POLL_MEMBER" -> {
                require(command, 5);
                yield brokerService.poll(
                        command.args().get(0),
                        command.args().get(1),
                        new PollRequest(integer(command.args().get(3), "max"), Boolean.parseBoolean(command.args().get(4)), command.args().get(2))
                );
            }
            case "JOIN_GROUP" -> {
                require(command, 3);
                yield brokerService.joinGroup(command.args().get(0), command.args().get(1), command.args().get(2));
            }
            case "LEAVE_GROUP" -> {
                require(command, 3);
                yield brokerService.leaveGroup(command.args().get(0), command.args().get(1), command.args().get(2));
            }
            case "ASSIGNMENTS" -> {
                require(command, 2);
                yield brokerService.assignments(command.args().get(0), command.args().get(1));
            }
            case "COMMIT" -> {
                require(command, 4);
                yield brokerService.commitOffset(
                        command.args().get(0),
                        command.args().get(1),
                        new CommitOffsetRequest(integer(command.args().get(2), "partition"), longValue(command.args().get(3), "offset"))
                );
            }
            case "OFFSETS" -> {
                require(command, 2);
                yield brokerService.offsets(command.args().get(0), command.args().get(1));
            }
            case "STATS" -> brokerAdminService.brokerStats();
            default -> throw new IllegalArgumentException("Unknown command: " + command.name());
        };
    }

    private void require(TcpCommand command, int arguments) {
        if (command.args().size() != arguments) {
            throw new IllegalArgumentException(command.name() + " requires " + arguments + " argument(s)");
        }
    }

    private int integer(String raw, String field) {
        try {
            return Integer.parseInt(raw);
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException(field + " must be an integer");
        }
    }

    private long longValue(String raw, String field) {
        try {
            return Long.parseLong(raw);
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException(field + " must be a long");
        }
    }

    private String error(String message) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("status", "error");
        payload.put("message", message == null ? "Unknown error" : message);
        try {
            return objectMapper.writeValueAsString(payload) + "\n";
        } catch (JsonProcessingException ex) {
            return "{\"status\":\"error\",\"message\":\"serialization failure\"}\n";
        }
    }
}
