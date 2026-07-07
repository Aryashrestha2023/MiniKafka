package com.minikafka.broker;

import com.minikafka.api.BadRequestException;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

@Component
public class ConsumerGroupCoordinator {

    private final ConcurrentHashMap<GroupTopic, ConcurrentSkipListSet<String>> memberships = new ConcurrentHashMap<>();

    public Map<String, List<Integer>> join(String groupId, String topicName, String memberId, int partitionCount) {
        validate(memberId);
        memberships.computeIfAbsent(new GroupTopic(groupId, topicName), ignored -> new ConcurrentSkipListSet<>())
                .add(memberId);
        return assignments(groupId, topicName, partitionCount);
    }

    public Map<String, List<Integer>> leave(String groupId, String topicName, String memberId, int partitionCount) {
        validate(memberId);
        GroupTopic key = new GroupTopic(groupId, topicName);
        ConcurrentSkipListSet<String> members = memberships.get(key);
        if (members != null) {
            members.remove(memberId);
            if (members.isEmpty()) {
                memberships.remove(key);
            }
        }
        return assignments(groupId, topicName, partitionCount);
    }

    public Map<String, List<Integer>> assignments(String groupId, String topicName, int partitionCount) {
        if (partitionCount < 1) {
            throw new BadRequestException("partitionCount must be positive");
        }
        List<String> members = members(groupId, topicName);
        Map<String, List<Integer>> assignments = new LinkedHashMap<>();
        members.forEach(member -> assignments.put(member, new ArrayList<>()));
        if (members.isEmpty()) {
            return assignments;
        }
        for (int partition = 0; partition < partitionCount; partition++) {
            String owner = members.get(partition % members.size());
            assignments.get(owner).add(partition);
        }
        return assignments;
    }

    public List<Integer> assignmentFor(String groupId, String topicName, String memberId, int partitionCount) {
        if (memberId == null || memberId.isBlank()) {
            List<Integer> allPartitions = new ArrayList<>();
            for (int partition = 0; partition < partitionCount; partition++) {
                allPartitions.add(partition);
            }
            return allPartitions;
        }
        join(groupId, topicName, memberId, partitionCount);
        return assignments(groupId, topicName, partitionCount).getOrDefault(memberId, List.of());
    }

    public List<String> members(String groupId, String topicName) {
        ConcurrentSkipListSet<String> members = memberships.get(new GroupTopic(groupId, topicName));
        return members == null ? List.of() : List.copyOf(members);
    }

    public void clearTopic(String topicName) {
        memberships.keySet().removeIf(key -> key.topicName().equals(topicName));
    }

    private void validate(String memberId) {
        if (memberId == null || memberId.isBlank()) {
            throw new BadRequestException("memberId is required");
        }
    }

    private record GroupTopic(String groupId, String topicName) {
    }
}
