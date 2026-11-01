package com.example.smartscaleai.worker;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Component
public class InferenceWorker {

    private final StringRedisTemplate redisTemplate;

    @Value("${smartscale.stream.key}")
    private String streamKey;

    @Value("${smartscale.stream.group}")
    private String groupName;

    @Value("${smartscale.stream.dlq:inference:jobs:dlq}")
    private String dlqKey;

    @Value("${smartscale.worker.max-attempts:3}")
    private int maxAttempts;

    @Value("${smartscale.worker.enabled:true}")
    private boolean enabled;

    private final String consumerName = "worker-" + UUID.randomUUID();

    public InferenceWorker(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    // ------------------------------------------------------------------------
    // Loop 1: Ingests brand new, unread jobs from the stream
    // ------------------------------------------------------------------------
    @Scheduled(fixedDelay = 500)
    public void pollQueue() {
        if (!enabled) {
            return;
        }

        List<MapRecord<String, Object, Object>> records =
                redisTemplate.opsForStream().read(
                        Consumer.from(groupName, consumerName),
                        StreamReadOptions.empty()
                                .count(1)
                                .block(Duration.ofSeconds(1)),
                        StreamOffset.create(
                                streamKey,
                                ReadOffset.lastConsumed()
                        )
                );

        if (records == null || records.isEmpty()) {
            return;
        }

        for (MapRecord<String, Object, Object> record : records) {
            processRecord(record);
        }
    }

    // ------------------------------------------------------------------------
    // Loop 2: Recovers abandoned jobs from crashed workers (idle > 30 seconds)
    // ------------------------------------------------------------------------
    @Scheduled(fixedDelay = 10000)
    public void recoverAbandonedJobs() {
        if (!enabled) {
            return;
        }

        try {
            PendingMessages pendingMessages = redisTemplate.opsForStream().pending(
                    streamKey,
                    groupName,
                    Range.unbounded(),
                    10
            );

            if (pendingMessages == null || pendingMessages.isEmpty()) {
                return;
            }

            for (PendingMessage pending : pendingMessages) {
                if (pending.getElapsedTimeSinceLastDelivery().compareTo(Duration.ofSeconds(30)) > 0) {

                    if (pending.getTotalDeliveryCount() >= maxAttempts) {
                        moveToDeadLetterQueue(pending.getId());
                        continue;
                    }

                    List<MapRecord<String, Object, Object>> claimedRecords =
                            redisTemplate.opsForStream().claim(
                                    streamKey,
                                    groupName,
                                    consumerName,
                                    Duration.ofSeconds(30),
                                    pending.getId()
                            );

                    if (claimedRecords != null) {
                        for (MapRecord<String, Object, Object> rawRecord : claimedRecords) {
                            processRecord(rawRecord);
                        }
                    }
                }
            }
        } catch (Exception ignored) {
            // Suppress exceptions during initial startup or when stream is empty
        }
    }

    // ------------------------------------------------------------------------
    // Core Execution Logic (with Idempotency Check & Null Safety)
    // ------------------------------------------------------------------------
    private void processRecord(MapRecord<String, Object, Object> record) {
        Object jobIdObj = record.getValue().get("jobId");

        // Safely filter out initialization records that have no jobId
        if (jobIdObj == null) {
            acknowledge(record.getId());
            return;
        }

        String jobId = jobIdObj.toString();
        String statusKey = "job:" + jobId;

        Object promptObj = record.getValue().get("prompt");
        String prompt = (promptObj != null) ? promptObj.toString() : "";

        try {
            // Idempotency: Skip execution if already finished
            Object currentStatus = redisTemplate.opsForHash().get(statusKey, "status");
            if ("COMPLETED".equals(currentStatus)) {
                acknowledge(record.getId());
                return;
            }

            redisTemplate.opsForHash().putAll(
                    statusKey,
                    Map.of(
                            "status", "PROCESSING",
                            "worker", consumerName,
                            "updatedAt", Instant.now().toString()
                    )
            );

            String result = runMockInference(prompt);

            redisTemplate.opsForHash().putAll(
                    statusKey,
                    Map.of(
                            "status", "COMPLETED",
                            "result", result,
                            "error", "",
                            "updatedAt", Instant.now().toString()
                    )
            );

        } catch (Exception exception) {
            redisTemplate.opsForHash().putAll(
                    statusKey,
                    Map.of(
                            "status", "FAILED",
                            "error", "Inference failed: " + exception.getMessage(),
                            "updatedAt", Instant.now().toString()
                    )
            );
        }

        // Always acknowledge only after saving the result
        acknowledge(record.getId());
        // After acknowledging:
    }

    // ------------------------------------------------------------------------
    // Dead-Letter Queue Routing
    // ------------------------------------------------------------------------
    private void moveToDeadLetterQueue(RecordId recordId) {
        List<MapRecord<String, Object, Object>> records = redisTemplate.opsForStream().range(
                streamKey,
                Range.closed(recordId.getValue(), recordId.getValue())
        );

        if (records != null && !records.isEmpty()) {
            MapRecord<String, Object, Object> record = records.get(0);
            Object jobIdObj = record.getValue().get("jobId");

            redisTemplate.opsForStream().add(dlqKey, record.getValue());

            if (jobIdObj != null) {
                redisTemplate.opsForHash().putAll("job:" + jobIdObj.toString(), Map.of(
                        "status", "FAILED_EXHAUSTED",
                        "error", "Exceeded max retry threshold (" + maxAttempts + "). Moved to DLQ.",
                        "updatedAt", Instant.now().toString()
                ));
            }
        }

        acknowledge(recordId);
        // After acknowledging:
    }

    private String runMockInference(String prompt) throws InterruptedException {
        Thread.sleep(1500);

        if (prompt == null || prompt.isBlank()) {
            throw new IllegalArgumentException("Prompt cannot be empty");
        }

        return "Mock AI response: I received your prompt: "
                + prompt
                + ". This response simulates AI inference.";
    }

    private void acknowledge(RecordId recordId) {
        redisTemplate.opsForStream().acknowledge(
                streamKey,
                groupName,
                recordId
        );
        redisTemplate.opsForStream().delete(streamKey, recordId);
    }
}