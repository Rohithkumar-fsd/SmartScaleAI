
package com.example.smartscaleai.service;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class JobService {

    private final StringRedisTemplate redisTemplate;

    @Value("${smartscale.stream.key}")
    private String streamKey;


    public Map<String, String> submitJob(String prompt) {
        String jobId = UUID.randomUUID().toString();
        String now = Instant.now().toString();

        String statusKey = getStatusKey(jobId);

        Map<String, String> jobStatus = new HashMap<>();
        jobStatus.put("jobId", jobId);
        jobStatus.put("prompt", prompt);
        jobStatus.put("status", "PENDING");
        jobStatus.put("result", "");
        jobStatus.put("error", "");
        jobStatus.put("createdAt", now);
        jobStatus.put("updatedAt", now);

        // Store the job status before publishing the job.
        redisTemplate.opsForHash().putAll(statusKey, jobStatus);

        try {
            RecordId recordId = redisTemplate.opsForStream().add(
                    MapRecord.create(
                            streamKey,
                            Map.of(
                                    "jobId", jobId,
                                    "prompt", prompt
                            )
                    )
            );

            if (recordId == null) {
                throw new IllegalStateException(
                        "Redis did not return a stream record ID"
                );
            }

            return Map.of(
                    "jobId", jobId,
                    "status", "PENDING"
            );

        } catch (RuntimeException exception) {
            redisTemplate.opsForHash().put(
                    statusKey,
                    "status",
                    "FAILED"
            );
            redisTemplate.opsForHash().put(
                    statusKey,
                    "error",
                    "Unable to enqueue job"
            );

            throw exception;
        }
    }

    public Map<Object, Object> getJob(String jobId) {
        Map<Object, Object> job = redisTemplate.opsForHash()
                .entries(getStatusKey(jobId));

        if (job.isEmpty()) {
            throw new ResponseStatusException(
                    HttpStatus.NOT_FOUND,
                    "Job not found: " + jobId
            );
        }

        return job;
    }

    private String getStatusKey(String jobId) {
        return "job:" + jobId;
    }
}
