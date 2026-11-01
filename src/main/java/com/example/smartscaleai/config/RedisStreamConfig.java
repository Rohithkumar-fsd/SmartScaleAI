
package com.example.smartscaleai.config;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.core.StringRedisTemplate;

import jakarta.annotation.PostConstruct;
import java.util.Map;

@Configuration
@RequiredArgsConstructor
public class RedisStreamConfig {

    private final StringRedisTemplate redisTemplate;

    @Value("${smartscale.stream.key}")
    private String streamKey;

    @Value("${smartscale.stream.group}")
    private String groupName;

    @PostConstruct
    public void initializeConsumerGroup() {
        try {
            redisTemplate.opsForStream().createGroup(
                    streamKey,
                    ReadOffset.latest(),
                    groupName
            );

            System.out.println("Created Redis consumer group: " + groupName);

        } catch (Exception exception) {
            Throwable cause = exception;

            while (cause != null) {
                String message = cause.getMessage();

                if (message != null && message.contains("BUSYGROUP")) {
                    System.out.println(
                            "Consumer group already exists: " + groupName
                    );
                    return;
                }

                cause = cause.getCause();
            }

            throw exception;
        }
    }
}
