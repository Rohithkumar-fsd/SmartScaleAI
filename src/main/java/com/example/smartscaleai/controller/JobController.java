
package com.example.smartscaleai.controller;

import com.example.smartscaleai.dto.CreateJobRequest;
import com.example.smartscaleai.service.JobService;

import jakarta.validation.Valid;

import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/jobs")
@RequiredArgsConstructor
public class JobController {

    private final JobService jobService;

    @PostMapping
    public ResponseEntity<Map<String, String>> submitJob(
            @Valid @RequestBody CreateJobRequest request) {

        Map<String, String> response =
                jobService.submitJob(request.prompt());

        return ResponseEntity.accepted().body(response);
    }

    @GetMapping("/{jobId}")
    public ResponseEntity<Map<Object, Object>> getJob(
            @PathVariable String jobId) {

        return ResponseEntity.ok(
                jobService.getJob(jobId)
        );
    }
}
