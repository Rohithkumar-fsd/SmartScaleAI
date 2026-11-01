
package com.example.smartscaleai.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

public record CreateJobRequest(

        @NotBlank(message = "Prompt cannot be empty")
        @Size(max = 2000, message = "Prompt must be at most 2000 characters")
        String prompt

) {}
