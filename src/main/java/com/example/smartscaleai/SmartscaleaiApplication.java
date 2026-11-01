package com.example.smartscaleai;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class SmartscaleaiApplication {

	public static void main(String[] args) {
		SpringApplication.run(SmartscaleaiApplication.class, args);
	}

}
