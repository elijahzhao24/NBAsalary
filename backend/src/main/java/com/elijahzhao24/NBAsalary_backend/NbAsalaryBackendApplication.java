package com.elijahzhao24.NBAsalary_backend;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
@RestController
public class NbAsalaryBackendApplication {

	public static void main(String[] args) {
		SpringApplication.run(NbAsalaryBackendApplication.class, args);
	}

    @GetMapping
    public String hello() {
        return "Hello World";
    }
}
