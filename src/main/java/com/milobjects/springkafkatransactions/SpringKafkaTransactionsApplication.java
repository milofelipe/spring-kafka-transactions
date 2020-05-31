package com.milobjects.springkafkatransactions;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@Slf4j
public class SpringKafkaTransactionsApplication {

    public static void main(String[] args) {
        SpringApplication.run(SpringKafkaTransactionsApplication.class, args);
    }

}
