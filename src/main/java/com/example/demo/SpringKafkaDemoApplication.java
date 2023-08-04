package com.example.demo;

import com.example.demo.producer.DemoProducer;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class SpringKafkaDemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(SpringKafkaDemoApplication.class, args);
    }

    @Bean
    ApplicationRunner myApplicationRunner(DemoProducer demoProducer) {
        return args -> {
            demoProducer.syncSendToTopic1("Hello, topic-01");
            demoProducer.asyncSendToTopic2("Hello, topic-02");
        };
    }
}
