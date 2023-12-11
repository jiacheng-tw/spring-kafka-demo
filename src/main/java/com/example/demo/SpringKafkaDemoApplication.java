package com.example.demo;

import com.example.demo.producer.DemoProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;

import java.util.Arrays;

@Slf4j
@SpringBootApplication
public class SpringKafkaDemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(SpringKafkaDemoApplication.class, args);
    }

    @Bean
    @Profile("test")
    ApplicationRunner myApplicationRunner(ApplicationContext context, DemoProducer demoProducer) {
        return args -> {
            log.info("KafkaTemplate beans: {}", Arrays.toString(context.getBeanNamesForType(KafkaTemplate.class)));
            log.info("KafkaMessageListenerContainer beans: {}",
                    Arrays.toString(context.getBeanNamesForType(KafkaMessageListenerContainer.class)));

            demoProducer.syncSendToTopic1("Hello, topic-01");
            demoProducer.asyncSendToTopic2("Hello, topic-02");
            demoProducer.sendToTopic3WithReplying("Hello, topic-03");
        };
    }
}
