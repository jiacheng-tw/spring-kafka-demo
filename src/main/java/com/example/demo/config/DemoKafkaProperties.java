package com.example.demo.config;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@RequiredArgsConstructor
@ConfigurationProperties("demo.kafka")
public class DemoKafkaProperties {

    private final TopicProperties topic1;

    private final TopicProperties topic2;

    public record TopicProperties(String name, Integer partitions, String consumerId) {}
}
