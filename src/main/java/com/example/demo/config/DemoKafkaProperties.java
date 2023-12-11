package com.example.demo.config;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;
import java.util.function.Function;

@Getter
@RequiredArgsConstructor
@ConfigurationProperties("demo.kafka")
public class DemoKafkaProperties {

    private final List<TopicProperties> topics;

    public String getTopicName(String key) {
        return getTopicProperty(key, TopicProperties::name);
    }

    public String getTopicConsumerId(String key) {
        return getTopicProperty(key, TopicProperties::consumerId);
    }

    public List<String> getTopicKeys() {
        return topics.stream().map(TopicProperties::key).toList();
    }

    private String getTopicProperty(String key, Function<TopicProperties, String> getter) {
        return topics.stream().filter(topic -> topic.key.equals(key)).findFirst().map(getter).orElse(null);
    }

    public record TopicProperties(String key, String name, Integer partitions, String consumerId) {}
}
