package com.example.demo.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaAdmin.NewTopics;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.LogIfLevelEnabled;

import java.util.Map;

@Slf4j
@EnableKafka
@Configuration
public class KafkaConfig {

    @Bean
    public NewTopics kafkaTopics() {
        return new NewTopics(
                TopicBuilder.name("topic-01").build(),
                TopicBuilder.name("topic-02").partitions(2).build()
        );
    }

    @Bean
    public KafkaTemplate<String, String> stringKafkaTemplate(ProducerFactory<String, String> producerFactory) {
        Map<String, Object> configOverrides = Map.of(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
        );
        return new KafkaTemplate<>(producerFactory, configOverrides);
    }

    @Bean
    public KafkaMessageListenerContainer<String, String> myKafkaMessageListenerContainer() {
        Map<String, Object> consumerConfigs = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class
        );
        var kafkaConsumerFactory = new DefaultKafkaConsumerFactory<String, String>(consumerConfigs);

        var containerProperties = new ContainerProperties("topic-01");
        containerProperties.setCommitLogLevel(LogIfLevelEnabled.Level.INFO);
        containerProperties.setGroupId("demo-consumer-01");
        containerProperties.setMessageListener(
                (MessageListener<String, String>) consumerRecord -> log.info("Kafka receive new message: {}", consumerRecord));

        return new KafkaMessageListenerContainer<>(kafkaConsumerFactory, containerProperties);
    }
}
