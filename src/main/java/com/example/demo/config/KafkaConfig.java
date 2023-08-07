package com.example.demo.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
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
@EnableConfigurationProperties(DemoKafkaProperties.class)
public class KafkaConfig {

    @Bean
    public NewTopics kafkaTopics(DemoKafkaProperties demoKafkaProperties) {
        return new NewTopics(
                TopicBuilder.name(demoKafkaProperties.getTopic1().name())
                        .partitions(demoKafkaProperties.getTopic1().partitions()).build(),
                TopicBuilder.name(demoKafkaProperties.getTopic2().name())
                        .partitions(demoKafkaProperties.getTopic2().partitions()).build()
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
    public KafkaMessageListenerContainer<String, String> myKafkaMessageListenerContainer(
            @Value("${spring.kafka.bootstrap-servers:localhost:9092}") String bootstrapServers,
            DemoKafkaProperties demoKafkaProperties) {
        Map<String, Object> consumerConfigs = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class
        );
        var kafkaConsumerFactory = new DefaultKafkaConsumerFactory<String, String>(consumerConfigs);

        var containerProperties = new ContainerProperties(demoKafkaProperties.getTopic1().name());
        containerProperties.setCommitLogLevel(LogIfLevelEnabled.Level.INFO);
        containerProperties.setGroupId(demoKafkaProperties.getTopic1().consumerId());
        containerProperties.setMessageListener(
                (MessageListener<String, String>) consumerRecord -> log.info("Kafka receive new message: {}", consumerRecord));

        return new KafkaMessageListenerContainer<>(kafkaConsumerFactory, containerProperties);
    }
}
