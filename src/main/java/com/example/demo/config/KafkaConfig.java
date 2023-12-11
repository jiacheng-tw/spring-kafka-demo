package com.example.demo.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaAdmin.NewTopics;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.support.LogIfLevelEnabled;

import java.util.Map;

@Slf4j
@EnableKafka
@Configuration
@EnableConfigurationProperties(DemoKafkaProperties.class)
public class KafkaConfig {

    @Bean
    public NewTopics kafkaTopics(DemoKafkaProperties demoKafkaProperties) {
        NewTopic[] newTopics = demoKafkaProperties.getTopics().stream()
                .map(properties -> TopicBuilder
                        .name(properties.name())
                        .partitions(properties.partitions()).build())
                .toArray(NewTopic[]::new);
        return new NewTopics(newTopics);
    }

    @Bean
    public KafkaTemplate<String, String> stringKafkaTemplate(ProducerFactory<String, String> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    public ReplyingKafkaTemplate<String, String, String> replyingKafkaTemplate(
            ProducerFactory<String, String> producerFactory,
            ConcurrentKafkaListenerContainerFactory<String, String> containerFactory,
            KafkaTemplate<String, String> stringKafkaTemplate,
            DemoKafkaProperties demoKafkaProperties) {
        containerFactory.setReplyTemplate(stringKafkaTemplate);

        var repliesContainer = containerFactory.createContainer(demoKafkaProperties.getTopicName("reply"));
        repliesContainer.getContainerProperties().setGroupId(demoKafkaProperties.getTopicConsumerId("reply"));
        repliesContainer.setAutoStartup(false);

        return new ReplyingKafkaTemplate<>(producerFactory, repliesContainer);
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

        var containerProperties = new ContainerProperties(demoKafkaProperties.getTopicName("sync"));
        containerProperties.setCommitLogLevel(LogIfLevelEnabled.Level.INFO);
        containerProperties.setGroupId(demoKafkaProperties.getTopicConsumerId("sync"));
        containerProperties.setMessageListener(
                (MessageListener<String, String>) consumerRecord -> log.info("Kafka receive new message: {}", consumerRecord));

        return new KafkaMessageListenerContainer<>(kafkaConsumerFactory, containerProperties);
    }
}
