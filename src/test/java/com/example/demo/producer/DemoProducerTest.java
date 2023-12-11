package com.example.demo.producer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
class DemoProducerTest {

    private static KafkaMessageListenerContainer<String, String> kafkaContainer;

    private static BlockingQueue<ConsumerRecord<String, String>> consumerRecords;

    @Autowired
    private DemoProducer demoProducer;

    @BeforeAll
    static void beforeAll() {
        Map<String, Object> consumerConfigs = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ConsumerConfig.GROUP_ID_CONFIG, "demo-test-consumer",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        DefaultKafkaConsumerFactory<String, String> consumerFactory = new DefaultKafkaConsumerFactory<>(consumerConfigs);
        ContainerProperties containerProperties = new ContainerProperties("topic-for-sync", "topic-for-async", "topic-for-request", "topic-for-reply");
        kafkaContainer = new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);

        consumerRecords = new LinkedBlockingDeque<>(3);
        kafkaContainer.setupMessageListener((MessageListener<String, String>) consumerRecords::add);

        kafkaContainer.start();
    }

    @AfterAll
    static void afterAll() {
        kafkaContainer.stop();
    }

    @Test
    void shouldSyncSendToTopic() throws InterruptedException {
        consumerRecords.clear();
        demoProducer.syncSendToTopic1("my test message to topic for sync");
        Thread.sleep(1000L);

        ConsumerRecord<String, String> record = consumerRecords.poll();

        assertThat(record).isNotNull();
        assertThat(record.topic()).isEqualTo("topic-for-sync");
        assertThat(record.key()).isAlphanumeric();
        assertThat(record.value()).isEqualTo("my test message to topic for sync");
    }

    @Test
    void shouldAsyncSendToTopic() throws InterruptedException {
        consumerRecords.clear();
        demoProducer.asyncSendToTopic2("my test message to topic for async");
        Thread.sleep(1000L);

        ConsumerRecord<String, String> record = consumerRecords.poll();

        assertThat(record).isNotNull();
        assertThat(record.topic()).isEqualTo("topic-for-async");
        assertThat(record.key()).isAlphanumeric();
        assertThat(record.value()).isEqualTo("my test message to topic for async");
    }

    @Test
    void shouldSendToTopicAndReply() throws InterruptedException {
        consumerRecords.clear();
        demoProducer.sendToTopic3WithReplying("my test message to topic for request");
        Thread.sleep(1000L);

        ConsumerRecord<String, String> requestRecord = consumerRecords.poll();
        assertThat(requestRecord).isNotNull();
        assertThat(requestRecord.topic()).isEqualTo("topic-for-request");
        assertThat(requestRecord.key()).isAlphanumeric();
        assertThat(requestRecord.value()).isEqualTo("my test message to topic for request");

        ConsumerRecord<String, String> replyRecord = consumerRecords.poll();
        assertThat(replyRecord).isNotNull();
        assertThat(replyRecord.topic()).isEqualTo("topic-for-reply");
        assertThat(replyRecord.value()).isEqualTo("Reply message of my test message to topic for request");
    }
}
