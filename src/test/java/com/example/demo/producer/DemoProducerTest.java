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
        ContainerProperties containerProperties = new ContainerProperties("topic-01", "topic-02");
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
    void shouldSyncSendToTopic1() throws InterruptedException {
        consumerRecords.clear();
        demoProducer.syncSendToTopic1("my test message to topic 01");
        Thread.sleep(1000L);

        ConsumerRecord<String, String> record = consumerRecords.poll();

        assertThat(record).isNotNull();
        assertThat(record.topic()).isEqualTo("topic-01");
        assertThat(record.key()).isAlphanumeric();
        assertThat(record.value()).isEqualTo("my test message to topic 01");
    }

    @Test
    void shouldAsyncSendToTopic2() throws InterruptedException {
        consumerRecords.clear();
        demoProducer.asyncSendToTopic2("my test message to topic 02");
        Thread.sleep(1000L);

        ConsumerRecord<String, String> record = consumerRecords.poll();

        assertThat(record).isNotNull();
        assertThat(record.topic()).isEqualTo("topic-02");
        assertThat(record.key()).isAlphanumeric();
        assertThat(record.value()).isEqualTo("my test message to topic 02");
    }
}
