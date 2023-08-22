package com.example.demo.consumer;

import com.example.demo.repository.DemoRepository;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.util.Map;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
class DemoConsumerTest {

    private static Producer<String, String> kafkaProducer;

    @Autowired
    private DemoRepository<String, String> demoRepository;

    @BeforeAll
    static void beforeAll() {
        Map<String, Object> producerConfigs = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        DefaultKafkaProducerFactory<String, String> producerFactory = new DefaultKafkaProducerFactory<>(producerConfigs);
        kafkaProducer = producerFactory.createProducer();
    }

    @AfterAll
    static void afterAll() {
        kafkaProducer.close();
    }

    @Test
    void shouldConsumeRecordFromTopic02() throws InterruptedException {
        Future<RecordMetadata> sendResult = kafkaProducer.send(new ProducerRecord<>("topic-02", "test-key", "my test value"));
        Thread.sleep(1000L);

        assertThat(sendResult).isDone();
        assertThat(demoRepository.get("test-key")).isEqualTo("my test value");
    }
}
