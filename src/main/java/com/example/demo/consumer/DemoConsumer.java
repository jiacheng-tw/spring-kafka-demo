package com.example.demo.consumer;

import com.example.demo.repository.DemoRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class DemoConsumer {

    private final DemoRepository<String, String> demoRepository;

    @KafkaListener(id = "${demo.kafka.topics[1].consumer-id}", topics = {"${demo.kafka.topics[1].name}"})
    public void listen(@Payload String value,
                       @Header(KafkaHeaders.RECEIVED_KEY) String key,
                       @Header(KafkaHeaders.OFFSET) String offset) {
        log.info("Kafka receive new message: [key: {}, value: {}, offset: {}]", key, value, offset);
        demoRepository.save(key, value);
    }

    @KafkaListener(id = "${demo.kafka.topics[2].consumer-id}", topics = {"${demo.kafka.topics[2].name}"})
    @SendTo
    public String listenAndReply(ConsumerRecord<String, String> consumerRecord) {
        log.info("Kafka receive new message and reply: [key: {}, value: {}, offset: {}]",
                consumerRecord.key(), consumerRecord.value(), consumerRecord.offset());
        demoRepository.save(consumerRecord.key(), consumerRecord.value());
        return "Reply message of " + consumerRecord.value();
    }
}
