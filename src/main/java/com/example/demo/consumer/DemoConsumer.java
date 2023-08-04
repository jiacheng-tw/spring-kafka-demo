package com.example.demo.consumer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class DemoConsumer {

    @KafkaListener(id = "demo-consumer-02", topics = {"topic-02"})
    public void listen(@Payload String value,
                       @Header(KafkaHeaders.RECEIVED_KEY) String key,
                       @Header(KafkaHeaders.OFFSET) String offset) {
        log.info("Kafka receive new message: {key: {}, value: {}, offset: {}}", key, value, offset);
    }
}
