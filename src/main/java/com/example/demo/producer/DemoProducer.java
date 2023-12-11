package com.example.demo.producer;

import com.example.demo.config.DemoKafkaProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
@Service
@RequiredArgsConstructor
public class DemoProducer {

    private final KafkaTemplate<String, String> stringKafkaTemplate;

    private final ReplyingKafkaTemplate<String, String, String> replyingKafkaTemplate;

    private final DemoKafkaProperties demoKafkaProperties;

    public void syncSendToTopic1(String value) {
        syncSendKafka(demoKafkaProperties.getTopicName("sync"), Long.toString(System.currentTimeMillis()), value);
    }

    public void asyncSendToTopic2(String value) {
        asyncSendKafka(demoKafkaProperties.getTopicName("async"), Long.toString(System.currentTimeMillis()), value);
    }

    public void sendToTopic3WithReplying(String value) {
        sendToTopicReplying(demoKafkaProperties.getTopicName("request"), Long.toString(System.currentTimeMillis()), value);
    }

    private void syncSendKafka(String topic, String key, String value) {
        try {
            var sendResult = stringKafkaTemplate
                    .send(new ProducerRecord<>(topic, key, value))
                    .get(10, TimeUnit.SECONDS);
            log.info("Kafka sync send to {} successfully with offset {}", topic, sendResult.getRecordMetadata().offset());
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            log.error("Kafka sync send failed", e);
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void asyncSendKafka(String topic, String key, String value) {
        var sendResultFuture = stringKafkaTemplate.send(new ProducerRecord<>(topic, key, value));
        sendResultFuture.whenComplete((result, e) -> {
            if (Objects.isNull(e)) {
                log.info("Kafka async send to {} successfully with offset {}", topic, result.getRecordMetadata().offset());
            } else {
                log.error("Kafka async send failed", e);
            }
        });
    }

    private void sendToTopicReplying(String topic, String key, String value) {
        var producerRecord = new ProducerRecord<>(topic, key, value);
        var requestReplyFuture = replyingKafkaTemplate.sendAndReceive(producerRecord);
        try {
            SendResult<String, String> sendResult = requestReplyFuture.getSendFuture().get(6, TimeUnit.SECONDS);
            log.info("Kafka send to topic {} successfully with offset {}",
                    sendResult.getProducerRecord().topic(), sendResult.getRecordMetadata().offset());

            ConsumerRecord<String, String> consumerRecord = requestReplyFuture.get(10, TimeUnit.SECONDS);
            log.info("Kafka reply from topic {} successfully with value {}", topic, consumerRecord.value());
        } catch (ExecutionException | InterruptedException | TimeoutException e) {
            log.error("Kafka send replying failed", e);
        }
    }
}
