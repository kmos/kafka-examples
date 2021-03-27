package io.mosfet.kafka.examples.simple.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;

public interface Consumer {
    @KafkaListener(topics = "simple.text", groupId = "simple")
    void onMessage(@Payload String message, @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition);
}
