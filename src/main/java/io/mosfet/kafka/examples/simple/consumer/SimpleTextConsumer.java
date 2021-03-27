package io.mosfet.kafka.examples.simple.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;

public class SimpleTextConsumer implements Consumer {

    public final ConsumerService consumerService;

    public SimpleTextConsumer(ConsumerService consumerService) {
        this.consumerService = consumerService;
    }

    @Override
    @KafkaListener(topics = "simple.text", groupId = "simple")
    public void onMessage(@Payload String message, @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
        consumerService.call("received from partition" + partition + " message: " + message);
    }
}
