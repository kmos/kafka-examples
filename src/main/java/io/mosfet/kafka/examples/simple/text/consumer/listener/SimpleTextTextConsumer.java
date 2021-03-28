package io.mosfet.kafka.examples.simple.text.consumer.listener;

import io.mosfet.kafka.examples.simple.text.consumer.service.ConsumerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;

public class SimpleTextTextConsumer implements TextConsumer {

    public static final String TOPIC = "simple.text";
    public final ConsumerService consumerService;
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleTextTextConsumer.class);

    public SimpleTextTextConsumer(ConsumerService consumerService) {
        this.consumerService = consumerService;
    }

    @Override
    @KafkaListener(topics = TOPIC, groupId = "simple.textGroup")
    public void onMessage(@Payload String message, @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
        LOGGER.info("received message from topic: {}", TOPIC);
        consumerService.call("received from partition " + partition + " message: " + message);
    }
}
