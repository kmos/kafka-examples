package io.mosfet.kafka.examples.simple.text.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;

public class SimpleTextConsumer implements Consumer {

    public static final String TOPIC = "simple.text";
    public final ConsumerService consumerService;
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleTextConsumer.class);

    public SimpleTextConsumer(ConsumerService consumerService) {
        this.consumerService = consumerService;
    }

    @Override
    @KafkaListener(topics = TOPIC, groupId = "simple")
    public void onMessage(@Payload String message, @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
        LOGGER.info("received message from topic: {}", TOPIC);
        consumerService.call("received from partition " + partition + " message: " + message);
    }
}
