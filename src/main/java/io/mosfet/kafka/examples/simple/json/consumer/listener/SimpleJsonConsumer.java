package io.mosfet.kafka.examples.simple.json.consumer.listener;

import io.mosfet.kafka.examples.simple.json.consumer.service.ConsumerJsonService;
import io.mosfet.kafka.examples.simple.json.message.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;

public class SimpleJsonConsumer implements JsonConsumer {

    public static final String TOPIC = "simple.text";
    public final ConsumerJsonService consumerService;
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleJsonConsumer.class);

    public SimpleJsonConsumer(ConsumerJsonService consumerService) {
        this.consumerService = consumerService;
    }

    @Override
    @KafkaListener(topics = TOPIC, groupId = "simple.textGroup")
    public void onMessage(@Payload Order message, @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
        LOGGER.info("received message from topic: {}", TOPIC);
        consumerService.call(message, partition);
    }
}
