package io.mosfet.kafka.examples.simple.json.producer.pub;

import io.mosfet.kafka.examples.simple.json.message.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;

public class SimpleJsonProducer implements JsonProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private static final String TOPIC = "simple.json";
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleJsonProducer.class);

    public SimpleJsonProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void send(Order message) {
        kafkaTemplate.send(TOPIC, message);
        LOGGER.info("sent message over kafka on topic: {}", TOPIC);
    }
}
