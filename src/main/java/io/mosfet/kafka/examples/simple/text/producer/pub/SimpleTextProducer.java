package io.mosfet.kafka.examples.simple.text.producer.pub;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;

public class SimpleTextProducer implements TextProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private static final String TOPIC = "simple.text";
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleTextProducer.class);

    public SimpleTextProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void send(String message) {
        kafkaTemplate.send(TOPIC, message);
        LOGGER.info("sent message over kafka on topic: {}", TOPIC);
    }
}
