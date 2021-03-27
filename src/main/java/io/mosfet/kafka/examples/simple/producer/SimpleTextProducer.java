package io.mosfet.kafka.examples.simple.producer;

import org.springframework.kafka.core.KafkaTemplate;

public class SimpleTextProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private static final String TOPIC = "simple.text";

    public SimpleTextProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(String message) {
        System.out.println("**try to send a message**");
        kafkaTemplate.send(TOPIC, message);
    }
}
