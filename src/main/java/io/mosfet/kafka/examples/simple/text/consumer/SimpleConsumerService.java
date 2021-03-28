package io.mosfet.kafka.examples.simple.text.consumer;

public class SimpleConsumerService implements ConsumerService {
    @Override
    public void call(String text) {
        System.out.println(text);
    }
}
