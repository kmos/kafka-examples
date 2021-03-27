package io.mosfet.kafka.examples.simple.consumer;

public class SimpleConsumerService implements ConsumerService {
    @Override
    public void call(String text) {
        System.out.println(text);
    }
}
