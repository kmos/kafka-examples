package io.mosfet.kafka.examples.simple.text.consumer.listener;

public interface TextConsumer {
    void onMessage(String message, int partition);
}
