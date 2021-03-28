package io.mosfet.kafka.examples.simple.text.consumer.listener;

public interface Consumer {
    void onMessage(String message, int partition);
}
