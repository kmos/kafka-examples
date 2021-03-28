package io.mosfet.kafka.examples.simple.json.consumer.message;

public interface JsonConsumer {
    void onMessage(Order message, int partition);
}
