package io.mosfet.kafka.examples.simple.json.consumer.listener;

import io.mosfet.kafka.examples.simple.json.message.Order;

public interface JsonConsumer {
    void onMessage(Order message, int partition);
}
