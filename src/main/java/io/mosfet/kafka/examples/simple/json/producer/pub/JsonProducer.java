package io.mosfet.kafka.examples.simple.json.producer.pub;

import io.mosfet.kafka.examples.simple.json.message.Order;

public interface JsonProducer {
    void send(Order message);
}
