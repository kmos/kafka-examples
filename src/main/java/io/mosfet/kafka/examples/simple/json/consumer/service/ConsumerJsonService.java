package io.mosfet.kafka.examples.simple.json.consumer.service;

import io.mosfet.kafka.examples.simple.json.message.Order;

public interface ConsumerJsonService {
    void call(Order order, int partition);
}
