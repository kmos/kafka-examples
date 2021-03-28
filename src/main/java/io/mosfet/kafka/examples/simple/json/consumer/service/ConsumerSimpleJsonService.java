package io.mosfet.kafka.examples.simple.json.consumer.service;

import io.mosfet.kafka.examples.simple.json.consumer.message.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerSimpleJsonService implements ConsumerJsonService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerSimpleJsonService.class);

    @Override
    public void call(Order order, int partition) {
        LOGGER.info("received message for order with id {} and address {} and product {}",
                order.getId(),
                order.getAddress(),
                order.getProduct());
    }
}
