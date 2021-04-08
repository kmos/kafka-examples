package io.mosfet.simple.json;

import io.mosfet.kafka.examples.simple.json.message.Order;

public class OrderUtil {

    public static Order order(int id, String address, String product) {
        Order message = new Order();
        message.setAddress(address);
        message.setId(id);
        message.setProduct(product);
        return message;
    }
}