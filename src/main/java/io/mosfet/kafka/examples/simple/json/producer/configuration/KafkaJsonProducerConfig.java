package io.mosfet.kafka.examples.simple.json.producer.configuration;

import com.fasterxml.jackson.databind.JsonSerializer;
import io.mosfet.kafka.examples.configuration.KafkaConfig;
import io.mosfet.kafka.examples.simple.json.message.Order;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaJsonProducerConfig {

    @Bean
    public ProducerFactory<String, Order> producerJsonFactory(KafkaConfig kafkaConfig) {

        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getBootstrapAddresses());
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public KafkaTemplate<String, Order> kafkaJsonTemplate(ProducerFactory<String, Order> producerJsonFactory) {
        return new KafkaTemplate<>(producerJsonFactory);
    }
}
