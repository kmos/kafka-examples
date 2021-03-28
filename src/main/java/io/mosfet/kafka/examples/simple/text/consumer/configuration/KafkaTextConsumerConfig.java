package io.mosfet.kafka.examples.simple.text.consumer.configuration;

import io.mosfet.kafka.examples.configuration.KafkaConfig;
import io.mosfet.kafka.examples.simple.text.consumer.listener.Consumer;
import io.mosfet.kafka.examples.simple.text.consumer.listener.SimpleTextConsumer;
import io.mosfet.kafka.examples.simple.text.consumer.service.ConsumerService;
import io.mosfet.kafka.examples.simple.text.consumer.service.SimpleConsumerService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.HashMap;
import java.util.Map;

@EnableKafka
@Configuration
public class KafkaTextConsumerConfig {

    @Bean
    public ConsumerFactory<String, String> consumerTextFactory(KafkaConfig kafkaConfig) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getBootstrapAddresses());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaConfig.getGroupId());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(ConsumerFactory<String, String> consumerTextFactory) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerTextFactory);

        return factory;
    }

    @Bean
    ConsumerService simpleConsumerService() {
        return new SimpleConsumerService();
    }

    @Bean
    Consumer consumer(ConsumerService consumerService) {
        return new SimpleTextConsumer(consumerService);
    }
}
