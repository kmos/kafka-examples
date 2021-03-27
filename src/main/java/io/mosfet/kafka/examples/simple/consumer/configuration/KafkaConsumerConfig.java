package io.mosfet.kafka.examples.simple.consumer.configuration;

import io.mosfet.kafka.examples.KafkaConfig;
import io.mosfet.kafka.examples.simple.consumer.Consumer;
import io.mosfet.kafka.examples.simple.consumer.ConsumerService;
import io.mosfet.kafka.examples.simple.consumer.SimpleConsumerService;
import io.mosfet.kafka.examples.simple.consumer.SimpleTextConsumer;
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
public class KafkaConsumerConfig {

    @Bean
    public ConsumerFactory<String, String> consumerFactory(KafkaConfig kafkaConfig) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getBootstrapAddresses());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaConfig.getGroupId());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(ConsumerFactory<String, String> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);

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
