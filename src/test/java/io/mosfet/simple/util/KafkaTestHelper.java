package io.mosfet.simple.util;

import io.mosfet.kafka.examples.simple.text.consumer.listener.TextConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListener;

import java.util.Map;
import java.util.Optional;

public class KafkaTestHelper {

    private final String bootstrapAddress;
    private final String topic;
    private final int partitions;
    private final Optional<KafkaTemplate> kafkaTemplate;

    public KafkaTestHelper(String bootstrapAddress, String topic, int partitions) {
        this.bootstrapAddress = bootstrapAddress;
        this.topic = topic;
        this.partitions = partitions;
        this.kafkaTemplate = Optional.empty();
    }

    public KafkaTestHelper(String bootstrapAddress, String topic, int partitions, KafkaTemplate<String, String> kafkaTemplate) {
        this.bootstrapAddress = bootstrapAddress;
        this.topic = topic;
        this.partitions = partitions;
        this.kafkaTemplate = Optional.of(kafkaTemplate);
    }

    public ConcurrentMessageListenerContainer<String, String> createConsumer(MessageListener<String, String> listener, String consumerGroup, Deserializer deserializer) {
        ContainerProperties containerProperties = new ContainerProperties(topic);
        containerProperties.setMessageListener(listener);

        ConcurrentMessageListenerContainer<String, String> container = new ConcurrentMessageListenerContainer<>(new DefaultKafkaConsumerFactory<>(
                Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress, ConsumerConfig.GROUP_ID_CONFIG, consumerGroup),
                new StringDeserializer(),
                deserializer),
                containerProperties);

        container.start();

        return container;
    }

    public ConcurrentMessageListenerContainer<String, String> createConsumer(TextConsumer textConsumer, String consumerGroup, StringDeserializer deserializer) {
        ContainerProperties containerProperties = new ContainerProperties(topic);
        containerProperties.setMessageListener((MessageListener<String, String>) record -> {
            try {
                textConsumer.onMessage(record.value(), record.partition());
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        ConcurrentMessageListenerContainer<String, String> container = new ConcurrentMessageListenerContainer<>(new DefaultKafkaConsumerFactory<>(
                Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress, ConsumerConfig.GROUP_ID_CONFIG, consumerGroup),
                new StringDeserializer(),
                deserializer),
                containerProperties);

        container.start();
        return container;
    }

    public Optional<KafkaTemplate> getKafkaTemplate() {
        return kafkaTemplate;
    }

    public int getPartitions() {
        return partitions;
    }
}
