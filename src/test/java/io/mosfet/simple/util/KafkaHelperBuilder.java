package io.mosfet.simple.util;

import io.mosfet.kafka.examples.simple.text.consumer.Consumer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListener;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public final class KafkaHelperBuilder {

    private final String bootstrapAddress;
    private final String topic;
    private int partitions = 1;
    private Consumer consumer;
    private boolean template;
    private String consumerGroup = "simple.aConsumerGroup";

    public KafkaHelperBuilder(String bootstrapAddress, String topic) {
        this.bootstrapAddress = bootstrapAddress;
        this.topic = topic;
    }

    public KafkaHelperBuilder partitions(int partitions) {
        this.partitions = partitions;
        return this;
    }

    public KafkaHelperBuilder consumer(Consumer consumer) {
        this.consumer = consumer;
        return this;
    }

    public KafkaHelperBuilder consumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
        return this;
    }

    public KafkaHelperBuilder withKafkaTemplate() {
        this.template = true;
        return this;
    }

    public KafkaTestHelper build()
    {
        try {
            createTopic(bootstrapAddress, topic, partitions);
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }

        if (consumer!=null) {
            initializeVanillaConsumer(topic, consumerGroup, bootstrapAddress, consumer);
        }

        if (template) {
            return new KafkaTestHelper(bootstrapAddress, topic, partitions, new KafkaTemplate<>(
                    new DefaultKafkaProducerFactory<>(Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress,
                            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class))));
        }

        return new KafkaTestHelper(bootstrapAddress, topic, partitions);
    }

    private void createTopic(String bootstrapServers, String topic, int partitions) throws InterruptedException, ExecutionException {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        AdminClient.create(conf)
                .createTopics(Collections.singletonList(TopicBuilder.name(topic)
                        .partitions(partitions)
                        .build()))
                .all()
                .get();
    }

    private void initializeVanillaConsumer(String topic, String consumerGroup, String bootstrapServers, Consumer consumer) {
        ContainerProperties containerProperties = new ContainerProperties(topic);

        containerProperties.setMessageListener((MessageListener<String, String>) record ->
        {
            try {
                consumer.onMessage(record.value(), record.partition());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        new ConcurrentMessageListenerContainer<>(new DefaultKafkaConsumerFactory<>(
                Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers, ConsumerConfig.GROUP_ID_CONFIG, consumerGroup),
                new StringDeserializer(),
                new StringDeserializer()),
                containerProperties).start();
    }
}
