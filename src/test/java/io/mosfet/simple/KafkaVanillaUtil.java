package io.mosfet.simple;

import io.mosfet.kafka.examples.simple.consumer.Consumer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListener;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class KafkaVanillaUtil {
    public static void createTopic(String bootstrapServers, String topic, int partitions) throws InterruptedException, ExecutionException {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        AdminClient.create(conf)
                .createTopics(Collections.singletonList(TopicBuilder.name(topic)
                        .partitions(partitions)
                        .build()))
                .all()
                .get();
    }

    public static void initializeVanillaConsumer(String topic, String consumerGroup, String bootstrapServers, Consumer consumer) {
        ContainerProperties containerProperties = new ContainerProperties(topic);

        containerProperties.setMessageListener((MessageListener<String, String>) record ->
                consumer.onMessage(record.value(), record.partition()));

        new ConcurrentMessageListenerContainer<String, String>(new DefaultKafkaConsumerFactory<String, String>(
                Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers, ConsumerConfig.GROUP_ID_CONFIG, consumerGroup),
                new StringDeserializer(),
                new StringDeserializer()),
                containerProperties).start();
    }
}