package io.mosfet.simple.util;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public final class KafkaHelperBuilder {

    private final String bootstrapAddress;
    private final String topic;
    private int partitions = 1;
    private boolean template;
    private Class<?> valueSerializer = StringSerializer.class;

    public KafkaHelperBuilder(String bootstrapAddress, String topic) {
        this.bootstrapAddress = bootstrapAddress;
        this.topic = topic;
    }

    public KafkaHelperBuilder partitions(int partitions) {
        this.partitions = partitions;
        return this;
    }

    public KafkaHelperBuilder withKafkaTemplate() {
        this.template = true;
        return this;
    }

    public KafkaHelperBuilder withSerializer(Class<?> serializer) {
        this.valueSerializer = serializer;
        return this;
    }

    public KafkaTestHelper build()
    {
        try {
            createTopic(bootstrapAddress, topic, partitions);
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }

        if (template) {
            return new KafkaTestHelper(bootstrapAddress, topic, partitions, new KafkaTemplate<>(
                    new DefaultKafkaProducerFactory<>(Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress,
                            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer))));
        }

        return new KafkaTestHelper(bootstrapAddress, topic, partitions);
    }

    private void createTopic(String bootstrapServers, String topic, int partitions) throws InterruptedException, ExecutionException {
        Map<String, Object> conf = new HashMap<>();
        conf.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        AdminClient.create(conf)
                .createTopics(Collections.singletonList(TopicBuilder.name(topic)
                        .partitions(partitions)
                        .build()))
                .all()
                .get();
    }

}
