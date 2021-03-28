package io.mosfet.simple.configuration;

import io.mosfet.kafka.examples.configuration.KafkaConfig;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@Testcontainers
@Configuration
public class KafkaTestEnvConfig {

    @Bean
    public KafkaConfig kafkaConfig(KafkaContainer kafkaContainer) {
        return new KafkaConfig(kafkaContainer.getBootstrapServers(), "simple.textGroup");
    }

    @Bean
    public KafkaContainer kafkaContainer() throws ExecutionException, InterruptedException {
        KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.4.3"));
        kafkaContainer.start();

        createTopic("simple.text", kafkaContainer.getBootstrapServers());

        return kafkaContainer;
    }

    private void createTopic(String topic, String bootstrapServers) throws InterruptedException, ExecutionException {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        AdminClient.create(configs)
                .createTopics(Collections.singletonList(TopicBuilder.name(topic)
                        .partitions(1)
                        .build()))
                .all()
                .get();
    }
}
