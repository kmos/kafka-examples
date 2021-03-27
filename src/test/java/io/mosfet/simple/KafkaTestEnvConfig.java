package io.mosfet.simple;

import io.mosfet.kafka.examples.KafkaConfig;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@Testcontainers
@Configuration
public class KafkaTestEnvConfig {

    @Container
    private KafkaContainer kafkaContainer;

    @Bean
    public KafkaConfig kafkaConfig(KafkaContainer kafkaContainer) {
        return new KafkaConfig(kafkaContainer.getBootstrapServers(), "simple.mygroup");
    }

    @Bean
    public KafkaContainer kafkaContainer() throws ExecutionException, InterruptedException {
        kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.4.3"));
        kafkaContainer.start();

        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());

        AdminClient.create(configs)
                .createTopics(Collections.singletonList(TopicBuilder.name("simple.text")
                        .partitions(1)
                        .build()))
                .all()
                .get();

        return kafkaContainer;
    }
}
