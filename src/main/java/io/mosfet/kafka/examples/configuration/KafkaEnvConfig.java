package io.mosfet.kafka.examples.configuration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Configuration
@Profile("!test")
public class KafkaEnvConfig {

    @Bean
    public KafkaConfig kafkaConfig(
            @Value(value = "${kafka.bootstrapAddress}") String bootstrapAddress,
            @Value(value = "${kafka.groupid}") String groupId
    ) {
        return new KafkaConfig(bootstrapAddress, groupId);
    }
}
