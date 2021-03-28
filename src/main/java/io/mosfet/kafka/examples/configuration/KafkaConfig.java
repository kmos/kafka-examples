package io.mosfet.kafka.examples.configuration;

public class KafkaConfig {

    private final String bootstrapAddresses;
    private final String groupId;

    public KafkaConfig(String bootstrapAddresses, String groupId) {
        this.bootstrapAddresses = bootstrapAddresses;
        this.groupId = groupId;
    }

    public String getBootstrapAddresses() {
        return bootstrapAddresses;
    }

    public String getGroupId() {
        return groupId;
    }
}
