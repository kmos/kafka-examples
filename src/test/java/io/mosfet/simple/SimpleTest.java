package io.mosfet.simple;

import io.mosfet.kafka.examples.simple.consumer.Consumer;
import io.mosfet.kafka.examples.simple.consumer.ConsumerService;
import io.mosfet.kafka.examples.simple.consumer.SimpleTextConsumer;
import io.mosfet.kafka.examples.simple.producer.SimpleTextProducer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListener;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.awaitility.Awaitility.await;

@Testcontainers
class SimpleTest {

    public static final String TOPIC = "simple.text";

    private KafkaTemplate<String, String> kafkaTemplate;

    @Container
    private KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.1.1"));

    private TestableConsumerService consumerService;


    @BeforeEach
    void setUp() throws ExecutionException, InterruptedException {
        kafkaContainer.start();

        Map<String, Object> config = new HashMap<>();

        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        kafkaTemplate = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(config));

        createTopic(kafkaContainer.getBootstrapServers(), TOPIC, 1);

        consumerService = new TestableConsumerService();

        initializeVanillaConsumer(TOPIC, "simple.mygroup", kafkaContainer.getBootstrapServers(), new SimpleTextConsumer(consumerService));
    }

    static class TestableConsumerService implements ConsumerService {
        private boolean called;

        @Override
        public void call(String text) {
            called = true;
        }

        public boolean isCalled() {
            return called;
        }
    }


    public static void createTopic(String bootstrapServers, String topic, int partitions) throws InterruptedException, java.util.concurrent.ExecutionException {
        Map<String, Object> conf = new HashMap<>();
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

        new ConcurrentMessageListenerContainer<>(new DefaultKafkaConsumerFactory<>(
                Map.of(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers, GROUP_ID_CONFIG, consumerGroup),
                new StringDeserializer(),
                new StringDeserializer()),
                containerProperties).start();
    }

    @AfterEach
    void tearDown() {
        kafkaContainer.stop();
    }

    @Test
    @DisplayName("I want to send a text message in a kafka queue and read it")
    void givenAMessageReadIt() throws InterruptedException {
        Thread.sleep(5000);

        SimpleTextProducer simpleTextProducer = new SimpleTextProducer(kafkaTemplate);
        simpleTextProducer.sendMessage("my message");

        await().atMost(5, TimeUnit.SECONDS).until(() -> consumerService.isCalled());
    }

}