package io.mosfet.simple;

import io.mosfet.kafka.examples.KafkaConfig;
import io.mosfet.kafka.examples.simple.consumer.Consumer;
import io.mosfet.kafka.examples.simple.consumer.ConsumerService;
import io.mosfet.kafka.examples.simple.producer.SimpleTextProducer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.jmock.Mockery;
import org.jmock.junit5.JUnit5Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.containers.KafkaContainer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.awaitility.Awaitility.await;

@Import(KafkaTestEnvConfig.class)
@SpringBootTest
@ActiveProfiles("test")
class SimpleTest {

    public static final String TOPIC = "simple.text";

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private KafkaContainer kafkaContainer;

    private ConsumerService consumerService;


    @RegisterExtension
    Mockery context  = new JUnit5Mockery() {
        {
            setImposteriser(ClassImposteriser.INSTANCE);
        }
    };

/*    @BeforeEach
    void setUp() throws ExecutionException, InterruptedException {
        consumerService = context.mock(ConsumerService.class);

        kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.4.3"));
        kafkaContainer.start();

        Map<String, Object> config = new HashMap<>();

        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        kafkaTemplate = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(config));

        //createTopic(kafkaContainer.getBootstrapServers(), TOPIC, 1);

        //initializeVanillaConsumer(TOPIC, "simple.mygroup", kafkaContainer.getBootstrapServers(), new SimpleTextConsumer(consumerService));
    }*/

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
    void testMe() {
        SimpleTextProducer simpleTextProducer = new SimpleTextProducer(kafkaTemplate);
        simpleTextProducer.sendMessage("my message");

        await().atMost(30, TimeUnit.SECONDS);
    }

}