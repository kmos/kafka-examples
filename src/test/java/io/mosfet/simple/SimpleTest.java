package io.mosfet.simple;

import io.mosfet.kafka.examples.simple.consumer.ConsumerService;
import io.mosfet.kafka.examples.simple.consumer.SimpleTextConsumer;
import io.mosfet.kafka.examples.simple.producer.SimpleTextProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

@Testcontainers
class SimpleTest {

    public static final String TOPIC = "simple.text";

    private KafkaTemplate<String, String> kafkaTemplate;

    @Container
    private final KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.1.1"));

    private TestableConsumerService consumerService;


    @BeforeEach
    void setUp() throws ExecutionException, InterruptedException {
        kafkaContainer.start();

        kafkaTemplate = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(
                Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)));

        KafkaVanillaUtil.createTopic(kafkaContainer.getBootstrapServers(), TOPIC, 1);

        consumerService = new TestableConsumerService();

        KafkaVanillaUtil.initializeVanillaConsumer(TOPIC, "simple.mygroup", kafkaContainer.getBootstrapServers(), new SimpleTextConsumer(consumerService));
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
}
