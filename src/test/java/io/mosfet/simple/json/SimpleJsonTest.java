package io.mosfet.simple.json;

import io.mosfet.kafka.examples.simple.json.producer.pub.JsonProducer;
import io.mosfet.kafka.examples.simple.json.producer.pub.SimpleJsonProducer;
import io.mosfet.kafka.examples.simple.text.consumer.service.ConsumerService;
import io.mosfet.kafka.examples.simple.text.producer.pub.SimpleTextProducer;
import io.mosfet.simple.util.KafkaHelperBuilder;
import io.mosfet.simple.util.KafkaTestHelper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.*;
import org.mockito.Mockito;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static io.mosfet.simple.json.OrderUtil.order;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.springframework.kafka.test.utils.ContainerTestUtils.waitForAssignment;

@Testcontainers
class SimpleJsonTest {

    @Container
    private final KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.1.1"));

    public static final String CONSUMER_GROUP = "simple.mygroup";
    public static final String TOPIC = "simple.json";
    private static final String MESSAGE = "my message";

    private KafkaTestHelper kafkaTestHelper;

    @BeforeEach
    void setUp() {
        kafkaContainer.start();

        kafkaTestHelper = new KafkaHelperBuilder(kafkaContainer.getBootstrapServers(), TOPIC)
                .withKafkaTemplate()
                .withSerializer(JsonSerializer.class)
                .build();

    }

    @AfterEach
    void tearDown() {
        kafkaContainer.stop();
    }

    @Test
    @DisplayName("I want to send a text message in a kafka queue and be sure that is consumed")
    void givenAMessageReadIt() {
        MessageListener<String, String> listener = Mockito.mock(MessageListener.class);

        JsonDeserializer deserializer = new JsonDeserializer();
        deserializer.addTrustedPackages("io.mosfet.kafka.examples.simple.json.message");

        waitForAssignment(kafkaTestHelper.createConsumer(listener, CONSUMER_GROUP, deserializer), kafkaTestHelper.getPartitions());

        KafkaTemplate<String, Object> kafkaTemplate = kafkaTestHelper
                .getKafkaTemplate()
                .get();

        JsonProducer jsonProducer = new SimpleJsonProducer(kafkaTemplate);

        jsonProducer.send(order(1, "anAddress", "myproduct"));

        verify(listener, timeout(1000).times(1)).onMessage(any(ConsumerRecord.class));
    }

    @Test
    @DisplayName("I want to send a text message in a kafka queue and check the message")
    @Disabled
    void givenAMessageReadItCorrectly() {

        ConsumerService consumerService = Mockito.mock(ConsumerService.class);
        MessageListener<String, String> consumer = text -> consumerService.call(text.value());

        waitForAssignment(kafkaTestHelper.createConsumer(consumer, CONSUMER_GROUP, new StringDeserializer()), kafkaTestHelper.getPartitions());

        kafkaTestHelper.getKafkaTemplate()
                .map(SimpleTextProducer::new)
                .orElseThrow()
                .send(MESSAGE);

        verify(consumerService, timeout(1000).times(1)).call(MESSAGE);
    }

}
