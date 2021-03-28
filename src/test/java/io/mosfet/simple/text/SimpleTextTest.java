package io.mosfet.simple.text;

import io.mosfet.kafka.examples.simple.text.consumer.ConsumerService;
import io.mosfet.kafka.examples.simple.text.producer.SimpleTextProducer;
import io.mosfet.simple.util.KafkaHelperBuilder;
import io.mosfet.simple.util.KafkaTestHelper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.kafka.listener.MessageListener;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.springframework.kafka.test.utils.ContainerTestUtils.waitForAssignment;

@Testcontainers
class SimpleTextTest {

    @Container
    private final KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.1.1"));

    public static final String CONSUMER_GROUP = "simple.mygroup";
    public static final String TOPIC = "simple.text";
    private static final String MESSAGE = "my message";

    private KafkaTestHelper kafkaTestHelper;

    @BeforeEach
    void setUp() {
        kafkaContainer.start();

        kafkaTestHelper = new KafkaHelperBuilder(kafkaContainer.getBootstrapServers(), TOPIC)
                .withKafkaTemplate()
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

        waitForAssignment(kafkaTestHelper.createConsumer(listener, CONSUMER_GROUP), kafkaTestHelper.getPartitions());

        SimpleTextProducer simpleTextProducer = new SimpleTextProducer(kafkaTestHelper.getKafkaTemplate().get());
        simpleTextProducer.send(MESSAGE);

        verify(listener, timeout(1000).times(1)).onMessage(any(ConsumerRecord.class));
    }

    @Test
    @DisplayName("I want to send a text message in a kafka queue and check the message")
    void givenAMessageReadItCorrectly() {

        ConsumerService consumerService = Mockito.mock(ConsumerService.class);
        MessageListener<String, String> consumer = text -> consumerService.call(text.value());

        waitForAssignment(kafkaTestHelper.createConsumer(consumer, CONSUMER_GROUP), kafkaTestHelper.getPartitions());

        kafkaTestHelper.getKafkaTemplate()
                .map(SimpleTextProducer::new)
                .orElseThrow()
                .send(MESSAGE);

        verify(consumerService, timeout(1000).times(1)).call(MESSAGE);
    }

}
