package io.mosfet.simple.text;

import io.mosfet.kafka.examples.simple.text.consumer.SimpleTextConsumer;
import io.mosfet.kafka.examples.simple.text.producer.SimpleTextProducer;
import io.mosfet.simple.util.KafkaHelperBuilder;
import io.mosfet.simple.util.KafkaTestHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.springframework.kafka.test.utils.ContainerTestUtils.waitForAssignment;

@Testcontainers
class SimpleTextTest {

    @Container
    private final KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.1.1"));

    public static final String CONSUMER_GROUP = "simple.mygroup";
    public static final String TOPIC = "simple.text";

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
        AtomicBoolean isCalled = new AtomicBoolean(false);

        waitForAssignment(
                kafkaTestHelper.createConsumer(new SimpleTextConsumer(text -> isCalled.set(true)), CONSUMER_GROUP),
                kafkaTestHelper.getPartitions());

        SimpleTextProducer simpleTextProducer = new SimpleTextProducer(kafkaTestHelper.getKafkaTemplate().get());
        simpleTextProducer.send("my message");

        await().atMost(5, TimeUnit.SECONDS).until(isCalled::get);
    }

    @Test
    @DisplayName("I want to send a text message in a kafka queue and check the message")
    void givenAMessageReadItCorrectly() {
        String expectedMessage = "my message";

        AtomicBoolean isCalled = new AtomicBoolean(false);
        AtomicReference<String> actualMessage = new AtomicReference<>("");

        waitForAssignment(kafkaTestHelper.createConsumer(text -> {
            isCalled.set(true);
            actualMessage.set(text.value());
            }, CONSUMER_GROUP), kafkaTestHelper.getPartitions());

        kafkaTestHelper.getKafkaTemplate()
                .map(SimpleTextProducer::new)
                .orElseThrow()
                .send(expectedMessage);

        await().atMost(10, TimeUnit.SECONDS)
                .until(isCalled::get);

        assertEquals(expectedMessage, actualMessage.get());
    }

}
