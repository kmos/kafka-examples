package io.mosfet.simple.text;

import io.mosfet.kafka.examples.simple.text.consumer.service.ConsumerService;
import io.mosfet.kafka.examples.simple.text.producer.SimpleTextProducer;
import io.mosfet.simple.configuration.KafkaTestEnvConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

@Testcontainers
@SpringBootTest
@Import(KafkaTestEnvConfig.class)
@ActiveProfiles("test")
class SimpleTextSpringTestIT {

    @Autowired
    private KafkaContainer kafkaContainer;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @MockBean
    ConsumerService consumerService;

    @AfterEach
    void tearDown() {
        kafkaContainer.stop();
    }

    @Test
    @Disabled
    @DisplayName("I want to send a text message in a kafka queue and be sure that is consumed")
    void givenAMessageReadIt() throws InterruptedException {
        Thread.sleep(5000);

        SimpleTextProducer simpleTextProducer = new SimpleTextProducer(kafkaTemplate);
        simpleTextProducer.send("my message");

        verify(consumerService, timeout(15000).times(1)).call(any(String.class));
    }

    @Test
    @DisplayName("I want to send a text message in a kafka queue and check the message")
    void givenAMessageReadItCorrectly() throws InterruptedException {
        Thread.sleep(5000);

        String expectedMessage = "my message";

        SimpleTextProducer simpleTextProducer = new SimpleTextProducer(kafkaTemplate);
        simpleTextProducer.send("my message");

        verify(consumerService, timeout(15000).times(1))
                .call("received from partition 0 message: " + expectedMessage);
    }

}
