package com.milobjects.springkafkatransactions;

import com.milobjects.springkafkatransactions.entity.MessageRepository;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.with;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@EmbeddedKafka(
        partitions = 1,
        topics = {"messages", "topic-a", "topic-b"},
        brokerProperties = {
                "transaction.state.log.replication.factor=1",
                "transaction.state.log.min.isr=1"
        })
class SpringKafkaTransactionsApplicationTests {

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    @Autowired
    private MessageRepository messageRepository;

    @Test
    void processMessageSuccessfully() {
        Consumer<String, String> consumer = createConsumer("test-group-1");

        sendMessageToTopic("Message 1", "messages");
        assertMessageCountInDb("Message 1", 1);

        ConsumerRecords<String, String> consumerRecords = KafkaTestUtils.getRecords(consumer);
        assertTrue(verifyMessageIsInTopic("Notification A: Message 1", "topic-a", consumerRecords));
        assertTrue(verifyMessageIsInTopic("Notification B: Message 1", "topic-b", consumerRecords));
    }

    @Test
    void processMessageWithException() {
        Consumer<String, String> consumer = createConsumer("test-group-2");

        sendMessageToTopic("error", "messages");
        assertMessageCountInDb("error", 0);

        ConsumerRecords<String, String> consumerRecords = KafkaTestUtils.getRecords(consumer);
        assertFalse(verifyMessageIsInTopic("Notification A: error", "topic-a", consumerRecords));
        assertFalse(verifyMessageIsInTopic("Notification B: error", "topic-b", consumerRecords));
    }

    private Consumer<String, String> createConsumer(String group) {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(group, "false", embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(
                consumerProps);
        Consumer<String, String> consumer = cf.createConsumer();
        embeddedKafkaBroker.consumeFromAllEmbeddedTopics(consumer);

        return consumer;
    }

    private void sendMessageToTopic(String message, String topic) {
        kafkaTemplate.executeInTransaction(t -> t.send(topic, message));
    }

    private void assertMessageCountInDb(String message, int count) {
        with().pollDelay(5, TimeUnit.SECONDS).await().until(
                () -> messageRepository.findByMessage(message).size() == count);
    }

    private boolean verifyMessageIsInTopic(String message, String topic, ConsumerRecords<String, String> consumerRecords) {
        Iterable<ConsumerRecord<String, String>> topicRecords = consumerRecords.records(topic);

        for (ConsumerRecord<String, String> topicRecord : topicRecords) {
            if (topicRecord.value().equals(message)) {
                return true;
            }
        }

        return false;
    }

}
