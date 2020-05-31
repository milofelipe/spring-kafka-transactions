package com.milobjects.springkafkatransactions;

import com.milobjects.springkafkatransactions.entity.Message;
import com.milobjects.springkafkatransactions.entity.MessageRepository;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@KafkaListener(topics = "messages")
public class MessageHandler {

    private final MessageRepository messageRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public MessageHandler(MessageRepository messageRepository, KafkaTemplate<String, String> kafkaTemplate) {
        this.messageRepository = messageRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @SneakyThrows
    @KafkaHandler
    public void process(String message) {
        log.info("Process: {}", message);
        kafkaTemplate.send("topic-a", String.format("Key-A-%s", message), String.format("Notification A: %s", message));
        messageRepository.save(new Message(message));
        kafkaTemplate.send("topic-b", String.format("Key-B-¬%s", message), String.format("Notification B: %s", message));

        if ("error".equals(message)) {
            throw new MessageConversionException("Explicit exception thrown");
        }
    }

}
