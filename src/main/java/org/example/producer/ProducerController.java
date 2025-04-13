package org.example.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
public class ProducerController {

    private static final Logger logger = LoggerFactory.getLogger(ProducerController.class);
    private static final String TARGET_TOPIC = "topic-c"; // Topic the Scheduler consumes

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private ObjectMapper objectMapper; // Jackson ObjectMapper provided by Spring Boot

    @GetMapping("/ping")
    public String pong() {
        logger.info("pong");

        return "pong";
    }

    @PostMapping("/schedule")
    public String sendMessage(@RequestBody ScheduleRequest request) {
        logger.info("get sendMessage request");
        try {
            // Generate a unique key for the message (optional, but good practice)
            String key = UUID.randomUUID().toString();

            // Construct the JSON message expected by the Scheduler service
            // {"delayMinutes": 5, "payload": "{\"message\":\"data for service b\"}"}
            ObjectNode messageNode = objectMapper.createObjectNode();
            messageNode.put("delayMinutes", request.delayMinutes);
            // Put the payload AS A STRING (it might be JSON string itself, or plain text)
            messageNode.put("payload", request.payload);

            String messageValue = objectMapper.writeValueAsString(messageNode);

            logger.info("Sending message to topic [{}]: Key='{}', Value='{}'", TARGET_TOPIC, key, messageValue);
            kafkaTemplate.send(TARGET_TOPIC, key, messageValue);

            return "Message sent successfully to " + TARGET_TOPIC + " with key " + key;

        } catch (Exception e) {
            logger.error("Error sending message to Kafka", e);
            return "Error sending message: " + e.getMessage();
        }
    }

    // Define a simple DTO (Data Transfer Object) for the request body
    static class ScheduleRequest {
        public int delayMinutes;
        public String payload; // This will be the content for the 'payload' field
    }
}
