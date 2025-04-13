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

@RestController // Marks this as a REST controller component
public class ProducerController {

    private static final Logger logger = LoggerFactory.getLogger(ProducerController.class);
    private static final String TARGET_TOPIC = "topic-c"; // Topic the Scheduler consumes

    // Spring injects these beans because they are available in the application context
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    // Define a simple DTO (Data Transfer Object) for the request body
    // Can be kept here or moved to its own file (e.g., ScheduleRequest.java) if preferred

    @GetMapping("/ping")
    public String pong() {
        logger.info("pong");

        return "pong";
    }

    @PostMapping("/schedule")
    // Spring Boot/Jackson automatically deserialize the JSON body into the ScheduleRequest record
    public String sendMessage(@RequestBody ScheduleRequest request) {
        try {
            String key = UUID.randomUUID().toString();

            ObjectNode messageNode = objectMapper.createObjectNode();
            // Use accessor methods for records
            messageNode.put("delaySeconds", request.delaySeconds());
            messageNode.put("payload", request.payload());

            String messageValue = objectMapper.writeValueAsString(messageNode);

            logger.info("Sending message to topic [{}]: Key='{}', Value='{}'", TARGET_TOPIC, key, messageValue);
            kafkaTemplate.send(TARGET_TOPIC, key, messageValue);

            return "Message sent successfully to " + TARGET_TOPIC + " with key " + key;

        } catch (Exception e) {
            logger.error("Error sending message to Kafka", e);
            return "Error sending message: " + e.getMessage();
        }
    }
}

