package org.example.listener;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@SpringBootApplication
public class ListenerApplication {

    public static void main(String[] args) {
        SpringApplication.run(ListenerApplication.class, args);
    }
}

@Service
class TopicListenerService {

    private static final Logger logger = LoggerFactory.getLogger(TopicListenerService.class);
    private static final String LISTEN_TOPIC = "topic-b-again"; // Topic produced by Scheduler

    @KafkaListener(topics = LISTEN_TOPIC, groupId = "topic-b-again-listener-group")
    public void listen(ConsumerRecord<String, String> record) {
        logger.info("Received message on topic [{}] Partition [{}] Offset [{}]: Key='{}', Value='{}'",
                record.topic(),
                record.partition(),
                record.offset(),
                record.key(), // Note: This will be the 'schedulingKey' from the Scheduler
                record.value()); // This should be the original 'payload' string sent by the Producer
    }
}