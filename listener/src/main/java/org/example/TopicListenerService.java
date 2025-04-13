package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class TopicListenerService {

    private static final Logger logger = LoggerFactory.getLogger(TopicListenerService.class);
    private static final String LISTEN_TOPIC = "topic-b-again"; // Topic produced by Scheduler

    @KafkaListener(topics = LISTEN_TOPIC, groupId = "topic-b-again-listener-group-2")
    public void listen(ConsumerRecord<String, String> record) {
        logger.info("Received message on topic [{}] Partition [{}] Offset [{}]: Key='{}', Value='{}'",
                record.topic(),
                record.partition(),
                record.offset(),
                record.key(), // Note: This will be the 'schedulingKey' from the Scheduler
                record.value()); // This should be the original 'payload' string sent by the Producer
    }
}