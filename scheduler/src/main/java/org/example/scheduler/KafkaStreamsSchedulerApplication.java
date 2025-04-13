package org.example.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;

/**
 * Main Spring Boot application class for the Kafka Streams Scheduler.
 * Enables Kafka Streams and relies on component scanning to find topology and config beans.
 */
@SpringBootApplication
@EnableKafkaStreams
public class KafkaStreamsSchedulerApplication {

    private static final Logger logger = LoggerFactory.getLogger(KafkaStreamsSchedulerApplication.class);

    public static void main(String[] args) {
        logger.info("Starting Kafka Streams Scheduler Application...");
        SpringApplication.run(KafkaStreamsSchedulerApplication.class, args);
        logger.info("Kafka Streams Scheduler Application Finished Startup.");
    }
}