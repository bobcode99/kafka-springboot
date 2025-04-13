package org.example.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
// ComponentScan will find SchedulerTopology and JsonSerdeConfig
@EnableKafkaStreams
public class KafkaStreamsSchedulerApplication {

    private static final Logger logger = LoggerFactory.getLogger(KafkaStreamsSchedulerApplication.class);

    public static void main(String[] args) {
        logger.info("Starting Kafka Streams Scheduler Application...");
        SpringApplication.run(KafkaStreamsSchedulerApplication.class, args);
        logger.info("Kafka Streams Scheduler Application Finished Startup.");

    }
    // All beans (Topology, Serdes) are defined in other @Configuration/@Component classes
}