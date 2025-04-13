package org.example.scheduler.topology;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.example.scheduler.model.ScheduleRequestDto;
import org.example.scheduler.model.ScheduledJobValue;
import org.example.scheduler.processor.SchedulingTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Defines the Kafka Streams topology for the scheduling service.
 */
@Component
public class SchedulerTopology {

    // Constants for topic names and store name
    public static final String INPUT_TOPIC = "topic-c";
    public static final String OUTPUT_TOPIC = "topic-b-again";
    public static final String STATE_STORE_NAME = "scheduled-jobs-store";
    private static final Logger logger = LoggerFactory.getLogger(SchedulerTopology.class);
    private static final Duration PUNCTUATION_INTERVAL = Duration.ofSeconds(1);

    // Autowire the necessary SerDes from JsonSerdeConfig
    @Autowired
    private Serde<String> stringSerde;
    @Autowired
    private Serde<ScheduleRequestDto> scheduleRequestDtoSerde;
    @Autowired
    private Serde<ScheduledJobValue> scheduledJobValueSerde;

    @Bean
    public KStream<String, ScheduleRequestDto> schedulingStreamTopology(StreamsBuilder builder) {
        logger.info("Building Kafka Streams scheduling topology...");

        // 1. Define State Store with object value SerDe
        StoreBuilder<KeyValueStore<String, ScheduledJobValue>> storeBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(STATE_STORE_NAME),
                        stringSerde,           // Key Serde
                        scheduledJobValueSerde // Value Serde (for ScheduledJobValue object)
                );
        builder.addStateStore(storeBuilder);
        logger.info("State store '{}' added with Serdes: Key={}, Value={}",
                STATE_STORE_NAME, stringSerde.getClass().getName(), scheduledJobValueSerde.getClass().getName());

        // 2. Consume Input with object value SerDe
        KStream<String, ScheduleRequestDto> inputStream = builder.stream(
                INPUT_TOPIC,
                Consumed.with(stringSerde, scheduleRequestDtoSerde) // Use DTO Serde for value
        );
        logger.info("Consuming from topic '{}' with Serdes: Key={}, Value={}",
                INPUT_TOPIC, stringSerde.getClass().getName(), scheduleRequestDtoSerde.getClass().getName());

        // 3. Apply Transformer (output handled internally via context.forward)
        inputStream.transform(
                () -> new SchedulingTransformer(STATE_STORE_NAME, OUTPUT_TOPIC, PUNCTUATION_INTERVAL),
                STATE_STORE_NAME // Associate with the state store
        ).to(OUTPUT_TOPIC);

        logger.info("SchedulingTransformer configured. Output via context.forward() to topic '{}'.", OUTPUT_TOPIC);
        logger.info("Kafka Streams topology built successfully.");

        // Return the input stream to satisfy the bean contract
        return inputStream;
    }
}