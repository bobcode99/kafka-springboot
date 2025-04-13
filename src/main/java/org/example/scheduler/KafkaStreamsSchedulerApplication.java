package org.example.scheduler;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafkaStreams;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

@SpringBootApplication
@EnableKafkaStreams
public class KafkaStreamsSchedulerApplication {

    private static final Logger logger = LoggerFactory.getLogger(KafkaStreamsSchedulerApplication.class);
    // Input topic (from Service B)
    private static final String INPUT_TOPIC = "topic-c";
    // Output topic (to trigger Service B again)
    private static final String OUTPUT_TOPIC = "topic-b-again";
    // Name for the persistent state store
    private static final String STATE_STORE_NAME = "scheduled-jobs-store";
    // How often the punctuator checks for due messages
    private static final Duration PUNCTUATION_INTERVAL = Duration.ofSeconds(1); // Check every 30 seconds

    public static void main(String[] args) {
        SpringApplication.run(KafkaStreamsSchedulerApplication.class, args);
    }

    /**
     * Configure Kafka Streams properties.
     */

    /**
     * Define the Kafka Streams topology using StreamsBuilder.
     */
    @Bean
    public KStream<String, String> kStream(StreamsBuilder builder) {

        // 1. Define the Persistent State Store
        // Stores mapping: schedulingKey -> scheduledTimeMillis|originalMessageValue
        // The schedulingKey ensures uniqueness for items waiting in the store.
        builder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(STATE_STORE_NAME), // Use persistent store for fault tolerance
                Serdes.String(), // Key: Unique scheduling key (e.g., originalKey + timestamp)
                Serdes.String()  // Value: "scheduledTimeMillis|originalMessageValue"
        ));

        // 2. Consume from the Input Topic (topic-c)
        KStream<String, String> inputStream = builder.stream(INPUT_TOPIC);

        // 3. Process Messages: Schedule them using a Transformer
        // The transformer interacts with the state store and schedules the punctuator.
        // It outputs messages *only* when they are due via the punctuator.
        KStream<String, String> scheduledStream = inputStream.transform(
                // Supplier for the Transformer instance
                () -> new SchedulingTransformer(STATE_STORE_NAME, PUNCTUATION_INTERVAL),
                // Name of the state store used by this transformer
                STATE_STORE_NAME
        );

        // 4. Produce Due Messages to the Output Topic (topic-b-again)
        scheduledStream.to(OUTPUT_TOPIC);

        // The KStream returned here represents the final output stream (due messages)
        // It's primarily used by Spring Kafka to know the topology is built.
        return scheduledStream;
    }

    /**
     * Custom Transformer implementation for scheduling messages.
     * - Receives messages from topic-c.
     * - Parses delay and payload from the message.
     * - Calculates the scheduled time.
     * - Stores scheduling info (scheduled time + original payload) in the state store.
     * - Uses a Punctuator to periodically check the state store for due messages.
     * - Forwards due messages downstream (to topic-b-again).
     */
    static class SchedulingTransformer implements Transformer<String, String, KeyValue<String, String>> {

        private final String stateStoreName;
        private final Duration punctuationInterval;
        private final ObjectMapper objectMapper = new ObjectMapper(); // For parsing JSON

        private ProcessorContext context;
        private KeyValueStore<String, String> stateStore;

        public SchedulingTransformer(String stateStoreName, Duration punctuationInterval) {
            this.stateStoreName = stateStoreName;
            this.punctuationInterval = punctuationInterval;
        }

        @Override
        public void init(ProcessorContext context) {
            this.context = context;
            this.stateStore = context.getStateStore(stateStoreName); // Cast is safe here

            logger.info("Initializing SchedulingTransformer for task {}", context.taskId());

            // Schedule the Punctuator to run periodically based on WALL_CLOCK_TIME
            context.schedule(punctuationInterval, PunctuationType.WALL_CLOCK_TIME, this::punctuate);
        }

        /**
         * Processes each incoming message from topic-c.
         * Calculates schedule time and stores it. Does NOT forward immediately.
         */
        @Override
        public KeyValue<String, String> transform(String key, String value) {
            logger.debug("Received message - Key: {}, Value: {}", key, value);
            if (value == null) {
                logger.warn("Received null value for key {}, skipping.", key);
                return null; // Skip null messages
            }

            try {
                // 1. Parse the incoming JSON message value
                JsonNode rootNode = objectMapper.readTree(value);
                if (!rootNode.has("delayMinutes") || !rootNode.has("payload")) {
                    logger.error("Invalid message format received for key {}. Missing 'delayMinutes' or 'payload'. Value: {}", key, value);
                    return null; // Skip invalid messages
                }

                long delayMinutes = rootNode.get("delayMinutes").asLong();
                // We keep the payload as a string (original message for topic-b-again)
                String originalPayload = rootNode.get("payload").toString(); // Keep it as JSON string or extract raw string if needed

                if (delayMinutes <= 0) {
                    logger.warn("Received non-positive delay ({}) for key {}, scheduling for immediate processing.", delayMinutes, key);
                    delayMinutes = 0; // Schedule immediately (or handle as error if preferred)
                }

                // 2. Calculate the exact time this message should be processed
                long scheduledTimeMillis = Instant.now().plus(Duration.ofMinutes(delayMinutes)).toEpochMilli();

                // 3. Create a unique key for the state store entry
                // Using original key + timestamp helps if the same key needs rescheduling
                // Or use a UUID if the original key isn't suitable for uniqueness in the *scheduling* context.
                String schedulingKey = key + "-" + UUID.randomUUID(); // Ensure uniqueness within the store

                // 4. Construct the value to store: "scheduledTime|originalPayload"
                String storeValue = scheduledTimeMillis + "|" + originalPayload;

                // 5. Store in the state store
                stateStore.put(schedulingKey, storeValue);
                logger.info("Scheduled job - Scheduling Key: {}, Original Key: {}, Scheduled Time: {}, Delay: {} mins",
                        schedulingKey, key, Instant.ofEpochMilli(scheduledTimeMillis), delayMinutes);

            } catch (JsonProcessingException e) {
                logger.error("Failed to parse JSON message for key {}: {}", key, value, e);
                // Decide how to handle: skip, send to DLQ, etc. Skipping here.
            } catch (Exception e) {
                logger.error("Unexpected error processing message for key {}: {}", key, value, e);
                // Skipping message due to unexpected error.
            }

            // Do NOT forward the message here. The punctuator will handle forwarding.
            return null;
        }

        /**
         * This method is called periodically by the schedule defined in init().
         * It iterates through the state store and forwards any messages whose scheduled time has passed.
         */
        private void punctuate(long currentTimestamp) {
            //logger.trace("Punctuator running at timestamp {}", currentTimestamp); // Can be verbose
            try (KeyValueIterator<String, String> iterator = stateStore.all()) {
                while (iterator.hasNext()) {
                    KeyValue<String, String> entry = iterator.next();
                    String schedulingKey = entry.key;
                    String storedValue = entry.value;

                    String[] parts = storedValue.split("\\|", 2);
                    if (parts.length != 2) {
                        logger.error("Invalid format in state store for key {}. Value: '{}'. Skipping entry.", schedulingKey, storedValue);
                        // Consider removing the invalid entry to prevent repeated errors
                        // stateStore.delete(schedulingKey);
                        continue;
                    }

                    try {
                        long scheduledTimeMillis = Long.parseLong(parts[0]);
                        String originalPayload = parts[1];

                        // Check if the message is due
                        if (currentTimestamp >= scheduledTimeMillis) {
                            logger.info("Message due - Scheduling Key: {}. Forwarding to {}", schedulingKey, OUTPUT_TOPIC);

                            // Forward the *original payload* with its *original key* (if available/appropriate)
                            // Decide what key to use for topic-b-again. Often the *original* key matters.
                            // We don't have the original key directly here, only the schedulingKey.
                            // IF the original key is needed downstream and not part of the payload,
                            // you might need to store it differently, e.g., include it in the state store value:
                            // Store: "scheduledTime|originalKey|originalPayload"
                            // Then parse and forward `context.forward(originalKey, originalPayload)`

                            // Assuming original key is not strictly needed or is part of payload:
                            // Forwarding with the schedulingKey might be okay, or use null/new key.
                            // Let's assume the relevant ID (like UUID) is *inside* the originalPayload.
                            context.forward(schedulingKey, originalPayload); // Forwarding payload, key might need adjustment based on ServiceB needs

                            // Remove the message from the state store *after* forwarding
                            stateStore.delete(schedulingKey);
                        }
                        // else: Message is not due yet, leave it in the store.

                    } catch (NumberFormatException e) {
                        logger.error("Invalid scheduled time format in state store for key {}. Value: '{}'. Skipping entry.", schedulingKey, storedValue, e);
                        // stateStore.delete(schedulingKey); // Consider removing malformed entry
                    } catch (Exception e) {
                        logger.error("Error processing scheduled message for key {}: {}", schedulingKey, storedValue, e);
                        // Don't delete, maybe retry later? Or implement specific error handling.
                    }
                }
            } catch (Exception e) {
                // Catch potential exceptions during state store iteration
                logger.error("Error during punctuation scan of state store.", e);
                // This might indicate a bigger issue with the state store or task.
                // Kafka Streams error handling mechanisms (e.g., production exception handler) might take over.
            }
        }


        @Override
        public void close() {
            logger.info("Closing SchedulingTransformer for task {}", context != null ? context.taskId() : "N/A");
            // Close resources if any were opened (ObjectMapper doesn't usually need explicit closing here)
            // State store is managed by Kafka Streams framework.
        }
    }
}