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
    // Input topic
    private static final String INPUT_TOPIC = "topic-c";
    // Output topic
    private static final String OUTPUT_TOPIC = "topic-b-again";
    // Name for the persistent state store
    private static final String STATE_STORE_NAME = "scheduled-jobs-store";
    // How often the punctuator checks for due messages - Changed to 1 second
    private static final Duration PUNCTUATION_INTERVAL = Duration.ofSeconds(1);

    public static void main(String[] args) {
        SpringApplication.run(KafkaStreamsSchedulerApplication.class, args);
    }

    @Bean
    public KStream<String, String> kStream(StreamsBuilder builder) {
        logger.info("Building Kafka Streams topology...");

        builder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(STATE_STORE_NAME),
                Serdes.String(),
                Serdes.String()
        ));
        logger.info("State store '{}' added.", STATE_STORE_NAME);

        KStream<String, String> inputStream = builder.stream(INPUT_TOPIC);
        logger.info("Consuming from topic '{}'.", INPUT_TOPIC);

        KStream<String, String> scheduledStream = inputStream.transform(
                () -> new SchedulingTransformer(STATE_STORE_NAME, PUNCTUATION_INTERVAL),
                STATE_STORE_NAME
        );
        logger.info("Transformer configured.");

        scheduledStream.to(OUTPUT_TOPIC);
        logger.info("Producing scheduled messages to topic '{}'.", OUTPUT_TOPIC);

        logger.info("Kafka Streams topology built successfully.");
        return scheduledStream;
    }

    static class SchedulingTransformer implements Transformer<String, String, KeyValue<String, String>> {

        private final String stateStoreName;
        private final Duration punctuationInterval;
        private final ObjectMapper objectMapper = new ObjectMapper();

        private ProcessorContext context;
        private KeyValueStore<String, String> stateStore;

        public SchedulingTransformer(String stateStoreName, Duration punctuationInterval) {
            this.stateStoreName = stateStoreName;
            this.punctuationInterval = punctuationInterval;
        }

        @Override
        public void init(ProcessorContext context) {
            this.context = context;
            this.stateStore = context.getStateStore(stateStoreName);
            logger.info("Initializing SchedulingTransformer for task {}", context.taskId());
            // Schedule punctuator based on wall clock time
            context.schedule(punctuationInterval, PunctuationType.WALL_CLOCK_TIME, this::punctuate);
        }

        @Override
        public KeyValue<String, String> transform(String key, String value) {
            logger.debug("Received message - Key: {}, Value: {}", key, value);
            if (value == null) {
                logger.warn("Received null value for key {}, skipping.", key);
                return null;
            }

            try {
                // 1. Parse for 'delaySeconds'
                JsonNode rootNode = objectMapper.readTree(value);
                // *** CHANGED: Look for delaySeconds ***
                if (!rootNode.has("delaySeconds") || !rootNode.has("payload")) {
                    logger.error("Invalid message format received for key {}. Missing 'delaySeconds' or 'payload'. Value: {}", key, value);
                    return null;
                }

                // *** CHANGED: Get delaySeconds ***
                long delaySeconds = rootNode.get("delaySeconds").asLong();
                String originalPayload = rootNode.get("payload").toString();

                if (delaySeconds < 0) { // Allow 0 second delay for immediate processing via punctuator
                    logger.warn("Received negative delay ({}) for key {}, treating as 0.", delaySeconds, key);
                    delaySeconds = 0;
                }

                // 2. Calculate scheduled time using seconds
                // *** CHANGED: Use Duration.ofSeconds ***
                long scheduledTimeMillis = Instant.now().plus(Duration.ofSeconds(delaySeconds)).toEpochMilli();

                // 3. Create unique key
                String schedulingKey = (key != null ? key : "null") + "-" + UUID.randomUUID(); // Handle null key safely

                // 4. Construct store value
                String storeValue = scheduledTimeMillis + "|" + originalPayload;

                // 5. Store
                stateStore.put(schedulingKey, storeValue);
                // *** CHANGED: Log delay in seconds ***
                logger.info("Scheduled job - Scheduling Key: {}, Original Key: {}, Scheduled Time: {}, Delay: {} secs",
                        schedulingKey, key, Instant.ofEpochMilli(scheduledTimeMillis), delaySeconds);

            } catch (JsonProcessingException e) {
                logger.error("Failed to parse JSON message for key {}: {}", key, value, e);
            } catch (Exception e) {
                logger.error("Unexpected error processing message for key {}: {}", key, value, e);
            }

            return null; // Don't forward immediately
        }

        private void punctuate(long currentTimestamp) {
            logger.trace("Punctuator running at timestamp {}", currentTimestamp); // Use TRACE level for frequent logs
            try (KeyValueIterator<String, String> iterator = stateStore.all()) {
                while (iterator.hasNext()) {
                    KeyValue<String, String> entry = iterator.next();
                    String schedulingKey = entry.key;
                    String storedValue = entry.value;

                    // Safely split, checking for null/empty storedValue
                    if (storedValue == null || storedValue.isEmpty()) {
                        logger.warn("Skipping entry with null or empty value for key {}", schedulingKey);
                        continue;
                    }
                    String[] parts = storedValue.split("\\|", 2);

                    if (parts.length != 2) {
                        logger.error("Invalid format in state store for key {}. Value: '{}'. Skipping entry.", schedulingKey, storedValue);
                        continue;
                    }

                    try {
                        long scheduledTimeMillis = Long.parseLong(parts[0]);
                        String originalPayload = parts[1];

                        // Check if due (current time is at or after scheduled time)
                        if (currentTimestamp >= scheduledTimeMillis) {
                            logger.info("Message due - Scheduling Key: {}. Forwarding to {}", schedulingKey, OUTPUT_TOPIC);

                            // Forward the original payload. Decide on the key for the output topic.
                            // Using the schedulingKey might leak internal state. Often, the relevant key
                            // is inside the payload itself, or you might use the original message key if stored.
                            // For simplicity here, we forward with the schedulingKey, but consider if this is right.
                            context.forward(schedulingKey, originalPayload);

                            // Remove *after* successful forward (within the transaction)
                            stateStore.delete(schedulingKey);
                        }
                        // else: Not due yet

                    } catch (NumberFormatException e) {
                        logger.error("Invalid scheduled time format in state store for key {}. Value: '{}'. Skipping entry.", schedulingKey, storedValue, e);
                        // Optionally delete malformed entry: stateStore.delete(schedulingKey);
                    } catch (Exception e) {
                        // Catch specific exceptions if possible, e.g., KafkaException
                        logger.error("Error processing scheduled message for key {}: {}", schedulingKey, storedValue, e);
                        // Decide on retry/error handling. Don't delete if transient.
                    }
                }
            } catch (Exception e) {
                logger.error("Error during punctuation scan of state store.", e);
                // This might indicate a more significant issue.
            }
        }

        @Override
        public void close() {
            logger.info("Closing SchedulingTransformer for task {}", context != null ? context.taskId() : "N/A");
        }
    }
}