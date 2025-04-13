package org.example.scheduler.processor;


import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.example.scheduler.model.ScheduleRequestDto;
import org.example.scheduler.model.ScheduledJobValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

// Note: This class is no longer static and inner, assumes it's in its own file
// Transformer<InputKey, InputValue, OutputKeyValue>
public class SchedulingTransformer implements Transformer<String, ScheduleRequestDto, KeyValue<String, String>> {

    private static final Logger logger = LoggerFactory.getLogger(SchedulingTransformer.class);

    private final String stateStoreName;
    private final String outputTopic; // Pass output topic name

    private ProcessorContext context;
    // State Store: Key=schedulingKey (String), Value=ScheduledJobValue (Object)
    private KeyValueStore<String, ScheduledJobValue> stateStore;

    public SchedulingTransformer(String stateStoreName, String outputTopic) {
        this.stateStoreName = stateStoreName;
        this.outputTopic = outputTopic; // Store output topic
    }

    @Override
    @SuppressWarnings("unchecked") // Suppress warning for cast from getStateStore
    public void init(ProcessorContext context) {
        this.context = context;
        // Get the state store, assuming it's configured with <String, ScheduledJobValue> SerDes
        this.stateStore = context.getStateStore(stateStoreName);
        if (this.stateStore == null) {
            throw new IllegalStateException("State store [" + stateStoreName + "] not found.");
        }
        logger.info("Initializing SchedulingTransformer for task {} with state store {}", context.taskId(), stateStoreName);

        // Schedule punctuator - use a sensible interval (e.g., 1 second)
        context.schedule(Duration.ofSeconds(1), PunctuationType.WALL_CLOCK_TIME, this::punctuate);
    }

    @Override
    public KeyValue<String, String> transform(String key, ScheduleRequestDto value) {
        // Input value is now automatically deserialized into ScheduleRequestDto
        logger.debug("Received message - Key: {}, Value: {}", key, value);
        if (value == null) {
            logger.warn("Received null value for key {}, skipping.", key);
            return null;
        }

        try {
            long delaySeconds = value.delaySeconds();
            String originalPayload = value.payload(); // Payload as passed in

            if (!StringUtils.hasText(originalPayload)) {
                logger.warn("Received empty or null payload for key {}, skipping.", key);
                return null;
            }

            if (delaySeconds < 0) {
                logger.warn("Received negative delay ({}) for key {}, treating as 0.", delaySeconds, key);
                delaySeconds = 0;
            }

            long scheduledTimeMillis = Instant.now().plus(Duration.ofSeconds(delaySeconds)).toEpochMilli();

            // Use a UUID for the scheduling key to ensure uniqueness, store original key in value
            String schedulingKey = UUID.randomUUID().toString();
            ScheduledJobValue jobValue = new ScheduledJobValue(scheduledTimeMillis, key, originalPayload);

            // Store the structured value object
            stateStore.put(schedulingKey, jobValue);
            logger.info("Scheduled job - Scheduling Key: {}, Original Key: {}, Scheduled Time: {}, Delay: {} secs",
                    schedulingKey, key, Instant.ofEpochMilli(scheduledTimeMillis), delaySeconds);

        } catch (Exception e) {
            // Catch specific exceptions if possible
            logger.error("Unexpected error processing message for key {}: {}", key, value, e);
            // Consider error handling strategy (skip, DLQ, etc.)
        }

        return null; // Don't forward immediately
    }

    private void punctuate(long currentTimestamp) {
        logger.trace("Punctuator running at timestamp {}", currentTimestamp);
        try (KeyValueIterator<String, ScheduledJobValue> iterator = stateStore.all()) {
            while (iterator.hasNext()) {
                KeyValue<String, ScheduledJobValue> entry = iterator.next();
                String schedulingKey = entry.key;
                ScheduledJobValue jobValue = entry.value;

                if (jobValue == null) {
                    logger.warn("Found null job value in state store for key {}, skipping.", schedulingKey);
                    continue; // Skip potentially corrupted entries
                }

                try {
                    long scheduledTimeMillis = jobValue.scheduledTimeMillis();

                    if (currentTimestamp >= scheduledTimeMillis) {
                        String originalKey = jobValue.originalKey();
                        String originalPayload = jobValue.originalPayload();

                        logger.info("Message due - Scheduling Key: {}, Original Key: {}. Forwarding to {}",
                                schedulingKey, originalKey, outputTopic);

                        // Forward the original payload using the original key
                        context.forward(originalKey, originalPayload);

                        // Remove *after* successful forward attempt
                        stateStore.delete(schedulingKey);
                    }
                } catch (Exception e) {
                    logger.error("Error processing scheduled message for scheduling key {}: {}", schedulingKey, jobValue, e);
                    // Decide if the entry should be deleted or retried later based on the error
                }
            }
        } catch (Exception e) {
            logger.error("Error during punctuation scan of state store.", e);
        }
    }

    @Override
    public void close() {
        logger.info("Closing SchedulingTransformer for task {}", context != null ? context.taskId() : "N/A");
        // State store is managed by Kafka Streams
    }
}