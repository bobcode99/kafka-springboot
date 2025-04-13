package org.example.scheduler.processor;

import org.apache.kafka.common.errors.SerializationException;
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

/**
 * Kafka Streams Transformer responsible for scheduling messages.
 * Uses structured objects (ScheduledJobValue) in the state store.
 */
public class SchedulingTransformer implements Transformer<String, ScheduleRequestDto, KeyValue<String, String>> { // Output type signature doesn't matter much here

    private static final Logger logger = LoggerFactory.getLogger(SchedulingTransformer.class);

    private final String stateStoreName;
    private final String outputTopic;
    private final Duration punctuationInterval;

    private ProcessorContext context;
    private KeyValueStore<String, ScheduledJobValue> stateStore; // Store holds objects

    public SchedulingTransformer(String stateStoreName, String outputTopic, Duration punctuationInterval) {
        this.stateStoreName = stateStoreName;
        this.outputTopic = outputTopic;
        this.punctuationInterval = punctuationInterval;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        this.context = context;
        try {
            // Get store, expecting <String, ScheduledJobValue> due to topology config
            this.stateStore = context.getStateStore(stateStoreName);
            if (this.stateStore == null) {
                throw new IllegalStateException("State store [" + stateStoreName + "] not found.");
            }
            logger.info("Initializing SchedulingTransformer for task {} with state store {}", context.taskId(), stateStoreName);
            context.schedule(this.punctuationInterval, PunctuationType.WALL_CLOCK_TIME, this::punctuate);
            logger.info("Punctuator scheduled for task {} with interval {}", context.taskId(), this.punctuationInterval);
        } catch (Exception e) {
            logger.error("Error during SchedulingTransformer initialization for task {}:", context.taskId(), e);
            throw new RuntimeException("Failed to initialize SchedulingTransformer", e);
        }
    }

    @Override
    public KeyValue<String, String> transform(String key, ScheduleRequestDto value) {
        logger.debug("Received DTO - Key: {}, Value: {}", key, value);
        if (value == null) {
            logger.warn("Received null value DTO for key {}, skipping.", key);
            return null;
        }

        try {
            long delaySeconds = value.delaySeconds();
            String originalPayload = value.payload();

            if (!StringUtils.hasText(originalPayload)) {
                logger.warn("Received request with empty/null payload for key {}, skipping.", key);
                return null;
            }
            if (delaySeconds < 0) {
                logger.warn("Received negative delay ({}) for key {}, treating as 0.", delaySeconds, key);
                delaySeconds = 0;
            }

            long scheduledTimeMillis = Instant.now().plus(Duration.ofSeconds(delaySeconds)).toEpochMilli();
            String schedulingKey = UUID.randomUUID().toString(); // Unique key for THIS scheduled item
            // Create the structured value, storing the original key
            ScheduledJobValue jobValue = new ScheduledJobValue(scheduledTimeMillis, key, originalPayload);

            // Store the object (requires ScheduledJobValueSerde)
            stateStore.put(schedulingKey, jobValue);
            logger.info("Scheduled job - Scheduling Key: {}, Original Key: {}, Target Time: {}, Delay: {} secs",
                    schedulingKey, key, Instant.ofEpochMilli(scheduledTimeMillis), delaySeconds);

        } catch (SerializationException e) { // More specific catch
            logger.error("Serialization error processing input for key {}: {}", key, value, e);
        } catch (Exception e) {
            logger.error("Unexpected error scheduling message for key {}: {}", key, value, e);
        }
        return null; // No immediate output from transform itself
    }

    private void punctuate(long currentTimestamp) {
        logger.trace("Punctuator running at timestamp {} for task {}", currentTimestamp, context.taskId());
        int forwardedCount = 0;
        // Iterate over the store holding ScheduledJobValue objects
        try (KeyValueIterator<String, ScheduledJobValue> iterator = stateStore.all()) {
            while (iterator.hasNext()) {
                KeyValue<String, ScheduledJobValue> entry = iterator.next();
                String schedulingKey = entry.key;
                ScheduledJobValue jobValue = entry.value; // Get the object directly

                if (jobValue == null || jobValue.scheduledTimeMillis() <= 0 || jobValue.originalPayload() == null) {
                    logger.warn("Found invalid/null job value in state store for key {}, skipping. Value: {}", schedulingKey, jobValue);
                    continue;
                }

                try {
                    // Check if due using the value object's field
                    if (currentTimestamp >= jobValue.scheduledTimeMillis()) {
                        String originalKey = jobValue.originalKey();
                        String originalPayload = jobValue.originalPayload();

                        logger.info("Message due - Scheduling Key: {}, Original Key: {}. Attempting forward to {}",
                                schedulingKey, originalKey, outputTopic);

                        // Forward the original key and payload (both Strings)
                        context.forward(originalKey, originalPayload);
                        forwardedCount++;
                        logger.debug("Forward successful for Scheduling Key: {}", schedulingKey);

                        // Delete entry from state store
                        stateStore.delete(schedulingKey);
                        logger.debug("Deleted scheduling key {} from state store.", schedulingKey);
                    }
                } catch (Exception e) {
                    logger.error("Error during forward/delete for scheduling key {}: {}. JobValue: {}", schedulingKey, e.getMessage(), jobValue, e);
                }
            }
        } catch (Exception e) {
            logger.error("Error during punctuation scan of state store for task {}.", context.taskId(), e);
        }
        if (forwardedCount > 0) {
            logger.info("Punctuator forwarded {} messages for task {}.", forwardedCount, context.taskId());
        }
    }

    @Override
    public void close() {
        logger.info("Closing SchedulingTransformer for task {}", context != null ? context.taskId() : "N/A");
    }
}