package org.example.scheduler.topology;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.example.scheduler.model.ScheduleRequestDto;
import org.example.scheduler.model.ScheduledJobValue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JsonSerde; // Ensure correct JsonSerde import

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class SchedulerTopologyTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, ScheduleRequestDto> inputTopic;
    private TestOutputTopic<String, String> outputTopic; // Output is <OriginalKey, OriginalPayload> (both String)
    private KeyValueStore<String, ScheduledJobValue> stateStore;

    // Serdes needed for the test driver
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Serde<String> stringSerde = Serdes.String();
    // Use the same JsonSerde configuration as the main application for consistency
    private final Serde<ScheduleRequestDto> requestSerde = new JsonSerde<>(ScheduleRequestDto.class, objectMapper).noTypeInfo();
    private final Serde<ScheduledJobValue> jobValueSerde = new JsonSerde<>(ScheduledJobValue.class, objectMapper).noTypeInfo();
    private final Serde<String> outputValueSerde = Serdes.String(); // Output payload is String

    // Keep Topology and Properties accessible for reuse in restart test
    private Topology topology;
    private Properties props;
    private Instant testStartTime; // Store the initial start time

    @BeforeEach
    void setUp() {
        // 1. Define Topology (same logic as SchedulerTopology bean)
        StreamsBuilder builder = new StreamsBuilder();
        builder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(SchedulerTopology.STATE_STORE_NAME),
                stringSerde,
                jobValueSerde // Use the correct value serde for the store
        ));
        builder.stream(SchedulerTopology.INPUT_TOPIC, Consumed.with(stringSerde, requestSerde))
                .transform(() -> new org.example.scheduler.processor.SchedulingTransformer(
                                SchedulerTopology.STATE_STORE_NAME,
                                SchedulerTopology.OUTPUT_TOPIC,
                                Duration.ofSeconds(1)), // Match punctuation interval
                        SchedulerTopology.STATE_STORE_NAME);
        topology = builder.build(); // Store topology
        System.out.println("Test Topology:\n" + topology.describe());

        // 2. Setup Kafka Streams properties
        props = new Properties(); // Store props
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-scheduler-topology"); // Use distinct test app ID
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        // *** IMPORTANT: Configure default Serdes for the test driver ***
        // These match the Serdes used when defining the state store and consuming/producing topics implicitly
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        // Although we specify value Serdes explicitly in the topology, setting a default helps.
        // Let's set default value serde to String as the final output is String.
        // For intermediate steps using JSON, we use specific Serdes from beans or explicit .with()
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        // If state dir needs explicit setting for test isolation/cleanup:
        // props.put(StreamsConfig.STATE_DIR_CONFIG, Files.createTempDirectory("kafka-streams-test").toString());

        // 3. Create TopologyTestDriver
        testStartTime = Instant.now();
        testDriver = new TopologyTestDriver(topology, props, testStartTime);

        // 4. Setup test topics and state store access
        inputTopic = testDriver.createInputTopic(SchedulerTopology.INPUT_TOPIC, stringSerde.serializer(), requestSerde.serializer());
        outputTopic = testDriver.createOutputTopic(SchedulerTopology.OUTPUT_TOPIC, stringSerde.deserializer(), outputValueSerde.deserializer());
        stateStore = testDriver.getKeyValueStore(SchedulerTopology.STATE_STORE_NAME);
        assertThat(stateStore).isNotNull();
    }

    @AfterEach
    void tearDown() {
        if (testDriver != null) {
            try {
                testDriver.close(); // Cleans up state by default
            } catch (Exception e) {
                System.err.println("Error closing TopologyTestDriver: " + e.getMessage());
            }
        }
        // Close Serdes if they hold resources (JsonSerde typically doesn't)
        requestSerde.close();
        jobValueSerde.close();
    }

    // --- Test methods remain the same as previous answer ---
    // shouldScheduleAndForwardMessageAfterDelay()
    // shouldForwardOverdueMessageImmediatelyAfterRestart()

    @Test
    void shouldScheduleAndForwardMessageAfterDelay() {
        String key = "testKey1";
        String payload = "{\"data\":\"payload for normal delay test\"}";
        int delaySeconds = 5;
        ScheduleRequestDto request = new ScheduleRequestDto(delaySeconds, payload);

        Instant expectedSendTimeLowerBound = testStartTime.plusSeconds(delaySeconds);

        inputTopic.pipeInput(key, request, testStartTime);
        // Give time for the record to be processed and stored
        testDriver.advanceWallClockTime(Duration.ofMillis(100));
        assertThat(stateStore.approximateNumEntries()).isEqualTo(1);

        Duration advance1 = Duration.ofSeconds(delaySeconds - 1);
        testDriver.advanceWallClockTime(advance1); // Advance to T+(delay-1)
        assertThat(outputTopic.isEmpty()).isTrue();
        assertThat(stateStore.approximateNumEntries()).isEqualTo(1);

        Duration advance2 = Duration.ofSeconds(2); // Go past due time + punctuation interval
        testDriver.advanceWallClockTime(advance2);

        assertThat(outputTopic.isEmpty()).isFalse();
        KeyValue<String, String> outputRecord = outputTopic.readKeyValue();
        assertThat(outputRecord.key).isEqualTo(key);
        assertThat(outputRecord.value).isEqualTo(payload);
        assertThat(outputTopic.isEmpty()).isTrue();
        assertThat(stateStore.approximateNumEntries()).isEqualTo(0);

        Instant finalEffectiveTime = testStartTime.plus(Duration.ofMillis(100)).plus(advance1).plus(advance2);
        System.out.println("Test 1 - Expected Send Time approx: " + expectedSendTimeLowerBound + ", Final Effective Driver Time: " + finalEffectiveTime);
    }

    @Test
    void shouldForwardOverdueMessageImmediatelyAfterRestart() {
        String key = "testKeyRestart";
        String payload = "{\"data\":\"payload for restart test\"}";
        int delaySeconds = 10;
        ScheduleRequestDto request = new ScheduleRequestDto(delaySeconds, payload);

        Instant dueTime = testStartTime.plusSeconds(delaySeconds);

        inputTopic.pipeInput(key, request, testStartTime);
        // Give time for the record to be processed and stored
        testDriver.advanceWallClockTime(Duration.ofMillis(100));
        assertThat(stateStore.approximateNumEntries()).isEqualTo(1);

        Duration advanceBeforeClose = Duration.ofSeconds(delaySeconds - 2); // Advance to T+8s
        testDriver.advanceWallClockTime(advanceBeforeClose);
        assertThat(outputTopic.isEmpty()).isTrue();

        // --- Simulate Restart ---
        System.out.println("Test 2 - Closing driver at effective time: " + testStartTime.plus(Duration.ofMillis(100)).plus(advanceBeforeClose));
        testDriver.close();

        Instant restartTime = dueTime.plusSeconds(5); // Restart at T+15s (well after due time T+10s)
        System.out.println("Test 2 - Restarting driver at wall clock time: " + restartTime);
        testDriver = new TopologyTestDriver(topology, props, restartTime); // REUSE topology & props

        // Re-initialize topics and state store access
        outputTopic = testDriver.createOutputTopic(SchedulerTopology.OUTPUT_TOPIC, stringSerde.deserializer(), outputValueSerde.deserializer());
        stateStore = testDriver.getKeyValueStore(SchedulerTopology.STATE_STORE_NAME);
        assertThat(stateStore).isNotNull();
        // Check state store *after* advancing time slightly to allow recovery/punctuation
        // assertThat(stateStore.approximateNumEntries()).isEqualTo(1); // May not be accurate immediately

        // --- Verification after Restart ---
        Duration advanceAfterRestart = Duration.ofSeconds(1); // Advance to trigger punctuation
        testDriver.advanceWallClockTime(advanceAfterRestart); // Advance to T+16s effective time

        assertThat(outputTopic.isEmpty()).isFalse(); // Should have produced immediately
        KeyValue<String, String> outputRecord = outputTopic.readKeyValue();
        assertThat(outputRecord.key).isEqualTo(key);
        assertThat(outputRecord.value).isEqualTo(payload);
        assertThat(outputTopic.isEmpty()).isTrue();
        assertThat(stateStore.approximateNumEntries()).isEqualTo(0); // Should be empty now

        System.out.println("Test 2 - Message due at approx: " + dueTime + ", Sent after restart at effective time: " + restartTime.plus(advanceAfterRestart));
    }
}