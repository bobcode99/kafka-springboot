package org.example.scheduler.topology;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.state.Stores;
import org.example.scheduler.model.ScheduleRequestDto;
import org.example.scheduler.model.ScheduledJobValue;
import org.example.scheduler.processor.SchedulingTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component // Make this a Spring component so it can be scanned
public class SchedulerTopology {

    // Define constants centrally or inject from properties
    public static final String INPUT_TOPIC = "topic-c";
    public static final String OUTPUT_TOPIC = "topic-b-again";
    public static final String STATE_STORE_NAME = "scheduled-jobs-store";
    private static final Logger logger = LoggerFactory.getLogger(SchedulerTopology.class);
    // Autowire the Serdes we defined in JsonSerdeConfig
    @Autowired
    private Serde<String> stringSerde;

    @Autowired
    private Serde<ScheduleRequestDto> scheduleRequestDtoSerde;

    @Autowired
    private Serde<ScheduledJobValue> scheduledJobValueSerde;

    @Bean
    public KStream<String, String> schedulingStreamTopology(StreamsBuilder builder) {
        logger.info("Building Kafka Streams scheduling topology...");

        // 1. Define the State Store with specific Serdes
        builder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(STATE_STORE_NAME),
                stringSerde,            // Key Serde for store
                scheduledJobValueSerde  // Value Serde for store (our custom object)
        ));
        logger.info("State store '{}' added with String/ScheduledJobValue Serdes.", STATE_STORE_NAME);

        // 2. Consume from Input Topic using specific Serdes
        KStream<String, ScheduleRequestDto> inputStream = builder.stream(
                INPUT_TOPIC,
                Consumed.with(stringSerde, scheduleRequestDtoSerde) // Use specific Serdes
        );
        logger.info("Consuming from topic '{}' with String/ScheduleRequestDto Serdes.", INPUT_TOPIC);

        // 3. Transform using our updated transformer
        // The transformer output is KeyValue<OriginalKey, OriginalPayload> (both String)
        // It doesn't directly output a KStream value, forwarding happens via context.forward
        inputStream.transform(
                () -> new SchedulingTransformer(STATE_STORE_NAME, OUTPUT_TOPIC),
                STATE_STORE_NAME // Name of the state store used by the transformer
        ).to(OUTPUT_TOPIC);
        logger.info("SchedulingTransformer configured.");


        // Note: Since the transformer uses context.forward(), the resulting KStream
        // from transform() is effectively empty for the main topology path.
        // We still need the `to()` sink defined for the messages forwarded by the processor.
        // However, we directly forward to OUTPUT_TOPIC inside the transformer.
        // A common pattern is to just return null or an empty stream here if all output
        // happens via context.forward. Let's keep the `to()` for clarity, although it
        // might be redundant if the forwarded messages target the same topic.
        // It's crucial that the types match what context.forward produces.

        // We define the sink topic type, assuming context.forward sends <String, String>
        // builder.stream(...).to(OUTPUT_TOPIC, Produced.with(stringSerde, stringSerde));
        // Since `transform` uses context.forward and doesn't return KeyValue pairs to this stream,
        // directly applying `.to()` here might not be correct or needed. The forwarding handles the sink.

        logger.info("Messages will be forwarded to topic '{}' by the transformer.", OUTPUT_TOPIC);
        logger.info("Kafka Streams topology built successfully.");

        // Return the input stream just to satisfy the @Bean KStream return type requirement,
        // although the main processing happens via the Transformer + Punctuator.
        // Alternatively, change the bean method to return void or Topology.
        return inputStream.mapValues(v -> (v != null ? v.payload() : null)); // Example: Map back to string payload
    }
}