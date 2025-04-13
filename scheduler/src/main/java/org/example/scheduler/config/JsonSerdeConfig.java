package org.example.scheduler.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.example.scheduler.model.ScheduleRequestDto;
import org.example.scheduler.model.ScheduledJobValue;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

/**
 * Configures JSON Serializer/Deserializer (SerDes) beans for custom DTOs/Records
 * used in the Kafka Streams topology.
 */
@Configuration
public class JsonSerdeConfig {
    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper(); // 👈 add this to resolve the error
    }
    @Bean
    public Serde<ScheduleRequestDto> scheduleRequestDtoSerde(ObjectMapper objectMapper) {
        return new JsonSerde<>(ScheduleRequestDto.class, objectMapper).noTypeInfo();
    }

    @Bean
    public Serde<ScheduledJobValue> scheduledJobValueSerde(ObjectMapper objectMapper) {
        return new JsonSerde<>(ScheduledJobValue.class, objectMapper).noTypeInfo();
    }

    @Bean
    public Serde<String> stringSerde() {
        return Serdes.String();
    }
}