package org.example.scheduler.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.example.scheduler.model.ScheduleRequestDto;
import org.example.scheduler.model.ScheduledJobValue;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

@Configuration
public class JsonSerdeConfig {

    // Spring Boot provides an ObjectMapper bean by default
    @Bean
    public Serde<ScheduleRequestDto> scheduleRequestDtoSerde(ObjectMapper objectMapper) {
        // Create a JsonSerde for the input DTO
        return new JsonSerde<>(ScheduleRequestDto.class, objectMapper).noTypeInfo();
    }

    @Bean
    public Serde<ScheduledJobValue> scheduledJobValueSerde(ObjectMapper objectMapper) {
        // Create a JsonSerde for the state store value object
        return new JsonSerde<>(ScheduledJobValue.class, objectMapper).noTypeInfo();
    }

    // Optional: If you want to explicitly provide the default String Serde as a bean
    @Bean
    public Serde<String> stringSerde() {
        return Serdes.String();
    }
}