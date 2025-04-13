package org.example.scheduler.model;

public record ScheduleRequestDto(
        int delaySeconds,
        String payload // The actual payload intended for topic-b-again
) {}