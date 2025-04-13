package org.example.scheduler.model;


// Represents the value stored in the state store
public record ScheduledJobValue(
        long scheduledTimeMillis, // Absolute time when the job is due
        String originalKey,       // Store the original key from topic-c
        String originalPayload    // The payload to be forwarded
) {}