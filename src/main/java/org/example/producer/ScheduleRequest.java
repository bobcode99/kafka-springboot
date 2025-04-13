package org.example.producer;

public record ScheduleRequest(
        int delaySeconds,
        String payload
) {
    // No explicit constructor, accessors (delaySeconds(), payload()),
    // equals(), hashCode(), and toString() are automatically generated.
}