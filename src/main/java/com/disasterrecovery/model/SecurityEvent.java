package com.disasterrecovery.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.Objects;

/**
 * Core security event schema used throughout the system.
 */
public final class SecurityEvent {

    private final String cid;
    private final Instant eventTimestamp;
    private final long eventTime;
    private final String eventType;
    private final String eventId;

    @JsonCreator
    public SecurityEvent(
            @JsonProperty("cid") String cid,
            @JsonProperty("event_timestamp") Instant eventTimestamp,
            @JsonProperty("event_time") long eventTime,
            @JsonProperty("event_type") String eventType,
            @JsonProperty("event_id") String eventId) {
        this.cid = Objects.requireNonNull(cid, "cid");
        this.eventTimestamp = Objects.requireNonNull(eventTimestamp, "eventTimestamp");
        this.eventTime = eventTime;
        this.eventType = Objects.requireNonNull(eventType, "eventType");
        this.eventId = Objects.requireNonNull(eventId, "eventId");
    }

    @JsonProperty("cid")
    public String getCid() {
        return cid;
    }

    @JsonProperty("event_timestamp")
    public Instant getEventTimestamp() {
        return eventTimestamp;
    }

    @JsonProperty("event_time")
    public long getEventTime() {
        return eventTime;
    }

    @JsonProperty("event_type")
    public String getEventType() {
        return eventType;
    }

    @JsonProperty("event_id")
    public String getEventId() {
        return eventId;
    }
}
