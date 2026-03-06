package com.disasterrecovery.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

public final class ReplayJobMetrics {

    private final long totalEvents;
    private final long eventsSent;
    private final long eventsFailed;
    private final Instant startedAt;
    private final Instant completedAt;
    private final Instant lastUpdatedAt;

    public ReplayJobMetrics(long totalEvents,
                            long eventsSent,
                            long eventsFailed,
                            Instant startedAt,
                            Instant completedAt,
                            Instant lastUpdatedAt) {
        this.totalEvents = totalEvents;
        this.eventsSent = eventsSent;
        this.eventsFailed = eventsFailed;
        this.startedAt = startedAt;
        this.completedAt = completedAt;
        this.lastUpdatedAt = lastUpdatedAt;
    }

    @JsonProperty("total_events")
    public long getTotalEvents() { return totalEvents; }

    @JsonProperty("events_sent")
    public long getEventsSent() { return eventsSent; }

    @JsonProperty("events_failed")
    public long getEventsFailed() { return eventsFailed; }

    @JsonProperty("started_at")
    public Instant getStartedAt() { return startedAt; }

    @JsonProperty("completed_at")
    public Instant getCompletedAt() { return completedAt; }

    @JsonProperty("last_updated_at")
    public Instant getLastUpdatedAt() { return lastUpdatedAt; }

    public ReplayJobMetrics withTotals(long totalEvents) {
        return new ReplayJobMetrics(totalEvents, eventsSent, eventsFailed, startedAt, completedAt, lastUpdatedAt);
    }

    public ReplayJobMetrics plusSent(long delta) {
        return new ReplayJobMetrics(totalEvents, eventsSent + delta, eventsFailed, startedAt, completedAt, Instant.now());
    }

    public ReplayJobMetrics plusFailed(long delta) {
        return new ReplayJobMetrics(totalEvents, eventsSent, eventsFailed + delta, startedAt, completedAt, Instant.now());
    }

    public ReplayJobMetrics markStarted() {
        Instant now = Instant.now();
        return new ReplayJobMetrics(totalEvents, eventsSent, eventsFailed, now, completedAt, now);
    }

    public ReplayJobMetrics markCompleted() {
        Instant now = Instant.now();
        return new ReplayJobMetrics(totalEvents, eventsSent, eventsFailed, startedAt, now, now);
    }
}
