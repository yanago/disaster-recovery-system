package com.disasterrecovery.api.dto;

import com.disasterrecovery.model.ReplayJobConfig;
import com.disasterrecovery.model.ReplayJobMetrics;
import com.disasterrecovery.model.ReplayJobState;
import com.fasterxml.jackson.annotation.JsonProperty;

public final class ReplayJobDetailResponse {

    private final String id;
    private final ReplayJobState state;
    private final ReplayJobConfig config;
    private final ReplayJobMetrics metrics;

    public ReplayJobDetailResponse(String id, ReplayJobState state, ReplayJobConfig config, ReplayJobMetrics metrics) {
        this.id = id;
        this.state = state;
        this.config = config;
        this.metrics = metrics;
    }

    @JsonProperty("id") public String getId() { return id; }
    @JsonProperty("state") public ReplayJobState getState() { return state; }
    @JsonProperty("config") public ReplayJobConfig getConfig() { return config; }
    @JsonProperty("metrics") public ReplayJobMetrics getMetrics() { return metrics; }
}
