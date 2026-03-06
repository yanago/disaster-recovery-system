package com.disasterrecovery.api.dto;

import com.disasterrecovery.model.ReplayJobState;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

public final class ReplayJobSummary {

    private final String id;
    private final String sourceTable;
    private final String cid;
    private final ReplayJobState state;
    private final String destinationType;
    private final Instant createdAt;

    public ReplayJobSummary(String id, String sourceTable, String cid,
                            ReplayJobState state, String destinationType, Instant createdAt) {
        this.id = id;
        this.sourceTable = sourceTable;
        this.cid = cid;
        this.state = state;
        this.destinationType = destinationType;
        this.createdAt = createdAt;
    }

    @JsonProperty("id") public String getId() { return id; }
    @JsonProperty("source_table") public String getSourceTable() { return sourceTable; }
    @JsonProperty("cid") public String getCid() { return cid; }
    @JsonProperty("state") public ReplayJobState getState() { return state; }
    @JsonProperty("destination_type") public String getDestinationType() { return destinationType; }
    @JsonProperty("created_at") public Instant getCreatedAt() { return createdAt; }
}
