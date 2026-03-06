package com.disasterrecovery.model;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.time.Duration;
import java.util.Objects;

public final class ReplayJobConfig {

    private final String sourceTable;
    private final String cidFilter;
    private final Long startEventTime;
    private final Long endEventTime;
    private final ReplayDestinationType destinationType;
    private final String kafkaBootstrapServers;
    private final String kafkaTopic;
    private final String restEndpointUrl;
    private final int maxEventsPerSecond;
    private final int batchSize;
    private final String description;

    public ReplayJobConfig(String sourceTable,
                           String cidFilter,
                           Long startEventTime,
                           Long endEventTime,
                           ReplayDestinationType destinationType,
                           String kafkaBootstrapServers,
                           String kafkaTopic,
                           String restEndpointUrl,
                           int maxEventsPerSecond,
                           int batchSize,
                           String description) {
        this.sourceTable = Objects.requireNonNull(sourceTable, "sourceTable");
        this.cidFilter = cidFilter;
        this.startEventTime = startEventTime;
        this.endEventTime = endEventTime;
        this.destinationType = Objects.requireNonNull(destinationType, "destinationType");
        this.kafkaBootstrapServers = kafkaBootstrapServers;
        this.kafkaTopic = kafkaTopic;
        this.restEndpointUrl = restEndpointUrl;
        this.maxEventsPerSecond = maxEventsPerSecond;
        this.batchSize = batchSize;
        this.description = description;
    }

    public String getSourceTable() { return sourceTable; }
    public String getCidFilter() { return cidFilter; }
    public Long getStartEventTime() { return startEventTime; }
    public Long getEndEventTime() { return endEventTime; }
    public ReplayDestinationType getDestinationType() { return destinationType; }
    public String getKafkaBootstrapServers() { return kafkaBootstrapServers; }
    public String getKafkaTopic() { return kafkaTopic; }
    public String getRestEndpointUrl() { return restEndpointUrl; }
    public int getMaxEventsPerSecond() { return maxEventsPerSecond; }
    public int getBatchSize() { return batchSize; }
    public String getDescription() { return description; }

    @JsonIgnore
    public Duration tickInterval() {
        return Duration.ofSeconds(1);
    }
}
