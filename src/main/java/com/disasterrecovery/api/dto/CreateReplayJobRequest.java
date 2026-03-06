package com.disasterrecovery.api.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public final class CreateReplayJobRequest {

    private final String sourceTable;
    private final String cid;
    private final Long startEventTime;
    private final Long endEventTime;
    private final String destinationType;
    private final String kafkaBootstrapServers;
    private final String kafkaTopic;
    private final String restEndpointUrl;
    private final Integer maxEventsPerSecond;
    private final Integer batchSize;
    private final String description;

    @JsonCreator
    public CreateReplayJobRequest(
            @JsonProperty("source_table") String sourceTable,
            @JsonProperty("cid") String cid,
            @JsonProperty("start_event_time") Long startEventTime,
            @JsonProperty("end_event_time") Long endEventTime,
            @JsonProperty("destination_type") String destinationType,
            @JsonProperty("kafka_bootstrap_servers") String kafkaBootstrapServers,
            @JsonProperty("kafka_topic") String kafkaTopic,
            @JsonProperty("rest_endpoint_url") String restEndpointUrl,
            @JsonProperty("max_events_per_second") Integer maxEventsPerSecond,
            @JsonProperty("batch_size") Integer batchSize,
            @JsonProperty("description") String description) {
        this.sourceTable = sourceTable;
        this.cid = cid;
        this.startEventTime = startEventTime;
        this.endEventTime = endEventTime;
        this.destinationType = destinationType;
        this.kafkaBootstrapServers = kafkaBootstrapServers;
        this.kafkaTopic = kafkaTopic;
        this.restEndpointUrl = restEndpointUrl;
        this.maxEventsPerSecond = maxEventsPerSecond;
        this.batchSize = batchSize;
        this.description = description;
    }

    @JsonProperty("source_table") public String getSourceTable() { return sourceTable; }
    @JsonProperty("cid") public String getCid() { return cid; }
    @JsonProperty("start_event_time") public Long getStartEventTime() { return startEventTime; }
    @JsonProperty("end_event_time") public Long getEndEventTime() { return endEventTime; }
    @JsonProperty("destination_type") public String getDestinationType() { return destinationType; }
    @JsonProperty("kafka_bootstrap_servers") public String getKafkaBootstrapServers() { return kafkaBootstrapServers; }
    @JsonProperty("kafka_topic") public String getKafkaTopic() { return kafkaTopic; }
    @JsonProperty("rest_endpoint_url") public String getRestEndpointUrl() { return restEndpointUrl; }
    @JsonProperty("max_events_per_second") public Integer getMaxEventsPerSecond() { return maxEventsPerSecond; }
    @JsonProperty("batch_size") public Integer getBatchSize() { return batchSize; }
    @JsonProperty("description") public String getDescription() { return description; }
}
