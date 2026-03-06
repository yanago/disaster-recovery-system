package com.disasterrecovery.sink;

import com.disasterrecovery.model.ReplayDestinationType;
import com.disasterrecovery.model.ReplayJobConfig;

public final class EventSinkFactory {

    private EventSinkFactory() {
    }

    public static EventSink create(ReplayJobConfig cfg) {
        if (cfg.getDestinationType() == ReplayDestinationType.KAFKA) {
            if (cfg.getKafkaBootstrapServers() == null || cfg.getKafkaBootstrapServers().isBlank()) {
                throw new IllegalArgumentException("kafka_bootstrap_servers is required for KAFKA destination");
            }
            if (cfg.getKafkaTopic() == null || cfg.getKafkaTopic().isBlank()) {
                throw new IllegalArgumentException("kafka_topic is required for KAFKA destination");
            }
            return new KafkaEventSink(cfg.getKafkaBootstrapServers(), cfg.getKafkaTopic());
        }

        if (cfg.getDestinationType() == ReplayDestinationType.REST) {
            if (cfg.getRestEndpointUrl() == null || cfg.getRestEndpointUrl().isBlank()) {
                throw new IllegalArgumentException("rest_endpoint_url is required for REST destination");
            }
            return new RestEventSink(cfg.getRestEndpointUrl());
        }

        throw new IllegalArgumentException("Unsupported destination_type: " + cfg.getDestinationType());
    }
}

