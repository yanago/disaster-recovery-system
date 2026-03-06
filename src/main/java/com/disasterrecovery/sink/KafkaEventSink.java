package com.disasterrecovery.sink;

import com.disasterrecovery.model.SecurityEvent;
import com.disasterrecovery.util.Json;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public final class KafkaEventSink implements EventSink {

    private final KafkaProducer<String, String> producer;
    private final String topic;

    public KafkaEventSink(String bootstrapServers, String topic) {
        this.topic = Objects.requireNonNull(topic, "topic");
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Objects.requireNonNull(bootstrapServers, "bootstrapServers"));
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "5");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        this.producer = new KafkaProducer<>(props);
    }

    @Override
    public CompletionStage<Void> send(SecurityEvent event) {
        String payload;
        try {
            payload = Json.mapper().writeValueAsString(event);
        } catch (JsonProcessingException e) {
            return CompletableFuture.failedFuture(e);
        }

        CompletableFuture<Void> fut = new CompletableFuture<>();
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, event.getCid(), payload);
        producer.send(record, (md, ex) -> {
            if (ex != null) {
                fut.completeExceptionally(ex);
            } else {
                fut.complete(null);
            }
        });
        return fut;
    }

    @Override
    public void close() {
        producer.flush();
        producer.close();
    }
}

