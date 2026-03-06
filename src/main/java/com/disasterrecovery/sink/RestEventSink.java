package com.disasterrecovery.sink;

import com.disasterrecovery.model.SecurityEvent;
import com.disasterrecovery.util.Json;
import com.fasterxml.jackson.core.JsonProcessingException;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public final class RestEventSink implements EventSink {

    private final HttpClient httpClient;
    private final URI endpoint;

    public RestEventSink(String endpointUrl) {
        this.endpoint = URI.create(Objects.requireNonNull(endpointUrl, "endpointUrl"));
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
    }

    @Override
    public CompletionStage<Void> send(SecurityEvent event) {
        String payload;
        try {
            payload = Json.mapper().writeValueAsString(event);
        } catch (JsonProcessingException e) {
            return CompletableFuture.failedFuture(e);
        }

        HttpRequest req = HttpRequest.newBuilder(endpoint)
                .timeout(Duration.ofSeconds(10))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(payload))
                .build();

        return httpClient.sendAsync(req, HttpResponse.BodyHandlers.discarding())
                .thenCompose(resp -> {
                    int code = resp.statusCode();
                    if (code >= 200 && code < 300) {
                        return CompletableFuture.completedFuture(null);
                    }
                    return CompletableFuture.failedFuture(new RuntimeException("REST sink non-2xx: " + code));
                });
    }

    @Override
    public void close() {
        // HttpClient has no explicit close.
    }
}

