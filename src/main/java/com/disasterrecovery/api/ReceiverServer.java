package com.disasterrecovery.api;

import com.disasterrecovery.model.SecurityEvent;
import com.disasterrecovery.util.Json;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Simple downstream REST consumer for demo purposes.
 * Exposes POST /ingest and GET /stats.
 */
public final class ReceiverServer {

    private final ObjectMapper om = Json.mapper();
    private final HttpServer server;
    private final AtomicLong received = new AtomicLong();

    public ReceiverServer(int port) throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/ingest", this::ingest);
        server.createContext("/stats", this::stats);
    }

    public void start() {
        server.start();
    }

    public void stop(int delaySeconds) {
        server.stop(delaySeconds);
    }

    private void ingest(HttpExchange ex) throws IOException {
        if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) {
            sendText(ex, 405, "method not allowed\n");
            return;
        }
        try {
            om.readValue(ex.getRequestBody().readAllBytes(), SecurityEvent.class);
            long n = received.incrementAndGet();
            sendJson(ex, 200, Map.of("ok", true, "received_total", n));
        } catch (Exception e) {
            sendJson(ex, 400, Map.of("ok", false, "message", e.getMessage()));
        }
    }

    private void stats(HttpExchange ex) throws IOException {
        if (!"GET".equalsIgnoreCase(ex.getRequestMethod())) {
            sendText(ex, 405, "method not allowed\n");
            return;
        }
        sendJson(ex, 200, Map.of("received_total", received.get()));
    }

    private void sendJson(HttpExchange ex, int code, Object body) throws IOException {
        byte[] json = om.writeValueAsBytes(body);
        Headers h = ex.getResponseHeaders();
        h.set("Content-Type", "application/json");
        ex.sendResponseHeaders(code, json.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(json);
        } finally {
            ex.close();
        }
    }

    private void sendText(HttpExchange ex, int code, String text) throws IOException {
        byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
        Headers h = ex.getResponseHeaders();
        h.set("Content-Type", "text/plain; charset=utf-8");
        ex.sendResponseHeaders(code, bytes.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(bytes);
        } finally {
            ex.close();
        }
    }
}

