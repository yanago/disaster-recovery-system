package com.disasterrecovery.api;

import com.disasterrecovery.actor.ReplayJobActor;
import com.disasterrecovery.actor.ReplayJobManagerActor;
import com.disasterrecovery.api.dto.CreateReplayJobRequest;
import com.disasterrecovery.api.dto.ErrorResponse;
import com.disasterrecovery.model.ReplayDestinationType;
import com.disasterrecovery.model.ReplayJobConfig;
import com.disasterrecovery.util.Json;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.Scheduler;
import org.apache.pekko.actor.typed.javadsl.AskPattern;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

public final class ApiServer {

    private final ObjectMapper om = Json.mapper();
    private final HttpServer server;
    private final ActorSystem<?> system;
    private final Scheduler scheduler;
    private final ActorRef<ReplayJobManagerActor.Command> manager;
    private final Duration timeout;

    public ApiServer(ActorSystem<?> system,
                     ActorRef<ReplayJobManagerActor.Command> manager,
                     int port,
                     Duration timeout) throws IOException {
        this.system = Objects.requireNonNull(system, "system");
        this.scheduler = system.scheduler();
        this.manager = Objects.requireNonNull(manager, "manager");
        this.timeout = Objects.requireNonNull(timeout, "timeout");
        this.server = HttpServer.create(new InetSocketAddress(port), 0);

        server.createContext("/health", this::health);
        server.createContext("/metrics", this::metrics);
        server.createContext("/api/v1/replay/jobs", new JobsHandler());
    }

    public void start() {
        server.start();
    }

    public void stop(int delaySeconds) {
        server.stop(delaySeconds);
    }

    private void health(HttpExchange ex) throws IOException {
        if (!"GET".equalsIgnoreCase(ex.getRequestMethod())) {
            respond(ex, 405, new ErrorResponse("method not allowed"));
            return;
        }
        respondText(ex, 200, "ok\n");
    }

    private void metrics(HttpExchange ex) throws IOException {
        if (!"GET".equalsIgnoreCase(ex.getRequestMethod())) {
            respond(ex, 405, new ErrorResponse("method not allowed"));
            return;
        }
        CompletionStage<ReplayJobManagerActor.ListJobsResponse> stage =
                AskPattern.ask(manager, ReplayJobManagerActor.ListJobs::new, timeout, scheduler);
        stage.whenComplete((res, err) -> {
            try {
                if (err != null) {
                    respondText(ex, 500, "metrics_error 1\n");
                    return;
                }
                String body = "replay_jobs_total " + res.jobs().size() + "\n";
                respondText(ex, 200, body);
            } catch (IOException ignored) {
            }
        });
    }

    private final class JobsHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange ex) throws IOException {
            String method = ex.getRequestMethod();
            String path = ex.getRequestURI().getPath();

            // Exact collection path: /api/v1/replay/jobs
            if ("/api/v1/replay/jobs".equals(path)) {
                if ("POST".equalsIgnoreCase(method)) {
                    handleCreate(ex);
                    return;
                }
                if ("GET".equalsIgnoreCase(method)) {
                    handleList(ex);
                    return;
                }
                respond(ex, 405, new ErrorResponse("method not allowed"));
                return;
            }

            // Item paths:
            // /api/v1/replay/jobs/{id}
            // /api/v1/replay/jobs/{id}/start|pause|resume|cancel
            // /api/v1/replay/jobs/{id}/status|metrics
            String prefix = "/api/v1/replay/jobs/";
            if (!path.startsWith(prefix)) {
                respond(ex, 404, new ErrorResponse("not found"));
                return;
            }

            String rest = path.substring(prefix.length());
            String[] parts = rest.split("/");
            if (parts.length == 1) {
                if ("GET".equalsIgnoreCase(method)) {
                    handleGet(ex, parts[0]);
                    return;
                }
                respond(ex, 405, new ErrorResponse("method not allowed"));
                return;
            }
            if (parts.length == 2) {
                String id = parts[0];
                String action = parts[1];
                if ("POST".equalsIgnoreCase(method)) {
                    switch (action) {
                        case "start" -> handleAction(ex, id, "start");
                        case "pause" -> handleAction(ex, id, "pause");
                        case "resume" -> handleAction(ex, id, "resume");
                        case "cancel" -> handleAction(ex, id, "cancel");
                        default -> respond(ex, 404, new ErrorResponse("not found"));
                    }
                    return;
                }
                if ("GET".equalsIgnoreCase(method)) {
                    switch (action) {
                        case "status" -> handleStatus(ex, id);
                        case "metrics" -> handleMetrics(ex, id);
                        default -> respond(ex, 404, new ErrorResponse("not found"));
                    }
                    return;
                }
            }

            respond(ex, 404, new ErrorResponse("not found"));
        }

        private void handleCreate(HttpExchange ex) throws IOException {
            CreateReplayJobRequest req;
            try {
                req = om.readValue(readAll(ex.getRequestBody()), CreateReplayJobRequest.class);
            } catch (Exception e) {
                respond(ex, 400, new ErrorResponse("invalid json: " + e.getMessage()));
                return;
            }

            ReplayJobConfig cfg;
            try {
                cfg = toConfig(req);
            } catch (Exception e) {
                respond(ex, 400, new ErrorResponse(e.getMessage()));
                return;
            }

            CompletionStage<ReplayJobManagerActor.CreateJobResponse> stage =
                    AskPattern.ask(manager, replyTo -> new ReplayJobManagerActor.CreateJob(cfg, replyTo), timeout, scheduler);
            stage.whenComplete((res, err) -> {
                try {
                    if (err != null) {
                        respond(ex, 500, new ErrorResponse("create failed: " + err.getMessage()));
                        return;
                    }
                    if (!res.ok()) {
                        respond(ex, 400, new ErrorResponse(res.message()));
                        return;
                    }
                    respond(ex, 201, Map.of("id", res.id()));
                } catch (IOException ignored) {
                }
            });
        }

        private void handleList(HttpExchange ex) throws IOException {
            CompletionStage<ReplayJobManagerActor.ListJobsResponse> stage =
                    AskPattern.ask(manager, ReplayJobManagerActor.ListJobs::new, timeout, scheduler);
            stage.whenComplete((res, err) -> {
                try {
                    if (err != null) {
                        respond(ex, 500, new ErrorResponse("list failed: " + err.getMessage()));
                        return;
                    }
                    respond(ex, 200, res.jobs());
                } catch (IOException ignored) {
                }
            });
        }

        private void handleGet(HttpExchange ex, String id) throws IOException {
            CompletionStage<ReplayJobManagerActor.GetJobResponse> stage =
                    AskPattern.ask(manager, replyTo -> new ReplayJobManagerActor.GetJob(id, replyTo), timeout, scheduler);
            stage.whenComplete((res, err) -> {
                try {
                    if (err != null) {
                        respond(ex, 500, new ErrorResponse("get failed: " + err.getMessage()));
                        return;
                    }
                    if (!res.ok()) {
                        respond(ex, 404, new ErrorResponse(res.message()));
                        return;
                    }
                    respond(ex, 200, res.job());
                } catch (IOException ignored) {
                }
            });
        }

        private void handleStatus(HttpExchange ex, String id) throws IOException {
            CompletionStage<ReplayJobManagerActor.StatusResponse> stage =
                    AskPattern.ask(manager, replyTo -> new ReplayJobManagerActor.GetStatus(id, replyTo), timeout, scheduler);
            stage.whenComplete((res, err) -> {
                try {
                    if (err != null) {
                        respond(ex, 500, new ErrorResponse("status failed: " + err.getMessage()));
                        return;
                    }
                    if (!res.ok()) {
                        respond(ex, 404, new ErrorResponse(res.message()));
                        return;
                    }
                    respond(ex, 200, Map.of("state", res.state().name()));
                } catch (IOException ignored) {
                }
            });
        }

        private void handleMetrics(HttpExchange ex, String id) throws IOException {
            CompletionStage<ReplayJobManagerActor.MetricsResponse> stage =
                    AskPattern.ask(manager, replyTo -> new ReplayJobManagerActor.GetMetrics(id, replyTo), timeout, scheduler);
            stage.whenComplete((res, err) -> {
                try {
                    if (err != null) {
                        respond(ex, 500, new ErrorResponse("metrics failed: " + err.getMessage()));
                        return;
                    }
                    if (!res.ok()) {
                        respond(ex, 404, new ErrorResponse(res.message()));
                        return;
                    }
                    respond(ex, 200, res.metrics());
                } catch (IOException ignored) {
                }
            });
        }

        private void handleAction(HttpExchange ex, String id, String action) throws IOException {
            CompletionStage<ReplayJobActor.ActionResult> stage = switch (action) {
                case "start" ->
                        AskPattern.ask(manager, replyTo -> new ReplayJobManagerActor.StartJob(id, replyTo), timeout, scheduler);
                case "pause" ->
                        AskPattern.ask(manager, replyTo -> new ReplayJobManagerActor.PauseJob(id, replyTo), timeout, scheduler);
                case "resume" ->
                        AskPattern.ask(manager, replyTo -> new ReplayJobManagerActor.ResumeJob(id, replyTo), timeout, scheduler);
                case "cancel" ->
                        AskPattern.ask(manager, replyTo -> new ReplayJobManagerActor.CancelJob(id, replyTo), timeout, scheduler);
                default -> throw new IllegalArgumentException("unknown action: " + action);
            };

            stage.whenComplete((res, err) -> {
                try {
                    if (err != null) {
                        respond(ex, 500, new ErrorResponse(action + " failed: " + err.getMessage()));
                        return;
                    }
                    if (!res.ok()) {
                        // If job doesn't exist, manager returns bad("job not found..") here.
                        int code = res.message().startsWith("job not found") ? 404 : 409;
                        respond(ex, code, new ErrorResponse(res.message()));
                        return;
                    }
                    respond(ex, 200, Map.of("message", res.message()));
                } catch (IOException ignored) {
                }
            });
        }
    }

    private ReplayJobConfig toConfig(CreateReplayJobRequest req) {
        String destTypeRaw = req.getDestinationType();
        if (destTypeRaw == null || destTypeRaw.isBlank()) {
            throw new IllegalArgumentException("destination_type is required");
        }
        ReplayDestinationType destType;
        try {
            destType = ReplayDestinationType.valueOf(destTypeRaw.trim().toUpperCase());
        } catch (Exception e) {
            throw new IllegalArgumentException("destination_type must be KAFKA or REST");
        }

        int eps = req.getMaxEventsPerSecond() == null ? 2000 : req.getMaxEventsPerSecond();
        int batch = req.getBatchSize() == null ? 500 : req.getBatchSize();

        return new ReplayJobConfig(
                req.getSourceTable() == null ? "security.events" : req.getSourceTable(),
                req.getCid(),
                req.getStartEventTime(),
                req.getEndEventTime(),
                destType,
                req.getKafkaBootstrapServers(),
                req.getKafkaTopic(),
                req.getRestEndpointUrl(),
                eps,
                batch,
                req.getDescription()
        );
    }

    private static byte[] readAll(InputStream is) throws IOException {
        return is.readAllBytes();
    }

    private void respond(HttpExchange ex, int code, Object body) throws IOException {
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

    private void respondText(HttpExchange ex, int code, String text) throws IOException {
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

