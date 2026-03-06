package com.disasterrecovery.actor;

import com.disasterrecovery.api.dto.ReplayJobDetailResponse;
import com.disasterrecovery.api.dto.ReplayJobSummary;
import com.disasterrecovery.iceberg.IcebergEventStore;
import com.disasterrecovery.model.ReplayDestinationType;
import com.disasterrecovery.model.ReplayJobConfig;
import com.disasterrecovery.model.ReplayJobMetrics;
import com.disasterrecovery.model.ReplayJobState;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.*;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.Executor;

public final class ReplayJobManagerActor {

    public sealed interface Command permits CreateJob, StartJob, PauseJob, ResumeJob, CancelJob, GetJob, ListJobs, GetStatus, GetMetrics, JobUpdate {
    }

    public record CreateJob(ReplayJobConfig config, ActorRef<CreateJobResponse> replyTo) implements Command {
    }

    public record StartJob(String id, ActorRef<ReplayJobActor.ActionResult> replyTo) implements Command {
    }

    public record PauseJob(String id, ActorRef<ReplayJobActor.ActionResult> replyTo) implements Command {
    }

    public record ResumeJob(String id, ActorRef<ReplayJobActor.ActionResult> replyTo) implements Command {
    }

    public record CancelJob(String id, ActorRef<ReplayJobActor.ActionResult> replyTo) implements Command {
    }

    public record GetJob(String id, ActorRef<GetJobResponse> replyTo) implements Command {
    }

    public record ListJobs(ActorRef<ListJobsResponse> replyTo) implements Command {
    }

    public record GetStatus(String id, ActorRef<StatusResponse> replyTo) implements Command {
    }

    public record GetMetrics(String id, ActorRef<MetricsResponse> replyTo) implements Command {
    }

    public record JobUpdate(String id, ReplayJobState state, ReplayJobMetrics metrics) implements Command {
    }

    public record CreateJobResponse(boolean ok, String message, String id) {
        public static CreateJobResponse ok(String id) {
            return new CreateJobResponse(true, "created", id);
        }

        public static CreateJobResponse bad(String msg) {
            return new CreateJobResponse(false, msg, null);
        }
    }

    public record GetJobResponse(boolean ok, String message, ReplayJobDetailResponse job) {
        public static GetJobResponse ok(ReplayJobDetailResponse job) {
            return new GetJobResponse(true, "ok", job);
        }

        public static GetJobResponse notFound(String id) {
            return new GetJobResponse(false, "job not found: " + id, null);
        }
    }

    public record ListJobsResponse(List<ReplayJobSummary> jobs) {
    }

    public record StatusResponse(boolean ok, String message, ReplayJobState state) {
        public static StatusResponse ok(ReplayJobState state) {
            return new StatusResponse(true, "ok", state);
        }

        public static StatusResponse notFound(String id) {
            return new StatusResponse(false, "job not found: " + id, null);
        }
    }

    public record MetricsResponse(boolean ok, String message, ReplayJobMetrics metrics) {
        public static MetricsResponse ok(ReplayJobMetrics metrics) {
            return new MetricsResponse(true, "ok", metrics);
        }

        public static MetricsResponse notFound(String id) {
            return new MetricsResponse(false, "job not found: " + id, null);
        }
    }

    public static Behavior<Command> create(IcebergEventStore store, Executor blockingIoExecutor) {
        return Behaviors.setup(ctx -> new ReplayJobManagerActor(store, blockingIoExecutor, ctx).behavior());
    }

    private record JobEntry(String id,
                            ReplayJobConfig config,
                            Instant createdAt,
                            ActorRef<ReplayJobActor.Command> ref,
                            ReplayJobState state,
                            ReplayJobMetrics metrics) {
    }

    private final IcebergEventStore store;
    private final Executor blockingIoExecutor;
    private final ActorContext<Command> context;
    private final Map<String, JobEntry> jobs = new LinkedHashMap<>();

    private ReplayJobManagerActor(IcebergEventStore store, Executor blockingIoExecutor, ActorContext<Command> context) {
        this.store = Objects.requireNonNull(store, "store");
        this.blockingIoExecutor = Objects.requireNonNull(blockingIoExecutor, "blockingIoExecutor");
        this.context = context;
    }

    private Behavior<Command> behavior() {
        return Behaviors.receive(Command.class)
                .onMessage(CreateJob.class, this::onCreate)
                .onMessage(StartJob.class, this::onStart)
                .onMessage(PauseJob.class, this::onPause)
                .onMessage(ResumeJob.class, this::onResume)
                .onMessage(CancelJob.class, this::onCancel)
                .onMessage(GetJob.class, this::onGetJob)
                .onMessage(ListJobs.class, this::onList)
                .onMessage(GetStatus.class, this::onStatus)
                .onMessage(GetMetrics.class, this::onMetrics)
                .onMessage(JobUpdate.class, this::onUpdate)
                .build();
    }

    private Behavior<Command> onCreate(CreateJob msg) {
        ReplayJobConfig cfg = msg.config();
        try {
            validate(cfg);
        } catch (Exception e) {
            msg.replyTo().tell(CreateJobResponse.bad(e.getMessage()));
            return Behaviors.same();
        }

        String id = UUID.randomUUID().toString();
        ActorRef<ReplayJobActor.Command> ref = context.spawn(
                ReplayJobActor.create(id, cfg, store, context.getSelf(), blockingIoExecutor),
                "replay-job-" + id.replace("-", "")
        );
        JobEntry entry = new JobEntry(
                id,
                cfg,
                Instant.now(),
                ref,
                ReplayJobState.CREATED,
                new ReplayJobMetrics(0, 0, 0, null, null, Instant.now())
        );
        jobs.put(id, entry);
        msg.replyTo().tell(CreateJobResponse.ok(id));
        return Behaviors.same();
    }

    private Behavior<Command> onStart(StartJob msg) {
        JobEntry e = jobs.get(msg.id());
        if (e == null) {
            msg.replyTo().tell(ReplayJobActor.ActionResult.bad("job not found: " + msg.id()));
            return Behaviors.same();
        }
        e.ref().tell(new ReplayJobActor.Start(msg.replyTo()));
        return Behaviors.same();
    }

    private Behavior<Command> onPause(PauseJob msg) {
        JobEntry e = jobs.get(msg.id());
        if (e == null) {
            msg.replyTo().tell(ReplayJobActor.ActionResult.bad("job not found: " + msg.id()));
            return Behaviors.same();
        }
        e.ref().tell(new ReplayJobActor.Pause(msg.replyTo()));
        return Behaviors.same();
    }

    private Behavior<Command> onResume(ResumeJob msg) {
        JobEntry e = jobs.get(msg.id());
        if (e == null) {
            msg.replyTo().tell(ReplayJobActor.ActionResult.bad("job not found: " + msg.id()));
            return Behaviors.same();
        }
        e.ref().tell(new ReplayJobActor.Resume(msg.replyTo()));
        return Behaviors.same();
    }

    private Behavior<Command> onCancel(CancelJob msg) {
        JobEntry e = jobs.get(msg.id());
        if (e == null) {
            msg.replyTo().tell(ReplayJobActor.ActionResult.bad("job not found: " + msg.id()));
            return Behaviors.same();
        }
        e.ref().tell(new ReplayJobActor.Cancel(msg.replyTo()));
        return Behaviors.same();
    }

    private Behavior<Command> onGetJob(GetJob msg) {
        JobEntry e = jobs.get(msg.id());
        if (e == null) {
            msg.replyTo().tell(GetJobResponse.notFound(msg.id()));
            return Behaviors.same();
        }
        ReplayJobDetailResponse dto = new ReplayJobDetailResponse(e.id(), e.state(), e.config(), e.metrics());
        msg.replyTo().tell(GetJobResponse.ok(dto));
        return Behaviors.same();
    }

    private Behavior<Command> onList(ListJobs msg) {
        List<ReplayJobSummary> out = new ArrayList<>(jobs.size());
        for (JobEntry e : jobs.values()) {
            out.add(new ReplayJobSummary(
                    e.id(),
                    e.config().getSourceTable(),
                    e.config().getCidFilter(),
                    e.state(),
                    e.config().getDestinationType().name(),
                    e.createdAt()
            ));
        }
        msg.replyTo().tell(new ListJobsResponse(out));
        return Behaviors.same();
    }

    private Behavior<Command> onStatus(GetStatus msg) {
        JobEntry e = jobs.get(msg.id());
        if (e == null) {
            msg.replyTo().tell(StatusResponse.notFound(msg.id()));
            return Behaviors.same();
        }
        msg.replyTo().tell(StatusResponse.ok(e.state()));
        return Behaviors.same();
    }

    private Behavior<Command> onMetrics(GetMetrics msg) {
        JobEntry e = jobs.get(msg.id());
        if (e == null) {
            msg.replyTo().tell(MetricsResponse.notFound(msg.id()));
            return Behaviors.same();
        }
        msg.replyTo().tell(MetricsResponse.ok(e.metrics()));
        return Behaviors.same();
    }

    private Behavior<Command> onUpdate(JobUpdate msg) {
        JobEntry e = jobs.get(msg.id());
        if (e == null) {
            return Behaviors.same();
        }
        jobs.put(msg.id(), new JobEntry(e.id(), e.config(), e.createdAt(), e.ref(), msg.state(), msg.metrics()));
        return Behaviors.same();
    }

    private static void validate(ReplayJobConfig cfg) {
        if (cfg.getSourceTable() == null || cfg.getSourceTable().isBlank()) {
            throw new IllegalArgumentException("source_table is required");
        }
        if (cfg.getMaxEventsPerSecond() <= 0) {
            throw new IllegalArgumentException("max_events_per_second must be > 0");
        }
        if (cfg.getBatchSize() <= 0) {
            throw new IllegalArgumentException("batch_size must be > 0");
        }
        if (cfg.getDestinationType() == ReplayDestinationType.KAFKA) {
            if (cfg.getKafkaBootstrapServers() == null || cfg.getKafkaBootstrapServers().isBlank()) {
                throw new IllegalArgumentException("kafka_bootstrap_servers is required for KAFKA destination");
            }
            if (cfg.getKafkaTopic() == null || cfg.getKafkaTopic().isBlank()) {
                throw new IllegalArgumentException("kafka_topic is required for KAFKA destination");
            }
        } else if (cfg.getDestinationType() == ReplayDestinationType.REST) {
            if (cfg.getRestEndpointUrl() == null || cfg.getRestEndpointUrl().isBlank()) {
                throw new IllegalArgumentException("rest_endpoint_url is required for REST destination");
            }
        } else {
            throw new IllegalArgumentException("unsupported destination_type: " + cfg.getDestinationType());
        }
    }
}

