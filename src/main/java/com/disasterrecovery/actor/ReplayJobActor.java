package com.disasterrecovery.actor;

import com.disasterrecovery.iceberg.IcebergEventStore;
import com.disasterrecovery.model.ReplayJobConfig;
import com.disasterrecovery.model.ReplayJobMetrics;
import com.disasterrecovery.model.ReplayJobState;
import com.disasterrecovery.model.SecurityEvent;
import com.disasterrecovery.sink.EventSink;
import com.disasterrecovery.sink.EventSinkFactory;
import com.disasterrecovery.source.EventCursor;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.*;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

public final class ReplayJobActor {

    public sealed interface Command permits Start, Pause, Resume, Cancel, Tick, BatchCompleted, InitializeCompleted, GetSnapshot {
    }

    public record Start(ActorRef<ActionResult> replyTo) implements Command {
    }

    public record Pause(ActorRef<ActionResult> replyTo) implements Command {
    }

    public record Resume(ActorRef<ActionResult> replyTo) implements Command {
    }

    public record Cancel(ActorRef<ActionResult> replyTo) implements Command {
    }

    public record GetSnapshot(ActorRef<Snapshot> replyTo) implements Command {
    }

    // internal
    private enum Tick implements Command {INSTANCE}

    private record InitializeCompleted(EventCursor cursor, EventSink sink, long totalRecords) implements Command {
    }

    private record BatchCompleted(long sent, long failed, boolean exhausted) implements Command {
    }

    public record ActionResult(boolean ok, String message) {
        public static ActionResult ok(String msg) {
            return new ActionResult(true, msg);
        }

        public static ActionResult bad(String msg) {
            return new ActionResult(false, msg);
        }
    }

    public record Snapshot(ReplayJobState state, ReplayJobMetrics metrics) {
    }

    public static Behavior<Command> create(String jobId,
                                          ReplayJobConfig config,
                                          IcebergEventStore store,
                                          ActorRef<ReplayJobManagerActor.Command> manager,
                                          Executor blockingIoExecutor) {
        return Behaviors.setup(ctx -> Behaviors.withTimers(timers ->
                new ReplayJobActor(jobId, config, store, manager, blockingIoExecutor, ctx, timers).behavior()
        ));
    }

    private final String jobId;
    private final ReplayJobConfig config;
    private final IcebergEventStore store;
    private final ActorRef<ReplayJobManagerActor.Command> manager;
    private final Executor blockingIoExecutor;
    private final ActorContext<Command> context;
    private final TimerScheduler<Command> timers;

    private ReplayJobState state = ReplayJobState.CREATED;
    private ReplayJobMetrics metrics = new ReplayJobMetrics(0, 0, 0, null, null, Instant.now());
    private EventCursor cursor;
    private EventSink sink;

    private ReplayJobActor(String jobId,
                           ReplayJobConfig config,
                           IcebergEventStore store,
                           ActorRef<ReplayJobManagerActor.Command> manager,
                           Executor blockingIoExecutor,
                           ActorContext<Command> context,
                           TimerScheduler<Command> timers) {
        this.jobId = Objects.requireNonNull(jobId, "jobId");
        this.config = Objects.requireNonNull(config, "config");
        this.store = Objects.requireNonNull(store, "store");
        this.manager = Objects.requireNonNull(manager, "manager");
        this.blockingIoExecutor = Objects.requireNonNull(blockingIoExecutor, "blockingIoExecutor");
        this.context = context;
        this.timers = timers;
    }

    private Behavior<Command> behavior() {
        return Behaviors.receive(Command.class)
                .onMessage(Start.class, this::onStart)
                .onMessage(Pause.class, this::onPause)
                .onMessage(Resume.class, this::onResume)
                .onMessage(Cancel.class, this::onCancel)
                .onMessage(GetSnapshot.class, this::onGetSnapshot)
                .onMessage(Tick.class, msg -> onTick())
                .onMessage(InitializeCompleted.class, this::onInitializeCompleted)
                .onMessage(BatchCompleted.class, this::onBatchCompleted)
                .build();
    }

    private Behavior<Command> onStart(Start msg) {
        if (state == ReplayJobState.RUNNING) {
            msg.replyTo().tell(ActionResult.ok("already running"));
            return Behaviors.same();
        }
        if (state == ReplayJobState.PAUSED) {
            state = ReplayJobState.RUNNING;
            manager.tell(new ReplayJobManagerActor.JobUpdate(jobId, state, metrics));
            msg.replyTo().tell(ActionResult.ok("resumed"));
            return Behaviors.same();
        }
        if (state != ReplayJobState.CREATED) {
            msg.replyTo().tell(ActionResult.bad("cannot start from state " + state));
            return Behaviors.same();
        }

        state = ReplayJobState.RUNNING;
        metrics = metrics.markStarted();
        manager.tell(new ReplayJobManagerActor.JobUpdate(jobId, state, metrics));

        // Initialize source cursor + sink on a blocking pool.
        CompletionStage<InitializeCompleted> init = CompletableFuture.supplyAsync(() -> {
            Table table = store.loadOrCreate(config.getSourceTable());
            Expression filter = store.buildFilter(config.getCidFilter(), config.getStartEventTime(), config.getEndEventTime());
            long total = store.estimateTotalRecords(table, filter);
            EventCursor cur = store.openCursor(table, filter);
            EventSink s = EventSinkFactory.create(config);
            return new InitializeCompleted(cur, s, total);
        }, blockingIoExecutor);

        context.pipeToSelf(init, (ok, ex) -> {
            if (ex != null) {
                return new InitializeCompleted(null, null, -1);
            }
            return ok;
        });

        timers.startTimerAtFixedRate(Tick.INSTANCE, config.tickInterval());
        msg.replyTo().tell(ActionResult.ok("started"));
        return Behaviors.same();
    }

    private Behavior<Command> onInitializeCompleted(InitializeCompleted msg) {
        if (msg.cursor == null || msg.sink == null || msg.totalRecords < 0) {
            state = ReplayJobState.FAILED;
            closeQuietly();
            manager.tell(new ReplayJobManagerActor.JobUpdate(jobId, state, metrics));
            return Behaviors.same();
        }
        this.cursor = msg.cursor;
        this.sink = msg.sink;
        this.metrics = metrics.withTotals(msg.totalRecords);
        manager.tell(new ReplayJobManagerActor.JobUpdate(jobId, state, metrics));
        return Behaviors.same();
    }

    private Behavior<Command> onPause(Pause msg) {
        if (state != ReplayJobState.RUNNING) {
            msg.replyTo().tell(ActionResult.bad("cannot pause from state " + state));
            return Behaviors.same();
        }
        state = ReplayJobState.PAUSED;
        manager.tell(new ReplayJobManagerActor.JobUpdate(jobId, state, metrics));
        msg.replyTo().tell(ActionResult.ok("paused"));
        return Behaviors.same();
    }

    private Behavior<Command> onResume(Resume msg) {
        if (state != ReplayJobState.PAUSED) {
            msg.replyTo().tell(ActionResult.bad("cannot resume from state " + state));
            return Behaviors.same();
        }
        state = ReplayJobState.RUNNING;
        manager.tell(new ReplayJobManagerActor.JobUpdate(jobId, state, metrics));
        msg.replyTo().tell(ActionResult.ok("resumed"));
        return Behaviors.same();
    }

    private Behavior<Command> onCancel(Cancel msg) {
        if (state == ReplayJobState.CANCELLED || state == ReplayJobState.COMPLETED) {
            msg.replyTo().tell(ActionResult.ok("already finished"));
            return Behaviors.same();
        }
        state = ReplayJobState.CANCELLED;
        timers.cancelAll();
        closeQuietly();
        manager.tell(new ReplayJobManagerActor.JobUpdate(jobId, state, metrics));
        msg.replyTo().tell(ActionResult.ok("cancelled"));
        return Behaviors.same();
    }

    private Behavior<Command> onGetSnapshot(GetSnapshot msg) {
        msg.replyTo().tell(new Snapshot(state, metrics));
        return Behaviors.same();
    }

    private Behavior<Command> onTick() {
        if (state != ReplayJobState.RUNNING) {
            return Behaviors.same();
        }
        if (cursor == null || sink == null) {
            return Behaviors.same();
        }

        int n = Math.max(1, Math.min(config.getBatchSize(), config.getMaxEventsPerSecond()));
        List<SecurityEvent> batch = new ArrayList<>(n);
        for (int i = 0; i < n && cursor.hasNext(); i++) {
            batch.add(cursor.next());
        }
        boolean exhausted = !cursor.hasNext();
        if (batch.isEmpty() && exhausted) {
            state = ReplayJobState.COMPLETED;
            timers.cancelAll();
            closeQuietly();
            metrics = metrics.markCompleted();
            manager.tell(new ReplayJobManagerActor.JobUpdate(jobId, state, metrics));
            return Behaviors.same();
        }

        CompletionStage<BatchCompleted> sendStage = sendBatch(batch, exhausted);
        context.pipeToSelf(sendStage, (ok, ex) -> {
            if (ex != null) {
                return new BatchCompleted(0, batch.size(), exhausted);
            }
            return ok;
        });
        return Behaviors.same();
    }

    private CompletionStage<BatchCompleted> sendBatch(List<SecurityEvent> batch, boolean exhausted) {
        List<CompletableFuture<Boolean>> results = new ArrayList<>(batch.size());
        for (SecurityEvent e : batch) {
            CompletionStage<Void> cs = sink.send(e);
            CompletableFuture<Boolean> r = toCompletableFuture(cs).handle((v, ex) -> ex == null);
            results.add(r);
        }

        CompletableFuture<Void> all = CompletableFuture.allOf(results.toArray(new CompletableFuture[0]));
        return all.thenApply(ignored -> {
            long sent = 0;
            long failed = 0;
            for (CompletableFuture<Boolean> r : results) {
                if (Boolean.TRUE.equals(r.join())) {
                    sent++;
                } else {
                    failed++;
                }
            }
            return new BatchCompleted(sent, failed, exhausted);
        });
    }

    private Behavior<Command> onBatchCompleted(BatchCompleted msg) {
        if (state != ReplayJobState.RUNNING) {
            return Behaviors.same();
        }
        if (msg.sent > 0) {
            metrics = metrics.plusSent(msg.sent);
        }
        if (msg.failed > 0) {
            metrics = metrics.plusFailed(msg.failed);
        }
        manager.tell(new ReplayJobManagerActor.JobUpdate(jobId, state, metrics));

        if (msg.exhausted) {
            state = ReplayJobState.COMPLETED;
            timers.cancelAll();
            closeQuietly();
            metrics = metrics.markCompleted();
            manager.tell(new ReplayJobManagerActor.JobUpdate(jobId, state, metrics));
        }
        return Behaviors.same();
    }

    private static CompletableFuture<Void> toCompletableFuture(CompletionStage<Void> cs) {
        if (cs instanceof CompletableFuture<Void> cf) {
            return cf;
        }
        CompletableFuture<Void> f = new CompletableFuture<>();
        cs.whenComplete((v, ex) -> {
            if (ex != null) {
                f.completeExceptionally(ex);
            } else {
                f.complete(null);
            }
        });
        return f;
    }

    private void closeQuietly() {
        if (cursor != null) {
            try {
                cursor.close();
            } catch (Exception ignored) {
            }
            cursor = null;
        }
        if (sink != null) {
            try {
                sink.close();
            } catch (Exception ignored) {
            }
            sink = null;
        }
    }
}

