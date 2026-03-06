package com.disasterrecovery;

import com.disasterrecovery.actor.ReplayJobManagerActor;
import com.disasterrecovery.api.ApiServer;
import com.disasterrecovery.api.ReceiverServer;
import com.disasterrecovery.iceberg.IcebergCatalogProvider;
import com.disasterrecovery.iceberg.IcebergEventStore;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.expressions.Expressions;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.javadsl.Behaviors;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class Main {

    public static void main(String[] args) throws Exception {
        int httpPort = Integer.parseInt(System.getenv().getOrDefault("HTTP_PORT", "8080"));
        Duration apiTimeout = Duration.ofSeconds(Long.parseLong(System.getenv().getOrDefault("API_TIMEOUT_SECONDS", "5")));

        boolean enableReceiver = Boolean.parseBoolean(System.getenv().getOrDefault("ENABLE_RECEIVER", "true"));
        int receiverPort = Integer.parseInt(System.getenv().getOrDefault("RECEIVER_PORT", "9090"));

        String sourceTable = System.getenv().getOrDefault("SOURCE_TABLE", "security.events");
        boolean initDemoData = Boolean.parseBoolean(System.getenv().getOrDefault("INIT_DEMO_DATA", "true"));
        int demoEvents = Integer.parseInt(System.getenv().getOrDefault("DEMO_EVENTS", "50000"));
        int demoBatch = Integer.parseInt(System.getenv().getOrDefault("DEMO_WRITE_BATCH", "5000"));

        ExecutorService blockingIo = Executors.newFixedThreadPool(
                Integer.parseInt(System.getenv().getOrDefault("BLOCKING_IO_THREADS", "6"))
        );

        Catalog catalog = IcebergCatalogProvider.createHadoopCatalog();
        IcebergEventStore store = new IcebergEventStore(catalog);

        if (initDemoData) {
            Table t = store.loadOrCreate(sourceTable);
            long existing = store.estimateTotalRecords(t, Expressions.alwaysTrue());
            if (existing < demoEvents) {
                store.generateDemoData(t, (int) (demoEvents - existing), demoBatch, existing);
            }
        }

        ActorSystem<Void> system = ActorSystem.create(Behaviors.empty(), "disaster-recovery-replay");
        ActorRef<ReplayJobManagerActor.Command> manager =
                system.systemActorOf(ReplayJobManagerActor.create(store, blockingIo), "job-manager");

        ApiServer api = new ApiServer(system, manager, httpPort, apiTimeout);
        api.start();

        ReceiverServer receiver = null;
        if (enableReceiver) {
            receiver = new ReceiverServer(receiverPort);
            receiver.start();
        }

        ReceiverServer finalReceiver = receiver;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                api.stop(0);
            } catch (Exception ignored) {
            }
            try {
                if (finalReceiver != null) {
                    finalReceiver.stop(0);
                }
            } catch (Exception ignored) {
            }
            try {
                system.terminate();
            } catch (Exception ignored) {
            }
            try {
                blockingIo.shutdownNow();
            } catch (Exception ignored) {
            }
        }));

        // Keep running.
        system.getWhenTerminated().toCompletableFuture().join();
    }
}

