package com.disasterrecovery.sink;

import com.disasterrecovery.model.SecurityEvent;

import java.util.concurrent.CompletionStage;

public interface EventSink extends AutoCloseable {

    CompletionStage<Void> send(SecurityEvent event);

    @Override
    void close();
}

