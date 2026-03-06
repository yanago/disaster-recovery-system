package com.disasterrecovery.source;

import com.disasterrecovery.model.SecurityEvent;

/**
 * Streaming cursor over events from a data source.
 */
public interface EventCursor extends AutoCloseable {

    boolean hasNext();

    SecurityEvent next();

    @Override
    void close();
}

