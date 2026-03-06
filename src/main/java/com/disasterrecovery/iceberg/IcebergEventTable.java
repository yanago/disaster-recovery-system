package com.disasterrecovery.iceberg;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;

public final class IcebergEventTable {

    private IcebergEventTable() {
    }

    public static final String FIELD_CID = "cid";
    public static final String FIELD_EVENT_TIMESTAMP = "event_timestamp";
    public static final String FIELD_EVENT_TIME = "event_time";
    public static final String FIELD_EVENT_TYPE = "event_type";
    public static final String FIELD_EVENT_ID = "event_id";

    public static Schema schema() {
        return new Schema(
                Types.NestedField.required(1, FIELD_CID, Types.StringType.get()),
                Types.NestedField.required(2, FIELD_EVENT_TIMESTAMP, Types.TimestampType.withZone()),
                Types.NestedField.required(3, FIELD_EVENT_TIME, Types.LongType.get()),
                Types.NestedField.required(4, FIELD_EVENT_TYPE, Types.StringType.get()),
                Types.NestedField.required(5, FIELD_EVENT_ID, Types.StringType.get())
        );
    }

    public static PartitionSpec spec(Schema schema) {
        // Unpartitioned keeps demo writes trivial and predictable.
        // (Partitioning can be added later; it requires writing files that respect partition values.)
        return PartitionSpec.unpartitioned();
    }

    public static TableIdentifier parseTableId(String table) {
        // Accept "namespace.table" or just "table" (defaults to "security.table").
        if (table == null || table.isBlank()) {
            return TableIdentifier.of(Namespace.of("security"), "events");
        }
        String trimmed = table.trim();
        if (!trimmed.contains(".")) {
            return TableIdentifier.of(Namespace.of("security"), trimmed);
        }
        String[] parts = trimmed.split("\\.");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid table identifier: " + table + " (expected namespace.table)");
        }
        return TableIdentifier.of(Namespace.of(parts[0]), parts[1]);
    }

    public static void ensureNamespace(Catalog catalog, Namespace ns) {
        try {
            catalog.createNamespace(ns);
        } catch (Exception ignored) {
            // Namespace already exists or catalog doesn't support namespaces; ignore for demo.
        }
    }
}

