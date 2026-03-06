package com.disasterrecovery.iceberg;

import com.disasterrecovery.model.SecurityEvent;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import com.disasterrecovery.source.EventCursor;

public final class IcebergEventStore {

    private final Catalog catalog;

    public IcebergEventStore(Catalog catalog) {
        this.catalog = Objects.requireNonNull(catalog, "catalog");
    }

    public Table loadOrCreate(String tableName) {
        TableIdentifier id = IcebergEventTable.parseTableId(tableName);
        Namespace ns = id.namespace();
        IcebergEventTable.ensureNamespace(catalog, ns);

        try {
            return catalog.loadTable(id);
        } catch (Exception notFound) {
            Schema schema = IcebergEventTable.schema();
            PartitionSpec spec = IcebergEventTable.spec(schema);
            return catalog.createTable(id, schema, spec);
        }
    }

    public long estimateTotalRecords(Table table, Expression filter) {
        long total = 0;
        TableScan scan = table.newScan().filter(filter);
        try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
            for (FileScanTask task : tasks) {
                total += task.file().recordCount();
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to plan Iceberg scan files", e);
        }
        return total;
    }

    public CloseableIterable<Record> scan(Table table, Expression filter) {
        // Data API provides a streaming iterable of Records.
        return org.apache.iceberg.data.IcebergGenerics.read(table)
                .where(filter)
                .build();
    }

    public Expression buildFilter(String cid, Long startEventTime, Long endEventTime) {
        Expression expr = Expressions.alwaysTrue();
        if (cid != null && !cid.isBlank()) {
            expr = Expressions.and(expr, Expressions.equal(IcebergEventTable.FIELD_CID, cid));
        }
        if (startEventTime != null) {
            expr = Expressions.and(expr, Expressions.greaterThanOrEqual(IcebergEventTable.FIELD_EVENT_TIME, startEventTime));
        }
        if (endEventTime != null) {
            expr = Expressions.and(expr, Expressions.lessThan(IcebergEventTable.FIELD_EVENT_TIME, endEventTime));
        }
        return expr;
    }

    public SecurityEvent toEvent(Record r) {
        String cid = (String) r.getField(IcebergEventTable.FIELD_CID);
        Instant ts = (Instant) r.getField(IcebergEventTable.FIELD_EVENT_TIMESTAMP);
        Long eventTime = (Long) r.getField(IcebergEventTable.FIELD_EVENT_TIME);
        String type = (String) r.getField(IcebergEventTable.FIELD_EVENT_TYPE);
        String id = (String) r.getField(IcebergEventTable.FIELD_EVENT_ID);
        return new SecurityEvent(cid, ts, eventTime, type, id);
    }

    public void appendEvents(Table table, List<SecurityEvent> events, String dataFileNameHint) {
        if (events.isEmpty()) {
            return;
        }

        Schema schema = table.schema();
        PartitionSpec spec = table.spec();

        String fileName = (dataFileNameHint == null || dataFileNameHint.isBlank())
                ? java.util.UUID.randomUUID().toString()
                : dataFileNameHint.trim() + "-" + java.util.UUID.randomUUID();

        String filePath = table.location() + "/data/" + fileName + ".parquet";
        OutputFile outputFile = table.io().newOutputFile(filePath);

        long recordCount = 0;
        try (FileAppender<Record> appender = Parquet.writeData(outputFile)
                .schema(schema)
                .createWriterFunc(GenericParquetWriter::buildWriter)
                .overwrite()
                .build()) {
            for (SecurityEvent e : events) {
                GenericRecord rec = GenericRecord.create(schema);
                rec.setField(IcebergEventTable.FIELD_CID, e.getCid());
                rec.setField(IcebergEventTable.FIELD_EVENT_TIMESTAMP, e.getEventTimestamp());
                rec.setField(IcebergEventTable.FIELD_EVENT_TIME, e.getEventTime());
                rec.setField(IcebergEventTable.FIELD_EVENT_TYPE, e.getEventType());
                rec.setField(IcebergEventTable.FIELD_EVENT_ID, e.getEventId());
                appender.add(rec);
                recordCount++;
            }
        } catch (IOException ex) {
            throw new RuntimeException("Failed to write parquet data file for Iceberg table", ex);
        }

        long fileSize;
        try {
            fileSize = outputFile.toInputFile().getLength();
        } catch (IOException e) {
            throw new RuntimeException("Failed to stat Iceberg data file size", e);
        }

        DataFile dataFile = DataFiles.builder(spec)
                .withPath(filePath)
                .withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(fileSize)
                .withRecordCount(recordCount)
                .build();

        table.newAppend()
                .appendFile(dataFile)
                .commit();
    }

    public void generateDemoData(Table table, int eventsCount, int batchSize) {
        generateDemoData(table, eventsCount, batchSize, 0);
    }

    public void generateDemoData(Table table, int eventsCount, int batchSize, long startIndex) {
        int safeBatch = Math.max(1, batchSize);
        int total = Math.max(0, eventsCount);

        long base = Instant.parse("2024-10-15T00:00:00.000Z").toEpochMilli();
        String[] types = new String[]{"ProcessStart", "NetworkConnect", "FileWrite", "RegistrySet", "ProcessExit"};
        String[] cids = new String[]{"customer-a1b2c3", "customer-d4e5f6", "customer-112233"};

        List<SecurityEvent> batch = new ArrayList<>(safeBatch);
        for (int i = 0; i < total; i++) {
            long idx = startIndex + i;
            long eventTime = base + (idx * 25L); // spaced; makes throttling demo visible
            Instant ts = Instant.ofEpochMilli(eventTime);
            String cid = cids[i % cids.length];
            String type = types[i % types.length];
            String eventId = java.util.UUID.randomUUID().toString();
            batch.add(new SecurityEvent(cid, ts, eventTime, type, eventId));

            if (batch.size() >= safeBatch) {
                appendEvents(table, batch, "demo");
                batch.clear();
            }
        }
        if (!batch.isEmpty()) {
            appendEvents(table, batch, "demo");
        }
    }

    public Iterator<SecurityEvent> scanAsEvents(Table table, Expression filter) {
        CloseableIterable<Record> iterable = scan(table, filter);
        // The caller must close the iterable; we wrap to expose only Iterator but keep reference.
        return new Iterator<>() {
            private final Iterator<Record> it = iterable.iterator();

            @Override
            public boolean hasNext() {
                boolean has = it.hasNext();
                if (!has) {
                    try {
                        iterable.close();
                    } catch (IOException ignored) {
                    }
                }
                return has;
            }

            @Override
            public SecurityEvent next() {
                return toEvent(it.next());
            }
        };
    }

    public EventCursor openCursor(Table table, Expression filter) {
        CloseableIterable<Record> iterable = scan(table, filter);
        Iterator<Record> it = iterable.iterator();
        return new EventCursor() {
            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public SecurityEvent next() {
                return toEvent(it.next());
            }

            @Override
            public void close() {
                try {
                    iterable.close();
                } catch (IOException ignored) {
                }
            }
        };
    }
}

