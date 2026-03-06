package com.disasterrecovery.iceberg;

import java.nio.file.Path;
import java.nio.file.Paths;

public final class IcebergPaths {

    private IcebergPaths() {
    }

    public static Path warehousePath() {
        String raw = System.getenv().getOrDefault("WAREHOUSE_PATH", "./local-warehouse");
        return Paths.get(raw).toAbsolutePath().normalize();
    }
}

