package com.disasterrecovery.iceberg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hadoop.HadoopCatalog;

import java.util.HashMap;
import java.util.Map;

public final class IcebergCatalogProvider {

    private IcebergCatalogProvider() {
    }

    public static Catalog createHadoopCatalog() {
        Configuration conf = new Configuration();
        // Local filesystem for demo; in k8s this is backed by a PV mount.
        conf.set("fs.defaultFS", "file:///");

        HadoopCatalog catalog = new HadoopCatalog(conf, new Path(IcebergPaths.warehousePath().toString()));

        // Keep this around so future catalog types (REST, Nessie) can be swapped in.
        Map<String, String> props = new HashMap<>();
        catalog.initialize("hadoop", props);
        return catalog;
    }
}

