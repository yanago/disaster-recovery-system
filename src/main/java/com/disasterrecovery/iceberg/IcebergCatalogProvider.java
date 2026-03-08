package com.disasterrecovery.iceberg;

import org.apache.hadoop.conf.Configuration;
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

        String warehousePath = IcebergPaths.warehousePath().toString();
        Map<String, String> props = new HashMap<>();
        props.put("warehouse", warehousePath);

        HadoopCatalog catalog = new HadoopCatalog(conf, warehousePath);
        catalog.initialize("hadoop", props);
        return catalog;
    }
}

