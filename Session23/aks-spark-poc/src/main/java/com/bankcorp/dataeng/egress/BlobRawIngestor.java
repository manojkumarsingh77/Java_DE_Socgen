package com.bankcorp.dataeng.egress;

import com.bankcorp.dataeng.config.AppConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * STAGE 1 of the medallion flow: RAW ZONE.
 *
 * Writes the synthetic ledger events as-is (no transformation, no dedup) to
 * the RAW zone, mimicking a core-banking mainframe extract landing in
 * Azure Blob Storage (`raw-<env>` container) before any Spark processing
 * occurs. Format = JSON (line-delimited) to mirror how real core-banking
 * extracts typically arrive (unstructured/semi-structured, not yet
 * schema-enforced).
 *
 * When AppConfig.AZURE_MODE=true, the same `.write()` call transparently
 * targets `wasbs://raw-<env>@<account>.blob.core.windows.net/...` because
 * Hadoop-Azure's `NativeAzureFileSystem` is registered on the classpath
 * (hadoop-azure dependency in pom.xml) and Spark resolves the URI scheme
 * automatically - ZERO code change between local and cloud execution.
 */
public final class BlobRawIngestor {

    private static final Logger LOG = LoggerFactory.getLogger(BlobRawIngestor.class);

    private BlobRawIngestor() {
    }

    public static void landInRawZone(Dataset<Row> syntheticData) {
        String rawPath = AppConfig.rawZonePath();
        LOG.info("Landing {} rows into RAW zone at: {}", syntheticData.count(), rawPath);

        syntheticData
                .write()
                .mode(SaveMode.Overwrite)
                .json(rawPath);

        LOG.info("RAW zone write complete. Source system simulation: CORE-BANKING-MAINFRAME-EXTRACT");
    }

    public static Dataset<Row> readFromRawZone(org.apache.spark.sql.SparkSession spark) {
        String rawPath = AppConfig.rawZonePath();
        LOG.info("Reading RAW zone from: {}", rawPath);
        return spark.read().json(rawPath);
    }
}
