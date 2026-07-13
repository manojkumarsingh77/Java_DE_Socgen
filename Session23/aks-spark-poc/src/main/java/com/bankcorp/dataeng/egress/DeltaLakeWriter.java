package com.bankcorp.dataeng.egress;

import com.bankcorp.dataeng.config.AppConfig;
import io.delta.tables.DeltaTable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * STAGE 2 of the medallion flow: CURATED ZONE (Delta Lake / ACID).
 *
 * Writes the reconciled ledger dataset as a partitioned Delta table on
 * ADLS Gen2 (or local disk in offline mode). Delta Lake provides:
 *   - ACID transactions (via the `_delta_log` transaction log)
 *   - Schema enforcement (rejects malformed reconciliation output)
 *   - Time travel (VERSION AS OF) for audit/regulatory replay - critical
 *     for Retail Banking regulatory reconciliation evidence trails.
 *
 * MERGE (upsert) is used instead of blind overwrite/append because ledger
 * reconciliation runs incrementally - a transaction that was PENDING in run N
 * may become RECONCILED in run N+1, and the curated table must reflect the
 * latest ledgerStatus per transactionId, not duplicate rows.
 */
public final class DeltaLakeWriter {

    private static final Logger LOG = LoggerFactory.getLogger(DeltaLakeWriter.class);

    private DeltaLakeWriter() {
    }

    public static void upsertCuratedLedger(SparkSession spark, Dataset<Row> reconciledBatch) {
        String curatedPath = AppConfig.curatedDeltaPath();

        boolean tableExists = DeltaTable.isDeltaTable(spark, curatedPath);

        if (!tableExists) {
            LOG.info("No existing Delta table at {}. Bootstrapping via initial write.", curatedPath);
            reconciledBatch
                    .write()
                    .format("delta")
                    .partitionBy("region")
                    .mode("overwrite")
                    .save(curatedPath);
            LOG.info("Delta table bootstrapped and partitioned by 'region' at {}", curatedPath);
            return;
        }

        LOG.info("Existing Delta table found at {}. Executing MERGE (upsert) on transactionId.", curatedPath);
        DeltaTable curatedTable = DeltaTable.forPath(spark, curatedPath);

        curatedTable.as("curated")
                .merge(reconciledBatch.as("incoming"), "curated.transactionId = incoming.transactionId")
                .whenMatched().updateAll()
                .whenNotMatched().insertAll()
                .execute();

        LOG.info("MERGE complete. Curated Delta table updated at {}", curatedPath);
    }

    /** Demonstrates Delta Lake time travel - used for regulatory audit replay. */
    public static Dataset<Row> readCuratedAsOfVersion(SparkSession spark, long version) {
        String curatedPath = AppConfig.curatedDeltaPath();
        return spark.read()
                .format("delta")
                .option("versionAsOf", version)
                .load(curatedPath);
    }
}
