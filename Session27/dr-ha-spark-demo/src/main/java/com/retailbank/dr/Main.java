package com.retailbank.dr;

import com.retailbank.dr.config.AppConfig;
import com.retailbank.dr.dr.DrDrillReport;
import com.retailbank.dr.dr.FailureDrillOrchestrator;
import com.retailbank.dr.generator.SyntheticBankingDataGenerator;
import com.retailbank.dr.replication.ReplicationEngine;
import com.retailbank.dr.util.ReportWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Entry point: "Cross-Region Failover Simulation" for a retail-banking transaction ledger.
 *
 * Execution phases:
 *   PHASE 0 — Spark session + Delta table bootstrap
 *   PHASE 1 — Steady-state ingestion: commit to PRIMARY, async-replicate to SECONDARY,
 *             periodic backups (RPO/RTO under normal operation ~ 0)
 *   PHASE 2 — Inject controlled failure at the configured batch (Failure Drill)
 *   PHASE 3 — Execute failover runbook (detect -> promote -> repoint)
 *   PHASE 4 — Compute + persist + print the RPO/RTO drill report
 *
 * Run locally with `sparkMaster=local[*]`; the identical jar runs unmodified in
 * cluster mode against AKS (see k8s/spark-application.yaml) with sparkMaster
 * supplied by spark-submit/the operator instead.
 */
public final class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        AppConfig config = AppConfig.defaultLocalDemo();

        SparkSession spark = SparkSession.builder()
                .appName(config.appName())
                .master(config.sparkMaster())
                // Delta Lake catalog + SQL extensions required for `format("delta")` and time travel
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.sql.shuffle.partitions", "8") // small local demo; tune for prod (200+ default)
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        SyntheticBankingDataGenerator generator = new SyntheticBankingDataGenerator(config, spark);
        ReplicationEngine engine = new ReplicationEngine(config);
        FailureDrillOrchestrator drill = new FailureDrillOrchestrator(config, engine);

        log.info("Bootstrapping empty Delta tables at:\n  PRIMARY   -> {}\n  SECONDARY -> {}",
                config.primaryTablePath(), config.secondaryTablePath());
        bootstrapEmptyDeltaTable(spark, config.primaryTablePath());
        bootstrapEmptyDeltaTable(spark, config.secondaryTablePath());

        Instant failureInjectedAt = null;
        boolean drillTriggered = false;

        try {
            for (int batchId = 1; batchId <= config.totalBatches(); batchId++) {

                if (!drillTriggered && batchId == config.failurePrimaryDownAtBatch() + 1) {
                    // ---- PHASE 2: inject failure right after the threshold batch committed ----
                    log.warn("### DR DRILL TRIGGER: injecting PRIMARY region failure before batch {} ###", batchId);
                    failureInjectedAt = drill.injectPrimaryFailure();
                    drillTriggered = true;
                }

                if (engine.isPrimaryDown()) {
                    // During the outage window, upstream systems queue/reject writes.
                    // We stop generating new primary commits and instead execute failover.
                    log.warn("Primary unavailable — halting new ingestion, proceeding to failover runbook.");
                    break;
                }

                Dataset<Row> batch = generator.generateBatch(batchId);
                batch.persist(); // reused for primary write, async replication, and count()

                engine.commitToPrimary(batch, batchId);
                Future<Void> replicationHandle = engine.replicateAsync(batch, batchId);
                engine.performBackupIfDue(batchId);

                // In steady state we don't block on replicationHandle (that's the point of
                // async DR replication) — but we keep the reference so a graceful shutdown
                // could optionally drain it. batch.unpersist() is deferred to keep the async
                // replication task's DataFrame reference valid.
                registerUnpersistOnCompletion(batch, replicationHandle);
            }

            // ---- PHASE 3 + 4: failover runbook + report ----
            if (failureInjectedAt == null) {
                // Defensive fallback: force a drill even if the batch loop finished first
                failureInjectedAt = drill.injectPrimaryFailure();
            }
            DrDrillReport report = drill.executeFailoverAndReport(failureInjectedAt);

            ReportWriter.printConsoleSummary(report);
            Path reportPath = ReportWriter.writeJson(report, config.reportOutputPath(),
                    "dr_drill_report_" + Instant.now().toEpochMilli() + ".json");
            log.info("Full report persisted to: {}", reportPath);

            System.exit(report.overallPassed() ? 0 : 1);

        } finally {
            engine.shutdown();
            spark.stop();
        }
    }

    /** Creates an empty, correctly-typed Delta table so downstream Append writes have a schema to match. */
    private static void bootstrapEmptyDeltaTable(SparkSession spark, Path path) {
        Dataset<Row> empty = spark.createDataFrame(
                java.util.Collections.emptyList(), SyntheticBankingDataGenerator.SCHEMA);
        empty.write().format("delta").mode("overwrite").save(path.toString());
    }

    private static void registerUnpersistOnCompletion(Dataset<Row> batch, Future<Void> handle) {
        Thread t = new Thread(() -> {
            try {
                handle.get();
            } catch (InterruptedException | ExecutionException e) {
                log.error("Async replication task failed", e);
            } finally {
                batch.unpersist();
            }
        });
        t.setDaemon(true);
        t.start();
    }
}
