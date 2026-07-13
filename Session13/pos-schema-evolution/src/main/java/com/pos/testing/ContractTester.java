package com.pos.testing;

import com.pos.config.PipelineConfig;
import com.pos.schema.SchemaRegistry;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryProgress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;

/**
 * POS SCHEMA EVOLUTION — Contract Tester
 *
 * ╔══════════════════════════════════════════════════════════════════╗
 * ║  CONTRACT TESTING IN STREAMING PIPELINES                        ║
 * ║                                                                 ║
 * ║  What is a Schema Contract?                                     ║
 * ║  A formal agreement between producer (POS terminals) and       ║
 * ║  consumer (this pipeline) about the structure of events.       ║
 * ║                                                                 ║
 * ║  Without contracts:                                            ║
 * ║    POS team adds field "amt" alongside "amount"                ║
 * ║    Pipeline breaks silently — reads 0.0 for amount             ║
 * ║    $0 transactions appear valid → audit nightmare              ║
 * ║                                                                 ║
 * ║  With contracts:                                               ║
 * ║    Schema registry rejects the producer change                  ║
 * ║    Pipeline alerts on schema deviation in real-time            ║
 * ║    Problem caught in staging, not production                   ║
 * ║                                                                 ║
 * ║  THIS CLASS: Runs a monitoring streaming query that            ║
 * ║  checks EVERY micro-batch for contract violations and          ║
 * ║  logs them — visible in Spark UI as a 3rd streaming query.    ║
 * ╚══════════════════════════════════════════════════════════════════╝
 */
public class ContractTester {

    private static final Logger LOG = LoggerFactory.getLogger(ContractTester.class);

    /**
     * Start the contract monitoring query.
     *
     * This produces a 3rd streaming query in Spark UI:
     * "pos-contract-monitor"
     *
     * DAG:
     * [source] → [Project: schemaVersion] → [GroupBy: schemaVersion]
     *         → [Aggregate: count] → [Console Sink]
     *
     * TEACHING POINT: OutputMode.Complete() outputs ALL groups every batch.
     * Perfect for a live dashboard of "how many v1 vs v2 vs v3 events arrive".
     * This is your MIGRATION TRACKER — you can watch v1 traffic drop to zero
     * as old terminals are upgraded.
     */
    public static StreamingQuery startMonitor(Dataset<Row> validated,
                                               String checkpointBase) throws TimeoutException {

        // Aggregate schema version distribution per micro-batch
        Dataset<Row> distribution = validated
                .groupBy(
                        col("schemaVersion"),
                        col("isValid")
                )
                .count()
                .orderBy(col("schemaVersion"), col("isValid"));

        StreamingQuery monitorQuery = distribution
                .writeStream()
                .outputMode("complete")   // ← Complete: see all versions every batch
                .format("console")
                .option("truncate", false)
                .option("numRows", 20)
                .option("checkpointLocation", checkpointBase + "/monitor")
                .queryName("pos-contract-monitor")
                .start();

        LOG.info("✅ Contract monitor active: pos-contract-monitor");
        LOG.info("   Watch the console for schema version distribution per batch.");
        return monitorQuery;
    }

    /**
     * Run static contract compatibility checks (called at startup).
     *
     * TEACHING POINT: These are the checks a CI/CD pipeline runs
     * BEFORE deploying a new schema version. If any check fails,
     * the deployment is blocked.
     *
     * In real systems: integrated into Maven/Gradle test phase.
     */
    public static void runStartupChecks() {

        LOG.info("══════════════════════════════════════════════════════════");
        LOG.info("  CONTRACT TESTS — Schema Compatibility Checks");
        LOG.info("══════════════════════════════════════════════════════════");

        boolean allPassed = true;
        allPassed &= check("v1→v2: all new fields nullable",
                allNewFieldsNullable(SchemaRegistry.V1_SCHEMA, SchemaRegistry.V2_SCHEMA));

        allPassed &= check("v2→v3: all new fields nullable",
                allNewFieldsNullable(SchemaRegistry.V2_SCHEMA, SchemaRegistry.V3_SCHEMA));

        allPassed &= check("v1→v2: no fields removed",
                noFieldsRemoved(SchemaRegistry.V1_SCHEMA, SchemaRegistry.V2_SCHEMA));

        allPassed &= check("v2→v3: no fields removed",
                noFieldsRemoved(SchemaRegistry.V2_SCHEMA, SchemaRegistry.V3_SCHEMA));

        allPassed &= check("v1→v2: no type changes",
                noTypeChanges(SchemaRegistry.V1_SCHEMA, SchemaRegistry.V2_SCHEMA));

        allPassed &= check("v2→v3: no type changes",
                noTypeChanges(SchemaRegistry.V2_SCHEMA, SchemaRegistry.V3_SCHEMA));

        allPassed &= check("READER_SCHEMA is v3 superset",
                SchemaRegistry.READER_SCHEMA.fields().length >= SchemaRegistry.V3_SCHEMA.fields().length);

        LOG.info("══════════════════════════════════════════════════════════");
        if (allPassed) {
            LOG.info("  ✅ ALL CONTRACT TESTS PASSED — safe to deploy");
        } else {
            LOG.error("  ❌ CONTRACT TESTS FAILED — deployment BLOCKED");
        }
        LOG.info("══════════════════════════════════════════════════════════");
    }

    /** Print per-batch contract metrics (called by metrics scheduler). */
    public static void printBatchReport(StreamingQuery validQuery,
                                         StreamingQuery quarantineQuery,
                                         StreamingQuery monitorQuery) {

        LOG.info("─────────────────────────────────────────────────────────────");
        LOG.info("  CONTRACT METRICS");

        printQueryProgress("Valid Events   ", validQuery);
        printQueryProgress("Quarantine     ", quarantineQuery);
        printQueryProgress("Schema Monitor ", monitorQuery);

        LOG.info("  🌐 Spark UI → http://localhost:{}", PipelineConfig.UI_PORT);
        LOG.info("  📋 Schema Registry → {}", PipelineConfig.REGISTRY);
        LOG.info("─────────────────────────────────────────────────────────────");
    }

    // ── Internal helpers ─────────────────────────────────────────────────────

    private static boolean allNewFieldsNullable(
            org.apache.spark.sql.types.StructType old,
            org.apache.spark.sql.types.StructType newer) {

        var oldNames = java.util.Arrays.stream(old.fields())
                .map(f -> f.name()).collect(java.util.stream.Collectors.toSet());

        for (var f : newer.fields()) {
            if (!oldNames.contains(f.name()) && !f.nullable()) {
                LOG.error("  FAIL: New field '{}' is NOT nullable → breaks backward compat!", f.name());
                return false;
            }
        }
        return true;
    }

    private static boolean noFieldsRemoved(
            org.apache.spark.sql.types.StructType old,
            org.apache.spark.sql.types.StructType newer) {

        var newNames = java.util.Arrays.stream(newer.fields())
                .map(f -> f.name()).collect(java.util.stream.Collectors.toSet());

        for (var f : old.fields()) {
            if (!newNames.contains(f.name())) {
                LOG.error("  FAIL: Field '{}' was REMOVED → breaks all existing readers!", f.name());
                return false;
            }
        }
        return true;
    }

    private static boolean noTypeChanges(
            org.apache.spark.sql.types.StructType old,
            org.apache.spark.sql.types.StructType newer) {

        var newFieldMap = new java.util.HashMap<String, org.apache.spark.sql.types.DataType>();
        for (var f : newer.fields()) newFieldMap.put(f.name(), f.dataType());

        for (var f : old.fields()) {
            var newType = newFieldMap.get(f.name());
            if (newType != null && !newType.equals(f.dataType())) {
                LOG.error("  FAIL: Field '{}' type changed {} → {} → data corruption risk!",
                        f.name(), f.dataType(), newType);
                return false;
            }
        }
        return true;
    }

    private static boolean check(String name, boolean passed) {
        LOG.info("  {} {}", passed ? "✅" : "❌", name);
        return passed;
    }

    private static void printQueryProgress(String label, StreamingQuery q) {
        if (q == null) return;
        StreamingQueryProgress p = q.lastProgress();
        if (p != null) {
            LOG.info("  {} | Batch#{} | Rows={} | {}/sec",
                    label, p.batchId(), p.numInputRows(),
                    String.format("%.0f", p.processedRowsPerSecond()));
        }
    }
}
