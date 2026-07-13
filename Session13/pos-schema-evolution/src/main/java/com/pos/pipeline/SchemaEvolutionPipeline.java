package com.pos.pipeline;

import com.pos.config.PipelineConfig;
import com.pos.schema.SchemaRegistry;
import com.pos.validator.DataQualityValidator;
import com.pos.testing.ContractTester;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;

/**
 * POS SCHEMA EVOLUTION — Core Pipeline
 *
 * ╔══════════════════════════════════════════════════════════════════════╗
 * ║  FULL DAG (3 Streaming Queries — all visible in Spark UI)          ║
 * ║                                                                     ║
 * ║  SOURCE: JSON files (multi-version: v1, v2, v3, corrupt)          ║
 * ║    │                                                                ║
 * ║    ▼  Stage 0: FileScan JSON                                       ║
 * ║  [Read with READER_SCHEMA (permissive superset)]                   ║
 * ║    │  Mode=PERMISSIVE → bad JSON goes to _corrupt_record column    ║
 * ║    │                                                                ║
 * ║    ▼  Stage 1: Project (schema enforcement enrichment)             ║
 * ║  [withColumn schemaVersion — detects v1/v2/v3 from payload]       ║
 * ║    │                                                                ║
 * ║    ▼  Stage 2: Project (data quality rules)                        ║
 * ║  [withColumn r1_completeness / r2_validity / r3_conformity / r4]  ║
 * ║  [withColumn validationError, isValid]                             ║
 * ║    │                                                                ║
 * ║    ├──────────────────────────────────────────────┐               ║
 * ║    │                                              │               ║
 * ║    ▼  Query 1                                     ▼  Query 2      ║
 * ║  [Filter isValid=true]                      [Filter isValid=false] ║
 * ║  [WriteToSink: valid JSON]                  [WriteToSink: quarantine]║
 * ║    │                                                                ║
 * ║    ▼  Query 3: Contract Monitor                                    ║
 * ║  [GroupBy schemaVersion, isValid]                                  ║
 * ║  [Aggregate count]                                                 ║
 * ║  [Console Sink — shows version distribution per batch]            ║
 * ╚══════════════════════════════════════════════════════════════════════╝
 *
 * SCHEMA ENFORCEMENT (two modes in Spark):
 *
 * Mode 1: FAILFAST  → entire batch fails if ANY record violates schema
 *   → Use in batch ETL where you control the source
 *   → Too strict for streaming (one bad record kills the stream)
 *
 * Mode 2: PERMISSIVE (default) → bad records go to _corrupt_record column
 *   → Use in streaming (we use this)
 *   → Bad records are handled gracefully → quarantine
 *   → Stream never stops due to one malformed event
 *
 * Mode 3: DROPMALFORMED → silently drops bad records
 *   → NEVER use in production streaming (silent data loss = audit nightmare)
 */
public class SchemaEvolutionPipeline {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaEvolutionPipeline.class);

    private StreamingQuery validQuery;
    private StreamingQuery quarantineQuery;
    private StreamingQuery monitorQuery;

    public void start(SparkSession spark, String sourceDir) throws TimeoutException {

        LOG.info("Starting Schema Evolution Pipeline...");

        // ══════════════════════════════════════════════════════════════════
        //  STAGE 0 + 1: READ SOURCE WITH SCHEMA ENFORCEMENT
        //
        //  KEY DECISION: Which schema do we use to READ the stream?
        //
        //  Option A: V1_SCHEMA → fails on v2/v3 records (too strict)
        //  Option B: V2_SCHEMA → fails on v3 records (still too strict)
        //  Option C: READER_SCHEMA (V3 superset) → handles ALL versions
        //
        //  We use Option C = READER_SCHEMA
        //  This is SCHEMA ENFORCEMENT via explicit schema declaration.
        //
        //  PERMISSIVE mode: records that don't parse at all go to
        //  _corrupt_record column instead of crashing the stream.
        //
        //  DAG node: [FileScan JSON] with schema projection pushdown
        // ══════════════════════════════════════════════════════════════════
        Dataset<Row> rawStream = spark
                .readStream()
                .option("maxFilesPerTrigger", PipelineConfig.MAX_FILES_PER_TRIGGER)
                .option("mode", "PERMISSIVE")             // ← safe schema enforcement
                .option("columnNameOfCorruptRecord", "_corrupt_record")
                .schema(SchemaRegistry.READER_SCHEMA      // ← explicit, versioned schema
                        // Add _corrupt_record field to capture parse failures
                        .add("_corrupt_record", "string", true))
                .json(sourceDir)

                // Filter out completely unparseable records (put to quarantine separately)
                // coalesce: for v1 records, optional v2/v3 fields will be null
                // This is the safe optional field handling pattern
                .withColumn("amount",
                        coalesce(col("amount"), lit(0.0)));  // default 0 triggers validity rule

        // ══════════════════════════════════════════════════════════════════
        //  STAGE 2: DATA VALIDATION RULES
        //  Applies all 4 validation pillars, adds isValid + validationError
        //  DAG: [Project(schemaVersion)] → [Project(rules)] → [Project(isValid)]
        // ══════════════════════════════════════════════════════════════════
        Dataset<Row> validated = DataQualityValidator.validate(rawStream);

        // ══════════════════════════════════════════════════════════════════
        //  STAGE 3: BRANCH INTO SINKS (2 queries from same validated stream)
        //  Spark caches the validated plan and shares it between both queries
        // ══════════════════════════════════════════════════════════════════
        StreamingQuery[] sinks = DataQualityValidator.startSinks(
                validated,
                PipelineConfig.VALID_OUT,
                PipelineConfig.QUARANTINE,
                PipelineConfig.CHECKPOINT
        );
        validQuery     = sinks[0];
        quarantineQuery = sinks[1];

        // ══════════════════════════════════════════════════════════════════
        //  STAGE 4: CONTRACT MONITORING QUERY (3rd streaming query)
        //  groupBy(schemaVersion, isValid) + count → console
        //  OutputMode.COMPLETE: shows ALL schema versions every batch
        // ══════════════════════════════════════════════════════════════════
        monitorQuery = ContractTester.startMonitor(validated, PipelineConfig.CHECKPOINT);

        LOG.info("╔══════════════════════════════════════════════════════════╗");
        LOG.info("║  3 STREAMING QUERIES ACTIVE                             ║");
        LOG.info("║  1. pos-valid-events      → valid sink                  ║");
        LOG.info("║  2. pos-quarantine-events → quarantine sink             ║");
        LOG.info("║  3. pos-contract-monitor  → console (version dashboard) ║");
        LOG.info("║                                                         ║");
        LOG.info("║  🌐 http://localhost:{}/streaming                    ║", PipelineConfig.UI_PORT);
        LOG.info("╚══════════════════════════════════════════════════════════╝");
    }

    public void printMetrics() {
        ContractTester.printBatchReport(validQuery, quarantineQuery, monitorQuery);
    }

    public void stop() {
        stopQuery(validQuery,      "pos-valid-events");
        stopQuery(quarantineQuery, "pos-quarantine-events");
        stopQuery(monitorQuery,    "pos-contract-monitor");
    }

    private void stopQuery(StreamingQuery q, String name) {
        if (q != null && q.isActive()) {
            try { q.stop(); LOG.info("✅ Stopped: {}", name); }
            catch (TimeoutException e) { LOG.warn("Timeout stopping {}", name); }
        }
    }

    public StreamingQuery getValidQuery()      { return validQuery; }
    public StreamingQuery getQuarantineQuery() { return quarantineQuery; }
    public StreamingQuery getMonitorQuery()    { return monitorQuery; }
}
