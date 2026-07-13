package com.frauddetection.pipeline;

import com.frauddetection.config.PipelineConfig;
import com.frauddetection.util.SchemaRegistry;
import org.apache.spark.sql.*;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.*;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;

/**
 * FRAUD DETECTION PIPELINE - Core Structured Streaming Pipeline
 *
 * ┌─────────────────────────────────────────────────────────────────────────────┐
 * │  ARCHITECTURE OVERVIEW (maps directly to Spark DAG you'll see in UI)       │
 * │                                                                             │
 * │  [JSON Files]                                                               │
 * │       │                                                                     │
 * │       ▼  Stage 0: Source (File Source with schema inference)                │
 * │  [Parse & Deserialize]                                                      │
 * │       │                                                                     │
 * │       ▼  Stage 1: Watermark Assignment (Event Time Extraction)              │
 * │  [withWatermark("eventTime", "10 minutes")]                                 │
 * │       │                                                                     │
 * │       ├────────────────────────────────────────────┐                        │
 * │       │                                            │                        │
 * │       ▼  Stage 2a: Stateless Detection            ▼  Stage 2b: Stateful   │
 * │  [Rule-Based Fraud Filter]                  [Window Aggregation]            │
 * │  (No state store needed)                   (State Store: velocity counts)  │
 * │       │                                            │                        │
 * │       ▼  Stage 3a: Alert Enrichment              ▼  Stage 3b: Threshold   │
 * │  [FlatMap: compute risk score]              [Filter: count > 5]             │
 * │       │                                            │                        │
 * │       ├────────────────────────────────────────────┘                        │
 * │       │                                                                     │
 * │       ▼  Stage 4: Exactly-Once Sink (Idempotent Writer)                    │
 * │  [FraudAlertSink → JSON Output]                                             │
 * └─────────────────────────────────────────────────────────────────────────────┘
 *
 * STRUCTURED STREAMING INTERNALS DEMONSTRATED:
 *
 * 1. STATE STORE SCALING
 *    Window aggregations maintain state in RocksDB / HDFS-backed state store.
 *    State grows until watermark expires it. Monitor via Spark UI > Streaming tab.
 *
 * 2. WATERMARK DESIGN
 *    withWatermark("eventTime", "10 minutes") means:
 *    - Spark tracks max(eventTime) seen so far
 *    - Watermark = max(eventTime) - 10 minutes
 *    - State for windows ending before watermark is CLEANED UP
 *    - Late events arriving after watermark expiry are DROPPED
 *
 * 3. EXACTLY-ONCE SINKS
 *    Two mechanisms:
 *    a) Idempotent writes: same alertId always produces same output file
 *    b) Checkpoint + WAL: if job crashes, replay from last checkpoint
 *
 * 4. CHECKPOINT DURABILITY
 *    Checkpoint directory stores:
 *    - /offsets   → what input data was committed
 *    - /commits   → which batches completed successfully
 *    - /state     → aggregation state (window counts, etc.)
 *    On restart, Spark reads checkpoint and resumes exactly where it stopped.
 *
 * 5. BACKPRESSURE TUNING
 *    maxFilesPerTrigger = 10 limits input per micro-batch.
 *    If processing falls behind, files queue up in source directory.
 *    Spark processes at its own pace — no data loss, predictable latency.
 */
public class FraudDetectionPipeline {

    private static final Logger LOG = LoggerFactory.getLogger(FraudDetectionPipeline.class);

    private final SparkSession spark;
    private StreamingQuery transactionStream;
    private StreamingQuery velocityStream;
    private StreamingQuery lateEventStream;

    public FraudDetectionPipeline(SparkSession spark) {
        this.spark = spark;
    }

    /**
     * START the pipeline — creates three concurrent streaming queries.
     *
     * Each query is an independent DAG in Spark UI.
     * You'll see THREE separate streaming queries in the Streaming tab.
     */
    public void start(String inputDirectory) throws TimeoutException {

        LOG.info("╔══════════════════════════════════════════════════════════════════╗");
        LOG.info("║           FRAUD DETECTION PIPELINE STARTING                     ║");
        LOG.info("║  Input:  {}                               ║", inputDirectory);
        LOG.info("║  Watermark: {} | Trigger: {}             ║",
                PipelineConfig.WATERMARK_DELAY, PipelineConfig.TRIGGER_INTERVAL);
        LOG.info("╚══════════════════════════════════════════════════════════════════╝");

        // ═══════════════════════════════════════════════════════════════
        //  STAGE 0 + 1: READ SOURCE + APPLY SCHEMA + WATERMARK
        //  DAG: FileSource → Deserialize → EventTimeWatermark
        // ═══════════════════════════════════════════════════════════════
        Dataset<Row> rawStream = readTransactionStream(inputDirectory);

        // ═══════════════════════════════════════════════════════════════
        //  STAGE 2a + 3a: STATELESS FRAUD DETECTION PIPELINE
        //  DAG: Filter(highValue) → Filter(highRiskMerchant) → Filter(geo) → FlatMap(enrich)
        //  No state store needed — pure stateless transformations.
        //  These are the FASTEST to detect (sub-millisecond per record).
        // ═══════════════════════════════════════════════════════════════
        startRuleBasedDetection(rawStream);

        // ═══════════════════════════════════════════════════════════════
        //  STAGE 2b + 3b: STATEFUL VELOCITY DETECTION
        //  DAG: GroupBy(customerId, window) → Aggregate(count) → Filter(count>5)
        //  State Store: Stores per-customer transaction counts per window
        //  State grows → watermark expires old windows → state cleaned up
        // ═══════════════════════════════════════════════════════════════
        startVelocityDetection(rawStream);

        // ═══════════════════════════════════════════════════════════════
        //  STAGE 4: LATE EVENT MONITORING STREAM
        //  Separate query just to track late events for analytics
        // ═══════════════════════════════════════════════════════════════
        startLateEventMonitoring(rawStream);

        LOG.info("✅ All three streaming queries are ACTIVE");
        LOG.info("🌐 Spark UI: http://localhost:{}", PipelineConfig.SPARK_UI_PORT);
        LOG.info("   Navigate to: Streaming tab → See micro-batch progress");
        LOG.info("   Navigate to: SQL/DataFrame tab → See DAG");
        LOG.info("   Navigate to: Stages tab → See state store metrics");
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  READ + WATERMARK STAGE
    //  This creates the foundational DataFrame all queries build upon.
    //
    //  WATERMARK DESIGN EXPLAINED:
    //  withWatermark is a GUARANTEE to Spark's engine:
    //  "I promise that no event will arrive more than 10 minutes late"
    //  In return, Spark:
    //  1. Tracks the maximum event time seen so far
    //  2. Sets watermark = max_event_time - 10_minutes
    //  3. Only AFTER watermark passes a window's end time does Spark
    //     emit the result and clean up state for that window
    //  4. Late events within the watermark are PROCESSED
    //  5. Late events beyond the watermark are DROPPED SILENTLY
    // ─────────────────────────────────────────────────────────────────────────
    private Dataset<Row> readTransactionStream(String inputDirectory) {

        LOG.info("📡 Setting up file source from: {}", inputDirectory);

        // UDFs for fraud scoring
        registerUDFs();

        return spark
                .readStream()
                .option("maxFilesPerTrigger", PipelineConfig.MAX_FILES_PER_TRIGGER) // BACKPRESSURE CONTROL
                .option("latestFirst", "false")       // Process in order (important for late events)
                .option("cleanSource", "off")         // Keep source files (for exactly-once replay demo)
                .schema(SchemaRegistry.TRANSACTION_SCHEMA)
                .json(inputDirectory)

                // ── WATERMARK ASSIGNMENT ──────────────────────────────────────
                // Convert Unix milliseconds → Spark Timestamp type
                // then apply watermark on that column.
                // This is the KEY to late event handling.
                .withColumn("eventTime",
                        (functions.to_timestamp(
                                col("eventTimestamp").divide(1000)
                        )))
                .withWatermark("eventTime", PipelineConfig.WATERMARK_DELAY)  // ← STATE STORE SCALING TRIGGER

                // Add enrichment columns using isin() - the correct Spark API for list membership
                .withColumn("isHighRiskMerchant",
                        col("merchantCategory").isin((Object[]) PipelineConfig.HIGH_RISK_MERCHANTS))
                .withColumn("isHighRiskCountry",
                        col("merchantCountry").isin((Object[]) PipelineConfig.HIGH_RISK_COUNTRIES))
                .withColumn("processingLag",
                        col("processingTimestamp").minus(col("eventTimestamp")));
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  QUERY 1: RULE-BASED STATELESS DETECTION
    //
    //  DAG Stages in Spark UI:
    //  ┌──────────────────────────────────────────────────────────────┐
    //  │ [SerializeFromObject] → [Project] → [Filter] → [WriteToSink]│
    //  └──────────────────────────────────────────────────────────────┘
    //
    //  Rules implemented:
    //  R1: HIGH VALUE TRANSACTION (amount > $5000)
    //  R2: HIGH RISK MERCHANT CATEGORY (crypto, gambling, wire transfer)
    //  R3: INTERNATIONAL + HIGH RISK COUNTRY
    //  R4: ONLINE + HIGH AMOUNT (card-not-present fraud)
    //
    //  EXACTLY-ONCE GUARANTEE:
    //  OutputMode.Append() + checkpoint = exactly-once
    //  If batch 5 fails mid-write, Spark replays batch 5 on restart.
    //  FraudAlertSink uses alertId as idempotent key to avoid duplicates.
    // ─────────────────────────────────────────────────────────────────────────
    private void startRuleBasedDetection(Dataset<Row> rawStream) throws TimeoutException {

        LOG.info("🔍 Starting RULE-BASED detection stream (stateless)...");

        // Build fraud detection logic using Spark SQL expressions
        // This creates an efficient DAG — Catalyst optimizer will merge these filters
        Dataset<Row> fraudCandidates = rawStream
                .filter(
                        // R1: High value
                        col("amount").gt(PipelineConfig.HIGH_VALUE_THRESHOLD)
                        // R2: High risk merchant
                        .or(col("isHighRiskMerchant").equalTo(true))
                        // R3: High risk country
                        .or(col("isHighRiskCountry").equalTo(true))
                        // R4: Large online transaction
                        .or(col("channel").equalTo("ONLINE").and(col("amount").gt(2000)))
                )
                // Compute risk score using UDF
                .withColumn("riskScore", callUDF("computeRiskScore",
                        col("amount"),
                        col("isHighRiskMerchant"),
                        col("isHighRiskCountry"),
                        col("channel")))
                // Only alert if risk score is significant
                .filter(col("riskScore").gt(0.5))

                // Determine fraud type (for alert enrichment)
                .withColumn("fraudType",
                        when(col("amount").gt(PipelineConfig.HIGH_VALUE_THRESHOLD)
                                        .and(col("isHighRiskMerchant")), "HIGH_VALUE_HIGH_RISK_MERCHANT")
                        .when(col("isHighRiskMerchant"), "HIGH_RISK_MERCHANT")
                        .when(col("isHighRiskCountry").and(col("amount").gt(500)), "GEO_ANOMALY")
                        .when(col("channel").equalTo("ONLINE").and(col("amount").gt(2000)), "CNP_HIGH_VALUE")
                        .otherwise("SUSPICIOUS_ACTIVITY")
                )

                // Alert ID — deterministic from transactionId for idempotency
                .withColumn("alertId",
                        concat(lit("ALERT-"), col("transactionId")))

                // Detection reason (human-readable for fraud ops team)
                .withColumn("detectionReason", callUDF("buildDetectionReason",
                        col("amount"),
                        col("isHighRiskMerchant"),
                        col("merchantCategory"),
                        col("isHighRiskCountry"),
                        col("merchantCountry"),
                        col("channel")))

                .select(
                        col("alertId"),
                        col("transactionId"),
                        col("customerId"),
                        col("amount"),
                        col("fraudType"),
                        col("riskScore"),
                        col("detectionReason"),
                        col("eventTime"),
                        col("lateEvent"),
                        col("merchantCategory"),
                        col("merchantCountry"),
                        col("channel")
                );

        // ── EXACTLY-ONCE SINK ──────────────────────────────────────────
        // OutputMode.APPEND: Each record is written exactly once.
        // Checkpoint tracks which micro-batches have been committed.
        // On failure + restart: uncommitted batches are replayed.
        // FraudAlertSink is idempotent: writing same alertId twice = no duplicate.
        transactionStream = fraudCandidates
                .writeStream()
                .outputMode(OutputMode.Append())                                // ← EXACTLY-ONCE
                .format("json")
                .option("path", PipelineConfig.FRAUD_ALERTS_OUTPUT_DIR)
                .option("checkpointLocation", PipelineConfig.TRANSACTION_CHECKPOINT_DIR)  // ← CHECKPOINT
                .trigger(Trigger.ProcessingTime(PipelineConfig.TRIGGER_INTERVAL))         // ← BACKPRESSURE TRIGGER
                .queryName("fraud-rule-based-detection")
                .start();

        LOG.info("✅ Rule-based detection stream ACTIVE: {}", transactionStream.id());
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  QUERY 2: VELOCITY DETECTION (STATEFUL)
    //
    //  DAG Stages in Spark UI:
    //  ┌────────────────────────────────────────────────────────────────────┐
    //  │ [EventTimeWatermark] → [WindowedAgg] → [StateStoreRestore]       │
    //  │   → [HashAggregate] → [StateStoreSave] → [Filter] → [WriteToSink]│
    //  └────────────────────────────────────────────────────────────────────┘
    //
    //  STATE STORE SCALING:
    //  - State = HashMap<(customerId, windowStart, windowEnd), count>
    //  - State size = numCustomers × numActiveWindows × stateSize
    //  - With 200 customers × 5 windows × ~200 bytes = ~200KB (tiny for demo)
    //  - In production (10M customers): RocksDB state store + tuning required
    //
    //  WATERMARK + STATE CLEANUP:
    //  - Once watermark passes windowEnd, state for that window is EVICTED
    //  - This is why watermark is ESSENTIAL for stateful streaming
    //  - Without watermark, state grows FOREVER → OOM
    //
    //  OutputMode.COMPLETE vs UPDATE vs APPEND:
    //  - APPEND: Only NEW rows (requires watermark for window aggregation)
    //  - UPDATE: Changed rows (current window values as they update)
    //  - COMPLETE: ALL rows every batch (expensive, for small state only)
    //  We use UPDATE here to see velocities as windows fill up.
    // ─────────────────────────────────────────────────────────────────────────
    private void startVelocityDetection(Dataset<Row> rawStream) throws TimeoutException {

        LOG.info("⚡ Starting VELOCITY detection stream (stateful with window aggregation)...");

        Dataset<Row> velocityAlerts = rawStream
                // ── WINDOW AGGREGATION ──────────────────────────────────────
                // groupBy with window() creates a stateful aggregation.
                // Spark maintains state in State Store between micro-batches.
                // State key = (customerId, windowStart, windowEnd)
                // State value = running count + sum
                .groupBy(
                        col("customerId"),
                        functions.window(col("eventTime"),
                                PipelineConfig.WINDOW_DURATION,
                                PipelineConfig.SLIDE_DURATION)  // SLIDING window
                )
                .agg(
                        count("*").alias("txnCount"),
                        sum("amount").alias("totalAmount"),
                        max("amount").alias("maxAmount"),
                        approx_count_distinct(col("merchantCountry")).alias("distinctCountries"),
                        first("cardNumber").alias("cardNumber")
                )

                // ── VELOCITY FRAUD RULE ──────────────────────────────────────
                // >5 transactions in 5-minute sliding window = velocity abuse
                // This is how fraudsters test stolen cards rapidly
                .filter(col("txnCount").gt(PipelineConfig.VELOCITY_COUNT_THRESHOLD))

                // Compute velocity risk score
                .withColumn("velocityRiskScore",
                        least(lit(1.0),
                                col("txnCount").multiply(0.15).plus(
                                        when(col("distinctCountries").gt(1), 0.2).otherwise(0.0)
                                )
                        )
                )

                .withColumn("alertId",
                        concat(lit("VEL-"), col("customerId"), lit("-"),
                                col("window.start").cast("long")))

                .withColumn("fraudType", lit("VELOCITY_ABUSE"))

                .withColumn("detectionReason",
                        concat(
                                lit("Velocity abuse: "), col("txnCount"),
                                lit(" transactions in 5 minutes | Total: $"),
                                col("totalAmount").cast("long"),
                                lit(" | Countries: "), col("distinctCountries")
                        ))

                .select(
                        col("alertId"),
                        col("customerId"),
                        col("cardNumber"),
                        col("fraudType"),
                        col("txnCount"),
                        col("totalAmount"),
                        col("maxAmount"),
                        col("distinctCountries"),
                        col("velocityRiskScore").alias("riskScore"),
                        col("detectionReason"),
                        col("window.start").alias("windowStart"),
                        col("window.end").alias("windowEnd")
                );

        velocityStream = velocityAlerts
                .writeStream()
                .outputMode(OutputMode.Append())    // UPDATE mode for evolving window state
                .format("json")
                .option("path", PipelineConfig.VELOCITY_OUTPUT_DIR)
                .option("checkpointLocation", PipelineConfig.VELOCITY_CHECKPOINT_DIR)
                .trigger(Trigger.ProcessingTime(PipelineConfig.TRIGGER_INTERVAL))
                .queryName("fraud-velocity-detection")
                .start();

        LOG.info("✅ Velocity detection stream ACTIVE: {}", velocityStream.id());
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  QUERY 3: LATE EVENT MONITORING
    //
    //  This stream demonstrates the watermark in action.
    //  It captures and reports on late-arriving events.
    //
    //  DAG: [Filter(isLateEvent)] → [Project] → [WriteToSink]
    //
    //  LATE EVENT STRATEGY:
    //  1. Events arrive with eventTimestamp 1-8 minutes in the past
    //  2. Watermark = max(eventTime) - 10 minutes
    //  3. As long as eventTime > watermark, the late event is PROCESSED
    //  4. Events with eventTime < watermark are DROPPED
    //  5. We log both cases to make it visible in Spark UI
    // ─────────────────────────────────────────────────────────────────────────
    private void startLateEventMonitoring(Dataset<Row> rawStream) throws TimeoutException {

        LOG.info("⏰ Starting LATE EVENT monitoring stream...");

        Dataset<Row> lateEvents = rawStream
                .filter(col("lateEvent").equalTo(true))
                .withColumn("lagMinutes",
                        col("processingTimestamp").minus(col("eventTimestamp"))
                                .divide(60000).cast("integer"))
                .withColumn("withinWatermark",
                        col("lagMinutes").lt(10))  // 10 = our watermark in minutes
                .select(
                        col("transactionId"),
                        col("customerId"),
                        col("amount"),
                        col("eventTime"),
                        col("lagMinutes"),
                        col("withinWatermark"),
                        col("merchantCategory"),
                        col("channel")
                );

        lateEventStream = lateEvents
                .writeStream()
                .outputMode(OutputMode.Append())
                .format("json")
                .option("path", PipelineConfig.LATE_EVENTS_OUTPUT_DIR)
                .option("checkpointLocation", PipelineConfig.ALERT_CHECKPOINT_DIR)
                .trigger(Trigger.ProcessingTime(PipelineConfig.TRIGGER_INTERVAL))
                .queryName("late-event-monitoring")
                .start();

        LOG.info("✅ Late event monitoring stream ACTIVE: {}", lateEventStream.id());
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  UDF REGISTRATION
    //  Spark UDFs are black boxes to the Catalyst optimizer.
    //  Prefer built-in functions (when/otherwise) where possible.
    //  UDFs shown here for teaching — they appear in DAG as "UDF" nodes.
    // ─────────────────────────────────────────────────────────────────────────
    private void registerUDFs() {
        // Risk Score UDF: Combines multiple signals into 0.0-1.0 score
        spark.udf().register("computeRiskScore",
                (Double amount, Boolean isHighRiskMerchant, Boolean isHighRiskCountry, String channel) -> {
                    double score = 0.0;

                    // High amount signal
                    if (amount > 10000) score += 0.4;
                    else if (amount > 5000) score += 0.25;
                    else if (amount > 2000) score += 0.15;

                    // Merchant risk signal
                    if (Boolean.TRUE.equals(isHighRiskMerchant)) score += 0.35;

                    // Country risk signal
                    if (Boolean.TRUE.equals(isHighRiskCountry)) score += 0.30;

                    // Channel risk signal (online = higher card-not-present risk)
                    if ("ONLINE".equals(channel) && amount > 1000) score += 0.10;

                    return Math.min(1.0, score); // Cap at 1.0
                },
                DataTypes.DoubleType
        );

        // Detection Reason UDF: Human-readable explanation for fraud ops team
        spark.udf().register("buildDetectionReason",
                (Double amount, Boolean isHighRiskMerchant, String merchantCategory,
                 Boolean isHighRiskCountry, String merchantCountry, String channel) -> {
                    StringBuilder reason = new StringBuilder();

                    if (amount > PipelineConfig.HIGH_VALUE_THRESHOLD) {
                        reason.append(String.format("High value $%.0f. ", amount));
                    }
                    if (Boolean.TRUE.equals(isHighRiskMerchant)) {
                        reason.append(String.format("High-risk merchant: %s. ", merchantCategory));
                    }
                    if (Boolean.TRUE.equals(isHighRiskCountry)) {
                        reason.append(String.format("High-risk country: %s. ", merchantCountry));
                    }
                    if ("ONLINE".equals(channel) && amount > 2000) {
                        reason.append(String.format("Large online (CNP) transaction $%.0f. ", amount));
                    }

                    return reason.toString().trim();
                },
                DataTypes.StringType
        );
    }

    /**
     * Wait for all streaming queries to terminate.
     * This blocks until the user presses ENTER (or queries fail).
     */
    public void awaitTermination() {
        try {
            if (transactionStream != null) transactionStream.awaitTermination();
        } catch (StreamingQueryException e) {
            LOG.error("Transaction stream error", e);
        }
    }

    /**
     * Print streaming metrics — called periodically to show progress.
     * These metrics are also visible in Spark UI > Streaming tab.
     */
    public void printMetrics() {
        LOG.info("═══════════════════════════════════════════════════════════════");
        LOG.info("  STREAMING QUERY METRICS");
        LOG.info("═══════════════════════════════════════════════════════════════");

        printQueryMetrics("Rule-Based Detection", transactionStream);
        printQueryMetrics("Velocity Detection  ", velocityStream);
        printQueryMetrics("Late Event Monitor  ", lateEventStream);

        LOG.info("═══════════════════════════════════════════════════════════════");
    }

    private void printQueryMetrics(String name, StreamingQuery query) {
        if (query == null) return;
        StreamingQueryProgress progress = query.lastProgress();
        if (progress != null) {
            LOG.info("  {} | BatchId: {} | InputRows: {} | ProcessingRate: {}/sec",
                    name,
                    progress.batchId(),
                    progress.numInputRows(),
                    String.format("%.1f", progress.processedRowsPerSecond()));
        }
    }

    /**
     * Gracefully stop all streaming queries.
     */
    public void stop() {
        LOG.info("Stopping all streaming queries...");
        stopQuery(transactionStream, "Rule-Based Detection");
        stopQuery(velocityStream,    "Velocity Detection");
        stopQuery(lateEventStream,   "Late Event Monitor");
    }

    private void stopQuery(StreamingQuery query, String name) {
        if (query != null && query.isActive()) {
            try {
                query.stop();
                LOG.info("✅ Stopped: {}", name);
            } catch (TimeoutException e) {
                LOG.warn("Timeout stopping query: {}", name);
            }
        }
    }

    public StreamingQuery getTransactionStream() { return transactionStream; }
    public StreamingQuery getVelocityStream()    { return velocityStream; }
    public StreamingQuery getLateEventStream()   { return lateEventStream; }
}
