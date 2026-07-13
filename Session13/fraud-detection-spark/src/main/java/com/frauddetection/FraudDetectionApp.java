package com.frauddetection;

import com.frauddetection.config.PipelineConfig;
import com.frauddetection.generator.TransactionGenerator;
import com.frauddetection.pipeline.FraudDetectionPipeline;
import com.frauddetection.sink.FraudAlertSink;
import com.frauddetection.util.SparkSessionFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryProgress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Scanner;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * ╔══════════════════════════════════════════════════════════════════════════╗
 * ║                                                                          ║
 * ║   FRAUD DETECTION PIPELINE                                               ║
 * ║   Apache Spark Structured Streaming — Corporate Training Demo            ║
 * ║                                                                          ║
 * ║   BUSINESS REQUIREMENT                                                   ║
 * ║   ──────────────────────────────────────────────────────────────────     ║
 * ║   GlobalBank Corp processes 2 million card transactions per minute       ║
 * ║   across 50 countries. Current fraud detection is batch-based:           ║
 * ║   fraudulent charges are detected 4-6 hours AFTER the transaction.       ║
 * ║   This results in $180M annual fraud losses and 12,000 compromised       ║
 * ║   customers per month.                                                   ║
 * ║                                                                          ║
 * ║   PROBLEM STATEMENT                                                      ║
 * ║   ──────────────────────────────────────────────────────────────────     ║
 * ║   Design and implement a real-time fraud detection pipeline that:         ║
 * ║   • Detects fraud within 5 seconds (P99 latency SLA)                    ║
 * ║   • Handles late-arriving events from mobile apps (up to 10 min late)   ║
 * ║   • Guarantees exactly-once alert delivery (no missed or duplicate)      ║
 * ║   • Recovers from failures without data loss (checkpoint durability)     ║
 * ║   • Handles 10x traffic spikes during peak hours (backpressure tuning)  ║
 * ║   • Maintains stateful velocity fraud detection at scale (state store)   ║
 * ║                                                                          ║
 * ║   TECHNICAL SOLUTION: Apache Spark Structured Streaming                  ║
 * ║                                                                          ║
 * ║   TOPICS COVERED IN THIS DEMO:                                           ║
 * ║   ① Structured Streaming Internals                                       ║
 * ║   ② State Store Scaling (velocity window aggregation)                   ║
 * ║   ③ Watermark Design (10-min late event handling)                       ║
 * ║   ④ Exactly-Once Sinks (checkpoint + idempotent writes)                 ║
 * ║   ⑤ Checkpoint Durability (failure recovery)                            ║
 * ║   ⑥ Backpressure Tuning (maxFilesPerTrigger, trigger intervals)        ║
 * ║                                                                          ║
 * ╚══════════════════════════════════════════════════════════════════════════╝
 */
public class FraudDetectionApp {

    private static final Logger LOG = LoggerFactory.getLogger(FraudDetectionApp.class);

    // Transaction source directory — generator writes here, Spark reads from here
    private static final String TRANSACTION_SOURCE_DIR = "/tmp/fraud-detection-source";

    public static void main(String[] args) throws Exception {

        // ── STEP 0: PRINT BANNER ─────────────────────────────────────────────
        printBanner();

        // ── STEP 1: CLEAN UP PREVIOUS RUNS ──────────────────────────────────
        LOG.info("STEP 1: Cleaning up previous demo artifacts...");
        cleanupPreviousRun();

        // ── STEP 2: INITIALIZE OUTPUT DIRECTORIES ────────────────────────────
        LOG.info("STEP 2: Initialising output and checkpoint directories...");
        FraudAlertSink.initialize();
        Files.createDirectories(Paths.get(TRANSACTION_SOURCE_DIR));

        // ── STEP 3: CREATE SPARK SESSION ─────────────────────────────────────
        LOG.info("STEP 3: Creating SparkSession (Java 17 + M1 Mac optimised)...");
        SparkSession spark = SparkSessionFactory.create();

        // ── STEP 4: CREATE PIPELINE AND GENERATOR ────────────────────────────
        LOG.info("STEP 4: Creating Fraud Detection Pipeline...");
        FraudDetectionPipeline pipeline = new FraudDetectionPipeline(spark);

        LOG.info("STEP 4b: Creating Transaction Generator (simulates bank feed)...");
        TransactionGenerator generator = new TransactionGenerator(TRANSACTION_SOURCE_DIR);

        // ── STEP 5: START PIPELINE ────────────────────────────────────────────
        LOG.info("STEP 5: Starting all Structured Streaming queries...");
        pipeline.start(TRANSACTION_SOURCE_DIR);

        // Small pause to let queries initialize
        Thread.sleep(3000);

        // ── STEP 6: START DATA GENERATOR ─────────────────────────────────────
        LOG.info("STEP 6: Starting transaction data generator...");
        generator.start();

        // ── STEP 7: SCHEDULE METRICS REPORTING ───────────────────────────────
        ScheduledExecutorService metricsScheduler = startMetricsReporting(pipeline, generator, spark);

        // ── STEP 8: PRINT NAVIGATION GUIDE ───────────────────────────────────
        Thread.sleep(5000); // Let first batches process
        printSparkUINavigationGuide();
        printDAGExplainer();

        // ── STEP 9: INTERACTIVE MENU ──────────────────────────────────────────
        // Process runs CONTINUOUSLY until user presses ENTER
        runInteractiveMenu(pipeline, generator, spark);

        // ── STEP 10: GRACEFUL SHUTDOWN ────────────────────────────────────────
        LOG.info("STEP 10: Graceful shutdown initiated...");
        metricsScheduler.shutdownNow();
        generator.stop();

        // Final summary before shutdown
        printFinalSummary(pipeline, generator, spark);

        pipeline.stop();

        Thread.sleep(2000); // Allow final flush

        spark.stop();

        LOG.info("╔══════════════════════════════════════════════════════════════╗");
        LOG.info("║  DEMO COMPLETE — Thank you!                                  ║");
        LOG.info("║  Output files: /tmp/fraud-detection-output/                  ║");
        LOG.info("║  Checkpoints:  /tmp/fraud-detection-checkpoints/             ║");
        LOG.info("╚══════════════════════════════════════════════════════════════╝");
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  INTERACTIVE MENU
    //  Runs until user presses ENTER. Shows available commands for the demo.
    // ─────────────────────────────────────────────────────────────────────────
    private static void runInteractiveMenu(FraudDetectionPipeline pipeline,
                                           TransactionGenerator generator,
                                           SparkSession spark) {
        LOG.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        LOG.info("  DEMO IS RUNNING — COMMANDS AVAILABLE:");
        LOG.info("  ┌──────────────────────────────────────────────────────────┐");
        LOG.info("  │  Press ENTER  → Graceful shutdown                        │");
        LOG.info("  │  Type 'm'     → Print metrics now                        │");
        LOG.info("  │  Type 's'     → Print streaming query status             │");
        LOG.info("  │  Type 'c'     → Show checkpoint directory structure      │");
        LOG.info("  │  Type 'o'     → Show fraud alert output files            │");
        LOG.info("  └──────────────────────────────────────────────────────────┘");
        LOG.info("  🌐 Spark UI: http://localhost:{}", PipelineConfig.SPARK_UI_PORT);
        LOG.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

        Scanner scanner = new Scanner(System.in);
        boolean running = true;

        while (running) {
            try {
                if (System.in.available() > 0 || scanner.hasNextLine()) {
                    String input = scanner.nextLine().trim().toLowerCase();

                    if (input.isEmpty()) {
                        running = false;
                        LOG.info("ENTER pressed — initiating shutdown...");
                    } else if (input.equals("m")) {
                        pipeline.printMetrics();
                    } else if (input.equals("s")) {
                        printQueryStatus(pipeline);
                    } else if (input.equals("c")) {
                        printCheckpointStructure();
                    } else if (input.equals("o")) {
                        printOutputFiles();
                    } else {
                        LOG.info("Unknown command: '{}'. Press ENTER to exit.", input);
                    }
                }
                Thread.sleep(500);
            } catch (IOException | InterruptedException e) {
                running = false;
            }
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  METRICS SCHEDULER — prints live metrics every 15 seconds
    // ─────────────────────────────────────────────────────────────────────────
    private static ScheduledExecutorService startMetricsReporting(
            FraudDetectionPipeline pipeline,
            TransactionGenerator generator,
            SparkSession spark) {

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(() -> {
            try {
                LOG.info("───────────────────────────────────────────────────────────────");
                LOG.info("  📊 LIVE PIPELINE METRICS");
                LOG.info("───────────────────────────────────────────────────────────────");
                LOG.info("  Generator: {} total | {} fraud | {} late",
                        generator.getTotalGenerated(),
                        generator.getFraudEventsGenerated(),
                        generator.getLateEventsGenerated());

                // Print last progress for each stream
                StreamingQuery txnStream = pipeline.getTransactionStream();
                if (txnStream != null && txnStream.lastProgress() != null) {
                    StreamingQueryProgress p = txnStream.lastProgress();
                    LOG.info("  Rule-Based: Batch#{} | {} rows/batch | {}/sec",
                            p.batchId(), p.numInputRows(),
                            String.format("%.0f", p.processedRowsPerSecond()));
                }

                StreamingQuery velStream = pipeline.getVelocityStream();
                if (velStream != null && velStream.lastProgress() != null) {
                    StreamingQueryProgress p = velStream.lastProgress();
                    LOG.info("  Velocity:   Batch#{} | {} rows/batch | {}/sec | StateRows: {}",
                            p.batchId(), p.numInputRows(),
                            String.format("%.0f", p.processedRowsPerSecond()),
                            p.stateOperators().length > 0 ?
                                    p.stateOperators()[0].numRowsTotal() : "N/A");
                }

                LOG.info("  🌐 Spark UI → http://localhost:{}/streaming", PipelineConfig.SPARK_UI_PORT);
                LOG.info("───────────────────────────────────────────────────────────────");
            } catch (Exception e) {
                // Don't crash the metrics thread
                LOG.debug("Metrics error: {}", e.getMessage());
            }
        }, 15, 15, TimeUnit.SECONDS);

        return scheduler;
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  SPARK UI NAVIGATION GUIDE
    //  Critical for the training session — shows trainers exactly where to look
    // ─────────────────────────────────────────────────────────────────────────
    private static void printSparkUINavigationGuide() {
        LOG.info("");
        LOG.info("╔══════════════════════════════════════════════════════════════════════╗");
        LOG.info("║           🌐  SPARK UI NAVIGATION GUIDE                             ║");
        LOG.info("║              http://localhost:4040                                  ║");
        LOG.info("╠══════════════════════════════════════════════════════════════════════╣");
        LOG.info("║                                                                      ║");
        LOG.info("║  [1] STRUCTURED STREAMING TAB (most important!)                     ║");
        LOG.info("║      http://localhost:4040/streaming                                ║");
        LOG.info("║      ┌─────────────────────────────────────────────────────────┐   ║");
        LOG.info("║      │ You will see 3 streaming queries:                        │   ║");
        LOG.info("║      │  • fraud-rule-based-detection                           │   ║");
        LOG.info("║      │  • fraud-velocity-detection                             │   ║");
        LOG.info("║      │  • late-event-monitoring                                │   ║");
        LOG.info("║      │                                                          │   ║");
        LOG.info("║      │ For each query, click it to see:                        │   ║");
        LOG.info("║      │  → Input Rate (rows/sec from file source)               │   ║");
        LOG.info("║      │  → Processing Rate (rows/sec Spark processes)           │   ║");
        LOG.info("║      │  → Batch Duration (P99 latency — should be < 5s)       │   ║");
        LOG.info("║      │  → Watermark (current watermark value)                  │   ║");
        LOG.info("║      │  → State Store Size (for velocity detection)            │   ║");
        LOG.info("║      └─────────────────────────────────────────────────────────┘   ║");
        LOG.info("║                                                                      ║");
        LOG.info("║  [2] SQL / DATAFRAME TAB (See the Physical Plan / DAG)            ║");
        LOG.info("║      http://localhost:4040/SQL                                      ║");
        LOG.info("║      ┌─────────────────────────────────────────────────────────┐   ║");
        LOG.info("║      │ Click any completed batch plan to see:                   │   ║");
        LOG.info("║      │  → DAG visualization (nodes = operators)                │   ║");
        LOG.info("║      │  → Physical plan (how Catalyst compiled your code)      │   ║");
        LOG.info("║      │  → 'EventTimeWatermark' node (watermark operator)       │   ║");
        LOG.info("║      │  → 'StateStoreSave' node (velocity state write)         │   ║");
        LOG.info("║      │  → 'StateStoreRestore' node (velocity state read)       │   ║");
        LOG.info("║      │  → 'HashAggregate' node (velocity count aggregation)    │   ║");
        LOG.info("║      └─────────────────────────────────────────────────────────┘   ║");
        LOG.info("║                                                                      ║");
        LOG.info("║  [3] STAGES TAB (See task-level execution)                         ║");
        LOG.info("║      http://localhost:4040/stages                                   ║");
        LOG.info("║      ┌─────────────────────────────────────────────────────────┐   ║");
        LOG.info("║      │ Each DAG node becomes one or more Stages                 │   ║");
        LOG.info("║      │ Look for:                                                │   ║");
        LOG.info("║      │  → 'scan json' → source reading stage                  │   ║");
        LOG.info("║      │  → 'hashAgg'  → stateful aggregation stage             │   ║");
        LOG.info("║      │  → task duration vs GC time                            │   ║");
        LOG.info("║      └─────────────────────────────────────────────────────────┘   ║");
        LOG.info("║                                                                      ║");
        LOG.info("║  [4] JOBS TAB (See micro-batch jobs)                               ║");
        LOG.info("║      http://localhost:4040/jobs                                     ║");
        LOG.info("║      Every trigger = one Spark Job. Watch jobs appear every 2 sec ║");
        LOG.info("║                                                                      ║");
        LOG.info("╚══════════════════════════════════════════════════════════════════════╝");
        LOG.info("");
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  DAG EXPLAINER — Maps code to what trainees see in Spark UI
    // ─────────────────────────────────────────────────────────────────────────
    private static void printDAGExplainer() {
        LOG.info("");
        LOG.info("╔══════════════════════════════════════════════════════════════════════╗");
        LOG.info("║           📊  DAG EXPLAINER — CODE TO SPARK UI MAPPING             ║");
        LOG.info("╠══════════════════════════════════════════════════════════════════════╣");
        LOG.info("║                                                                      ║");
        LOG.info("║  QUERY 1: RULE-BASED FRAUD DETECTION DAG                           ║");
        LOG.info("║  ─────────────────────────────────────────────────────────────────  ║");
        LOG.info("║                                                                      ║");
        LOG.info("║  [FileScan JSON]                                                    ║");
        LOG.info("║       ↓  ← Your code: .readStream().json(inputDir)                 ║");
        LOG.info("║  [Project + Filter]                                                 ║");
        LOG.info("║       ↓  ← Your code: .withColumn('eventTime') + .withWatermark()  ║");
        LOG.info("║  [EventTimeWatermark]    ★ KEY NODE — WATERMARK APPLIED HERE       ║");
        LOG.info("║       ↓  ← withWatermark('eventTime', '10 minutes')                ║");
        LOG.info("║  [Filter]                                                           ║");
        LOG.info("║       ↓  ← .filter(amount > 5000 OR isHighRiskMerchant)            ║");
        LOG.info("║  [Project]                                                          ║");
        LOG.info("║       ↓  ← .withColumn('riskScore', callUDF('computeRiskScore'))   ║");
        LOG.info("║  [Filter]                                                           ║");
        LOG.info("║       ↓  ← .filter(riskScore > 0.5)                                ║");
        LOG.info("║  [WriteToDataSource (JSON Sink)]                                   ║");
        LOG.info("║       ↓  ← .writeStream().format('json').start()                   ║");
        LOG.info("║  [CHECKPOINT WRITE]      ★ EXACTLY-ONCE GUARANTEE                 ║");
        LOG.info("║                                                                      ║");
        LOG.info("║  QUERY 2: VELOCITY DETECTION DAG (STATEFUL)                        ║");
        LOG.info("║  ─────────────────────────────────────────────────────────────────  ║");
        LOG.info("║                                                                      ║");
        LOG.info("║  [EventTimeWatermark]    ★ SAME WATERMARK AS ABOVE                 ║");
        LOG.info("║       ↓                                                             ║");
        LOG.info("║  [StateStoreRestore]     ★ STATE STORE — READS PREVIOUS STATE      ║");
        LOG.info("║       ↓  ← Restores running window counts from RocksDB/HDFS        ║");
        LOG.info("║  [HashAggregate]                                                    ║");
        LOG.info("║       ↓  ← .agg(count('*'), sum('amount'), ...)                    ║");
        LOG.info("║  [StateStoreSave]        ★ STATE STORE — WRITES UPDATED STATE      ║");
        LOG.info("║       ↓  ← Saves new window counts for next micro-batch            ║");
        LOG.info("║  [Filter]                                                           ║");
        LOG.info("║       ↓  ← .filter(txnCount > 5)                                  ║");
        LOG.info("║  [Project + Sink]                                                  ║");
        LOG.info("║       ↓  ← velocity alert written to output                        ║");
        LOG.info("║                                                                      ║");
        LOG.info("║  HOW STATE STORE SCALING WORKS:                                    ║");
        LOG.info("║  • Each active window = one state store entry                      ║");
        LOG.info("║  • Windows: 5-minute duration, 1-minute slide = 5 active at once  ║");
        LOG.info("║  • Once watermark passes window.end → state is EVICTED             ║");
        LOG.info("║  • This prevents unbounded state growth (OOM prevention)          ║");
        LOG.info("║  • In Spark UI: Streaming tab → velocity query → 'State Rows'     ║");
        LOG.info("║                                                                      ║");
        LOG.info("╚══════════════════════════════════════════════════════════════════════╝");
        LOG.info("");
    }

    private static void printQueryStatus(FraudDetectionPipeline pipeline) {
        LOG.info("══════════════════════════════════════════════════════════════");
        LOG.info("  STREAMING QUERY STATUS");

        StreamingQuery[] queries = {
                pipeline.getTransactionStream(),
                pipeline.getVelocityStream(),
                pipeline.getLateEventStream()
        };
        String[] names = {"Rule-Based Detection", "Velocity Detection", "Late Event Monitor"};

        for (int i = 0; i < queries.length; i++) {
            StreamingQuery q = queries[i];
            if (q != null) {
                LOG.info("  {}: {} | Active: {} | Exception: {}",
                        names[i], q.id(), q.isActive(),
                        q.exception().isDefined() ? q.exception().get().getMessage() : "none");
            }
        }
        LOG.info("══════════════════════════════════════════════════════════════");
    }

    private static void printCheckpointStructure() {
        LOG.info("══════════════════════════════════════════════════════════════");
        LOG.info("  CHECKPOINT DIRECTORY STRUCTURE");
        LOG.info("  (This is how Spark guarantees exactly-once)");
        LOG.info("");
        LOG.info("  {}/ ", PipelineConfig.CHECKPOINT_BASE_DIR);
        printDirTree(new File(PipelineConfig.CHECKPOINT_BASE_DIR), "  │   ", 0, 3);
        LOG.info("══════════════════════════════════════════════════════════════");
        LOG.info("  MEANING OF CHECKPOINT SUBDIRS:");
        LOG.info("  /offsets  → What input offsets have been committed (WAL)");
        LOG.info("  /commits  → Which batch IDs have been fully written");
        LOG.info("  /state    → Stateful aggregation state (window counts)");
        LOG.info("  /sources  → Source log (which files were processed)");
        LOG.info("══════════════════════════════════════════════════════════════");
    }

    private static void printDirTree(File dir, String prefix, int depth, int maxDepth) {
        if (depth >= maxDepth || !dir.exists() || !dir.isDirectory()) return;
        File[] files = dir.listFiles();
        if (files == null) return;
        for (File f : files) {
            LOG.info("{}├── {} {}", prefix, f.getName(), f.isDirectory() ? "/" : "");
            if (f.isDirectory() && depth < maxDepth - 1) {
                printDirTree(f, prefix + "│   ", depth + 1, maxDepth);
            }
        }
    }

    private static void printOutputFiles() {
        LOG.info("══════════════════════════════════════════════════════════════");
        LOG.info("  FRAUD DETECTION OUTPUT FILES");
        countOutputFiles(PipelineConfig.FRAUD_ALERTS_OUTPUT_DIR,  "Fraud Alerts (Rule-Based)");
        countOutputFiles(PipelineConfig.VELOCITY_OUTPUT_DIR,      "Velocity Alerts");
        countOutputFiles(PipelineConfig.LATE_EVENTS_OUTPUT_DIR,   "Late Event Records");
        LOG.info("══════════════════════════════════════════════════════════════");
    }

    private static void countOutputFiles(String dir, String label) {
        File d = new File(dir);
        if (!d.exists()) {
            LOG.info("  {}: not yet written", label);
            return;
        }
        File[] files = d.listFiles(f -> f.getName().endsWith(".json"));
        int count = files != null ? files.length : 0;
        long totalBytes = 0;
        if (files != null) {
            for (File f : files) totalBytes += f.length();
        }
        LOG.info("  {}: {} files | {} bytes", label, count, totalBytes);
        if (files != null && files.length > 0) {
            LOG.info("    Sample: {}", files[0].getName());
        }
    }

    private static void printFinalSummary(FraudDetectionPipeline pipeline,
                                           TransactionGenerator generator,
                                           SparkSession spark) {
        LOG.info("");
        LOG.info("╔══════════════════════════════════════════════════════════════════════╗");
        LOG.info("║                    FINAL PIPELINE SUMMARY                           ║");
        LOG.info("╠══════════════════════════════════════════════════════════════════════╣");
        LOG.info("║  GENERATOR STATS:                                                   ║");
        LOG.info("║    Total Transactions Generated: {}                     ║", generator.getTotalGenerated());
        LOG.info("║    Fraud Events Generated:       {}                      ║", generator.getFraudEventsGenerated());
        LOG.info("║    Late Events Generated:        {}                      ║", generator.getLateEventsGenerated());
        LOG.info("╠══════════════════════════════════════════════════════════════════════╣");
        LOG.info("║  STREAMING QUERY FINAL STATS:                                       ║");

        StreamingQuery txn = pipeline.getTransactionStream();
        if (txn != null && txn.lastProgress() != null) {
            StreamingQueryProgress p = txn.lastProgress();
            LOG.info("║    Rule-Based: Processed {} batches | {} total rows          ║",
                    p.batchId() + 1, p.batchId() * p.numInputRows());
        }

        StreamingQuery vel = pipeline.getVelocityStream();
        if (vel != null && vel.lastProgress() != null) {
            StreamingQueryProgress p = vel.lastProgress();
            long stateRows = p.stateOperators().length > 0 ? p.stateOperators()[0].numRowsTotal() : 0;
            LOG.info("║    Velocity:   {} batches | State store rows: {}            ║",
                    p.batchId() + 1, stateRows);
        }

        LOG.info("╠══════════════════════════════════════════════════════════════╣");
        LOG.info("║  OUTPUT:                                                     ║");
        LOG.info("║    Fraud Alerts:  {}", PipelineConfig.FRAUD_ALERTS_OUTPUT_DIR);
        LOG.info("║    Velocity:      {}", PipelineConfig.VELOCITY_OUTPUT_DIR);
        LOG.info("║    Late Events:   {}", PipelineConfig.LATE_EVENTS_OUTPUT_DIR);
        LOG.info("║    Checkpoints:   {}", PipelineConfig.CHECKPOINT_BASE_DIR);
        LOG.info("╚══════════════════════════════════════════════════════════════╝");
    }

    private static void cleanupPreviousRun() {
        deleteDir(new File(TRANSACTION_SOURCE_DIR));
        deleteDir(new File(PipelineConfig.OUTPUT_BASE_DIR));
        deleteDir(new File(PipelineConfig.CHECKPOINT_BASE_DIR));
        LOG.info("  Previous demo artifacts cleaned.");
    }

    private static void deleteDir(File dir) {
        if (!dir.exists()) return;
        File[] files = dir.listFiles();
        if (files != null) {
            for (File f : files) {
                if (f.isDirectory()) deleteDir(f);
                else f.delete();
            }
        }
        dir.delete();
    }

    private static void printBanner() {
        System.out.println();
        System.out.println("╔══════════════════════════════════════════════════════════════════════════╗");
        System.out.println("║                                                                          ║");
        System.out.println("║    ████████╗██████╗  █████╗ ██╗   ██╗██████╗                          ║");
        System.out.println("║    ██╔════╝██╔══██╗██╔══██╗██║   ██║██╔══██╗                         ║");
        System.out.println("║    █████╗  ██████╔╝███████║██║   ██║██║  ██║                         ║");
        System.out.println("║    ██╔══╝  ██╔══██╗██╔══██║██║   ██║██║  ██║                         ║");
        System.out.println("║    ██║     ██║  ██║██║  ██║╚██████╔╝██████╔╝                         ║");
        System.out.println("║    ╚═╝     ╚═╝  ╚═╝╚═╝  ╚═╝ ╚═════╝ ╚═════╝                          ║");
        System.out.println("║                                                                          ║");
        System.out.println("║     DETECTION PIPELINE — Apache Spark Structured Streaming             ║");
        System.out.println("║     Corporate Training Demo | Java 17 | Spark 3.5 | M1 Mac            ║");
        System.out.println("║                                                                          ║");
        System.out.println("╠══════════════════════════════════════════════════════════════════════════╣");
        System.out.println("║  BUSINESS REQUIREMENT:                                                   ║");
        System.out.println("║  GlobalBank Corp loses $180M/year to fraud detected 4-6 hours late.     ║");
        System.out.println("║  Mission: Detect fraud in < 5 seconds (P99), zero data loss.           ║");
        System.out.println("╠══════════════════════════════════════════════════════════════════════════╣");
        System.out.println("║  TOPICS:  State Store Scaling | Watermark Design | Exactly-Once Sinks  ║");
        System.out.println("║           Checkpoint Durability | Backpressure Tuning                  ║");
        System.out.println("╚══════════════════════════════════════════════════════════════════════════╝");
        System.out.println();
    }
}
