package com.pos;

import com.pos.config.PipelineConfig;
import com.pos.generator.PosEventGenerator;
import com.pos.pipeline.SchemaEvolutionPipeline;
import com.pos.schema.SchemaRegistry;
import com.pos.testing.ContractTester;
import com.pos.util.SparkSessionFactory;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.Scanner;
import java.util.concurrent.*;

/**
 * ╔══════════════════════════════════════════════════════════════════════════╗
 * ║                                                                          ║
 * ║   SCHEMA EVOLUTION IN POS EVENTS                                         ║
 * ║   Apache Spark Structured Streaming — Corporate Training Demo            ║
 * ║                                                                          ║
 * ║   BUSINESS REQUIREMENT                                                   ║
 * ║   ─────────────────────────────────────────────────────────────────      ║
 * ║   RetailGiant Corp operates 8,000 POS terminals across 400 stores.      ║
 * ║   The data engineering team owns the centralised transaction pipeline   ║
 * ║   that feeds the finance, loyalty, and analytics platforms.             ║
 * ║                                                                          ║
 * ║   PROBLEM STATEMENT                                                      ║
 * ║   ─────────────────────────────────────────────────────────────────      ║
 * ║   In Q2, the POS firmware team silently added a "paymentMethod" field   ║
 * ║   to terminal events. The streaming pipeline — built against the old    ║
 * ║   schema — silently DROPPED this field. The loyalty platform received   ║
 * ║   3 months of events with null payment types. A $2.4M cashback report   ║
 * ║   was wrong. The CFO was not pleased.                                   ║
 * ║                                                                          ║
 * ║   ROOT CAUSE: No schema contract between producers and consumers.       ║
 * ║                                                                          ║
 * ║   SOLUTION:                                                              ║
 * ║   1. Schema Registry — track every version of every schema              ║
 * ║   2. Contract Testing — validate compatibility before deployment        ║
 * ║   3. Schema Enforcement — reader schema handles all versions safely     ║
 * ║   4. Data Validation Rules — quarantine bad data, never drop silently  ║
 * ║   5. Streaming Test Strategy — monitor version mix in real-time         ║
 * ║                                                                          ║
 * ║   TOPICS COVERED:                                                        ║
 * ║   ① Schema enforcement (PERMISSIVE reader, superset schema)            ║
 * ║   ② Contract testing (compatibility rules CI/CD checks)                ║
 * ║   ③ Schema registry concept (versioned manifest + compatibility report) ║
 * ║   ④ Data validation rules (completeness, validity, conformity,          ║
 * ║                             consistency pillars)                         ║
 * ║   ⑤ Streaming test strategies (live contract monitor query)            ║
 * ║                                                                          ║
 * ╚══════════════════════════════════════════════════════════════════════════╝
 */
public class PosSchemaEvolutionApp {

    private static final Logger LOG = LoggerFactory.getLogger(PosSchemaEvolutionApp.class);

    public static void main(String[] args) throws Exception {

        printBanner();

        // ── 1. Clean up ──────────────────────────────────────────────────
        LOG.info("STEP 1: Cleaning previous run...");
        deleteDir(new File(PipelineConfig.BASE));

        // ── 2. Schema Registry ───────────────────────────────────────────
        LOG.info("STEP 2: Initialising Schema Registry...");
        Files.createDirectories(Paths.get(PipelineConfig.SOURCE_DIR));
        Files.createDirectories(Paths.get(PipelineConfig.VALID_OUT));
        Files.createDirectories(Paths.get(PipelineConfig.QUARANTINE));
        Files.createDirectories(Paths.get(PipelineConfig.CHECKPOINT));
        SchemaRegistry.initialize();

        // ── 3. Contract Tests ────────────────────────────────────────────
        LOG.info("STEP 3: Running contract compatibility tests...");
        ContractTester.runStartupChecks();

        // ── 4. Spark Session ─────────────────────────────────────────────
        LOG.info("STEP 4: Creating SparkSession...");
        SparkSession spark = SparkSessionFactory.create();

        // ── 5. Pipeline ──────────────────────────────────────────────────
        LOG.info("STEP 5: Starting streaming pipeline...");
        SchemaEvolutionPipeline pipeline = new SchemaEvolutionPipeline();
        pipeline.start(spark, PipelineConfig.SOURCE_DIR);

        // ── 6. Generator ─────────────────────────────────────────────────
        Thread.sleep(3000); // let queries initialise
        LOG.info("STEP 6: Starting POS event generator...");
        PosEventGenerator generator = new PosEventGenerator();
        generator.start();

        // ── 7. Metrics scheduler ─────────────────────────────────────────
        ScheduledExecutorService metrics = Executors.newSingleThreadScheduledExecutor();
        metrics.scheduleAtFixedRate(pipeline::printMetrics, 12, 12, TimeUnit.SECONDS);

        // ── 8. Navigation guide (after first batches process) ────────────
        Thread.sleep(6000);
        printNavigationGuide();
        printDAGGuide();

        // ── 9. Interactive loop ──────────────────────────────────────────
        runInteractiveMenu(pipeline, generator, spark);

        // ── 10. Shutdown ─────────────────────────────────────────────────
        LOG.info("STEP 10: Graceful shutdown...");
        metrics.shutdownNow();
        generator.stop();
        printFinalSummary(pipeline, generator);
        pipeline.stop();
        Thread.sleep(2000);
        spark.stop();

        LOG.info("╔══════════════════════════════════════════════════════════╗");
        LOG.info("║  DEMO COMPLETE                                          ║");
        LOG.info("║  Valid output:    {}         ║", PipelineConfig.VALID_OUT);
        LOG.info("║  Quarantine:      {}    ║", PipelineConfig.QUARANTINE);
        LOG.info("║  Schema Registry: {}     ║", PipelineConfig.REGISTRY);
        LOG.info("╚══════════════════════════════════════════════════════════╝");
    }

    // ── Interactive menu ─────────────────────────────────────────────────────
    private static void runInteractiveMenu(SchemaEvolutionPipeline pipeline,
                                            PosEventGenerator generator,
                                            SparkSession spark) {
        LOG.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        LOG.info("  COMMANDS:");
        LOG.info("  ENTER → Shutdown  | m → Metrics  | s → Query status");
        LOG.info("  r → Registry files | q → Quarantine sample | o → Output count");
        LOG.info("  🌐 Spark UI: http://localhost:{}", PipelineConfig.UI_PORT);
        LOG.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

        Scanner scanner = new Scanner(System.in);
        while (true) {
            try {
                if (System.in.available() > 0 || scanner.hasNextLine()) {
                    String cmd = scanner.nextLine().trim().toLowerCase();
                    if (cmd.isEmpty()) { LOG.info("Shutdown requested..."); break; }
                    switch (cmd) {
                        case "m" -> pipeline.printMetrics();
                        case "s" -> printQueryStatus(pipeline);
                        case "r" -> printRegistry();
                        case "q" -> printQuarantineSample();
                        case "o" -> printOutputCount();
                        default  -> LOG.info("Unknown command. Press ENTER to exit.");
                    }
                }
                Thread.sleep(500);
            } catch (IOException | InterruptedException e) { break; }
        }
    }

    private static void printNavigationGuide() {
        LOG.info("");
        LOG.info("╔══════════════════════════════════════════════════════════════════════╗");
        LOG.info("║              🌐  SPARK UI NAVIGATION GUIDE                          ║");
        LOG.info("╠══════════════════════════════════════════════════════════════════════╣");
        LOG.info("║                                                                      ║");
        LOG.info("║  [1] STREAMING TAB → http://localhost:4040/streaming               ║");
        LOG.info("║      3 streaming queries:                                           ║");
        LOG.info("║      • pos-valid-events      → Append mode, valid records          ║");
        LOG.info("║      • pos-quarantine-events → Append mode, bad records            ║");
        LOG.info("║      • pos-contract-monitor  → Complete mode, version distribution ║");
        LOG.info("║                                                                      ║");
        LOG.info("║      Per query, observe:                                           ║");
        LOG.info("║      → Input Rate vs Processing Rate (backpressure visible here)   ║");
        LOG.info("║      → Batch Duration (should stay < trigger interval = 3s)        ║");
        LOG.info("║                                                                      ║");
        LOG.info("║  [2] SQL/DAG TAB → http://localhost:4040/SQL                       ║");
        LOG.info("║      Click any completed execution plan to see DAG                  ║");
        LOG.info("║      Key nodes to identify:                                        ║");
        LOG.info("║      → [FileScan JSON] — schema enforcement at source              ║");
        LOG.info("║      → [Project] ×3 — schemaVersion + rules + isValid columns     ║");
        LOG.info("║      → [Filter(isValid=true)]  — valid branch                     ║");
        LOG.info("║      → [Filter(isValid=false)] — quarantine branch                 ║");
        LOG.info("║      → [HashAggregate] — contract monitor count per version        ║");
        LOG.info("║                                                                      ║");
        LOG.info("║  [3] STAGES TAB → http://localhost:4040/stages                     ║");
        LOG.info("║      → 'scan json' stage — source read with schema projection      ║");
        LOG.info("║      → 'project' stage  — rule application                        ║");
        LOG.info("║      → 'hashAgg' stage  — version aggregation (monitor query)     ║");
        LOG.info("║                                                                      ║");
        LOG.info("║  [4] JOBS TAB → http://localhost:4040/jobs                         ║");
        LOG.info("║      One Job per 3-second trigger. Each job = one micro-batch.     ║");
        LOG.info("╚══════════════════════════════════════════════════════════════════════╝");
        LOG.info("");
    }

    private static void printDAGGuide() {
        LOG.info("");
        LOG.info("╔══════════════════════════════════════════════════════════════════════╗");
        LOG.info("║          📊  DAG ↔ CODE MAPPING                                    ║");
        LOG.info("╠══════════════════════════════════════════════════════════════════════╣");
        LOG.info("║                                                                      ║");
        LOG.info("║  CODE LINE                              → DAG NODE                  ║");
        LOG.info("║  ─────────────────────────────────────────────────────────────────  ║");
        LOG.info("║  .schema(READER_SCHEMA).json(sourceDir) → [FileScan JSON]          ║");
        LOG.info("║  ↑ SCHEMA ENFORCEMENT: explicit schema passed to Spark reader.     ║");
        LOG.info("║    Spark projects ONLY declared columns. Unknown fields ignored.   ║");
        LOG.info("║    _corrupt_record captures unparseable JSON. Never crash stream. ║");
        LOG.info("║                                                                      ║");
        LOG.info("║  .withColumn('schemaVersion', when(...)...)  → [Project]           ║");
        LOG.info("║  ↑ CONTRACT TESTING: we inspect the payload to detect version.    ║");
        LOG.info("║    This is your live migration dashboard input.                    ║");
        LOG.info("║                                                                      ║");
        LOG.info("║  DataQualityValidator.validate(rawStream)    → [Project ×4]        ║");
        LOG.info("║  ↑ DATA VALIDATION RULES: 4 pillars applied as computed columns.  ║");
        LOG.info("║    Catalyst merges these projections into ONE physical pass.       ║");
        LOG.info("║    No performance penalty for separate when() expressions.         ║");
        LOG.info("║                                                                      ║");
        LOG.info("║  .filter(isValid=true)  → valid sink   → [Filter] → [WriteToFile] ║");
        LOG.info("║  .filter(isValid=false) → quarantine   → [Filter] → [WriteToFile] ║");
        LOG.info("║  ↑ QUARANTINE PATTERN: Both branches share the [Project] parent.  ║");
        LOG.info("║    Spark reads source ONCE, applies rules ONCE, writes TWICE.     ║");
        LOG.info("║                                                                      ║");
        LOG.info("║  .groupBy(schemaVersion, isValid).count() → [HashAggregate]       ║");
        LOG.info("║  ↑ STREAMING TEST STRATEGY: Complete output mode = full histogram  ║");
        LOG.info("║    every batch. Watch v1 % drop as terminals upgrade (migration).  ║");
        LOG.info("╚══════════════════════════════════════════════════════════════════════╝");
        LOG.info("");
    }

    // ── Helper methods ───────────────────────────────────────────────────────

    private static void printQueryStatus(SchemaEvolutionPipeline pipeline) {
        LOG.info("══ QUERY STATUS ══════════════════════════════════════════════");
        printQ("pos-valid-events",      pipeline.getValidQuery());
        printQ("pos-quarantine-events", pipeline.getQuarantineQuery());
        printQ("pos-contract-monitor",  pipeline.getMonitorQuery());
        LOG.info("══════════════════════════════════════════════════════════════");
    }

    private static void printQ(String name, org.apache.spark.sql.streaming.StreamingQuery q) {
        if (q != null)
            LOG.info("  {} | active={} | exception={}",
                    name, q.isActive(),
                    q.exception().isDefined() ? q.exception().get().getMessage() : "none");
    }

    private static void printRegistry() {
        LOG.info("══ SCHEMA REGISTRY: {} ══", PipelineConfig.REGISTRY);
        File reg = new File(PipelineConfig.REGISTRY);
        if (reg.exists()) {
            File[] files = reg.listFiles();
            if (files != null) for (File f : files) LOG.info("  {}", f.getName());
        }
        LOG.info("  Run: cat {}/compatibility-report.txt", PipelineConfig.REGISTRY);
        LOG.info("══════════════════════════════════════════════════════════════");
    }

    private static void printQuarantineSample() {
        File q = new File(PipelineConfig.QUARANTINE);
        LOG.info("══ QUARANTINE SAMPLE: {} ══", PipelineConfig.QUARANTINE);
        if (q.exists()) {
            File[] files = q.listFiles(f -> f.getName().endsWith(".json"));
            if (files != null && files.length > 0) {
                try {
                    String content = Files.readString(files[0].toPath());
                    String[] lines = content.split("\n");
                    for (int i = 0; i < Math.min(3, lines.length); i++)
                        LOG.info("  {}", lines[i]);
                } catch (IOException e) { LOG.info("  (error reading)"); }
            } else { LOG.info("  (no quarantine files yet — wait a few batches)"); }
        }
        LOG.info("══════════════════════════════════════════════════════════════");
    }

    private static void printOutputCount() {
        LOG.info("══ OUTPUT FILE COUNTS ════════════════════════════════════════");
        countFiles("Valid",      PipelineConfig.VALID_OUT);
        countFiles("Quarantine", PipelineConfig.QUARANTINE);
        LOG.info("══════════════════════════════════════════════════════════════");
    }

    private static void countFiles(String label, String dir) {
        File d = new File(dir);
        if (!d.exists()) { LOG.info("  {}: not yet written", label); return; }
        File[] files = d.listFiles(f -> f.getName().endsWith(".json"));
        long bytes = 0;
        if (files != null) for (File f : files) bytes += f.length();
        LOG.info("  {}: {} files | {} bytes", label, files != null ? files.length : 0, bytes);
    }

    private static void printFinalSummary(SchemaEvolutionPipeline pipeline,
                                           PosEventGenerator generator) {
        LOG.info("");
        LOG.info("╔══════════════════════════════════════════════════════════╗");
        LOG.info("║  FINAL SUMMARY                                          ║");
        LOG.info("║  Generated: v1={} v2={} v3={} bad={}         ║",
                generator.getV1Count(), generator.getV2Count(),
                generator.getV3Count(), generator.getBadCount());
        LOG.info("╚══════════════════════════════════════════════════════════╝");
    }

    private static void deleteDir(File dir) {
        if (!dir.exists()) return;
        File[] files = dir.listFiles();
        if (files != null) for (File f : files) { if (f.isDirectory()) deleteDir(f); else f.delete(); }
        dir.delete();
    }

    private static void printBanner() {
        System.out.println();
        System.out.println("╔══════════════════════════════════════════════════════════════════════════╗");
        System.out.println("║                                                                          ║");
        System.out.println("║   🛒  SCHEMA EVOLUTION IN POS EVENTS                                    ║");
        System.out.println("║       Apache Spark Structured Streaming | Java 17 | M1 Mac             ║");
        System.out.println("║                                                                          ║");
        System.out.println("╠══════════════════════════════════════════════════════════════════════════╣");
        System.out.println("║  PROBLEM: RetailGiant's POS firmware team added 'paymentMethod'         ║");
        System.out.println("║  silently. Pipeline had no contract. Field was dropped. $2.4M report   ║");
        System.out.println("║  was wrong. CFO discovered it in quarterly review.                     ║");
        System.out.println("╠══════════════════════════════════════════════════════════════════════════╣");
        System.out.println("║  SOLUTION: Schema Registry + Contract Testing + Data Validation Rules  ║");
        System.out.println("╚══════════════════════════════════════════════════════════════════════════╝");
        System.out.println();
    }
}
