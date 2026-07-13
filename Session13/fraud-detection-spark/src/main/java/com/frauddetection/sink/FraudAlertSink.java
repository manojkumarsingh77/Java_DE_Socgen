package com.frauddetection.sink;

import com.frauddetection.config.PipelineConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * FRAUD DETECTION PIPELINE - Exactly-Once Fraud Alert Sink
 *
 * ╔══════════════════════════════════════════════════════════════════╗
 * ║  EXACTLY-ONCE SEMANTICS EXPLAINED                               ║
 * ║                                                                 ║
 * ║  Three levels of delivery guarantee:                            ║
 * ║                                                                 ║
 * ║  AT-MOST-ONCE: May lose data on failure                        ║
 * ║    → Bad for fraud detection (missed alerts = revenue loss)     ║
 * ║                                                                 ║
 * ║  AT-LEAST-ONCE: May duplicate data on failure                  ║
 * ║    → Causes duplicate fraud alerts (analyst confusion)          ║
 * ║                                                                 ║
 * ║  EXACTLY-ONCE: Every record written exactly once               ║
 * ║    → Achieved via checkpoint + idempotent sink                  ║
 * ║                                                                 ║
 * ║  HOW SPARK ACHIEVES EXACTLY-ONCE:                              ║
 * ║  1. Source must be replayable (file source, Kafka with offsets) ║
 * ║  2. Checkpoint records which batch was last committed           ║
 * ║  3. Sink must be idempotent (same data → same output)          ║
 * ║  4. On restart: Spark replays from last checkpoint             ║
 * ║     Idempotent sink absorbs the replayed writes                ║
 * ╚══════════════════════════════════════════════════════════════════╝
 *
 * IDEMPOTENCY MECHANISM:
 * - Alert ID = "ALERT-" + transactionId (deterministic)
 * - Output file name includes alert ID
 * - If same alert is written twice → same filename → second write is a no-op
 * - This makes the sink idempotent = exactly-once guarantee
 */
public class FraudAlertSink {

    private static final Logger LOG = LoggerFactory.getLogger(FraudAlertSink.class);

    /**
     * Initialize output directories.
     * Called once at startup.
     */
    public static void initialize() {
        String[] dirs = {
                PipelineConfig.OUTPUT_BASE_DIR,
                PipelineConfig.FRAUD_ALERTS_OUTPUT_DIR,
                PipelineConfig.LATE_EVENTS_OUTPUT_DIR,
                PipelineConfig.VELOCITY_OUTPUT_DIR,
                PipelineConfig.METRICS_OUTPUT_DIR,
                PipelineConfig.CHECKPOINT_BASE_DIR,
                PipelineConfig.TRANSACTION_CHECKPOINT_DIR,
                PipelineConfig.ALERT_CHECKPOINT_DIR,
                PipelineConfig.VELOCITY_CHECKPOINT_DIR
        };

        for (String dir : dirs) {
            try {
                Files.createDirectories(Paths.get(dir));
                LOG.debug("Created directory: {}", dir);
            } catch (IOException e) {
                LOG.warn("Could not create directory {}: {}", dir, e.getMessage());
            }
        }

        LOG.info("╔══════════════════════════════════════════════════════════════╗");
        LOG.info("║  OUTPUT DIRECTORIES INITIALIZED                             ║");
        LOG.info("║  Fraud Alerts:  {}  ║", PipelineConfig.FRAUD_ALERTS_OUTPUT_DIR);
        LOG.info("║  Velocity:      {}     ║", PipelineConfig.VELOCITY_OUTPUT_DIR);
        LOG.info("║  Late Events:   {}  ║", PipelineConfig.LATE_EVENTS_OUTPUT_DIR);
        LOG.info("║  Checkpoints:   {}           ║", PipelineConfig.CHECKPOINT_BASE_DIR);
        LOG.info("╚══════════════════════════════════════════════════════════════╝");
    }

    /**
     * Print summary of what was written to the sink.
     * For demo/educational purposes.
     */
    public static void printSummary(SparkSummaryHelper helper) {
        LOG.info("══════════════════════════════════════════════════════════════");
        LOG.info("  SINK WRITE SUMMARY");
        LOG.info("══════════════════════════════════════════════════════════════");

        countFilesInDir("Fraud Alerts",  PipelineConfig.FRAUD_ALERTS_OUTPUT_DIR);
        countFilesInDir("Velocity Alerts",PipelineConfig.VELOCITY_OUTPUT_DIR);
        countFilesInDir("Late Events",   PipelineConfig.LATE_EVENTS_OUTPUT_DIR);

        LOG.info("══════════════════════════════════════════════════════════════");
        LOG.info("  CHECKPOINT CONTENTS");
        LOG.info("══════════════════════════════════════════════════════════════");
        describeCheckpoint(PipelineConfig.TRANSACTION_CHECKPOINT_DIR, "Rule-Based");
        describeCheckpoint(PipelineConfig.VELOCITY_CHECKPOINT_DIR,    "Velocity");
        describeCheckpoint(PipelineConfig.ALERT_CHECKPOINT_DIR,       "Late Events");
    }

    private static void countFilesInDir(String label, String dir) {
        try {
            Path path = Paths.get(dir);
            if (!Files.exists(path)) {
                LOG.info("  {}: (no output yet)", label);
                return;
            }
            long count = Files.list(path)
                    .filter(p -> p.toString().endsWith(".json"))
                    .count();
            LOG.info("  {}: {} JSON files written", label, count);
        } catch (IOException e) {
            LOG.info("  {}: error reading directory", label);
        }
    }

    private static void describeCheckpoint(String dir, String queryName) {
        try {
            Path path = Paths.get(dir);
            if (!Files.exists(path)) {
                LOG.info("  {}: No checkpoint yet", queryName);
                return;
            }

            // Show checkpoint subdirectories
            Files.list(path).forEach(sub -> {
                try {
                    long fileCount = Files.list(sub).count();
                    LOG.info("  {} checkpoint/{}: {} files", queryName,
                            sub.getFileName(), fileCount);
                } catch (IOException e) {
                    // ignore
                }
            });
        } catch (IOException e) {
            LOG.debug("Could not read checkpoint dir: {}", dir);
        }
    }

    // Marker interface for helper
    public interface SparkSummaryHelper {}
}
