package com.frauddetection.util;

import com.frauddetection.config.PipelineConfig;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FRAUD DETECTION PIPELINE - SparkSession Factory
 *
 * ╔══════════════════════════════════════════════════════════════════╗
 * ║  SPARK SESSION CONFIGURATION FOR JAVA 17 + M1 MAC              ║
 * ║                                                                 ║
 * ║  Key configurations explained:                                  ║
 * ║                                                                 ║
 * ║  1. STATE STORE CONFIGURATION                                   ║
 * ║     spark.sql.streaming.stateStore.providerClass               ║
 * ║     Default: HDFSBackedStateStore (works with local FS too)    ║
 * ║     Production: RocksDBStateStore for TB-scale state           ║
 * ║                                                                 ║
 * ║  2. BACKPRESSURE TUNING                                         ║
 * ║     spark.sql.streaming.fileSource.log.compactInterval         ║
 * ║     Controls how often file source log is compacted            ║
 * ║                                                                 ║
 * ║  3. CHECKPOINT DURABILITY                                       ║
 * ║     spark.sql.streaming.checkpointFileManagerClass             ║
 * ║     Controls how checkpoint files are written atomically        ║
 * ║                                                                 ║
 * ║  4. JAVA 17 MODULE SYSTEM                                       ║
 * ║     Spark uses reflection heavily — Java 17 modules block this  ║
 * ║     We set --add-opens to allow Spark's internal reflections   ║
 * ╚══════════════════════════════════════════════════════════════════╝
 */
public class SparkSessionFactory {

    private static final Logger LOG = LoggerFactory.getLogger(SparkSessionFactory.class);

    public static SparkSession create() {
        LOG.info("Creating SparkSession (Java 17, M1 Mac optimised)...");

        SparkSession spark = SparkSession.builder()
                .appName(PipelineConfig.APP_NAME)
                .master(PipelineConfig.MASTER)

                // ── UI Configuration ──────────────────────────────────────────
                .config("spark.ui.port", String.valueOf(PipelineConfig.SPARK_UI_PORT))
                .config("spark.ui.enabled", "true")
                .config("spark.ui.retainedJobs", "50")
                .config("spark.ui.retainedStages", "50")
                .config("spark.ui.retainedTasks", "100")
                .config("spark.sql.ui.retainedExecutions", "50")
                .config("spark.streaming.ui.retainedBatches", "100")

                // ── State Store Configuration ─────────────────────────────────
                // Controls how stateful aggregation state is stored between batches
                // "maintenanceInterval" — how often state store runs cleanup
                .config("spark.sql.streaming.stateStore.maintenanceInterval", "30s")
                .config("spark.sql.streaming.stateStore.minDeltasForSnapshot",
                        String.valueOf(PipelineConfig.STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT))
                // Number of state store instances per stateful operator
                // Matches number of shuffle partitions (parallelism)
                .config("spark.sql.shuffle.partitions", "4")  // M1 has 4 performance cores

                // ── Backpressure + Trigger Configuration ─────────────────────
                // File source: max files read per trigger (backpressure control)
                .config("spark.sql.streaming.fileSource.log.compactInterval", "10")
                .config("spark.sql.streaming.fileSource.log.cleanupDelay", "60000")

                // ── Watermark Configuration ───────────────────────────────────
                // Allow multi-batch aggregations for late data
                .config("spark.sql.streaming.multipleWatermarkPolicy", "min")

                // ── Checkpoint Configuration ──────────────────────────────────
                // How long to wait for checkpoint write before failing
                .config("spark.sql.streaming.stopGracefullyOnShutdown", "true")

                // ── Performance for M1 Mac ────────────────────────────────────
                .config("spark.driver.memory", "2g")
                .config("spark.executor.memory", "2g")
                .config("spark.executor.cores", "4")

                // ── Java 17 Compatibility ─────────────────────────────────────
                // Spark uses sun.misc.Unsafe and reflection — requires these opens
                .config("spark.driver.extraJavaOptions",
                        "--add-opens=java.base/java.lang=ALL-UNNAMED " +
                        "--add-opens=java.base/java.util=ALL-UNNAMED " +
                        "--add-opens=java.base/java.nio=ALL-UNNAMED " +
                        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED " +
                        "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED " +
                        "--add-opens=java.base/java.io=ALL-UNNAMED " +
                        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED " +
                        "--add-opens=java.base/java.net=ALL-UNNAMED " +
                        "--add-opens=java.base/sun.security.action=ALL-UNNAMED " +
                        "-Dio.netty.tryReflectionSetAccessible=true")
                .config("spark.executor.extraJavaOptions",
                        "--add-opens=java.base/java.lang=ALL-UNNAMED " +
                        "--add-opens=java.base/java.util=ALL-UNNAMED " +
                        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED " +
                        "-Dio.netty.tryReflectionSetAccessible=true")

                // ── Serialization ─────────────────────────────────────────────
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.kryo.unsafe", "false") // Safer on M1 ARM

                // ── Logging ───────────────────────────────────────────────────
                .config("spark.eventLog.enabled", "false") // Disable for demo simplicity

                // ── Local Mode Specific ───────────────────────────────────────
                .config("spark.local.dir", "/tmp/spark-local")

                .getOrCreate();

        // Set log level via SparkContext (supresses Spark's internal INFO logs)
        spark.sparkContext().setLogLevel("WARN");

        LOG.info("╔══════════════════════════════════════════════════════════════╗");
        LOG.info("║  SPARK SESSION CREATED SUCCESSFULLY                         ║");
        LOG.info("║  Version: {}                                      ║", spark.version());
        LOG.info("║  UI:      http://localhost:{}                             ║", PipelineConfig.SPARK_UI_PORT);
        LOG.info("║  Master:  {}                                         ║", PipelineConfig.MASTER);
        LOG.info("╚══════════════════════════════════════════════════════════════╝");

        return spark;
    }
}
