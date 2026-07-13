package com.pos.util;

import com.pos.config.PipelineConfig;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkSessionFactory {

    private static final Logger LOG = LoggerFactory.getLogger(SparkSessionFactory.class);

    public static SparkSession create() {
        SparkSession spark = SparkSession.builder()
                .appName(PipelineConfig.APP_NAME)
                .master(PipelineConfig.MASTER)

                // ── UI ──────────────────────────────────────────────────────
                .config("spark.ui.port",               String.valueOf(PipelineConfig.UI_PORT))
                .config("spark.ui.enabled",            "true")
                .config("spark.streaming.ui.retainedBatches", "100")
                .config("spark.ui.retainedJobs",       "50")
                .config("spark.sql.ui.retainedExecutions", "50")

                // ── Streaming ───────────────────────────────────────────────
                .config("spark.sql.shuffle.partitions",           "4")
                .config("spark.sql.streaming.fileSource.log.compactInterval", "10")

                // ── Performance M1 ──────────────────────────────────────────
                .config("spark.driver.memory",  "2g")
                .config("spark.serializer",     "org.apache.spark.serializer.KryoSerializer")
                .config("spark.kryo.unsafe",    "false")

                // ── Java 17 opens ───────────────────────────────────────────
                .config("spark.driver.extraJavaOptions",
                        "--add-opens=java.base/java.lang=ALL-UNNAMED " +
                        "--add-opens=java.base/java.util=ALL-UNNAMED " +
                        "--add-opens=java.base/java.nio=ALL-UNNAMED " +
                        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED " +
                        "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED " +
                        "--add-opens=java.base/java.io=ALL-UNNAMED " +
                        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED " +
                        "--add-opens=java.base/java.net=ALL-UNNAMED " +
                        "-Dio.netty.tryReflectionSetAccessible=true")
                .config("spark.executor.extraJavaOptions",
                        "--add-opens=java.base/java.lang=ALL-UNNAMED " +
                        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED " +
                        "-Dio.netty.tryReflectionSetAccessible=true")
                .config("spark.eventLog.enabled", "false")
                .config("spark.local.dir",        "/tmp/spark-local-pos")

                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        LOG.info("╔══════════════════════════════════════════════════════╗");
        LOG.info("║  SparkSession created | v{}          ║", spark.version());
        LOG.info("║  UI → http://localhost:{}                         ║", PipelineConfig.UI_PORT);
        LOG.info("╚══════════════════════════════════════════════════════╝");

        return spark;
    }
}
