package com.npunext.bank.streaming.job;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.window;

/**
 * Long-running Retail POS real-time sales aggregator.
 *
 * <p>Unlike the batch fraud pre-processor (foundation module), this job
 * never terminates on its own — it runs indefinitely as a Structured
 * Streaming query, which is exactly the workload shape that makes
 * "Spark on AKS Operations" topics (driver/executor pod lifecycle under
 * sustained load, resource tuning against growing streaming state,
 * PodDisruptionBudgets protecting executors during node drains, and
 * rolling updates of the surrounding operational tooling) observable and
 * demonstrable — a job that finishes in ten seconds never needs any of
 * those safeguards.
 *
 * <p>The event source is Spark's built-in {@code rate} format — it
 * generates a self-contained, infinite stream of monotonically increasing
 * (timestamp, value) rows with no external dependency (no Event Hub/Kafka
 * required for this operations-focused lab), which we deterministically
 * project into synthetic POS transaction fields.
 */
public final class POSStreamingAggregatorJob {

    private static final String MASTER_OVERRIDE_PROPERTY = "spark.master.override";
    private static final int STORE_COUNT = 25;

    private POSStreamingAggregatorJob() {
    }

    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        int rowsPerSecond = args.length > 0 ? Integer.parseInt(args[0]) : 200;
        String windowDuration = args.length > 1 ? args[1] : "30 seconds";
        String watermarkDuration = args.length > 2 ? args[2] : "1 minute";

        SparkSession spark = buildSparkSession();
        run(spark, rowsPerSecond, windowDuration, watermarkDuration);
    }

    /**
     * See {@code FraudPreProcessorJob} in the foundation module for the
     * rationale behind the master-override system property: it lets the
     * exact same jar run unmodified in IntelliJ (local[*]) and under
     * spark-submit on AKS (k8s://...).
     */
    private static SparkSession buildSparkSession() {
        SparkSession.Builder builder = SparkSession.builder()
                .appName("RetailPOSStreamingAggregator");

        String masterOverride = System.getProperty(MASTER_OVERRIDE_PROPERTY);
        if (masterOverride != null && !masterOverride.isBlank()) {
            builder = builder.master(masterOverride);
        }

        return builder
                .config("spark.sql.shuffle.partitions", "8")
                .config("spark.sql.streaming.metricsEnabled", "true")
                .getOrCreate();
    }

    private static void run(SparkSession spark, int rowsPerSecond, String windowDuration,
                            String watermarkDuration) throws TimeoutException, StreamingQueryException {

        // --- Source: self-contained synthetic event generator -------------
        Dataset<Row> rateStream = spark.readStream()
                .format("rate")
                .option("rowsPerSecond", rowsPerSecond)
                .option("numPartitions", 4)
                .load();

        // --- Project the monotonic (timestamp, value) pair into POS fields.
        // Deterministic hash-style derivation keeps the demo reproducible
        // and avoids any external randomness source inside the stream.
        Dataset<Row> posEvents = rateStream
                .withColumn("store_id", expr("value % " + STORE_COUNT))
                .withColumn("amount", expr("round((value % 9973) / 100.0 + 5.0, 2)"))
                .withColumn("channel", expr(
                        "CASE WHEN value % 3 = 0 THEN 'POS' " +
                        "     WHEN value % 3 = 1 THEN 'ONLINE' " +
                        "     ELSE 'MOBILE_APP' END"))
                .withWatermark("timestamp", watermarkDuration);

        // --- Core Transformation: tumbling-window sales aggregation -------
        // The watermark above bounds how long Spark retains state-store
        // entries for late-arriving events — directly relevant to the
        // "resource tuning" discussion, since unbounded streaming state is
        // the single most common cause of executor memory growth/OOM in
        // production streaming jobs.
        Dataset<Row> windowedSales = posEvents
                .groupBy(
                        window(col("timestamp"), windowDuration),
                        col("store_id")
                )
                .agg(
                        expr("sum(amount) as total_sales"),
                        expr("count(*) as txn_count")
                )
                .orderBy(col("window"), col("store_id"));

        StreamingQuery query = windowedSales.writeStream()
                .outputMode("update")
                .format("console")
                .option("truncate", false)
                .option("numRows", 50)
                .trigger(Trigger.ProcessingTime("10 seconds"))
                .queryName("pos-streaming-aggregator")
                .start();

        System.out.println("=== Streaming query started: " + query.id() + " ===");
        System.out.println("=== Spark UI (driver, port 4040) exposes REST status at " +
                "/api/v1/applications and /api/v1/applications/{app-id}/executors ===");

        query.awaitTermination();
    }
}
