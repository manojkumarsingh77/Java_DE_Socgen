package com.paynova.obs;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;

public class ObservabilityStreamingJob {

    public static void main(String[] args) throws Exception {

        SparkSession spark = SparkSession.builder()
                .appName("PayNova-Observability")
                .master("local[*]")
                .config("spark.sql.shuffle.partitions", "4")
                .config("spark.sql.streaming.checkpointLocationClass",
                        "org.apache.spark.sql.streaming.LocalCheckpointLocation")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        StructType schema = new StructType()
                .add("payment_id", DataTypes.StringType)
                .add("customer_id", DataTypes.StringType)
                .add("amount", DataTypes.DoubleType)
                .add("currency", DataTypes.StringType)
                .add("status", DataTypes.StringType)
                .add("merchant_id", DataTypes.StringType)
                .add("event_ts", DataTypes.TimestampType)
                .add("correlation_id", DataTypes.StringType);

        Dataset<Row> raw = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "payments-raw")
                .option("startingOffsets", "latest")
                .option("failOnDataLoss", "false")
                .load();

        Dataset<Row> parsed = raw
                .selectExpr("CAST(value AS STRING) AS payload",
                        "topic", "partition", "offset", "timestamp AS ingest_ts")
                .select(
                        from_json(col("payload"), schema).alias("d"),
                        col("ingest_ts"))
                .select("d.*", "ingest_ts");

        Dataset<Row> enriched = parsed
                .withColumn("correlation_id",
                        when(col("correlation_id").isNull(),
                                concat(lit("corr-gen-"), uuid()))
                                .otherwise(col("correlation_id")))
                .withColumn("processing_latency_ms",
                        expr("(unix_timestamp(current_timestamp()) - unix_timestamp(event_ts)) * 1000"));

        StreamingQuery query = enriched.writeStream()
                .foreachBatch((Dataset<Row> batch, Long batchId) -> {
                    long start = System.currentTimeMillis();
                    long count = batch.count();
                    StructuredLogger.logBatchStart(batchId, (int) count);

                    try {
                        long success = batch.filter(col("status").equalTo("SUCCESS")).count();
                        long failed = batch.filter(col("status").equalTo("FAILED")).count();
                        long maxLatency = batch.agg(max("processing_latency_ms"))
                                .head().getLong(0);

                        // FIX: Use cast instead of .longValue() on primitive double
                        long avgLatency = (long) batch.agg(avg("processing_latency_ms"))
                                .head().getDouble(0);

                        StructuredLogger.logBatchEnd(batchId,
                                System.currentTimeMillis() - start, success, failed);
                        MDC_put("signal", "latency");
                        org.slf4j.MDC.put("batch_id", String.valueOf(batchId));
                        org.slf4j.LoggerFactory.getLogger("paynova.obs")
                                .info("event=golden_signals batch_id={} max_latency_ms={} avg_latency_ms={}",
                                        batchId, maxLatency, avgLatency);
                        org.slf4j.MDC.clear();

                        StructuredLogger.logSaturation(batchId,
                                Runtime.getRuntime().freeMemory() / (1024 * 1024),
                                (Runtime.getRuntime().totalMemory()
                                        - Runtime.getRuntime().freeMemory()) / (1024 * 1024));

                        batch.selectExpr("to_json(struct(*)) AS value")
                                .write().format("kafka")
                                .option("kafka.bootstrap.servers", "localhost:9092")
                                .option("topic", "payments-enriched")
                                .save();

                    } catch (Exception e) {
                        StructuredLogger.logError(batchId, "unknown", e);
                        batch.selectExpr("to_json(struct(*)) AS value")
                                .write().format("kafka")
                                .option("kafka.bootstrap.servers", "localhost:9092")
                                .option("topic", "payments-dead-letter")
                                .save();
                    }
                })
                .option("checkpointLocation", "/tmp/spark/paynova-obs-cp")
                .trigger(Trigger.ProcessingTime("10 seconds"))
                .outputMode("update")
                .start();

        query.awaitTermination();
    }

    private static void MDC_put(String k, String v) {
        org.slf4j.MDC.put(k, v);
    }
}