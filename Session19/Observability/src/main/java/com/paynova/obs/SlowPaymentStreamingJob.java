package com.paynova.obs;

import brave.Span;
import brave.Tracer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;

public class SlowPaymentStreamingJob {

    public static void main(String[] args) throws Exception {

        PrometheusServer.start(9095);
        Tracer tracer = ZipkinTracer.init("paynova-spark-streaming",
                "http://localhost:9411");

        SparkSession spark = SparkSession.builder()
                .appName("PayNova-Monitoring")
                .master("local[*]")
                .config("spark.sql.shuffle.partitions", "4")
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
                .load();

        Dataset<Row> enriched = raw
                .selectExpr("CAST(value AS STRING) AS payload", "partition", "offset")
                .select(from_json(col("payload"), schema).alias("d"),
                        col("partition"), col("offset"))
                .select("d.*", "partition", "offset")
                .withColumn("correlation_id",
                        when(col("correlation_id").isNull(),
                                concat(lit("corr-gen-"), uuid()))
                                .otherwise(col("correlation_id")))
                .withColumn("e2e_latency_sec",
                        unix_timestamp(current_timestamp())
                                .minus(unix_timestamp(col("event_ts"))));

        final double SLA_THRESHOLD_SEC = 5.0;

        var query = enriched.writeStream()
                .foreachBatch((Dataset<Row> batch, Long batchId) -> {

                    Span rootSpan = tracer.newTrace().name("micro_batch").start();
                    rootSpan.tag("batch_id", String.valueOf(batchId));
                    MetricsRegistry.BATCH_IN_FLIGHT.set(1);

                    Span parseSpan = tracer.newChild(rootSpan.context())
                            .name("parse_validate").start();
                    long t0 = System.nanoTime();
                    long count = batch.count();
                    parseSpan.tag("record_count", String.valueOf(count));
                    parseSpan.finish();

                    try {
                        Span processSpan = tracer.newChild(rootSpan.context())
                                .name("enrich_aggregate").start();

                        long success = batch.filter(col("status").equalTo("SUCCESS")).count();
                        long failed  = batch.filter(col("status").equalTo("FAILED")).count();

                        // Per-merchant metrics
                        batch.groupBy("merchant_id", "status").count().collectAsList()
                                .forEach(r -> {
                                    String merchant = r.getString(0);
                                    String status = r.getString(1);
                                    long c = r.getLong(2);
                                    MetricsRegistry.PAYMENTS_PROCESSED
                                            .labels(status, merchant).inc(c);
                                });

                        // Latency histogram per payment (per merchant)
                        batch.select("merchant_id", "e2e_latency_sec").collectAsList()
                                .forEach(r -> {
                                    String merchant = r.getString(0);
                                    double latency = r.getDouble(1);
                                    MetricsRegistry.PAYMENT_LATENCY
                                            .labels(merchant).observe(latency);
                                    if (latency > SLA_THRESHOLD_SEC) {
                                        org.slf4j.LoggerFactory.getLogger("paynova.mon.slow")
                                                .warn("event=slow_payment payment_id={} merchant={} latency_sec={} correlation_id={}",
                                                        r.getString(0), merchant, latency);
                                    }
                                });

                        // Consumer lag gauge per partition
                        batch.groupBy("partition").max("offset").collectAsList()
                                .forEach(r -> MetricsRegistry.CONSUMER_LAG
                                        .labels(String.valueOf(r.getInt(0)))
                                        .set(r.getLong(1)));

                        processSpan.finish();

                        // Sink
                        Span sinkSpan = tracer.newChild(rootSpan.context())
                                .name("kafka_sink").start();
                        batch.selectExpr("to_json(struct(*)) AS value")
                                .write().format("kafka")
                                .option("kafka.bootstrap.servers", "localhost:9092")
                                .option("topic", "payments-enriched")
                                .save();
                        sinkSpan.finish();

                        double durSec = (System.nanoTime() - t0) / 1e9;
                        MetricsRegistry.BATCH_DURATION.observe(durSec);

                    } catch (Exception e) {
                        rootSpan.error(e);
                        MetricsRegistry.PAYMENTS_PROCESSED
                                .labels("ERROR", "unknown").inc();
                    } finally {
                        MetricsRegistry.BATCH_IN_FLIGHT.set(0);
                        rootSpan.finish();
                    }
                })
                .option("checkpointLocation", "/tmp/spark/paynova-mon-cp")
                .trigger(Trigger.ProcessingTime("10 seconds"))
                .outputMode("update")
                .start();

        query.awaitTermination();
    }
}