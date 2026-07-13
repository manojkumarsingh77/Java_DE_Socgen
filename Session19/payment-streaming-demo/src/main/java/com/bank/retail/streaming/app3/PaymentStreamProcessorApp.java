package com.bank.retail.streaming.app3;

import com.bank.retail.streaming.model.PaymentOrderEvent;
import com.bank.retail.streaming.model.ProcessedPaymentEvent;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.io.File;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;

/**
 * ============================================================================
 *  APP 3 - SPARK STRUCTURED STREAMING PAYMENT PROCESSOR
 * ============================================================================
 * BUSINESS PROBLEM THIS SOLVES:
 * This is the production payment pipeline: it continuously reads orders off
 * Kafka, runs each one through fraud-check + simulated payment-gateway
 * processing (PaymentGatewaySimulator), reports Golden Signals for every
 * micro-batch (GoldenSignalsReporter), and durably lands the enriched result
 * in a Delta Lake table - the system of record App4 and App5 read from.
 *
 * >>> THE METHOD THAT WIRES THE WHOLE PIPELINE TOGETHER IS: runPipeline() <<<
 * (the per-record business logic itself lives in PaymentGatewaySimulator.processPayment())
 *
 * !!! REQUIRES VM OPTIONS to run on Java 17 - see README.md "VM Options" !!!
 * Without them you will see IllegalAccessError / InaccessibleObjectException
 * from Spark trying to reach into JDK internals that Java 17 locks down by
 * default.
 * ============================================================================
 */
public class PaymentStreamProcessorApp {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String ORDERS_TOPIC = "retail.payments.orders";

    // Everything this app writes (Delta table + streaming checkpoint) lands
    // under <project-root>/spark-demo-data/. Using the JVM's working
    // directory keeps this path portable across Windows/Mac/Linux without
    // any hardcoded drive letters or /tmp assumptions.
    private static final String BASE_DIR = System.getProperty("user.dir") + File.separator + "spark-demo-data";
    private static final String DELTA_PATH = BASE_DIR + File.separator + "delta" + File.separator + "processed_payments";
    private static final String CHECKPOINT_PATH = BASE_DIR + File.separator + "checkpoints" + File.separator + "payment_stream";

    public static void main(String[] args) throws Exception {

        SparkSession spark = buildSparkSession();

        // Keep Spark's own internal logging quiet - log4j2.properties already
        // does this, but setting it here too means it applies even if someone
        // runs this class with a different log config.
        spark.sparkContext().setLogLevel("WARN");

        System.out.println("================================================================");
        System.out.println(" Retail Banking Payment Stream Processor starting up");
        System.out.println(" Kafka topic   : " + ORDERS_TOPIC + " @ " + BOOTSTRAP_SERVERS);
        System.out.println(" Delta table   : " + DELTA_PATH);
        System.out.println(" Checkpoint    : " + CHECKPOINT_PATH);
        System.out.println(" Spark UI      : http://localhost:4040  (while this app is running)");
        System.out.println("================================================================");

        runPipeline(spark);
    }

    private static SparkSession buildSparkSession() {
        return SparkSession.builder()
                .appName("RetailBankingPaymentStreamProcessor")
                // local[*] : run Spark's driver AND executors inside this
                // one JVM, using all available CPU cores. Perfect for a
                // laptop demo - no separate Spark cluster needed.
                .master("local[*]")
                // These two configs register Delta Lake's SQL extensions
                // and catalog implementation with Spark - REQUIRED for
                // .format("delta") to work at all.
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                // Keeps the number of shuffle partitions small and sane for
                // a laptop-scale demo (Spark's default of 200 is tuned for
                // a real cluster, and is wasteful overhead here).
                .config("spark.sql.shuffle.partitions", "4")
                .getOrCreate();
    }

    /**
     * >>> SOLUTION METHOD <<<
     * Wires together: Kafka source -> JSON parsing -> typed Dataset ->
     * fraud/payment processing -> Golden Signals + Delta sink, and starts
     * the streaming query.
     */
    private static void runPipeline(SparkSession spark) throws Exception {

        StructType orderSchema = buildOrderSchema();

        // ---- SOURCE: read continuously from the Kafka topic ----
        Dataset<Row> kafkaRaw = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
                .option("subscribe", ORDERS_TOPIC)
                // "latest": only see orders published AFTER this app starts.
                // Use "earliest" instead if you want to replay everything
                // App2 has already published to this topic.
                .option("startingOffsets", "latest")
                // Don't crash the whole streaming query if one message is
                // unreadable (e.g. broker compaction edge cases) - log and
                // move on, the way a resilient production job would.
                .option("failOnDataLoss", "false")
                .load();

        // Kafka's "value" column is raw bytes; CAST to STRING gives us the
        // JSON text App2 published. from_json() then parses that text
        // against our explicit schema into a structured "data" column, and
        // .select("data.*") flattens it back out into top-level columns.
        Dataset<Row> parsed = kafkaRaw
                .selectExpr("CAST(value AS STRING) AS json")
                .select(from_json(col("json"), orderSchema).as("data"))
                .select("data.*");

        // Encoders.bean(...) turns the generic Dataset<Row> into a
        // COMPILE-TIME-CHECKED Dataset<PaymentOrderEvent>: from here on,
        // order.getAmount() / order.getChannel() etc. are real Java method
        // calls, not error-prone "row.getAs(\"amount\")" string lookups.
        Dataset<PaymentOrderEvent> orders = parsed.as(Encoders.bean(PaymentOrderEvent.class));

        // ---- PROCESSING: fraud check + simulated payment gateway ----
        // map(): runs PaymentGatewaySimulator.processPayment() once for
        // EVERY incoming order, independently and in parallel across
        // Spark's executor threads. A fresh PaymentGatewaySimulator is
        // created inside the lambda (it has no internal state) so nothing
        // about it needs to be Serializable for Spark to ship this closure
        // to executor threads.
        Dataset<ProcessedPaymentEvent> processed = orders.map(
                (MapFunction<PaymentOrderEvent, ProcessedPaymentEvent>) order ->
                        new PaymentGatewaySimulator().processPayment(order),
                Encoders.bean(ProcessedPaymentEvent.class)
        );

        // ---- SINK: every micro-batch, report Golden Signals AND persist to Delta ----
        VoidFunction2<Dataset<ProcessedPaymentEvent>, Long> batchHandler = (batchTyped, batchId) -> {
            Dataset<Row> batchDf = batchTyped.toDF();

            if (batchDf.isEmpty()) {
                // Nothing arrived this trigger interval - perfectly normal
                // between bursts of orders; nothing to report or write.
                return;
            }

            // persist(): the SAME batch data is read TWICE below (once for
            // Golden Signals aggregation, once for the Delta write). Without
            // caching, Spark would needlessly recompute the whole micro-batch
            // from Kafka again for the second read.
            batchDf.persist();
            try {
                new GoldenSignalsReporter().report(batchDf, batchId);

                // partitionBy("paymentStatus","eventDate"): physically lays
                // the Delta table out in folders like
                // paymentStatus=FAILED/eventDate=2026-06-29/... so that
                // App4's incident queries (which filter heavily by status
                // and date) only scan the relevant files instead of the
                // whole table - a standard data-lake performance practice.
                batchDf.write()
                        .format("delta")
                        .mode("append")
                        .partitionBy("paymentStatus", "eventDate")
                        .save(DELTA_PATH);
            } finally {
                batchDf.unpersist();
            }
        };

        StreamingQuery query = processed.writeStream()
                .outputMode("append")
                // Trigger every 5 seconds: groups whatever orders arrived in
                // that window into one micro-batch. Short enough to feel
                // "live" in a demo, long enough to see multiple orders land
                // together per Golden Signals log line.
                .trigger(Trigger.ProcessingTime("5 seconds"))
                .foreachBatch(batchHandler)
                .option("checkpointLocation", CHECKPOINT_PATH)
                .start();

        System.out.println("Streaming query started (id=" + query.id() + "). Waiting for orders... "
                + "Run App2 (PaymentOrderProducerApp) now if you haven't already.");

        // Blocks the main thread here forever, keeping the JVM (and the
        // streaming query) alive until you stop the run from IntelliJ.
        query.awaitTermination();
    }

    /**
     * Explicit schema for the JSON each Kafka message carries. WHY explicit
     * and not schema inference? Structured Streaming CANNOT infer a schema
     * from a live, unbounded stream (there's no fixed file to sample), so
     * from_json() always requires you to state the schema up front - and
     * doing so also fails fast/clearly if App2's JSON shape ever drifts.
     */
    private static StructType buildOrderSchema() {
        return new StructType()
                .add("correlationId", DataTypes.StringType)
                .add("orderId", DataTypes.StringType)
                .add("customerId", DataTypes.StringType)
                .add("customerName", DataTypes.StringType)
                .add("accountNumber", DataTypes.StringType)
                .add("ifscCode", DataTypes.StringType)
                .add("bankName", DataTypes.StringType)
                .add("channel", DataTypes.StringType)
                .add("merchantCategory", DataTypes.StringType)
                .add("merchantName", DataTypes.StringType)
                .add("amount", DataTypes.DoubleType)
                .add("currency", DataTypes.StringType)
                .add("deviceId", DataTypes.StringType)
                .add("newDevice", DataTypes.BooleanType)
                .add("city", DataTypes.StringType)
                .add("orderTimestamp", DataTypes.LongType);
    }
}
