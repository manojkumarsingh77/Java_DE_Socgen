package com.paynova.obs;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import java.util.Collections;

import static org.apache.spark.sql.functions.*;

public class ObservabilityStreamingJob1 {

    public static void main(String[] args) {
        SparkSession spark = null;
        try {
            spark = SparkSession.builder()
                    .appName("PayNova-Observability")
                    .master("local[*]")
                    .config("spark.sql.shuffle.partitions", "2")
                    .getOrCreate();

            spark.sparkContext().setLogLevel("ERROR");

            // ---------------------------------------------------------
            // SANITY CHECK: Warn if error-classes.json is missing
            // (This was the root cause of the silent ExceptionInInitializerError)
            // ---------------------------------------------------------
            if (SparkSession.class.getClassLoader().getResource("org/apache/spark/sql/error-classes.json") == null) {
                System.err.println("[WARNING] 'error-classes.json' is MISSING from the classpath!");
                System.err.println("[WARNING] Re-import Maven dependencies or don't shade Spark jars to fix silent crashes.");
            } else {
                System.out.println("[INFO] Spark error-classes.json found successfully on classpath.");
            }

            StructType schema = new StructType()
                    .add("payment_id", DataTypes.StringType)
                    .add("customer_id", DataTypes.StringType)
                    .add("amount", DataTypes.DoubleType)
                    .add("currency", DataTypes.StringType)
                    .add("status", DataTypes.StringType)
                    .add("merchant_id", DataTypes.StringType)
                    .add("event_ts", DataTypes.TimestampType)
                    .add("correlation_id", DataTypes.StringType);

            // Print the layout first as requested
            System.out.println("\n[SCHEMA] Initial Empty Target Structure Matrix:");
            spark.createDataFrame(Collections.emptyList(), schema).show(false);

            Dataset<Row> raw = spark.readStream()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", "localhost:9092")
                    .option("subscribe", "payments-raw")
                    .option("startingOffsets", "latest")
                    .option("failOnDataLoss", "false")
                    .load();

            Dataset<Row> parsed = raw
                    .selectExpr("CAST(value AS STRING) AS payload", "timestamp AS ingest_ts")
                    .select(from_json(col("payload"), schema).alias("d"), col("ingest_ts"))
                    .select("d.*", "ingest_ts");

            Dataset<Row> enriched = parsed.withColumn("correlation_id",
                            when(col("correlation_id").isNull(), concat(lit("corr-gen-"), uuid()))
                                    .otherwise(col("correlation_id")))
                    .withColumn("processing_latency_ms",
                            unix_timestamp(current_timestamp()).minus(unix_timestamp(col("event_ts"))).multiply(1000));

            System.out.println("[INFO] Stream Engine Context Active. Waiting for Kafka messages indefinitely...\n");

            // FIXED: Use a STABLE checkpoint path.
            // If you need a fresh start, manually delete this directory instead of changing the path.
            String stableCheckpointPath = System.getProperty("user.dir") + "/target/spark-checkpoints/paynova-obs";

            StreamingQuery query = enriched.writeStream()
                    .format("console")
                    .outputMode("append")
                    .option("truncate", "false")
                    .option("checkpointLocation", stableCheckpointPath)
                    .trigger(Trigger.ProcessingTime("5 seconds"))
                    .start();

            // Block main thread until termination
            query.awaitTermination();

            // If awaitTermination returns, check if the query actually failed
            if (query.exception().isDefined()) {
                System.err.println("[CRITICAL] Streaming query terminated with exception:");
                query.exception().get().printStackTrace();
            } else {
                System.out.println("[INFO] Streaming query terminated normally.");
            }

        } catch (Throwable t) {
            // FIXED: Catch Throwable instead of Exception to catch java.lang.Error cascades
            System.err.println("[CRITICAL] Main execution driver encountered a fatal error:");
            t.printStackTrace();
        } finally {
            if (spark != null) {
                spark.stop();
            }
        }
    }
}