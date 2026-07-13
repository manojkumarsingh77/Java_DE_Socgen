package com.retail;

import com.retail.schema.RetailSchemas;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

import static org.apache.spark.sql.functions.*;

public class RetailStreamingApp {

 public static void main(String[] args) throws Exception {

  SparkSession spark =
          SparkSession.builder()
                  .appName("Retail Streaming Demo")
                  .master("local[*]")
                  .config("spark.sql.shuffle.partitions", "4")
                  .config("spark.default.parallelism", "4")
                  .getOrCreate();

  spark.sparkContext().setLogLevel("WARN");

  System.out.println();
  System.out.println("===========================================");
  System.out.println(" RETAIL STREAMING ENGINE STARTED");
  System.out.println("===========================================");
  System.out.println("Spark Version : " + spark.version());
  System.out.println("Kafka Broker  : localhost:9092");
  System.out.println("Topic         : retail-orders");
  System.out.println("===========================================");

  Dataset<Row> kafkaDf =
          spark.readStream()
                  .format("kafka")
                  .option(
                          "kafka.bootstrap.servers",
                          "localhost:9092")
                  .option(
                          "subscribe",
                          "retail-orders")
                  .option(
                          "startingOffsets",
                          "latest")
                  .load();

  Dataset<Row> raw =
          kafkaDf.selectExpr(
                  "CAST(value AS STRING) as json",
                  "partition",
                  "offset",
                  "timestamp");

  Dataset<Row> parsed =
          raw.select(
                          col("partition"),
                          col("offset"),
                          col("timestamp"),
                          from_json(
                                  col("json"),
                                  RetailSchemas.orderSchema())
                                  .alias("data"))
                  .select(
                          col("partition"),
                          col("offset"),
                          col("timestamp"),
                          col("data.*"));

  /*
   * Validation Logic
   */
  Dataset<Row> tagged =
          parsed.withColumn(
                          "validation_status",
                          when(
                                  col("orderId").isNotNull()
                                          .and(length(trim(col("orderId"))).gt(0))
                                          .and(col("customerId").isNotNull())
                                          .and(length(trim(col("customerId"))).gt(0))
                                          .and(col("productId").isNotNull())
                                          .and(length(trim(col("productId"))).gt(0))
                                          .and(col("amount").gt(0)),
                                  lit("VALID"))
                                  .otherwise(lit("INVALID")))
                  .withColumn(
                          "validation_reason",
                          when(
                                  col("orderId").isNull()
                                          .or(length(trim(col("orderId"))).equalTo(0)),
                                  lit("Missing OrderId"))
                                  .when(
                                          col("customerId").isNull()
                                                  .or(length(trim(col("customerId"))).equalTo(0)),
                                          lit("Missing CustomerId"))
                                  .when(
                                          col("productId").isNull()
                                                  .or(length(trim(col("productId"))).equalTo(0)),
                                          lit("Missing ProductId"))
                                  .when(
                                          col("amount").leq(0),
                                          lit("Amount Must Be Greater Than Zero"))
                                  .otherwise(lit("Record Passed Validation")));

  Dataset<Row> output =
          tagged.select(
                  col("partition"),
                  col("offset"),
                  col("timestamp"),
                  col("orderId"),
                  col("customerId"),
                  col("productId"),
                  col("amount"),
                  col("validation_status"),
                  col("validation_reason"));

  StreamingQuery query =
          output.writeStream()
                  .format("console")
                  .outputMode("append")
                  .option("truncate", false)
                  .option("numRows", 100)
                  .option(
                          "checkpointLocation",
                          "/tmp/retail-console-checkpoint")
                  .queryName("RETAIL-VALIDATION")
                  .start();

  System.out.println();
  System.out.println("===========================================");
  System.out.println(" STREAMING STARTED");
  System.out.println("===========================================");
  System.out.println("VALID records  -> validation_status=VALID");
  System.out.println("INVALID records-> validation_status=INVALID");
  System.out.println("===========================================");

  spark.streams().awaitAnyTermination();
 }
}