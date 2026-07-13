package com.demo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class BeforeSchemaEnforcement {

    public static void main(String[] args) throws Exception {

        SparkSession spark =
                SparkSession.builder()
                        .appName("BeforeSchemaEnforcement")
                        .master("local[*]")
                        .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        Dataset<Row> kafkaDf =
                spark.readStream()
                        .format("kafka")
                        .option(
                                "kafka.bootstrap.servers",
                                "localhost:9092")
                        .option(
                                "subscribe",
                                "banking-transactions")
                        .option(
                                "startingOffsets",
                                "earliest")
                        .load();

        Dataset<Row> rawJson =
                kafkaDf.selectExpr(
                        "topic",
                        "partition",
                        "offset",
                        "CAST(value AS STRING) as json");

        rawJson.writeStream()
                .format("console")
                .outputMode("append")
                .option("truncate", false)
                .option(
                        "checkpointLocation",
                        "/tmp/before-schema-checkpoint")
                .start()
                .awaitTermination();
    }
}