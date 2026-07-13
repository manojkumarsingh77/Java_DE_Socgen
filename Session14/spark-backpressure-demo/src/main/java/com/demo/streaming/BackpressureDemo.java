package com.demo.streaming;

import org.apache.spark.sql.*;

public class BackpressureDemo {

    public static void main(String[] args)
            throws Exception {

        SparkSession spark =
                SparkSession.builder()
                        .appName("BackpressureDemo")
                        .master("local[*]")
                        .getOrCreate();

        Dataset<Row> kafkaDF =
                spark.readStream()
                        .format("kafka")
                        .option(
                                "kafka.bootstrap.servers",
                                "localhost:9092")
                        .option(
                                "subscribe",
                                "telecom-events")

                        .option(
                                "maxOffsetsPerTrigger",
                                "10000")

                        .load();

        kafkaDF.writeStream()
                .format("console")
                .option("truncate",false)
                .start()
                .awaitTermination();
    }
}