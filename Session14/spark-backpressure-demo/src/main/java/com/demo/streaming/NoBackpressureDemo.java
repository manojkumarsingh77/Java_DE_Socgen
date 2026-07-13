package com.demo.streaming;

import org.apache.spark.sql.*;

public class NoBackpressureDemo {

    public static void main(String[] args)
            throws Exception {

        SparkSession spark =
                SparkSession.builder()
                        .appName("NoBackpressure")
                        .master("local[*]")
                        .getOrCreate();

        Dataset<Row> kafkaDF =
                spark.readStream()
                        .format("kafka")
                        .option("kafka.bootstrap.servers",
                                "localhost:9092")
                        .option("subscribe",
                                "telecom-events")
                        .load();

        kafkaDF.writeStream()
                .format("console")
                .start()
                .awaitTermination();
    }
}