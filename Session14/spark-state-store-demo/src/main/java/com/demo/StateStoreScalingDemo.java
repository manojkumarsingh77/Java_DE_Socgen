package com.demo;

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;

import static org.apache.spark.sql.functions.*;

public class StateStoreScalingDemo {

    public static void main(String[] args) throws Exception {

        SparkSession spark =
                SparkSession.builder()
                        .appName("State Store Scaling Demo")
                        .master("local[*]")

                        .config(
                                "spark.sql.shuffle.partitions",
                                "8")

                        .config(
                                "spark.sql.streaming.stateStore.providerClass",
                                "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider")

                        .getOrCreate();
                         /*
                         .config(
                                 "spark.sql.streaming.stateStore.providerClass",
                                  "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
                          */

        spark.sparkContext().setLogLevel("ERROR");

        Dataset<Row> source = spark
                .readStream()
                .format("rate")
                .option("rowsPerSecond", 500000)
                .load();

        Dataset<Row> customerEvents =
                source.select(
                        col("timestamp"),

                        expr(
                                "concat('C', cast(value % 10 as string))")
                                .alias("customerId")
                );

        Dataset<Row> aggregated =
                customerEvents
                        .withWatermark(
                                "timestamp",
                                "1 minute")

                        .groupBy(
                                window(
                                        col("timestamp"),
                                        "1 minute"),
                                col("customerId")
                        )
                        .count();

        StreamingQuery query =
                aggregated.writeStream()
                        .format("console")
                        .outputMode("complete")
                        .option("truncate", false)

                        .option(
                                "checkpointLocation",
                                "checkpoints/state-store")

                        .start();

        query.awaitTermination();
    }
}