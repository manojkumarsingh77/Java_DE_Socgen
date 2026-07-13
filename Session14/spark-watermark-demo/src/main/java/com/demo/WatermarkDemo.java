package com.demo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class WatermarkDemo {

    public static void main(String[] args) throws Exception {

        SparkSession spark =
                SparkSession.builder()
                        .appName("WatermarkDemo")
                        .master("local[*]")
                        .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        Dataset<Row> sourceDF =
                spark.readStream()
                        .format("rate")
                        .option("rowsPerSecond", 5)
                        .load();

        Dataset<Row> transactions =
                sourceDF.select(
                        col("timestamp").alias("eventTime"),
                        expr("value % 5").alias("customerId"),
                        expr("(value * 10) % 1000").alias("amount")
                );

        Dataset<Row> aggregatedDF =
                transactions
                        .withWatermark(                           // Watermark
                                "eventTime",
                                "10 minutes"
                        )
                        .groupBy(
                                window(
                                        col("eventTime"),
                                        "1 minute"
                                ),
                                col("customerId")
                        )
                        .agg(
                                sum("amount")
                                        .alias("totalAmount")
                        );

        aggregatedDF.writeStream()
                .outputMode("update")
                .format("console")
                .option("truncate", false)
                .start()
                .awaitTermination();
    }
}