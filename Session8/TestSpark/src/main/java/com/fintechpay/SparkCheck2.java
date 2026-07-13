package com.fintechpay;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkCheck2 {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("Word Count")
                .master("local[*]")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        Dataset<Row> df = spark.read()
                .text("src/main/resources/input.txt");

        df.show(false);

        spark.stop();
    }
}