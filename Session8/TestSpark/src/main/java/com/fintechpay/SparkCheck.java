package com.fintechpay;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkCheck {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("Spark Check")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> data = spark.range(1, 11).toDF("number");

        data.show();

        System.out.println("Spark Version = " + spark.version());

        spark.stop();
    }
}