package com.retail;

import org.apache.spark.sql.*;
import java.util.Scanner;

import static org.apache.spark.sql.functions.*;

public class RetailLakehouseApp {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("Retail Lakehouse Demo")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> orders = spark.range(1000)
                .withColumn("orderId", concat(lit("ORD-"), col("id")))
                .withColumn("amount", expr("rand()*1000"));

        orders.write()
                .mode("overwrite")
                .parquet("data/parquet/orders");

        System.out.println("\n==============================================");
        System.out.println("Parquet dataset written successfully.");
        System.out.println("Spark UI: http://localhost:4040");
        System.out.println("Inspect Jobs, Stages, SQL, Storage, Executors.");
        System.out.println("Press ENTER to stop the application...");
        System.out.println("==============================================\n");

        // Keep Spark application alive
        new Scanner(System.in).nextLine();

        spark.stop();
        System.out.println("Spark application stopped.");
    }
}