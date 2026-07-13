package com.retail;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Scanner;

import static org.apache.spark.sql.functions.*;

public class RetailDeltaLakehouseApp {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("Retail Delta Lake Demo")
                .master("local[*]")

                // Delta Lake Configuration
                .config("spark.sql.extensions",
                        "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog",
                        "org.apache.spark.sql.delta.catalog.DeltaCatalog")

                .getOrCreate();

        Dataset<Row> orders = spark.range(1000)
                .withColumn("orderId", concat(lit("ORD-"), col("id")))
                .withColumn("customerId", expr("CAST(rand()*100 AS INT)"))
                .withColumn("amount", round(expr("rand()*1000"), 2))
                .withColumn("status",
                        expr("CASE WHEN rand() > 0.5 THEN 'COMPLETED' ELSE 'PENDING' END"));

        // Write as Delta Table
        orders.write()
                .format("delta")
                .mode("overwrite")
                .save("data/delta/orders");

        System.out.println("\n==============================================");
        System.out.println("Delta Table created successfully.");
        System.out.println("Location : data/delta/orders");
        System.out.println("Spark UI : http://localhost:4040");
        System.out.println("==============================================");

        System.out.println("\nReading Delta Table...\n");

        Dataset<Row> deltaOrders = spark.read()
                .format("delta")
                .load("data/delta/orders");

        deltaOrders.show(10, false);

        System.out.println("\nSchema");
        deltaOrders.printSchema();

        System.out.println("\nPress ENTER to stop the application...");
        new Scanner(System.in).nextLine();

        spark.stop();
    }
}