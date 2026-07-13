package com.retail;

import org.apache.spark.sql.*;
import java.util.Scanner;

import static org.apache.spark.sql.functions.*;

public class RetailDeltaSchemaEvolutionDemo {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("Delta Schema Evolution Demo")
                .master("local[*]")
                .config("spark.sql.extensions",
                        "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog",
                        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .getOrCreate();

        String path = "data/delta/schemaEvolution";

        //----------------------------------------------------
        // Version 1
        //----------------------------------------------------
        Dataset<Row> ordersV1 = spark.range(5)
                .withColumn("orderId", concat(lit("ORD-"), col("id")))
                .withColumn("amount", expr("rand()*1000"));

        ordersV1.write()
                .format("delta")
                .mode("overwrite")
                .save(path);

        System.out.println("Version 1 Created");

        //----------------------------------------------------
        // Version 2
        //----------------------------------------------------
        Dataset<Row> ordersV2 = spark.range(5,10)
                .withColumn("orderId", concat(lit("ORD-"), col("id")))
                .withColumn("amount", expr("rand()*1000"))
                .withColumn("paymentMode",
                        expr("CASE WHEN rand()>0.5 THEN 'CARD' ELSE 'UPI' END"));

        ordersV2.write()
                .format("delta")
                .mode("append")
                .option("mergeSchema","true")
                .save(path);

        System.out.println("Schema Evolution Completed");

        Dataset<Row> finalTable = spark.read()
                .format("delta")
                .load(path);

        finalTable.show(false);

        System.out.println("Final Schema");

        finalTable.printSchema();

        System.out.println("\nPress ENTER...");
        new Scanner(System.in).nextLine();

        spark.stop();
    }
}