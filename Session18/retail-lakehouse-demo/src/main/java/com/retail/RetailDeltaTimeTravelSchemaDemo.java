package com.retail;

import io.delta.tables.DeltaTable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.nio.file.*;
import java.util.Comparator;
import java.util.Scanner;

import static org.apache.spark.sql.functions.*;

public class RetailDeltaTimeTravelSchemaDemo {

    public static void main(String[] args) {

        String path = "data/delta/timeTravel";

        //-------------------------------------------------
        // Delete Existing Delta Table
        //-------------------------------------------------

        deleteDirectory(path);

        //-------------------------------------------------
        // Create Spark Session
        //-------------------------------------------------

        SparkSession spark = SparkSession.builder()
                .appName("Retail Delta Lake Demo")
                .master("local[*]")
                .config("spark.sql.extensions",
                        "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog",
                        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .getOrCreate();

        //-------------------------------------------------
        // VERSION 0
        //-------------------------------------------------

        Dataset<Row> ordersV0 = spark.range(5)
                .withColumn("orderId", concat(lit("ORD-"), col("id")))
                .withColumn("amount", round(expr("rand()*1000"), 2));

        ordersV0.write()
                .format("delta")
                .mode("overwrite")
                .save(path);

        System.out.println("✅ Version 0 Created");

        //-------------------------------------------------
        // VERSION 1
        //-------------------------------------------------

        Dataset<Row> ordersV1 = spark.range(5, 10)
                .withColumn("orderId", concat(lit("ORD-"), col("id")))
                .withColumn("amount", round(expr("rand()*1000"), 2));

        ordersV1.write()
                .format("delta")
                .mode("append")
                .save(path);

        System.out.println("✅ Version 1 Created");

        //-------------------------------------------------
        // VERSION 2 (Schema Evolution)
        //-------------------------------------------------

        Dataset<Row> ordersV2 = spark.range(10, 15)
                .withColumn("orderId", concat(lit("ORD-"), col("id")))
                .withColumn("amount", round(expr("rand()*1000"), 2))
                .withColumn("paymentMode",
                        expr("CASE WHEN rand() > 0.5 THEN 'CARD' ELSE 'UPI' END"));

        ordersV2.write()
                .format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .save(path);

        System.out.println("✅ Version 2 Created (Schema Evolution)");

        //-------------------------------------------------
        // CURRENT TABLE
        //-------------------------------------------------

        System.out.println("\n================ CURRENT TABLE ================\n");

        Dataset<Row> latest = spark.read()
                .format("delta")
                .load(path);

        latest.show(false);

        //-------------------------------------------------
        // CURRENT SCHEMA
        //-------------------------------------------------

        System.out.println("\n================ CURRENT SCHEMA ================\n");

        latest.printSchema();

        //-------------------------------------------------
        // DELTA HISTORY
        //-------------------------------------------------

        System.out.println("\n================ DELTA HISTORY ================\n");

        DeltaTable.forPath(spark, path)
                .history()
                .select(
                        "version",
                        "timestamp",
                        "operation",
                        "readVersion",
                        "operationMetrics"
                )
                .show(false);

        //-------------------------------------------------
        // VERSION 0
        //-------------------------------------------------

        System.out.println("\n================ VERSION 0 ====================\n");

        spark.read()
                .format("delta")
                .option("versionAsOf", 0)
                .load(path)
                .show(false);

        //-------------------------------------------------
        // VERSION 1
        //-------------------------------------------------

        System.out.println("\n================ VERSION 1 ====================\n");

        spark.read()
                .format("delta")
                .option("versionAsOf", 1)
                .load(path)
                .show(false);

        //-------------------------------------------------
        // VERSION 2
        //-------------------------------------------------

        System.out.println("\n================ VERSION 2 ====================\n");

        spark.read()
                .format("delta")
                .option("versionAsOf", 2)
                .load(path)
                .show(false);

        //-------------------------------------------------
        // Pause
        //-------------------------------------------------

        System.out.println("\n======================================================");
        System.out.println("Delta Table Location : " + path);
        System.out.println();
        System.out.println("Open the folder:");
        System.out.println("data");
        System.out.println(" └── delta");
        System.out.println("      └── timeTravel");
        System.out.println("           ├── _delta_log");
        System.out.println("           ├── part-0000....snappy.parquet");
        System.out.println();
        System.out.println("Spark UI : http://localhost:4040");
        System.out.println();
        System.out.println("Observe:");
        System.out.println("✓ Delta Transaction History");
        System.out.println("✓ Schema Evolution");
        System.out.println("✓ Time Travel");
        System.out.println();
        System.out.println("Press ENTER to exit...");
        System.out.println("======================================================");

        new Scanner(System.in).nextLine();

        spark.stop();

        System.out.println("Application stopped.");
    }

    //-------------------------------------------------
    // Utility Method
    //-------------------------------------------------

    private static void deleteDirectory(String directory) {

        Path path = Paths.get(directory);

        if (!Files.exists(path))
            return;

        try {

            Files.walk(path)
                    .sorted(Comparator.reverseOrder())
                    .forEach(p -> {
                        try {
                            Files.delete(p);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    });

            System.out.println("Old Delta table deleted.\n");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}