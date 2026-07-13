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

public class RetailDeltaTimeTravelDemo {

    public static void main(String[] args) {

        String path = "data/delta/timeTravel";

        //-------------------------------------------------
        // Delete existing Delta table
        //-------------------------------------------------

        deleteDirectory(path);

        //-------------------------------------------------
        // Create Spark Session
        //-------------------------------------------------

        SparkSession spark = SparkSession.builder()
                .appName("Delta Time Travel Demo")
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
                .withColumn("orderId",
                        concat(lit("ORD-"), col("id")))
                .withColumn("amount",
                        round(expr("rand()*1000"), 2));

        ordersV0.write()
                .format("delta")
                .mode("overwrite")
                .save(path);

        System.out.println("✅ Version 0 Created");

        //-------------------------------------------------
        // VERSION 1
        //-------------------------------------------------

        Dataset<Row> ordersV1 = spark.range(5, 10)
                .withColumn("orderId",
                        concat(lit("ORD-"), col("id")))
                .withColumn("amount",
                        round(expr("rand()*1000"), 2));

        ordersV1.write()
                .format("delta")
                .mode("append")
                .save(path);

        System.out.println("✅ Version 1 Created");

        //-------------------------------------------------
        // CURRENT TABLE
        //-------------------------------------------------

        System.out.println("\n================ CURRENT TABLE ================\n");

        spark.read()
                .format("delta")
                .load(path)
                .show(false);

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

        System.out.println("\n==============================================");
        System.out.println("Delta Table Location : " + path);
        System.out.println("Folder Structure:");
        System.out.println("data");
        System.out.println(" └── delta");
        System.out.println("      └── timeTravel");
        System.out.println("           ├── _delta_log");
        System.out.println("           ├── part-xxxxx.parquet");
        System.out.println();
        System.out.println("Spark UI : http://localhost:4040");
        System.out.println("Press ENTER to stop...");
        System.out.println("==============================================");

        new Scanner(System.in).nextLine();

        spark.stop();

        System.out.println("Application stopped.");
    }

    //-------------------------------------------------
    // Utility Method
    //-------------------------------------------------

    private static void deleteDirectory(String directory) {

        Path path = Paths.get(directory);

        if (!Files.exists(path)) {
            return;
        }

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