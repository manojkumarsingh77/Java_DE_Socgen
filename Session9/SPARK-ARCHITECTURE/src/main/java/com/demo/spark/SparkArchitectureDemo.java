package com.demo.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.util.Scanner;

import static org.apache.spark.sql.functions.*;

public class SparkArchitectureDemo {

    public static void main(String[] args) {

        SparkSession spark =
                SparkSession.builder()
                        .appName("Spark Architecture Demo")
                        .master("local[4]")
                        .config("spark.sql.shuffle.partitions", "4")
                        .config("spark.default.parallelism", "4")
                        .config("spark.sql.adaptive.enabled", "false")
                        .getOrCreate();

        System.out.println("\n================================================");
        System.out.println("DRIVER PROGRAM STARTED");
        System.out.println("================================================");

        Dataset<Row> sales =
                spark.range(5_000_000)

                        .withColumn(
                                "region",
                                expr(
                                        "CASE " +
                                                "WHEN id % 4 = 0 THEN 'North' " +
                                                "WHEN id % 4 = 1 THEN 'South' " +
                                                "WHEN id % 4 = 2 THEN 'East' " +
                                                "ELSE 'West' END"
                                ))

                        .withColumn(
                                "amount",
                                expr("CAST(rand()*1000 AS INT)")
                        )

                        .withColumn(
                                "customer_id",
                                concat(lit("CUST_"), col("id"))
                        )

                        .repartition(8);

        System.out.println("\nDataset Created");

        System.out.println(
                "\nNumber Of Partitions = "
                        + sales.rdd().getNumPartitions());

        System.out.println(
                "\nNo Spark Job Yet...");
        System.out.println(
                "Only DAG Lineage Is Created");

        Dataset<Row> highValueSales =
                sales.filter(col("amount").gt(500));

        Dataset<Row> enrichedSales =
                highValueSales.withColumn(
                        "tax",
                        round(
                                col("amount").multiply(0.18),
                                2
                        )
                );

        System.out.println(
                "\nTransformations Added To DAG");

        enrichedSales.cache();

        System.out.println(
                "\nDataset Cached In Executors");

        Dataset<Row> regionalRevenue =
                enrichedSales
                        .groupBy("region")
                        .agg(
                                sum("amount")
                                        .alias("total_revenue"),
                                avg("amount")
                                        .alias("avg_revenue"),
                                count("*")
                                        .alias("sales_count")
                        );

        System.out.println(
                "\nGROUP BY Detected");
        System.out.println(
                "Shuffle Stage Will Be Created");

        System.out.println(
                "\n=====================================");
        System.out.println("LOGICAL + PHYSICAL PLAN");
        System.out.println("=====================================");

        regionalRevenue.explain(true);

        System.out.println(
                "\n=====================================");
        System.out.println("ACTION STARTS JOB");
        System.out.println("=====================================");

        regionalRevenue.show(false);

        System.out.println(
                "\n=====================================");
        System.out.println("CACHE DEMO");
        System.out.println("=====================================");

        long start1 = System.currentTimeMillis();

        long count1 =
                enrichedSales.count();

        long end1 =
                System.currentTimeMillis();

        System.out.println(
                "First Count = "
                        + count1
                        + " Time = "
                        + (end1 - start1)
                        + " ms");

        long start2 =
                System.currentTimeMillis();

        long count2 =
                enrichedSales.count();

        long end2 =
                System.currentTimeMillis();

        System.out.println(
                "Second Count = "
                        + count2
                        + " Time = "
                        + (end2 - start2)
                        + " ms");

        System.out.println(
                "\nSecond Execution Faster Because Data Is Cached");

        System.out.println("\n====================================");
        System.out.println("SPARK UI DEMO MODE");
        System.out.println("====================================");

        System.out.println("Open Spark UI:");
        System.out.println("http://localhost:4040");

        System.out.println("\nPress ENTER to stop Spark...");

        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();

        spark.stop();

        System.out.println(
                "\nSpark Session Closed");
    }
}