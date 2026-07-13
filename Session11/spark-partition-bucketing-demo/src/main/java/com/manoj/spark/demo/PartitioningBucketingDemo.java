package com.manoj.spark.demo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class PartitioningBucketingDemo {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("PartitioningBucketingDemo")
                .config("spark.master", "local[*]")
                .config("spark.sql.shuffle.partitions", "8")
                .config("spark.default.parallelism", "8")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .config("spark.ui.enabled", "true")
                .config("spark.ui.port", "4040")
                .getOrCreate();

        try {

            System.out.println("\n====================================");
            System.out.println("Spark UI:");
            System.out.println(
                    spark.sparkContext().uiWebUrl().isDefined()
                            ? spark.sparkContext().uiWebUrl().get()
                            : "Not Available"
            );
            System.out.println("====================================\n");

            // =====================================================
            // Generate Demo Data
            // =====================================================

            Dataset<Row> orders = spark.range(1, 1_000_000)

                    .withColumnRenamed("id", "order_id")

                    .withColumn(
                            "customer_id",
                            functions.expr(
                                    "cast(rand()*10000 as int)"
                            )
                    )

                    .withColumn(
                            "country",
                            functions.expr(
                                    "CASE " +
                                            "WHEN rand() < 0.25 THEN 'IN' " +
                                            "WHEN rand() < 0.50 THEN 'US' " +
                                            "WHEN rand() < 0.75 THEN 'UK' " +
                                            "ELSE 'DE' END"
                            )
                    )

                    .withColumn(
                            "amount",
                            functions.expr(
                                    "round(rand()*10000,2)"
                            )
                    )

                    .withColumn(
                            "order_date",
                            functions.expr(
                                    "CASE " +
                                            "WHEN rand() < 0.5 THEN '2024-01-15' " +
                                            "ELSE '2025-03-20' END"
                            )
                    );

            orders = orders.withColumn(
                    "order_year",
                    functions.year(
                            functions.to_date(
                                    functions.col("order_date"),
                                    "yyyy-MM-dd"
                            )
                    )
            );

            System.out.println("Sample Data");
            orders.show(10, false);

            System.out.println(
                    "\nTotal Records = "
                            + orders.count()
            );

            // =====================================================
            // PARTITIONING
            // =====================================================

            long partitionStart =
                    System.currentTimeMillis();

            String partitionPath =
                    "output/partitioned_orders";

            orders.write()
                    .mode(SaveMode.Overwrite)
                    .partitionBy(
                            "country",
                            "order_year"
                    )
                    .parquet(partitionPath);

            long partitionCount =
                    spark.read()
                            .parquet(partitionPath)
                            .count();

            long partitionEnd =
                    System.currentTimeMillis();

            System.out.println("\n====================================");
            System.out.println("PARTITIONING COMPLETE");
            System.out.println("Records : "
                    + partitionCount);
            System.out.println("Time(ms): "
                    + (partitionEnd - partitionStart));
            System.out.println("====================================");

            pause(
                    "Inspect Spark UI for Partitioning.\n" +
                            "Press ENTER to continue..."
            );

            // =====================================================
            // BUCKETING CONCEPT DEMO
            // =====================================================

            long bucketStart =
                    System.currentTimeMillis();

            Dataset<Row> bucketedData =
                    orders.repartition(
                            8,
                            functions.col("customer_id")
                    );

            bucketedData.write()
                    .mode(SaveMode.Overwrite)
                    .parquet(
                            "output/customer_hash_distribution"
                    );

            long bucketCount =
                    spark.read()
                            .parquet(
                                    "output/customer_hash_distribution"
                            )
                            .count();

            long bucketEnd =
                    System.currentTimeMillis();

            System.out.println("\n====================================");
            System.out.println("HASH DISTRIBUTION COMPLETE");
            System.out.println("Records : "
                    + bucketCount);
            System.out.println("Time(ms): "
                    + (bucketEnd - bucketStart));
            System.out.println("Partitions : 8");
            System.out.println("Hash Column : customer_id");
            System.out.println("====================================");

            pause(
                    "Inspect Spark UI.\n" +
                            "Observe shuffle stages.\n" +
                            "Press ENTER to continue..."
            );

            // =====================================================
            // PARTITION PRUNING
            // =====================================================

            long pruningStart =
                    System.currentTimeMillis();

            Dataset<Row> filtered =
                    spark.read()
                            .parquet(partitionPath)
                            .filter(
                                    "country='IN' and order_year=2024"
                            );

            filtered.show(10, false);

            long filteredCount =
                    filtered.count();

            long pruningEnd =
                    System.currentTimeMillis();

            System.out.println("\n====================================");
            System.out.println("PARTITION PRUNING");
            System.out.println("Rows Returned : "
                    + filteredCount);
            System.out.println("Time(ms): "
                    + (pruningEnd - pruningStart));
            System.out.println("====================================");

            pause(
                    "Demo Complete.\n" +
                            "Explore Spark UI.\n" +
                            "Press ENTER to terminate."
            );

        } finally {

            spark.stop();

        }
    }

    private static void pause(String message) {

        try {

            System.out.println(
                    "\n===================================="
            );

            System.out.println(message);

            System.out.println(
                    "====================================\n"
            );

            System.in.read();

        } catch (Exception e) {

            e.printStackTrace();

        }
    }
}