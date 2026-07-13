package com.training.containerization.job;

import com.training.containerization.config.JobConfig;
import com.training.containerization.data.SalesRecord;
import com.training.containerization.data.SampleDataGenerator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import java.util.List;

import static org.apache.spark.sql.functions.*;

/**
 * A small but "real" Spark ETL job: aggregation + a ranking window function.
 * This class represents "the data platform workload" that everything else in the
 * demo (Docker build, JVM flags, resource limits, hardening) exists to run safely
 * and predictably in production.
 */
public class SalesAnalyticsJob {

    public static SparkSession createSparkSession(JobConfig config) {
        return SparkSession.builder()
                .appName("Containerization-Training-SalesAnalyticsJob")
                .master(config.sparkMaster)
                // keep the demo self-contained and avoid port clashes when re-run repeatedly
                .config("spark.ui.enabled", "false")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .config("spark.driver.host", "127.0.0.1")
                .config("spark.sql.shuffle.partitions", config.shufflePartitions)
                .config("spark.driver.memory", config.driverMemory)
                .config("spark.sql.adaptive.enabled", "true")
                .getOrCreate();
    }

    public void run(SparkSession spark, JobConfig config) {
        System.out.println("\n[SalesAnalyticsJob] Generating " + config.recordCount + " in-memory sales records ...");
        List<SalesRecord> records = SampleDataGenerator.generate(config.recordCount);

        Dataset<Row> sales = spark.createDataFrame(records, SalesRecord.class).cache();
        System.out.println("[SalesAnalyticsJob] Dataset materialized. Row count = " + sales.count());
        System.out.println("[SalesAnalyticsJob] spark.sql.shuffle.partitions = " + config.shufflePartitions
                + "  (tune this DOWN on memory/CPU-constrained containers to reduce task overhead)");

        // 1) Revenue by region + category (a wide shuffle-heavy aggregation)
        Dataset<Row> revenueByRegionCategory = sales.groupBy("region", "category")
                .agg(round(sum("revenue"), 2).alias("total_revenue"),
                        count("*").alias("transactions"))
                .orderBy(desc("total_revenue"));

        System.out.println("\n=== Revenue by Region & Category ===");
        revenueByRegionCategory.show(10, false);

        // 2) Top 3 products per region by revenue - window function (ranking)
        Dataset<Row> productRevenue = sales.groupBy("region", "product")
                .agg(round(sum("revenue"), 2).alias("total_revenue"));

        WindowSpec byRegionOrderedByRevenue = Window.partitionBy("region").orderBy(desc("total_revenue"));

        Dataset<Row> topProductsPerRegion = productRevenue
                .withColumn("rank", row_number().over(byRegionOrderedByRevenue))
                .filter(col("rank").leq(3))
                .orderBy("region", "rank");

        System.out.println("=== Top 3 Products per Region (window function) ===");
        topProductsPerRegion.show(20, false);

        // 3) Grand total - forces a final action/shuffle so learners see stage activity in Spark UI/logs
        Row totals = sales.agg(round(sum("revenue"), 2).alias("grand_total_revenue"),
                count("*").alias("total_transactions")).first();
        System.out.printf("%n[SalesAnalyticsJob] Grand total revenue = %s across %s transactions%n%n",
                totals.get(0), totals.get(1));

        sales.unpersist();
    }
}
