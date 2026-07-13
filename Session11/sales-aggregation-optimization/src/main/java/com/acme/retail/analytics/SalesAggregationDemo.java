package com.acme.retail.analytics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.desc;

public class SalesAggregationDemo {

    public static void main(String[] args) {
        int numDays = args.length > 0 ? Integer.parseInt(args[0]) : 30;
        long rowsPerDay = args.length > 1 ? Long.parseLong(args[1]) : 1_000_000L;
        int numProducts = args.length > 2 ? Integer.parseInt(args[2]) : 50_000;
        String basePath = args.length > 3 ? args[3] : "data/sales_warehouse";

        System.out.printf("Generating %d days * %d rows/day = %d rows%n",
                numDays, rowsPerDay, numDays * rowsPerDay);
        System.out.printf("Base path: %s%n", basePath);

        // Step 1: generate synthetic data (using baseline-ish session)
        SparkSession generatorSession = SparkSessionFactory.createBaselineSession("DataGenerator");
        SyntheticSalesDataGenerator generator = new SyntheticSalesDataGenerator();
        JobTimer.time("Data generation", () ->
        {
            generator.generate(generatorSession, basePath, numDays, rowsPerDay, numProducts);
            return null;
        });
        generatorSession.stop();

        // Step 2: baseline aggregation
        SparkSession baselineSession =
                SparkSessionFactory.createBaselineSession("SalesAggregationBaseline");
        BaselineSalesAggregationJob baselineJob = new BaselineSalesAggregationJob(basePath);

        Dataset<Row> baselineResult = JobTimer.time("Baseline aggregation", () ->
                baselineJob.run(baselineSession)
        );

        baselineResult.orderBy(desc("order_date"))
                .show(10, false);

        baselineSession.stop();

        // Step 3: optimized aggregation
        // Assume local[*] ~ 8 cores, you can tweak this based on actual machine
        SparkSession optimizedSession =
                SparkSessionFactory.createOptimizedSession("SalesAggregationOptimized", 8);
        OptimizedSalesAggregationJob optimizedJob = new OptimizedSalesAggregationJob(basePath);

        Dataset<Row> optimizedResult = JobTimer.time("Optimized aggregation", () ->
                optimizedJob.run(optimizedSession)
        );

        optimizedResult.orderBy(desc("order_date"))
                .show(10, false);

        optimizedSession.stop();

        System.out.println("Demo finished. Compare baseline vs optimized timings above.");
    }
}