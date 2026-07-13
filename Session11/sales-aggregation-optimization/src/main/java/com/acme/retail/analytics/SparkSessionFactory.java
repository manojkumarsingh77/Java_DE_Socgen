package com.acme.retail.analytics;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public final class SparkSessionFactory {

    private SparkSessionFactory() {
    }

    public static SparkSession createBaselineSession(String appName) {
        SparkConf conf = new SparkConf()
                .setAppName(appName)
                .setMaster("local[*]")
                // Intentionally modest executor memory to induce some spill
                .set("spark.executor.memory", "2g")
                .set("spark.driver.memory", "2g")
                // Leave default parallelism; set high shuffle partitions to create many small tasks
                .set("spark.sql.shuffle.partitions", "200")
                // Conservative memory fraction; more pressure on storage vs execution
                .set("spark.memory.fraction", "0.6")
                .set("spark.memory.storageFraction", "0.5")
                // AQE off in baseline
                .set("spark.sql.adaptive.enabled", "true")
                // Compression enabled, but defaults for buffers
                .set("spark.shuffle.compress", "true")
                .set("spark.shuffle.spill.compress", "true");

        return SparkSession.builder()
                .config(conf)
                .getOrCreate();
    }

    public static SparkSession createOptimizedSession(String appName, int totalCoresEstimate) {
        SparkConf conf = new SparkConf()
                .setAppName(appName)
                .setMaster("local[*]")
                // More headroom for shuffle-heavy aggregation
                .set("spark.executor.memory", "4g")
                .set("spark.driver.memory", "4g")
                // Rule of thumb: 2–3x total cores for shuffle partitions.[web:6][web:9][web:15]
                .set("spark.sql.shuffle.partitions", String.valueOf(totalCoresEstimate * 3))
                // Bias memory toward execution to reduce spill
                .set("spark.memory.fraction", "0.8")
                .set("spark.memory.storageFraction", "0.3")
                // AQE can coalesce small partitions and handle skew automatically.[web:10][web:15]
                .set("spark.sql.adaptive.enabled", "true")
                .set("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .set("spark.sql.adaptive.skewJoin.enabled", "true")
                .set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "134217728") // 128MB
                // Shuffle buffer tuning
                .set("spark.reducer.maxSizeInFlight", "96m")
                .set("spark.shuffle.file.buffer", "64k")
                // Off-heap for shuffle if needed
                .set("spark.memory.offHeap.enabled", "true")
                .set("spark.memory.offHeap.size", "1g")
                .set("spark.shuffle.compress", "true")
                .set("spark.shuffle.spill.compress", "true");

        return SparkSession.builder()
                .config(conf)
                .getOrCreate();
    }
}