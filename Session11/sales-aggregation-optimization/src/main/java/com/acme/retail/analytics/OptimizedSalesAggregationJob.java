package com.acme.retail.analytics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.broadcast;

public class OptimizedSalesAggregationJob {

    private final String basePath;

    public OptimizedSalesAggregationJob(String basePath) {
        this.basePath = basePath;
    }

    public Dataset<Row> run(SparkSession spark) {
        String factPath = basePath + "/fact_sales";
        String dimPath = basePath + "/dim_product";

        Dataset<Row> sales = spark.read().parquet(factPath);
        Dataset<Row> products = spark.read().parquet(dimPath);

        // Project only columns needed for aggregation and join
        Dataset<Row> prunedSales = sales.select(
                col("order_date"),
                col("event_ts"),
                col("order_id"),
                col("customer_id"),
                col("product_id"),
                col("country"),
                col("channel"),
                col("quantity"),
                col("unit_price"),
                col("discount_pct"),
                col("tax_amount")
        );

        // Filter early to reduce downstream data volume
        Dataset<Row> filteredSales = prunedSales.filter(
                expr("order_date >= date_sub(current_date(), 30)")
        );

        // Repartition by primary aggregation keys to improve locality and reduce skew impact
        Dataset<Row> repartitionedSales = filteredSales.repartition(
                col("order_date"), col("country")
        );

        // Cache repartitioned and filtered data (memory + disk)
        repartitionedSales.persist(StorageLevel.MEMORY_AND_DISK());

        // Broadcast smaller product dimension to avoid shuffle join
        Dataset<Row> broadcastProducts = broadcast(products);

        Dataset<Row> enriched = repartitionedSales
                .join(broadcastProducts, "product_id")
                .withColumn("gross_amount", expr("unit_price * quantity"))
                .withColumn("discount_amount", expr("gross_amount * discount_pct"))
                .withColumn("net_amount", expr("gross_amount - discount_amount + tax_amount"));

        Dataset<Row> aggregated = enriched.groupBy(
                        col("order_date"),
                        col("country"),
                        col("channel"),
                        col("category"),
                        col("subcategory")
                )
                .agg(
                        sum("net_amount").alias("total_net_sales"),
                        sum("gross_amount").alias("total_gross_sales"),
                        sum("quantity").alias("total_quantity"),
                        approx_count_distinct("customer_id").alias("unique_customers"),
                        countDistinct("order_id").alias("order_count")
                );

        // Force materialization to measure runtime
        aggregated.count();

        // Optional: unpersist if this job were part of a longer pipeline
        repartitionedSales.unpersist();

        return aggregated;
    }
}