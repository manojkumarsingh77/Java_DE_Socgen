package com.acme.retail.analytics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class BaselineSalesAggregationJob {

    private final String basePath;

    public BaselineSalesAggregationJob(String basePath) {
        this.basePath = basePath;
    }

    public Dataset<Row> run(SparkSession spark) {
        String factPath = basePath + "/fact_sales";
        String dimPath = basePath + "/dim_product";

        // Read everything (no column pruning)
        Dataset<Row> sales = spark.read().parquet(factPath);

        Dataset<Row> products = spark.read().parquet(dimPath);

        // Minimal filtering (last 30 days), but still wide rows
        Dataset<Row> filteredSales = sales.filter(expr("order_date >= date_sub(current_date(), 30)"));

        // Derive financial metrics late, after the join
        Dataset<Row> joined = filteredSales.join(
                products,
                filteredSales.col("product_id").equalTo(products.col("product_id")),
                "left"
        );

        Dataset<Row> withAmounts = joined
                .withColumn("gross_amount", expr("unit_price * quantity"))
                .withColumn("discount_amount", expr("gross_amount * discount_pct"))
                .withColumn("net_amount", expr("gross_amount - discount_amount + tax_amount"));

        // Shuffle-heavy aggregation on multiple dimensions without repartitioning
        Dataset<Row> aggregated = withAmounts.groupBy(
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

        // Force materialization
        aggregated.count();

        return aggregated;
    }
}