package com.retail.enrichment;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;

/**
 * Retail Enrichment Containerization - Case Study Job.
 *
 * Reads raw retail transactions, joins with product catalog and store
 * reference data, computes revenue / margin / value-segment enrichment,
 * prints a region-category summary, and writes enriched output.
 *
 * Designed to run identically:
 *   1) Locally inside IntelliJ (local[*] master)
 *   2) Inside a hardened, resource-constrained Docker container
 *
 * This class also prints what the JVM believes its resource envelope is
 * (CPUs / max heap) so students can directly observe the effect of
 * container resource constraints and JVM container-aware tuning flags.
 */
public class RetailEnrichmentJob {

    public static void main(String[] args) {

        String inputPath = args.length > 0 ? args[0] : "data";
        String outputPath = args.length > 1 ? args[1] : "output";
        String master = System.getenv().getOrDefault("SPARK_MASTER", "local[*]");
        String shufflePartitions = System.getenv().getOrDefault("SPARK_SHUFFLE_PARTITIONS", "4");

        printContainerResourceSnapshot();

        SparkSession spark = SparkSession.builder()
                .appName("RetailEnrichmentJob")
                .master(master)
                .config("spark.sql.shuffle.partitions", shufflePartitions)
                .config("spark.ui.showConsoleProgress", "false")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        try {
            Dataset<Row> transactionsRaw = spark.read()
                    .option("header", "true")
                    .schema(transactionSchema())
                    .csv(inputPath + "/transactions.csv");

            Dataset<Row> transactions = transactionsRaw
                    .withColumn("txn_date", to_date(col("txn_date"), "yyyy-MM-dd"));

            Dataset<Row> products = spark.read()
                    .option("header", "true")
                    .schema(productSchema())
                    .csv(inputPath + "/products.csv");

            Dataset<Row> stores = spark.read()
                    .option("header", "true")
                    .schema(storeSchema())
                    .csv(inputPath + "/stores.csv");

            Dataset<Row> enriched = transactions
                    .join(products, "product_id")
                    .join(stores, "store_id")
                    .withColumn("gross_revenue",
                            round(col("quantity").multiply(col("unit_price")), 2))
                    .withColumn("margin_amount",
                            round(col("quantity").multiply(col("unit_price").minus(col("cost_price"))), 2))
                    .withColumn("margin_pct",
                            round(col("margin_amount").divide(col("gross_revenue")).multiply(100), 2))
                    .withColumn("value_segment",
                            when(col("gross_revenue").geq(5000), "HIGH")
                                    .when(col("gross_revenue").geq(1500), "MEDIUM")
                                    .otherwise("LOW"))
                    .withColumn("txn_year_month", date_format(col("txn_date"), "yyyy-MM"))
                    .select("transaction_id", "txn_date", "txn_year_month", "store_id", "store_name",
                            "region", "state", "product_id", "product_name", "category", "brand",
                            "quantity", "unit_price", "cost_price", "gross_revenue",
                            "margin_amount", "margin_pct", "value_segment");

            enriched.cache();

            System.out.println("\n=== Enriched Transaction Sample (first 20 rows) ===");
            enriched.orderBy(col("txn_date")).show(20, false);

            Dataset<Row> regionCategorySummary = enriched.groupBy("region", "category")
                    .agg(
                            round(sum("gross_revenue"), 2).alias("total_revenue"),
                            round(sum("margin_amount"), 2).alias("total_margin"),
                            count("transaction_id").alias("txn_count")
                    )
                    .orderBy(col("total_revenue").desc());

            System.out.println("\n=== Region / Category Revenue Summary ===");
            regionCategorySummary.show(50, false);

            long highValueCount = enriched.filter(col("value_segment").equalTo("HIGH")).count();
            System.out.println("\nHigh-value transactions (>= 5000 revenue): " + highValueCount);

            enriched.write()
                    .mode(SaveMode.Overwrite)
                    .partitionBy("region")
                    .parquet(outputPath + "/enriched_transactions");

            regionCategorySummary.coalesce(1).write()
                    .mode(SaveMode.Overwrite)
                    .option("header", "true")
                    .csv(outputPath + "/region_category_summary");

            System.out.println("\nJob completed successfully. Output written to: " + outputPath);

        } finally {
            spark.stop();
        }
    }

    /**
     * Logs what the JVM sees as its resource envelope. When run under Docker
     * with --cpus / --memory (or docker-compose deploy.resources.limits),
     * these values should reflect the CONTAINER limits, not the host machine,
     * proving that JVM container-awareness + tuning flags are working.
     */
    private static void printContainerResourceSnapshot() {
        Runtime rt = Runtime.getRuntime();
        System.out.println("========================================================");
        System.out.println(" JVM Resource Snapshot (reflects container limits if any)");
        System.out.println("--------------------------------------------------------");
        System.out.println(" Available processors (JVM view) : " + rt.availableProcessors());
        System.out.println(" Max heap (JVM -Xmx effective)    : "
                + (rt.maxMemory() / (1024 * 1024)) + " MB");
        System.out.println(" Total memory currently allocated : "
                + (rt.totalMemory() / (1024 * 1024)) + " MB");
        System.out.println(" Free memory                      : "
                + (rt.freeMemory() / (1024 * 1024)) + " MB");
        System.out.println("========================================================\n");
    }

    private static StructType transactionSchema() {
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("transaction_id", DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("store_id", DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("product_id", DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("quantity", DataTypes.IntegerType, false));
        fields.add(DataTypes.createStructField("unit_price", DataTypes.DoubleType, false));
        fields.add(DataTypes.createStructField("txn_date", DataTypes.StringType, false));
        return DataTypes.createStructType(fields);
    }

    private static StructType productSchema() {
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("product_id", DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("product_name", DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("category", DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("brand", DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("cost_price", DataTypes.DoubleType, false));
        return DataTypes.createStructType(fields);
    }

    private static StructType storeSchema() {
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("store_id", DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("store_name", DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("region", DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("state", DataTypes.StringType, false));
        return DataTypes.createStructType(fields);
    }
}
