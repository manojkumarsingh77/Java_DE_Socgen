package com.retailbank.dataplatform.config;

import org.apache.spark.sql.SparkSession;

/**
 * Builds the {@link SparkSession} from a resolved {@link AppConfig}. Isolated in
 * its own class so both {@code Main} (batch entry point) and any future
 * streaming entry point share identical session construction / Delta wiring.
 */
public final class SparkSessionFactory {

    private SparkSessionFactory() {
    }

    public static SparkSession create(AppConfig config) {
        SparkSession.Builder builder = SparkSession.builder()
                .appName(config.spark().appName())
                .master(config.spark().master())
                .config("spark.sql.shuffle.partitions", config.spark().shufflePartitions())
                // Delta Lake catalog + SQL extension wiring — required for
                // `.format("delta")` reads/writes and for `MERGE`/`DELTE`/`UPDATE` SQL.
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                // Adaptive Query Execution stays on in every environment; it is what lets
                // the SAME code handle 25K rows in dev and 2M rows in prod without a
                // manually tuned partition count.
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true");

        return builder.getOrCreate();
    }
}
