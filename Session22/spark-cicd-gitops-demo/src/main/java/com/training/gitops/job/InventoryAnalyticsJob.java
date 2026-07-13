package com.training.gitops.job;

import com.training.gitops.data.StockMovement;
import com.training.gitops.data.StockMovementGenerator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

import static org.apache.spark.sql.functions.*;

/**
 * The real Spark ETL workload being built, versioned, scanned, and progressively
 * deployed throughout this demo. Every "deployment" to Dev/Stage/Prod, every
 * Blue/Green switch, and every Canary wave actually EXECUTES this job as its
 * health check / smoke test - a "deployment" that only prints text without
 * proving the artifact works is not a real deployment.
 */
public class InventoryAnalyticsJob {

    public static SparkSession createSparkSession(String appNameSuffix) {
        return SparkSession.builder()
                .appName("GitOps-Training-InventoryAnalyticsJob-" + appNameSuffix)
                .master("local[*]")
                .config("spark.ui.enabled", "false")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .config("spark.driver.host", "127.0.0.1")
                .config("spark.sql.shuffle.partitions", "4")
                .getOrCreate();
    }

    /**
     * Runs the job end-to-end and returns true if it completed without error -
     * this boolean is the "health check" result used by BlueGreenDeploymentManager
     * and CanaryReleaseManager to decide whether a deployment is safe to receive
     * traffic.
     */
    public boolean runSmokeTest(String environmentLabel, boolean injectFailure) {
        SparkSession spark = createSparkSession(environmentLabel);
        try {
            if (injectFailure) {
                // Deliberately simulate a broken deployment (e.g. a bad migration,
                // a bug in the new version) so learners can SEE Blue/Green and
                // Canary correctly refuse to shift traffic to an unhealthy release.
                throw new IllegalStateException("Simulated failure in environment [" + environmentLabel
                        + "] - injected via *_INJECT_FAILURE flag for demo purposes");
            }
            List<StockMovement> movements = StockMovementGenerator.generate(20_000);
            Dataset<Row> df = spark.createDataFrame(movements, StockMovement.class);

            Dataset<Row> netStockBySku = df.groupBy("sku")
                    .agg(sum("quantity").alias("net_quantity"),
                            round(sum(expr("quantity * unitCost")), 2).alias("net_value"))
                    .orderBy(desc("net_value"));

            long rowCount = df.count();
            System.out.println("  [smoke-test:" + environmentLabel + "] processed " + rowCount + " stock movements OK");
            netStockBySku.show(5, false);
            return true;
        } catch (Exception e) {
            System.out.println("  [smoke-test:" + environmentLabel + "] FAILED -> " + e.getMessage());
            return false;
        } finally {
            spark.stop();
        }
    }
}
