package com.retailbank.dr.config;

import java.nio.file.Path;

/**
 * Centralized, immutable run configuration for the Cross-Region Failover Simulation.
 *
 * In production this would be externalized via Spark conf / environment variables
 * injected by the AKS Deployment or SparkApplication CRD (see k8s/spark-application.yaml),
 * never hardcoded. For local IntelliJ execution we provide sane OS-agnostic defaults
 * rooted under the user's temp directory so the demo runs with zero setup on both
 * macOS (M1 Max) and Windows 11.
 */
public record AppConfig(
        String appName,
        String sparkMaster,

        // --- Storage layout: simulates two Azure regions using two local Delta paths.
        // In production these map to two ADLS Gen2 accounts in paired Azure regions,
        // e.g. abfss://ledger@bankprodeast.dfs.core.windows.net/txns  (primary, East US)
        //      abfss://ledger@bankprodwest.dfs.core.windows.net/txns (secondary, West US)
        String primaryRegionName,
        String secondaryRegionName,
        Path primaryTablePath,
        Path secondaryTablePath,
        Path backupRootPath,
        Path reportOutputPath,

        // --- Synthetic workload shape
        int totalBatches,
        int recordsPerBatch,
        long randomSeed,

        // --- Replication behavior
        long baseReplicationLagMillis,     // steady-state async replication lag
        long replicationJitterMillis,      // +/- jitter to model real network variance
        int backupEveryNBatches,           // periodic snapshot/backup cadence

        // --- Failure drill parameters
        int failurePrimaryDownAtBatch,     // batch index at which PRIMARY is simulated DOWN
        int failoverDetectionMillis,       // simulated health-probe detection time
        int failoverPromotionMillis,       // simulated time to promote secondary + repoint routing

        // --- DR SLA targets (what the business/compliance team mandated)
        long rpoTargetSeconds,             // e.g. 60s max acceptable data loss window
        long rtoTargetSeconds              // e.g. 300s max acceptable downtime
) {

    /** Zero-argument-friendly factory producing the standard demo configuration. */
    public static AppConfig defaultLocalDemo() {
        String tmp = System.getProperty("java.io.tmpdir");
        Path root = Path.of(tmp, "dr-ha-spark-demo");
        return new AppConfig(
                "CrossRegionFailoverSimulation-RetailBanking",
                "local[*]",
                "us-east-primary",
                "us-west-secondary",
                root.resolve("delta/primary_txn_ledger"),
                root.resolve("delta/secondary_txn_ledger"),
                root.resolve("backups"),
                root.resolve("reports"),
                20,          // totalBatches
                5_000,       // recordsPerBatch  -> 100k total txns
                42L,
                750L,        // baseReplicationLagMillis (~0.75s steady-state async replication)
                250L,        // jitter
                5,           // backup every 5 batches
                14,          // fail primary right after batch #14 commits
                5_000,       // 5s simulated detection
                12_000,      // 12s simulated promotion/repoint
                60L,         // RPO target: 60 seconds
                300L         // RTO target: 5 minutes
        );
    }
}
