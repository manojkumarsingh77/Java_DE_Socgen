package com.retailbank.dataplatform.config;

import com.typesafe.config.Config;

/**
 * Strongly-typed, immutable view over the resolved HOCON configuration.
 * Downstream classes depend on this record, never on {@link Config} directly —
 * this keeps the Typesafe Config API isolated to the config package and gives
 * every field compile-time safety.
 */
public record AppConfig(
        String environmentName,
        SparkSettings spark,
        DataSettings data,
        SecretsSettings secrets,
        ReconciliationSettings reconciliation
) {

    public record SparkSettings(String appName, int shufflePartitions, String master) { }

    public record DataSettings(
            long syntheticRecordCount,
            String outputFormat,
            String outputPath,
            String checkpointPath
    ) { }

    public record SecretsSettings(String provider, String keyVaultUri) { }

    public record ReconciliationSettings(int discrepancyThresholdCents) { }

    /**
     * Builds a typed {@link AppConfig} from the raw, fully-resolved {@link Config}
     * produced by {@link EnvironmentConfigLoader#load()}.
     */
    public static AppConfig from(Config resolved) {
        Config root = resolved.getConfig("retail-platform");

        SparkSettings spark = new SparkSettings(
                root.getString("spark.app-name"),
                root.getInt("spark.shuffle-partitions"),
                root.getString("spark.master")
        );

        DataSettings data = new DataSettings(
                root.getLong("data.synthetic-record-count"),
                root.getString("data.output-format"),
                root.getString("data.output-path"),
                root.getString("data.checkpoint-path")
        );

        SecretsSettings secrets = new SecretsSettings(
                root.getString("secrets.provider"),
                root.hasPath("secrets.key-vault-uri") ? root.getString("secrets.key-vault-uri") : ""
        );

        ReconciliationSettings reconciliation = new ReconciliationSettings(
                root.getInt("reconciliation.discrepancy-threshold-cents")
        );

        return new AppConfig(
                root.getString("environment.name"),
                spark,
                data,
                secrets,
                reconciliation
        );
    }
}
