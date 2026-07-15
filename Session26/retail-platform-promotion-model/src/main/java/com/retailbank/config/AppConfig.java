package com.retailbank.config;

import java.util.List;

/**
 * Strongly-typed view over the merged Typesafe Config tree. Records give us
 * immutability and compile-time field access instead of stringly-typed
 * config.getString("...") calls scattered through the pipeline.
 */
public record AppConfig(
        String environmentTag,
        String appName,
        SparkSettings spark,
        PromotionRules promotion,
        SecretsSettings secrets,
        StorageSettings storage
) {

    /** master is nullable: in test/prod it is deliberately absent so spark-submit --master wins. */
    public record SparkSettings(String master, int shufflePartitions) {}

    public record PromotionRules(double highValueThreshold, List<String> loyaltyBonusTiers, int riskScoreMaxEligible) {}

    /** provider is "local" (dev) or "keyvault" (test/prod). */
    public record SecretsSettings(String provider, String keyVaultUrl, String downstreamApiKeySecretName) {}

    public record StorageSettings(String format, String inputPath, String outputPath) {}
}
