package com.bankcorp.dataeng.config;

/**
 * TOPIC: Multi-Env Isolation.
 *
 * Encodes the physical and logical isolation boundary for a single environment
 * (dev / staging / prod). Every environment maps to:
 *   - a distinct Kubernetes namespace (enforced by NetworkPolicy + ResourceQuota in k8s/)
 *   - a distinct ADLS Gen2 filesystem path prefix (data isolation)
 *   - a distinct AKS node pool taint/toleration pair (compute isolation)
 *   - a distinct Key Vault secret scope (credential isolation)
 *
 * This record is the single source of truth the Java driver consults so that
 * the SAME jar, deployed via 3 different SparkApplication CRDs
 * (k8s/06-sparkapplication-{dev,staging,prod}.yaml), behaves correctly per
 * environment purely from environment variables / Spark conf - no code branching.
 */
public record EnvironmentProfile(
        String envName,
        String k8sNamespace,
        String nodePoolTaintKey,
        String nodePoolTaintValue,
        String adlsRawContainer,
        String adlsCuratedContainer,
        String keyVaultSecretPrefix,
        int maxExecutors,
        int minExecutors,
        String executorNodeSelector
) {

    public static EnvironmentProfile forName(String envName) {
        return switch (envName.toLowerCase()) {
            case "dev" -> new EnvironmentProfile(
                    "dev", "banking-dev",
                    "workload", "dev-spark",
                    "raw-dev", "curated-dev",
                    "dev-",
                    4, 1,
                    "agentpool=dev-userpool"
            );
            case "staging" -> new EnvironmentProfile(
                    "staging", "banking-staging",
                    "workload", "staging-spark",
                    "raw-staging", "curated-staging",
                    "staging-",
                    8, 2,
                    "agentpool=staging-userpool"
            );
            case "prod" -> new EnvironmentProfile(
                    "prod", "banking-prod",
                    "workload", "prod-spark",
                    "raw-prod", "curated-prod",
                    "prod-",
                    32, 4,
                    "agentpool=prodhighmem"
            );
            default -> throw new IllegalArgumentException(
                    "Unknown environment '" + envName + "'. Expected one of: dev, staging, prod");
        };
    }
}
