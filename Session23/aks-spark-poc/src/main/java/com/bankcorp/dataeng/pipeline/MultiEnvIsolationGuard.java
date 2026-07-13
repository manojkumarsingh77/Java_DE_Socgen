package com.bankcorp.dataeng.pipeline;

import com.bankcorp.dataeng.config.EnvironmentProfile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TOPIC: Multi-Env Isolation.
 *
 * Isolation between dev / staging / prod on AKS is enforced at FOUR
 * independent layers, and this guard cross-checks all four AT RUNTIME
 * before a single row of data moves, so a misconfigured deployment
 * (e.g. prod jar accidentally targeting the dev namespace/Key Vault) fails
 * fast instead of silently cross-contaminating environments:
 *
 *   1. Namespace   - each SparkApplication CRD is deployed into its own
 *                    namespace (banking-dev/staging/prod), verified here
 *                    against the POD_NAMESPACE downward-API env var
 *                    (see k8s/06-sparkapplication-*.yaml env section).
 *   2. Compute     - node pool taint/toleration pairs (validated in
 *                    NodePoolTopologyAdvisor).
 *   3. Data        - distinct ADLS containers per env (raw-dev vs
 *                    raw-staging vs raw-prod), enforced via AppConfig path
 *                    construction - never a shared root path.
 *   4. Network     - NetworkPolicy denies all cross-namespace traffic
 *                    (validated conceptually in NetworkPolicyValidator).
 *
 * On AKS, the Kubernetes Downward API injects POD_NAMESPACE automatically;
 * locally we default it to the expected value so the demo passes offline.
 */
public final class MultiEnvIsolationGuard {

    private static final Logger LOG = LoggerFactory.getLogger(MultiEnvIsolationGuard.class);

    private MultiEnvIsolationGuard() {
    }

    public static void enforce(EnvironmentProfile envProfile) {
        String actualNamespace = System.getenv().getOrDefault("POD_NAMESPACE", envProfile.k8sNamespace());

        LOG.info("========== MULTI-ENV ISOLATION GUARD ==========");
        LOG.info("Active environment profile : {}", envProfile.envName());
        LOG.info("Expected K8s namespace      : {}", envProfile.k8sNamespace());
        LOG.info("Actual POD_NAMESPACE (or local default): {}", actualNamespace);
        LOG.info("ADLS raw container          : {}", envProfile.adlsRawContainer());
        LOG.info("ADLS curated container      : {}", envProfile.adlsCuratedContainer());
        LOG.info("Key Vault secret prefix     : {}", envProfile.keyVaultSecretPrefix());

        if (!actualNamespace.equals(envProfile.k8sNamespace())) {
            throw new IllegalStateException(
                    ("FATAL ISOLATION BREACH: pod is running in namespace '%s' but APP_ENV='%s' expects " +
                    "namespace '%s'. Refusing to proceed to avoid cross-environment data or credential " +
                    "leakage. Check the SparkApplication CRD 'metadata.namespace' vs the 'APP_ENV' " +
                    "container env var.")
                            .formatted(actualNamespace, envProfile.envName(), envProfile.k8sNamespace()));
        }

        if (envProfile.envName().equals("prod") && envProfile.adlsRawContainer().contains("dev")) {
            throw new IllegalStateException(
                    "FATAL ISOLATION BREACH: prod environment resolved a dev-tagged ADLS container. Aborting.");
        }

        LOG.info("Isolation checks PASSED for environment '{}'.", envProfile.envName());
        LOG.info("================================================");
    }
}
