package com.retailbank.secrets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DEV-ONLY secrets provider. Reads secrets from process environment
 * variables using the convention:
 *
 * <pre>logicalName "promotion-downstream-api-key" -&gt; env var RETAILBANK_PROMOTION_DOWNSTREAM_API_KEY</pre>
 *
 * This exists purely so a developer can run the pipeline on a laptop with
 * zero Azure connectivity. It is intentionally never selected by the test or
 * prod config overlays - see {@code secrets.provider} in
 * application-test.conf / application-prod.conf, both of which are pinned to
 * "keyvault".
 */
public final class LocalEnvSecretsProvider implements SecretsProvider {

    private static final Logger log = LoggerFactory.getLogger(LocalEnvSecretsProvider.class);
    private static final String FALLBACK_DEMO_VALUE = "local-dev-placeholder-value";

    @Override
    public String getSecret(String logicalName) {
        String envVar = "RETAILBANK_" + logicalName.toUpperCase().replace('-', '_');
        String value = System.getenv(envVar);
        if (value == null || value.isBlank()) {
            log.warn("Env var {} not set - using non-sensitive demo fallback value for local dev run only", envVar);
            return FALLBACK_DEMO_VALUE;
        }
        return value;
    }

    @Override
    public String providerName() {
        return "local-env";
    }
}
