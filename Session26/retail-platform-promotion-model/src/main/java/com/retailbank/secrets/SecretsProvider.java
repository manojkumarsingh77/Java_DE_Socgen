package com.retailbank.secrets;

/**
 * Abstraction over "where does a secret value come from". The pipeline code
 * never knows or cares which implementation is active - that decision is
 * made once, in {@link SecretsProviderFactory}, driven entirely by
 * {@code secrets.provider} in the environment config file.
 */
public interface SecretsProvider {

    /**
     * @param logicalName the business name of the secret, e.g. "promotion-downstream-api-key"
     * @return the secret value. Never logged, never printed - callers must mask it themselves.
     */
    String getSecret(String logicalName);

    /** Human-readable name of the backing mechanism, safe to log. */
    String providerName();
}
