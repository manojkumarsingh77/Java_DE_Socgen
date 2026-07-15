package com.retailbank.dataplatform.secrets;

/**
 * Dev-only secrets provider backed by OS environment variables.
 *
 * <p>Secret name resolution: a logical secret name such as
 * {@code "adls-storage-account-key"} is translated to the environment variable
 * {@code RETAIL_SECRET_ADLS_STORAGE_ACCOUNT_KEY} (uppercased, hyphens to underscores,
 * prefixed with {@code RETAIL_SECRET_}). This keeps dev-only secrets out of any
 * config file and out of source control, while requiring zero Azure connectivity
 * to run the demo end-to-end on a laptop.</p>
 *
 * <p>This provider MUST NOT be selectable for the test or prod environment —
 * {@code application-test.conf} / {@code application-prod.conf} hardcode
 * {@code secrets.provider = "azure-key-vault"} for exactly this reason.</p>
 */
public final class LocalEnvSecretsProvider implements SecretsProvider {

    private static final String ENV_PREFIX = "RETAIL_SECRET_";

    @Override
    public String getSecret(String secretName) {
        String envVarName = ENV_PREFIX + secretName.toUpperCase().replace('-', '_');
        String value = System.getenv(envVarName);
        if (value == null || value.isBlank()) {
            throw new SecretNotFoundException(secretName, "local-env (" + envVarName + ")");
        }
        return value;
    }
}
