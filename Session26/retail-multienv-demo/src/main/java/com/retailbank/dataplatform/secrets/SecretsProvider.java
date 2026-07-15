package com.retailbank.dataplatform.secrets;

/**
 * Abstraction over "wherever secrets actually live for this environment".
 * The pipeline code depends only on this interface, never on a concrete
 * secrets backend — this is what makes the promotion dev -> test -> prod
 * possible without touching a single line of pipeline logic.
 */
public interface SecretsProvider {

    /**
     * Resolves a named secret. Implementations must never log the returned value.
     *
     * @param secretName logical secret name, e.g. "adls-storage-account-key"
     * @return the secret value
     * @throws SecretNotFoundException if the secret does not exist in the backing store
     */
    String getSecret(String secretName);

    /**
     * Factory that selects the correct {@link SecretsProvider} implementation
     * based on {@code retail-platform.secrets.provider}.
     */
    static SecretsProvider forProvider(String providerName, String keyVaultUri) {
        return switch (providerName) {
            case "local-env" -> new LocalEnvSecretsProvider();
            case "azure-key-vault" -> new AzureKeyVaultSecretsProvider(keyVaultUri);
            default -> throw new IllegalArgumentException(
                    "Unknown secrets provider '" + providerName + "'. Expected 'local-env' or 'azure-key-vault'.");
        };
    }

    class SecretNotFoundException extends RuntimeException {
        public SecretNotFoundException(String secretName, String backend) {
            super("Secret '" + secretName + "' was not found in backend '" + backend + "'.");
        }
    }
}
