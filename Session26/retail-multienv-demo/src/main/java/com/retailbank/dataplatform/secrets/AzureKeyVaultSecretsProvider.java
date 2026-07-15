package com.retailbank.dataplatform.secrets;

import com.azure.core.exception.ResourceNotFoundException;
import com.azure.identity.DefaultAzureCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.security.keyvault.secrets.SecretClient;
import com.azure.security.keyvault.secrets.SecretClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Test/prod secrets provider backed by Azure Key Vault.
 *
 * <p>Authentication uses {@link DefaultAzureCredential}, which on AKS resolves via
 * Workload Identity Federation (the pod's Kubernetes ServiceAccount is federated to
 * an Azure AD Application/Managed Identity — see the
 * {@code azure.workload.identity/*} annotations on the SparkApplication manifests
 * under {@code k8s/test} and {@code k8s/prod}). No client secret or certificate is
 * ever stored in the cluster: this is the whole point of Workload Identity.</p>
 *
 * <p>Secrets are cached in-process for the lifetime of the driver/executor JVM to
 * avoid hammering Key Vault's request-per-second limits under Spark's parallel
 * task execution — Key Vault throttles at the subscription level, and a
 * mis-cached-secret retry storm is a real, previously-seen failure mode.</p>
 */
public final class AzureKeyVaultSecretsProvider implements SecretsProvider {

    private static final Logger LOG = LoggerFactory.getLogger(AzureKeyVaultSecretsProvider.class);

    private final SecretClient secretClient;
    private final String keyVaultUri;
    private final Map<String, String> secretCache = new ConcurrentHashMap<>();

    public AzureKeyVaultSecretsProvider(String keyVaultUri) {
        if (keyVaultUri == null || keyVaultUri.isBlank()) {
            throw new IllegalArgumentException(
                    "secrets.key-vault-uri must be set when secrets.provider = 'azure-key-vault'.");
        }
        this.keyVaultUri = keyVaultUri;

        DefaultAzureCredential credential = new DefaultAzureCredentialBuilder().build();

        this.secretClient = new SecretClientBuilder()
                .vaultUrl(keyVaultUri)
                .credential(credential)
                .buildClient();

        LOG.info("Initialized Azure Key Vault secrets provider against '{}'", keyVaultUri);
    }

    @Override
    public String getSecret(String secretName) {
        return secretCache.computeIfAbsent(secretName, this::fetchFromKeyVault);
    }

    private String fetchFromKeyVault(String secretName) {
        try {
            LOG.info("Fetching secret '{}' from Key Vault (value is never logged)", secretName);
            return secretClient.getSecret(secretName).getValue();
        } catch (ResourceNotFoundException e) {
            throw new SecretNotFoundException(secretName, "azure-key-vault (" + keyVaultUri + ")");
        }
    }
}
