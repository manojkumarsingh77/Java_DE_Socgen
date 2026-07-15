package com.retailbank.secrets;

import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.security.keyvault.secrets.SecretClient;
import com.azure.security.keyvault.secrets.SecretClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TEST/PROD secrets provider. Uses {@link DefaultAzureCredentialBuilder},
 * which on AKS resolves through Azure AD Workload Identity (the pod's
 * projected service-account token federated to a Microsoft Entra
 * application) - no client secret is ever baked into the image, the config,
 * or a Kubernetes Secret. The only thing that differs between test and prod
 * is the Key Vault URL, which comes from {@code secrets.key.vault.url} in
 * the environment config overlay.
 */
public final class AzureKeyVaultSecretsProvider implements SecretsProvider {

    private static final Logger log = LoggerFactory.getLogger(AzureKeyVaultSecretsProvider.class);

    private final SecretClient secretClient;
    private final String vaultUrl;

    public AzureKeyVaultSecretsProvider(String vaultUrl) {
        if (vaultUrl == null || vaultUrl.isBlank()) {
            throw new IllegalStateException(
                    "secrets.key.vault.url is required when secrets.provider = keyvault");
        }
        this.vaultUrl = vaultUrl;
        this.secretClient = new SecretClientBuilder()
                .vaultUrl(vaultUrl)
                .credential(new DefaultAzureCredentialBuilder().build())
                .buildClient();
    }

    @Override
    public String getSecret(String logicalName) {
        log.info("Fetching secret '{}' from Key Vault {}", logicalName, vaultUrl);
        return secretClient.getSecret(logicalName).getValue();
    }

    @Override
    public String providerName() {
        return "azure-keyvault(" + vaultUrl + ")";
    }
}
