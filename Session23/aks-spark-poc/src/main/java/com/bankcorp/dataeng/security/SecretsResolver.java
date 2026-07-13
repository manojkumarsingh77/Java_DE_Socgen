package com.bankcorp.dataeng.security;

import com.azure.core.credential.TokenCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.security.keyvault.secrets.SecretClient;
import com.azure.security.keyvault.secrets.SecretClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

/**
 * TOPIC: Secrets Management.
 *
 * Demonstrates the THREE-TIER secret resolution chain used in production on
 * AKS, tried in this exact order (mirrors the CSI Secrets Store driver
 * behaviour configured in k8s/02-secrets-csi.yaml):
 *
 *   1. FILE MOUNT   - /mnt/secrets-store/<name>
 *                      Populated by the Azure Key Vault CSI Provider, which
 *                      mounts each Key Vault secret as an individual file
 *                      inside the pod's tmpfs volume (never touches etcd).
 *   2. K8S SECRET ENV VAR - injected via `envFrom.secretRef` in the
 *                      SparkApplication CRD pod template (base64 stored in
 *                      etcd, encrypted at rest via `--encryption-provider-config`).
 *   3. DIRECT KEY VAULT SDK CALL - `SecretClient.getSecret()` using
 *      Workload Identity Federation (DefaultAzureCredential), for callers
 *      that bypass the CSI mount entirely (e.g. local IntelliJ debugging
 *      against a real Key Vault with `az login`).
 *   4. LOCAL FALLBACK - hardcoded dev-safe default so the POC never crashes
 *      when run fully offline.
 */
public final class SecretsResolver {

    private static final Logger LOG = LoggerFactory.getLogger(SecretsResolver.class);
    private static final Path CSI_MOUNT_ROOT = Path.of("/mnt/secrets-store");

    private final String keyVaultUri;

    public SecretsResolver(String keyVaultUri) {
        this.keyVaultUri = keyVaultUri;
    }

    public String resolve(String secretName, String localFallback) {
        return resolveFromCsiMount(secretName)
                .or(() -> resolveFromEnv(secretName))
                .or(() -> resolveFromKeyVaultSdk(secretName))
                .orElseGet(() -> {
                    LOG.warn("Secret '{}' not found via CSI mount, env var, or Key Vault SDK. " +
                            "Falling back to local dev default (POC-safe, NOT for production).", secretName);
                    return localFallback;
                });
    }

    /** Tier 1: CSI Secrets Store driver file mount (production path on AKS). */
    private Optional<String> resolveFromCsiMount(String secretName) {
        Path secretFile = CSI_MOUNT_ROOT.resolve(secretName);
        if (Files.exists(secretFile) && Files.isReadable(secretFile)) {
            try {
                String value = Files.readString(secretFile, StandardCharsets.UTF_8).strip();
                LOG.info("Resolved secret '{}' from CSI Secrets Store mount ({})", secretName, secretFile);
                return Optional.of(value);
            } catch (IOException e) {
                LOG.warn("Found CSI mount file for '{}' but failed to read it: {}", secretName, e.getMessage());
            }
        }
        return Optional.empty();
    }

    /** Tier 2: Kubernetes Secret projected as environment variable. */
    private Optional<String> resolveFromEnv(String secretName) {
        String envKey = secretName.toUpperCase().replace('-', '_');
        String value = System.getenv(envKey);
        if (value != null && !value.isBlank()) {
            LOG.info("Resolved secret '{}' from environment variable '{}'", secretName, envKey);
            return Optional.of(value);
        }
        return Optional.empty();
    }

    /** Tier 3: Direct Azure Key Vault SDK call using Workload Identity Federation. */
    private Optional<String> resolveFromKeyVaultSdk(String secretName) {
        if (keyVaultUri == null || keyVaultUri.isBlank()) {
            return Optional.empty();
        }
        try {
            TokenCredential credential = new DefaultAzureCredentialBuilder().build();
            SecretClient client = new SecretClientBuilder()
                    .vaultUrl(keyVaultUri)
                    .credential(credential)
                    .buildClient();
            String value = client.getSecret(secretName).getValue();
            LOG.info("Resolved secret '{}' directly from Key Vault {}", secretName, keyVaultUri);
            return Optional.of(value);
        } catch (Exception e) {
            LOG.debug("Key Vault SDK resolution failed for '{}': {}", secretName, e.getMessage());
            return Optional.empty();
        }
    }
}
