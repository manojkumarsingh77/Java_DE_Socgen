package com.retailbank.secrets;

import com.retailbank.config.AppConfig;

/**
 * Single decision point for "which secrets backend is active". Everything
 * downstream of this factory works only against the {@link SecretsProvider}
 * interface, which is what makes it safe to promote the exact same jar from
 * dev -> test -> prod: the code never branches on environment, only the
 * config does.
 */
public final class SecretsProviderFactory {

    private SecretsProviderFactory() {
    }

    public static SecretsProvider create(AppConfig.SecretsSettings settings) {
        return switch (settings.provider()) {
            case "local" -> new LocalEnvSecretsProvider();
            case "keyvault" -> new AzureKeyVaultSecretsProvider(settings.keyVaultUrl());
            default -> throw new IllegalArgumentException(
                    "Unknown secrets.provider '" + settings.provider() + "'. Expected 'local' or 'keyvault'.");
        };
    }
}
