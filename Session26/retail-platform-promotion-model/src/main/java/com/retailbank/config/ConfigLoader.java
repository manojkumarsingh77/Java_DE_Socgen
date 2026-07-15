package com.retailbank.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * Resolves the active environment (dev / test / prod) and produces a typed
 * {@link AppConfig}.
 *
 * <p>Environment selection is a single switch: the {@code env} system
 * property. Everything else - which storage account, which Key Vault, how
 * many shuffle partitions - flows from that one switch via config files, so
 * promoting a build from test to prod never means editing code or
 * recompiling the jar; it means changing {@code -Denv=test} to
 * {@code -Denv=prod} (or the equivalent spark.kubernetes.driverEnv.ENV in the
 * SparkApplication CRD).</p>
 */
public final class ConfigLoader {

    private static final Logger log = LoggerFactory.getLogger(ConfigLoader.class);
    private static final List<String> VALID_ENVS = List.of("dev", "test", "prod");

    private ConfigLoader() {
    }

    public static AppConfig load() {
        String env = resolveEnv();
        log.info("Loading configuration for environment '{}'", env);

        Config overlay = ConfigFactory.parseResourcesAnySyntax("application-" + env);
        Config base = ConfigFactory.parseResourcesAnySyntax("application");

        Config merged = ConfigFactory.systemProperties()
                .withFallback(overlay)
                .withFallback(base)
                .resolve();

        return toAppConfig(env, merged);
    }

    private static String resolveEnv() {
        String env = System.getProperty("env", "dev").trim().toLowerCase();
        if (!VALID_ENVS.contains(env)) {
            throw new IllegalArgumentException(
                    "Unknown -Denv value '" + env + "'. Must be one of " + VALID_ENVS);
        }
        return env;
    }

    private static AppConfig toAppConfig(String env, Config c) {
        String master = c.hasPath("spark.master") ? c.getString("spark.master") : null;

        AppConfig.SparkSettings spark = new AppConfig.SparkSettings(
                master,
                c.getInt("spark.shuffle.partitions"));

        AppConfig.PromotionRules promotion = new AppConfig.PromotionRules(
                c.getDouble("promotion.high.value.threshold"),
                c.getStringList("promotion.loyalty.bonus.tiers"),
                c.getInt("promotion.risk.score.max.eligible"));

        AppConfig.SecretsSettings secrets = new AppConfig.SecretsSettings(
                c.getString("secrets.provider"),
                c.hasPath("secrets.key.vault.url") ? c.getString("secrets.key.vault.url") : "",
                c.getString("secrets.downstream.api.key.secret.name"));

        AppConfig.StorageSettings storage = new AppConfig.StorageSettings(
                c.getString("storage.format"),
                resolvePath(env, c.getString("storage.input.path")),
                resolvePath(env, c.getString("storage.output.path")));

        return new AppConfig(env, c.getString("app.name"), spark, promotion, secrets, storage);
    }

    /**
     * Dev paths are relative and get resolved under the OS temp directory so
     * the demo never writes outside a sandbox. Test/prod paths are already
     * fully qualified abfss:// URIs and pass through unchanged.
     */
    private static String resolvePath(String env, String configuredPath) {
        if (!"dev".equals(env) || configuredPath.contains("://")) {
            return configuredPath;
        }
        Path resolved = Paths.get(System.getProperty("java.io.tmpdir"), configuredPath);
        return resolved.toAbsolutePath().toString();
    }
}
