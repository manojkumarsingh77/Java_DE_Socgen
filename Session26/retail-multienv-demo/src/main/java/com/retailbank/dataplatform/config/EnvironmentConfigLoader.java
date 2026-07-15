package com.retailbank.dataplatform.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Set;

/**
 * Resolves which environment configuration file to load and merges it with
 * system-property overrides.
 *
 * <p>Resolution order (highest precedence to lowest), matching Typesafe Config
 * semantics of {@link ConfigFactory#load()}:</p>
 * <ol>
 *   <li>JVM system properties, e.g. {@code -Dretail.platform.spark.shuffle-partitions=32}
 *       passed via {@code spark-submit --conf spark.driver.extraJavaOptions} or IntelliJ VM Options.
 *       These override EVERYTHING, which is how an operator does an emergency
 *       single-value override in production without rebuilding the jar.</li>
 *   <li>{@code application-<env>.conf} — the environment-specific file selected by
 *       the {@code retail.env} system property (dev / test / prod). This is the
 *       file that changes as the artifact is PROMOTED between environments.</li>
 *   <li>{@code reference.conf} — packaged defaults, safety net only.</li>
 * </ol>
 *
 * <p>The environment is intentionally never inferred or defaulted silently:
 * if {@code -Dretail.env=...} is missing or not one of the known values, startup
 * fails fast. Silently defaulting to "dev" is exactly the kind of mistake that
 * causes a job to accidentally write into a production ADLS container.</p>
 */
public final class EnvironmentConfigLoader {

    private static final Logger LOG = LoggerFactory.getLogger(EnvironmentConfigLoader.class);

    private static final String ENV_SYSTEM_PROPERTY = "retail.env";
    private static final Set<String> KNOWN_ENVIRONMENTS = Set.of("dev", "test", "prod");

    private EnvironmentConfigLoader() {
    }

    /**
     * Loads and fully resolves the layered configuration for the current process.
     *
     * @return a resolved {@link Config} object combining system properties,
     *         the environment-specific file, and reference.conf defaults.
     */
    public static Config load() {
        String env = resolveEnvironmentNameOrFail();
        LOG.info("Resolved runtime environment = '{}'", env);

        String envFileName = "application-" + env + ".conf";

        // Allow an operator to override the config directory entirely (e.g. a
        // Kubernetes ConfigMap mounted at /opt/spark/conf/external) without
        // rebuilding the jar. Falls back to the classpath resource packaged
        // inside the jar for local IntelliJ runs.
        String externalConfigDir = System.getProperty("retail.config.dir");

        Config environmentConfig;
        if (externalConfigDir != null && !externalConfigDir.isBlank()) {
            File externalFile = new File(externalConfigDir, envFileName);
            if (!externalFile.exists()) {
                throw new IllegalStateException(
                        "retail.config.dir was set to '" + externalConfigDir
                                + "' but '" + externalFile.getAbsolutePath() + "' does not exist.");
            }
            LOG.info("Loading externalized config from mounted path: {}", externalFile.getAbsolutePath());
            environmentConfig = ConfigFactory.parseFile(externalFile, ConfigParseOptions.defaults());
        } else {
            LOG.info("Loading config bundled on classpath: {}", envFileName);
            environmentConfig = ConfigFactory.parseResourcesAnySyntax(envFileName);
            if (environmentConfig.isEmpty()) {
                throw new IllegalStateException(
                        "Could not find classpath resource '" + envFileName + "'. "
                                + "Known environments are " + KNOWN_ENVIRONMENTS + ".");
            }
        }

        // System properties (e.g. -Dretail.platform.spark.shuffle-partitions=32) win over
        // everything; reference.conf packaged in the jar is the final fallback.
        Config resolved = ConfigFactory.systemProperties()
                .withFallback(environmentConfig)
                .withFallback(ConfigFactory.parseResourcesAnySyntax("reference.conf"))
                .resolve();

        logNonSecretSummary(resolved, env);
        return resolved;
    }

    private static String resolveEnvironmentNameOrFail() {
        String env = System.getProperty(ENV_SYSTEM_PROPERTY);
        if (env == null || env.isBlank()) {
            throw new IllegalStateException(
                    "System property -D" + ENV_SYSTEM_PROPERTY + " is required (one of "
                            + KNOWN_ENVIRONMENTS + "). Refusing to default silently — "
                            + "see IntelliJ Run Configuration VM Options in LAB_GUIDE.md.");
        }
        env = env.toLowerCase().trim();
        if (!KNOWN_ENVIRONMENTS.contains(env)) {
            throw new IllegalStateException(
                    "Unknown -D" + ENV_SYSTEM_PROPERTY + "='" + env + "'. Must be one of "
                            + KNOWN_ENVIRONMENTS + ".");
        }
        return env;
    }

    private static void logNonSecretSummary(Config config, String env) {
        // Deliberately logs only structural/non-secret keys. The secrets{} block's
        // resolved values are never logged — see SecretsProvider implementations.
        List<String> summaryKeys = List.of(
                "retail-platform.environment.name",
                "retail-platform.spark.app-name",
                "retail-platform.spark.shuffle-partitions",
                "retail-platform.data.output-format",
                "retail-platform.data.output-path",
                "retail-platform.secrets.provider"
        );
        LOG.info("==== Effective configuration for environment '{}' ====", env);
        for (String key : summaryKeys) {
            String value = config.hasPath(key) ? config.getValue(key).unwrapped().toString() : "<missing>";
            LOG.info("  {} = {}", key, value);
        }
        LOG.info("=======================================================");
    }
}
