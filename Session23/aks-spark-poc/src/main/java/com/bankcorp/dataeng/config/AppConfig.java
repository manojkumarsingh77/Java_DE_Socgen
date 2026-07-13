package com.bankcorp.dataeng.config;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Central configuration resolver.
 *
 * DESIGN GOAL: "download and run without failure" on a laptop with ZERO Azure
 * account configured, while allowing a ONE-FLAG flip to real Azure ADLS Gen2 /
 * Blob Storage once the user supplies their own account details.
 *
 * Resolution order for every parameter (highest wins):
 *   1. JVM system property   (-Dkey=value           -> set via IntelliJ VM Options)
 *   2. Environment variable  (KEY=value              -> set via .env / shell)
 *   3. Hardcoded local-mode default (safe, offline, no Azure calls)
 *
 * Toggle AZURE_MODE=true to switch all storage I/O from local filesystem
 * (./data/raw, ./data/curated) to real abfss:// / wasbs:// paths built from
 * the AZURE_* parameters below.
 */
public final class AppConfig {

    private AppConfig() {
    }

    // ---------------------------------------------------------------
    // ENVIRONMENT SELECTOR (Multi-Env Isolation)
    // ---------------------------------------------------------------
    public static final String ACTIVE_ENV = resolve("APP_ENV", "dev");
    public static final EnvironmentProfile ENV_PROFILE = EnvironmentProfile.forName(ACTIVE_ENV);

    // ---------------------------------------------------------------
    // RUN MODE: local (offline, filesystem-backed) vs azure (real ADLS/Blob)
    // ---------------------------------------------------------------
    public static final boolean AZURE_MODE = Boolean.parseBoolean(resolve("AZURE_MODE", "false"));

    // ---------------------------------------------------------------
    // AZURE ACCOUNT PARAMETERS - fill these via .env or VM Options when
    // AZURE_MODE=true. Left blank-safe for local execution.
    // ---------------------------------------------------------------
    public static final String AZURE_STORAGE_ACCOUNT_NAME = resolve("AZURE_STORAGE_ACCOUNT_NAME", "");
    public static final String AZURE_STORAGE_ACCOUNT_KEY = resolve("AZURE_STORAGE_ACCOUNT_KEY", "");
    public static final String AZURE_ADLS_FILESYSTEM_RAW = resolve("AZURE_ADLS_FILESYSTEM_RAW", ENV_PROFILE.adlsRawContainer());
    public static final String AZURE_ADLS_FILESYSTEM_CURATED = resolve("AZURE_ADLS_FILESYSTEM_CURATED", ENV_PROFILE.adlsCuratedContainer());
    public static final String AZURE_TENANT_ID = resolve("AZURE_TENANT_ID", "");
    public static final String AZURE_CLIENT_ID = resolve("AZURE_CLIENT_ID", "");
    public static final String AZURE_CLIENT_SECRET = resolve("AZURE_CLIENT_SECRET", "");
    public static final String AZURE_KEYVAULT_URI = resolve("AZURE_KEYVAULT_URI", "");

    // ---------------------------------------------------------------
    // LOCAL FILESYSTEM PATHS (used when AZURE_MODE=false)
    // ---------------------------------------------------------------
    public static final Path LOCAL_ROOT = Paths.get(resolve("LOCAL_DATA_ROOT", "./data"));
    public static final Path LOCAL_RAW_PATH = LOCAL_ROOT.resolve(ENV_PROFILE.envName()).resolve("raw");
    public static final Path LOCAL_CURATED_PATH = LOCAL_ROOT.resolve(ENV_PROFILE.envName()).resolve("curated");
    public static final Path LOCAL_CHECKPOINT_PATH = LOCAL_ROOT.resolve(ENV_PROFILE.envName()).resolve("_checkpoints");

    // ---------------------------------------------------------------
    // SYNTHETIC DATA VOLUME (drives the Autoscaler Internals demo)
    // ---------------------------------------------------------------
    public static final int SYNTHETIC_ROW_COUNT = Integer.parseInt(resolve("SYNTHETIC_ROW_COUNT", "250000"));
    public static final int SYNTHETIC_BATCH_COUNT = Integer.parseInt(resolve("SYNTHETIC_BATCH_COUNT", "5"));
    public static final long RANDOM_SEED = Long.parseLong(resolve("RANDOM_SEED", "42"));

    // ---------------------------------------------------------------
    // SPARK RUNTIME
    // ---------------------------------------------------------------
    public static final String SPARK_MASTER = resolve("SPARK_MASTER", "local[*]");
    public static final int SHUFFLE_PARTITIONS = Integer.parseInt(resolve("SPARK_SHUFFLE_PARTITIONS", "8"));

    private static String resolve(String key, String defaultValue) {
        String sysProp = System.getProperty(key);
        if (sysProp != null && !sysProp.isBlank()) {
            return sysProp;
        }
        String envVar = System.getenv(key);
        if (envVar != null && !envVar.isBlank()) {
            return envVar;
        }
        return defaultValue;
    }

    /** Builds the fully-qualified curated Delta table path for the active environment. */
    public static String curatedDeltaPath() {
        if (AZURE_MODE) {
            requireAzureParams();
            return "abfss://%s@%s.dfs.core.windows.net/ledger_reconciliation/%s"
                    .formatted(AZURE_ADLS_FILESYSTEM_CURATED, AZURE_STORAGE_ACCOUNT_NAME, ENV_PROFILE.envName());
        }
        return LOCAL_CURATED_PATH.toAbsolutePath().toString();
    }

    /** Builds the fully-qualified raw zone path for the active environment. */
    public static String rawZonePath() {
        if (AZURE_MODE) {
            requireAzureParams();
            return "abfss://%s@%s.dfs.core.windows.net/ledger_reconciliation_raw/%s"
                    .formatted(AZURE_ADLS_FILESYSTEM_RAW, AZURE_STORAGE_ACCOUNT_NAME, ENV_PROFILE.envName());
        }
        return LOCAL_RAW_PATH.toAbsolutePath().toString();
    }

    private static void requireAzureParams() {
        if (AZURE_STORAGE_ACCOUNT_NAME.isBlank()) {
            throw new IllegalStateException(
                    "AZURE_MODE=true but AZURE_STORAGE_ACCOUNT_NAME is blank. " +
                    "Set it via .env or -DAZURE_STORAGE_ACCOUNT_NAME=<your-account> in VM Options.");
        }
    }
}
