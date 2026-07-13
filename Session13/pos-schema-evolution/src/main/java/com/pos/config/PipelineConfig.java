package com.pos.config;

/**
 * POS SCHEMA EVOLUTION — Configuration
 *
 * TEACHING NOTE: Centralised config = single place to tune the demo live.
 *
 * KEY CONCEPTS WIRED HERE:
 * ┌─────────────────────────────────────────────────────────────┐
 * │ SCHEMA ENFORCEMENT  → enforceSchema = true in Spark reader  │
 * │ CONTRACT TESTING    → CONTRACT_VERSION tracks expected schema│
 * │ SCHEMA REGISTRY     → REGISTRY_PATH stores versioned schemas │
 * │ DATA QUALITY        → QUARANTINE_PATH for bad records        │
 * │ BACKPRESSURE        → MAX_FILES_PER_TRIGGER limits load      │
 * └─────────────────────────────────────────────────────────────┘
 */
public final class PipelineConfig {

    // ── Spark ──────────────────────────────────────────────────
    public static final String APP_NAME    = "🛒 POS Schema Evolution Pipeline";
    public static final String MASTER      = "local[4]";
    public static final int    UI_PORT     = 4040;
    public static final String TRIGGER     = "3 seconds";

    // ── Paths ──────────────────────────────────────────────────
    public static final String BASE        = "/tmp/pos-demo";
    public static final String SOURCE_DIR  = BASE + "/source";
    public static final String VALID_OUT   = BASE + "/output/valid";
    public static final String QUARANTINE  = BASE + "/output/quarantine";
    public static final String REGISTRY    = BASE + "/schema-registry";
    public static final String CHECKPOINT  = BASE + "/checkpoints";

    // ── Schema Contract ────────────────────────────────────────
    // Bump this when a BREAKING change is introduced (teaches contract versioning)
    public static final String CONTRACT_VERSION = "v2";

    // ── Data Quality Thresholds ────────────────────────────────
    public static final double MAX_AMOUNT        = 99_999.99;
    public static final double MIN_AMOUNT        = 0.01;
    public static final int    MAX_ITEMS         = 500;
    public static final int    TERMINAL_ID_LEN   = 8;

    // ── Generator ─────────────────────────────────────────────
    public static final int  EVENTS_PER_BATCH    = 30;
    public static final int  BATCH_INTERVAL_SEC  = 2;
    // 10% v1 (old), 75% v2 (current), 10% v3 (future), 5% corrupt
    public static final int  MAX_FILES_PER_TRIGGER = 5;

    private PipelineConfig() {}
}
