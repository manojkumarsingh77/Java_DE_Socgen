package com.frauddetection.config;

/**
 * FRAUD DETECTION PIPELINE - Configuration Constants
 *
 * Centralised configuration for the Spark Structured Streaming pipeline.
 *
 * KEY CONCEPTS TAUGHT HERE:
 * ┌─────────────────────────────────────────────────────────────────┐
 * │  BACKPRESSURE TUNING                                            │
 * │  maxOffsetsPerTrigger controls how many records per micro-batch │
 * │  This prevents OOM and gives predictable latency               │
 * ├─────────────────────────────────────────────────────────────────┤
 * │  WATERMARK DESIGN                                               │
 * │  10-minute watermark handles late events (mobile network lags)  │
 * │  Spark drops events older than (max_event_time - watermark)     │
 * ├─────────────────────────────────────────────────────────────────┤
 * │  CHECKPOINT DURABILITY                                          │
 * │  Checkpoints enable exactly-once and failure recovery           │
 * │  Stored in local filesystem for this demo (use HDFS/S3 in prod) │
 * └─────────────────────────────────────────────────────────────────┘
 */
public class PipelineConfig {

    // ===== SPARK SESSION CONFIG =====
    public static final String APP_NAME = "🔍 Fraud Detection Pipeline - Spark Structured Streaming";
    public static final String MASTER    = "local[4]"; // 4 cores on M1 Max
    public static final int    SPARK_UI_PORT = 4040;

    // ===== WATERMARK CONFIG (Late Event Strategy) =====
    // Business Rule: Mobile app transactions can arrive up to 10 minutes late
    // due to network reconnection delays. We MUST still process them.
    public static final String WATERMARK_DELAY     = "10 minutes";
    public static final String EVENT_TIME_COLUMN   = "eventTime";

    // ===== WINDOW CONFIG (Velocity Detection) =====
    // Fraud Rule: >5 transactions in 5-minute window = velocity abuse
    public static final String WINDOW_DURATION     = "5 minutes";
    public static final String SLIDE_DURATION      = "1 minute";

    // ===== BACKPRESSURE CONFIG =====
    // Controls micro-batch size — critical for P99 latency target
    // If a micro-batch is too large, processing time > trigger interval = backlog
    public static final long   MAX_RECORDS_PER_TRIGGER   = 1000L;
    public static final String TRIGGER_INTERVAL          = "2 seconds"; // P99 target: 5s
    public static final int    MAX_FILES_PER_TRIGGER     = 10;

    // ===== STATE STORE CONFIG =====
    // State store holds aggregation state between micro-batches
    // For RocksDB state store, tune these for large state (production)
    public static final String STATE_STORE_PROVIDER     =
            "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider";
    public static final int    STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT = 10;

    // ===== CHECKPOINT CONFIG (Exactly-Once Guarantee) =====
    // Checkpoint dir stores: offsets, state, committed batch IDs
    // In production: use s3a://your-bucket/checkpoints/ or hdfs://...
    public static final String CHECKPOINT_BASE_DIR        = "/tmp/fraud-detection-checkpoints";
    public static final String TRANSACTION_CHECKPOINT_DIR = CHECKPOINT_BASE_DIR + "/transactions";
    public static final String ALERT_CHECKPOINT_DIR       = CHECKPOINT_BASE_DIR + "/alerts";
    public static final String VELOCITY_CHECKPOINT_DIR    = CHECKPOINT_BASE_DIR + "/velocity";

    // ===== FRAUD DETECTION THRESHOLDS =====
    // These map to real bank fraud rules
    public static final double HIGH_VALUE_THRESHOLD       = 5000.0;   // Flag transactions > $5000
    public static final double VELOCITY_RISK_THRESHOLD    = 3;        // >3 txns in 5-min window
    public static final int    VELOCITY_COUNT_THRESHOLD   = 5;        // Alert at 5+ txns/5min
    public static final double INTERNATIONAL_RISK_SCORE   = 0.65;
    public static final double GEO_ANOMALY_RISK_SCORE     = 0.80;
    public static final double HIGH_RISK_MERCHANT_SCORE   = 0.75;
    public static final double VELOCITY_BASE_RISK_SCORE   = 0.85;

    // ===== OUTPUT CONFIG =====
    public static final String OUTPUT_BASE_DIR             = "/tmp/fraud-detection-output";
    public static final String FRAUD_ALERTS_OUTPUT_DIR    = OUTPUT_BASE_DIR + "/fraud-alerts";
    public static final String LATE_EVENTS_OUTPUT_DIR     = OUTPUT_BASE_DIR + "/late-events";
    public static final String VELOCITY_OUTPUT_DIR        = OUTPUT_BASE_DIR + "/velocity-stats";
    public static final String METRICS_OUTPUT_DIR         = OUTPUT_BASE_DIR + "/metrics";

    // ===== TRANSACTION GENERATOR CONFIG =====
    // Simulates real-world transaction stream for the demo
    public static final int    TRANSACTIONS_PER_SECOND    = 50;  // Realistic bank load
    public static final double FRAUD_RATE                 = 0.08; // 8% fraud rate (realistic)
    public static final double LATE_EVENT_RATE            = 0.05; // 5% events arrive late
    public static final int    MAX_LATE_DELAY_MINUTES     = 8;   // Max 8 min late (< watermark)

    // High-risk merchant categories (real fraud patterns)
    public static final String[] HIGH_RISK_MERCHANTS = {
            "CRYPTO_EXCHANGE", "GAMBLING", "WIRE_TRANSFER",
            "MONEY_ORDER", "PAWN_SHOP"
    };

    // High-risk countries (for geo-anomaly detection)
    public static final String[] HIGH_RISK_COUNTRIES = {
            "NG", "RO", "UA", "VE", "ZW"
    };

    // Normal merchant categories
    public static final String[] NORMAL_MERCHANTS = {
            "GROCERY", "GAS_STATION", "RESTAURANT", "RETAIL",
            "PHARMACY", "AIRLINE", "HOTEL", "STREAMING"
    };

    // Normal countries
    public static final String[] NORMAL_COUNTRIES = {
            "US", "GB", "CA", "AU", "DE", "FR", "JP", "SG"
    };

    // ===== SPARK JVM ARGS for Java 17 =====
    // Required for Spark to work with Java 17's module system
    public static final String[] JVM_ARGS = {
            "--add-opens", "java.base/java.lang=ALL-UNNAMED",
            "--add-opens", "java.base/java.util=ALL-UNNAMED",
            "--add-opens", "java.base/java.nio=ALL-UNNAMED",
            "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED",
            "--add-opens", "java.base/sun.nio.cs=ALL-UNNAMED",
            "--add-opens", "java.base/java.io=ALL-UNNAMED",
            "--add-opens", "java.base/java.lang.reflect=ALL-UNNAMED",
            "--add-opens", "java.base/java.net=ALL-UNNAMED"
    };

    private PipelineConfig() {
        // Utility class — no instantiation
    }
}
