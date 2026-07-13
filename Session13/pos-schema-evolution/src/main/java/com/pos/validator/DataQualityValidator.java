package com.pos.validator;

import com.pos.config.PipelineConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;

/**
 * POS SCHEMA EVOLUTION — Data Quality Validator
 *
 * ╔══════════════════════════════════════════════════════════════════╗
 * ║  DATA VALIDATION RULES — The 5 Pillars                         ║
 * ║                                                                 ║
 * ║  1. COMPLETENESS   → required fields must not be null          ║
 * ║  2. VALIDITY       → values within business-defined ranges     ║
 * ║  3. CONSISTENCY    → cross-field rules (e.g., tax ≤ amount)   ║
 * ║  4. CONFORMITY     → format rules (e.g., terminalId = 8 chars) ║
 * ║  5. SCHEMA VERSION → event carries the schema version it used  ║
 * ║                                                                 ║
 * ║  QUARANTINE PATTERN (Dead Letter Queue):                       ║
 * ║  • Invalid records are NOT dropped                             ║
 * ║  • They are routed to a separate "quarantine" output           ║
 * ║  • Each quarantined record has a human-readable REASON         ║
 * ║  • Ops team can inspect, fix, and replay                       ║
 * ║                                                                 ║
 * ║  In Spark UI DAG you will see:                                 ║
 * ║  [Filter(isValid=true)]  → valid sink                          ║
 * ║  [Filter(isValid=false)] → quarantine sink                     ║
 * ║  Both share the same [Project(computeValidation)] parent node  ║
 * ╚══════════════════════════════════════════════════════════════════╝
 */
public class DataQualityValidator {

    private static final Logger LOG = LoggerFactory.getLogger(DataQualityValidator.class);

    /**
     * Apply all validation rules to the parsed stream.
     * Returns enriched Dataset with columns: isValid, validationError, schemaVersion
     *
     * DAG produced:
     * [EventTimeWatermark / source]
     *    → [Project: add schemaVersion column]
     *    → [Project: add validationError column  (UDF + when/otherwise)]
     *    → [Project: add isValid column]
     */
    public static Dataset<Row> validate(Dataset<Row> parsed) {

        return parsed
                // ── RULE 0: Detect which schema version this event came from ──────
                // This is CONTRACT TESTING in action:
                // We inspect the payload to infer what schema version produced it.
                // V1 → no loyaltyCardId column populated
                // V2 → loyaltyCardId or paymentMethod present
                // V3 → receiptEmail or selfCheckout present
                .withColumn("schemaVersion",
                        when(col("receiptEmail").isNotNull()
                                        .or(col("selfCheckout").isNotNull()), lit("v3"))
                        .when(col("loyaltyCardId").isNotNull()
                                        .or(col("paymentMethod").isNotNull()), lit("v2"))
                        .otherwise(lit("v1"))
                )

                // ── RULE 1: COMPLETENESS — required fields ────────────────────────
                // transactionId, terminalId, amount, eventTimestamp are non-nullable
                // But JSON parsing with PERMISSIVE mode can still produce nulls
                // (e.g., if the field was missing in the JSON entirely)
                .withColumn("r1_completeness",
                        when(col("transactionId").isNull(), lit("MISSING:transactionId"))
                        .when(col("terminalId").isNull(),    lit("MISSING:terminalId"))
                        .when(col("amount").isNull(),        lit("MISSING:amount"))
                        .when(col("eventTimestamp").isNull(),lit("MISSING:eventTimestamp"))
                        .otherwise(lit(null).cast(DataTypes.StringType))
                )

                // ── RULE 2: VALIDITY — amount in business range ───────────────────
                .withColumn("r2_validity",
                        when(col("amount").lt(PipelineConfig.MIN_AMOUNT),
                                concat(lit("INVALID_AMOUNT:"), col("amount")))
                        .when(col("amount").gt(PipelineConfig.MAX_AMOUNT),
                                concat(lit("AMOUNT_EXCEEDS_LIMIT:"), col("amount")))
                        .when(col("itemCount").isNotNull()
                                        .and(col("itemCount").gt(PipelineConfig.MAX_ITEMS)),
                                concat(lit("ITEM_COUNT_EXCEEDS_LIMIT:"), col("itemCount")))
                        .otherwise(lit(null).cast(DataTypes.StringType))
                )

                // ── RULE 3: CONFORMITY — terminalId format ────────────────────────
                // Business rule: terminal IDs are exactly 8 uppercase alphanumeric chars
                .withColumn("r3_conformity",
                        when(length(col("terminalId")).notEqual(PipelineConfig.TERMINAL_ID_LEN),
                                concat(lit("INVALID_TERMINAL_FORMAT:"), col("terminalId")))
                        .otherwise(lit(null).cast(DataTypes.StringType))
                )

                // ── RULE 4: CONSISTENCY — tax must not exceed amount ──────────────
                // Only applies when taxAmount is present (v2+ field)
                // coalesce() handles the case where taxAmount is null (v1 records)
                .withColumn("r4_consistency",
                        when(col("taxAmount").isNotNull()
                                        .and(col("taxAmount").gt(col("amount"))),
                                concat(lit("TAX_EXCEEDS_AMOUNT: tax="),
                                        col("taxAmount"), lit(" amount="), col("amount")))
                        .otherwise(lit(null).cast(DataTypes.StringType))
                )

                // ── AGGREGATE: Combine all rule results into one error string ──────
                .withColumn("validationError",
                        when(col("r1_completeness").isNotNull(), col("r1_completeness"))
                        .when(col("r2_validity").isNotNull(),    col("r2_validity"))
                        .when(col("r3_conformity").isNotNull(),  col("r3_conformity"))
                        .when(col("r4_consistency").isNotNull(), col("r4_consistency"))
                        .otherwise(lit(null).cast(DataTypes.StringType))
                )

                // ── isValid flag: null error = valid record ───────────────────────
                .withColumn("isValid", col("validationError").isNull())

                // ── Drop intermediate rule columns (keep DAG output clean) ────────
                .drop("r1_completeness", "r2_validity", "r3_conformity", "r4_consistency");
    }

    /**
     * Split validated stream into two sinks:
     *   1. Valid records   → enriched output path
     *   2. Invalid records → quarantine path (with reason)
     *
     * DAG TEACHING MOMENT:
     * Both queries share the SAME source stream (rawStream is reused).
     * Spark optimises this — it reads the source ONCE and branches the DAG.
     * In Spark UI you'll see TWO separate query DAGs sharing upstream nodes.
     */
    public static StreamingQuery[] startSinks(Dataset<Row> validated,
                                               String validPath,
                                               String quarantinePath,
                                               String checkpointBase) throws TimeoutException {

        // ── VALID SINK ─────────────────────────────────────────────────────────
        // Only fields that downstream consumers expect
        // Notice: schemaVersion is included — consumers can branch on it
        Dataset<Row> validRecords = validated
                .filter(col("isValid").equalTo(true))
                .select(
                        col("transactionId"),
                        col("terminalId"),
                        col("amount"),
                        col("eventTimestamp"),
                        col("itemCount"),
                        col("cashierId"),
                        // v2 optional fields — null for v1 records (safe)
                        col("loyaltyCardId"),
                        col("paymentMethod"),
                        col("taxAmount"),
                        col("storeRegion"),
                        // v3 optional fields — null for v1/v2 records (safe)
                        col("receiptEmail"),
                        col("selfCheckout"),
                        // metadata
                        col("schemaVersion"),
                        col("isValid")
                );

        StreamingQuery validQuery = validRecords
                .writeStream()
                .outputMode("append")
                .format("json")
                .option("path", validPath)
                .option("checkpointLocation", checkpointBase + "/valid")
                .queryName("pos-valid-events")
                .start();

        // ── QUARANTINE SINK ───────────────────────────────────────────────────
        // Bad records go here — with full payload + reason for debugging
        Dataset<Row> badRecords = validated
                .filter(col("isValid").equalTo(false))
                .select(
                        col("transactionId"),
                        col("terminalId"),
                        col("amount"),
                        col("validationError"),
                        col("schemaVersion"),
                        col("isValid")
                );

        StreamingQuery quarantineQuery = badRecords
                .writeStream()
                .outputMode("append")
                .format("json")
                .option("path", quarantinePath)
                .option("checkpointLocation", checkpointBase + "/quarantine")
                .queryName("pos-quarantine-events")
                .start();

        LOG.info("✅ Valid sink:      {}", validPath);
        LOG.info("✅ Quarantine sink: {}", quarantinePath);

        return new StreamingQuery[]{validQuery, quarantineQuery};
    }
}
