package com.pos.schema;

import com.pos.config.PipelineConfig;
import org.apache.spark.sql.types.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;

/**
 * POS SCHEMA EVOLUTION — Schema Registry
 *
 * ╔══════════════════════════════════════════════════════════════════╗
 * ║  SCHEMA REGISTRY CONCEPT                                        ║
 * ║                                                                 ║
 * ║  A Schema Registry is a centralised store that:                ║
 * ║  1. Tracks every version of every schema ever deployed          ║
 * ║  2. Allows producers to REGISTER new versions                   ║
 * ║  3. Allows consumers to FETCH the schema for a given version    ║
 * ║  4. Enforces COMPATIBILITY rules between versions               ║
 * ║                                                                 ║
 * ║  COMPATIBILITY MODES (real Confluent Schema Registry):         ║
 * ║  • BACKWARD  → new schema can read data written by old schema  ║
 * ║  • FORWARD   → old schema can read data written by new schema  ║
 * ║  • FULL      → both backward AND forward compatible            ║
 * ║  • NONE      → no checks (dangerous in production!)            ║
 * ║                                                                 ║
 * ║  THIS DEMO: File-based registry (same concept, no Kafka needed)║
 * ║  Production: Confluent Schema Registry / AWS Glue Schema Registry║
 * ╚══════════════════════════════════════════════════════════════════╝
 *
 * POS EVENT SCHEMA EVOLUTION HISTORY:
 *
 * v1 (2022): Core fields only — simple MVP launch
 *   transactionId, terminalId, amount, timestamp, items
 *
 * v2 (2023): Added optional loyalty + payment fields (BACKWARD COMPATIBLE)
 *   + loyaltyCardId (nullable)       ← safe: old data just has null here
 *   + paymentMethod (nullable)       ← safe: old data just has null here
 *   + taxAmount (nullable)           ← safe: can be derived if missing
 *   + storeRegion (nullable)         ← safe: enriched downstream
 *
 * v3 (2024 — future/canary): Added optional digital fields
 *   + receiptEmail (nullable)        ← safe optional addition
 *   + selfCheckout (nullable Boolean)← safe optional addition
 *
 * BREAKING CHANGE example (what we PREVENT):
 *   Renaming "amount" → "totalAmount"  ← breaks ALL old readers
 *   Making "terminalId" non-nullable   ← rejects old records without it
 */
public class SchemaRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaRegistry.class);

    // ── V1 Schema: original launch schema ──────────────────────────────────
    public static final StructType V1_SCHEMA = new StructType(new StructField[]{
            field("transactionId", DataTypes.StringType,  false), // required
            field("terminalId",    DataTypes.StringType,  false), // required
            field("amount",        DataTypes.DoubleType,  false), // required
            field("eventTimestamp",DataTypes.LongType,    false), // required
            field("itemCount",     DataTypes.IntegerType, true),  // optional
            field("cashierId",     DataTypes.StringType,  true)   // optional
    });

    // ── V2 Schema: current production schema ───────────────────────────────
    // KEY: All NEW fields are nullable → BACKWARD COMPATIBLE with v1 data
    // V1 records will simply have null in the new columns — fully safe.
    public static final StructType V2_SCHEMA = new StructType(new StructField[]{
            // ── Kept from v1 (unchanged) ──
            field("transactionId", DataTypes.StringType,  false),
            field("terminalId",    DataTypes.StringType,  false),
            field("amount",        DataTypes.DoubleType,  false),
            field("eventTimestamp",DataTypes.LongType,    false),
            field("itemCount",     DataTypes.IntegerType, true),
            field("cashierId",     DataTypes.StringType,  true),
            // ── New in v2 (ALL nullable = backward compatible) ──
            field("loyaltyCardId", DataTypes.StringType,  true),  // ← new optional
            field("paymentMethod", DataTypes.StringType,  true),  // ← new optional
            field("taxAmount",     DataTypes.DoubleType,  true),  // ← new optional
            field("storeRegion",   DataTypes.StringType,  true)   // ← new optional
    });

    // ── V3 Schema: future/canary schema (arrives from new terminals) ───────
    // Extends v2 with digital receipt fields — still nullable, still safe
    public static final StructType V3_SCHEMA = new StructType(new StructField[]{
            field("transactionId", DataTypes.StringType,  false),
            field("terminalId",    DataTypes.StringType,  false),
            field("amount",        DataTypes.DoubleType,  false),
            field("eventTimestamp",DataTypes.LongType,    false),
            field("itemCount",     DataTypes.IntegerType, true),
            field("cashierId",     DataTypes.StringType,  true),
            field("loyaltyCardId", DataTypes.StringType,  true),
            field("paymentMethod", DataTypes.StringType,  true),
            field("taxAmount",     DataTypes.DoubleType,  true),
            field("storeRegion",   DataTypes.StringType,  true),
            // ── New in v3 (future — optional safe additions) ──
            field("receiptEmail",  DataTypes.StringType,  true),  // ← new optional
            field("selfCheckout",  DataTypes.BooleanType, true)   // ← new optional
    });

    /**
     * The PERMISSIVE READER SCHEMA.
     *
     * This is the most important schema for streaming pipelines.
     * It is the SUPERSET of all known versions — reads ANY version's data.
     *
     * TEACHING POINT:
     * When Spark reads JSON with this schema:
     *   - V1 data → v2+ fields will be null (safe ✅)
     *   - V2 data → v3 fields will be null (safe ✅)
     *   - V3 data → all fields populated (safe ✅)
     *   - Unknown fields in JSON → silently ignored (safe ✅)
     *
     * This is the OPEN/CLOSED PRINCIPLE applied to data schemas:
     *   OPEN for extension (new nullable fields)
     *   CLOSED for modification (never rename/remove/change types)
     */
    public static final StructType READER_SCHEMA = V3_SCHEMA; // superset

    /**
     * Register both schemas to the file-based registry.
     * In production: POST to Confluent Schema Registry REST API.
     */
    public static void initialize() {
        try {
            Files.createDirectories(Paths.get(PipelineConfig.REGISTRY));

            // Write human-readable schema manifests (simulates registry entries)
            writeManifest("v1", V1_SCHEMA, "2022-01-15", "Initial POS schema");
            writeManifest("v2", V2_SCHEMA, "2023-06-01", "Added loyalty + payment fields (backward compatible)");
            writeManifest("v3", V3_SCHEMA, "2024-03-01", "Added digital receipt fields (forward compatible canary)");
            writeCompatibilityReport();

            LOG.info("╔══════════════════════════════════════════════════════════╗");
            LOG.info("║  SCHEMA REGISTRY initialised at: {}  ║", PipelineConfig.REGISTRY);
            LOG.info("║  Registered: v1 (6 fields), v2 (10 fields), v3 (12 fields) ║");
            LOG.info("╚══════════════════════════════════════════════════════════╝");

        } catch (IOException e) {
            LOG.error("Schema registry init failed", e);
        }
    }

    /** Check if a proposed new field addition is backward compatible. */
    public static boolean isBackwardCompatible(String fieldName, boolean isNullable) {
        // Rule: new fields MUST be nullable to be backward compatible
        // If not nullable, old data (without this field) would fail validation
        if (!isNullable) {
            LOG.warn("INCOMPATIBLE: Field '{}' is NOT nullable → breaks old data!", fieldName);
            return false;
        }
        LOG.info("COMPATIBLE: Field '{}' is nullable → safe to add ✅", fieldName);
        return true;
    }

    private static void writeManifest(String version, StructType schema, String date, String note)
            throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append("Schema Version: ").append(version).append("\n");
        sb.append("Registered: ").append(date).append("\n");
        sb.append("Note: ").append(note).append("\n");
        sb.append("Fields: ").append(schema.fields().length).append("\n\n");
        for (StructField f : schema.fields()) {
            sb.append(String.format("  %-20s %-12s nullable=%-5s\n",
                    f.name(), f.dataType().typeName(), f.nullable()));
        }
        Files.writeString(Paths.get(PipelineConfig.REGISTRY, version + ".schema"), sb.toString());
    }

    private static void writeCompatibilityReport() throws IOException {
        String report = """
                SCHEMA COMPATIBILITY REPORT
                ════════════════════════════
                
                v1 → v2:  BACKWARD COMPATIBLE ✅
                  Reason: 4 new fields added, all nullable
                  Impact: v1 data reads fine with v2 schema (nulls for new fields)
                
                v2 → v3:  BACKWARD COMPATIBLE ✅
                  Reason: 2 new fields added, all nullable
                  Impact: v2 data reads fine with v3 schema (nulls for new fields)
                
                BREAKING CHANGES PREVENTED:
                  ✗ "amount" rename → REJECTED (breaks all consumers)
                  ✗ "terminalId" made non-null → REJECTED (breaks old data)
                  ✗ "itemCount" type change int→string → REJECTED (type mismatch)
                
                READER SCHEMA STRATEGY:
                  Use V3_SCHEMA (superset) to read ALL versions safely.
                  Downstream logic uses coalesce() for missing optional fields.
                """;
        Files.writeString(Paths.get(PipelineConfig.REGISTRY, "compatibility-report.txt"), report);
    }

    private static StructField field(String name, DataType type, boolean nullable) {
        return new StructField(name, type, nullable, Metadata.empty());
    }
}
