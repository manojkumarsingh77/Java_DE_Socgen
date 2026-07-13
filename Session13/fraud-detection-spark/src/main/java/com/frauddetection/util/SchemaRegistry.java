package com.frauddetection.util;

import org.apache.spark.sql.types.*;

/**
 * FRAUD DETECTION PIPELINE - Schema Registry
 *
 * TEACHING POINT: Schema Inference vs Explicit Schema
 *
 * In production Structured Streaming, ALWAYS provide explicit schema.
 * Reasons:
 * 1. Schema inference requires reading ALL source data (expensive)
 * 2. Schema inference on streaming sources is not supported (only batch)
 * 3. Explicit schema = faster startup, guaranteed types
 * 4. Schema evolution can be managed explicitly
 *
 * This schema maps directly to the Transaction model class.
 */
public class SchemaRegistry {

    /**
     * Transaction Schema — mirrors Transaction.java fields.
     *
     * Spark SQL types:
     * - StringType  → Java String
     * - DoubleType  → Java double/Double
     * - LongType    → Java long/Long (for timestamps in ms)
     * - BooleanType → Java boolean/Boolean
     *
     * nullable=true is the default and is safer for streaming sources
     * where individual fields may be missing.
     */
    public static final StructType TRANSACTION_SCHEMA = new StructType(new StructField[]{
            new StructField("transactionId",        DataTypes.StringType,  true, Metadata.empty()),
            new StructField("customerId",           DataTypes.StringType,  true, Metadata.empty()),
            new StructField("cardNumber",           DataTypes.StringType,  true, Metadata.empty()),
            new StructField("amount",               DataTypes.DoubleType,  true, Metadata.empty()),
            new StructField("merchantCategory",     DataTypes.StringType,  true, Metadata.empty()),
            new StructField("merchantCountry",      DataTypes.StringType,  true, Metadata.empty()),
            new StructField("merchantCity",         DataTypes.StringType,  true, Metadata.empty()),
            new StructField("eventTimestamp",       DataTypes.LongType,    true, Metadata.empty()),
            new StructField("processingTimestamp",  DataTypes.LongType,    true, Metadata.empty()),
            new StructField("channel",              DataTypes.StringType,  true, Metadata.empty()),
            new StructField("lateEvent",            DataTypes.BooleanType, true, Metadata.empty())
    });

    /**
     * Fraud Alert Schema — for reading back alerts
     */
    public static final StructType FRAUD_ALERT_SCHEMA = new StructType(new StructField[]{
            new StructField("alertId",          DataTypes.StringType,  true, Metadata.empty()),
            new StructField("transactionId",    DataTypes.StringType,  true, Metadata.empty()),
            new StructField("customerId",       DataTypes.StringType,  true, Metadata.empty()),
            new StructField("amount",           DataTypes.DoubleType,  true, Metadata.empty()),
            new StructField("fraudType",        DataTypes.StringType,  true, Metadata.empty()),
            new StructField("riskScore",        DataTypes.DoubleType,  true, Metadata.empty()),
            new StructField("detectionReason",  DataTypes.StringType,  true, Metadata.empty()),
            new StructField("severity",         DataTypes.StringType,  true, Metadata.empty()),
            new StructField("wasLateEvent",     DataTypes.BooleanType, true, Metadata.empty())
    });

    private SchemaRegistry() {}
}
