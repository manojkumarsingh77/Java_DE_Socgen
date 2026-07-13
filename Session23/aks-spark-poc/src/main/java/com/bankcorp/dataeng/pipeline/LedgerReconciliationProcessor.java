package com.bankcorp.dataeng.pipeline;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.functions.*;

/**
 * TOPIC: Core Ledger Reconciliation (Retail Banking business scenario).
 *
 * Reconciles raw core-banking ledger events against themselves to identify:
 *   1. Duplicate postings (same transactionId posted twice - a common
 *      core-banking bug during network retries)
 *   2. Stuck PENDING transactions older than 24h (potential settlement failure)
 *   3. High-value REVERSAL transactions requiring supervisory sign-off
 *
 * This transformation is intentionally join/window-heavy so that, combined
 * with the region-skewed synthetic data (60% APAC-MUMBAI), it produces a
 * REAL data-skew physical plan - the exact condition Node Pools + Autoscaler
 * are provisioned to absorb in production.
 */
public final class LedgerReconciliationProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(LedgerReconciliationProcessor.class);

    private LedgerReconciliationProcessor() {
    }

    public static Dataset<Row> reconcile(SparkSession spark, Dataset<Row> rawLedger) {

        rawLedger.createOrReplaceTempView("raw_ledger");

        // Window over (accountNumber) ordered by event time - used to detect
        // duplicate postings within the same account stream.
        WindowSpec accountWindow = Window
                .partitionBy("accountNumber")
                .orderBy(col("eventEpochMillis"));

        Dataset<Row> withSequence = rawLedger
                .withColumn("postingSequence", row_number().over(accountWindow))
                .withColumn("priorAmount", lag("amount", 1).over(accountWindow))
                .withColumn("priorTxnType", lag("transactionType", 1).over(accountWindow));

        Dataset<Row> duplicateFlagged = withSequence.withColumn(
                "isDuplicatePosting",
                col("priorAmount").isNotNull()
                        .and(col("amount").equalTo(col("priorAmount")))
                        .and(col("transactionType").equalTo(col("priorTxnType")))
        );

        long staleThresholdMillis = System.currentTimeMillis() - (24L * 60 * 60 * 1000);

        Dataset<Row> reconciled = duplicateFlagged
                .withColumn("reconciliationStatus",
                        when(col("isDuplicatePosting"), lit("DUPLICATE_POSTING_ALERT"))
                        .when(col("ledgerStatus").equalTo("PENDING")
                                        .and(col("eventEpochMillis").lt(staleThresholdMillis)),
                                lit("STALE_PENDING_SETTLEMENT_RISK"))
                        .when(col("transactionType").equalTo("REVERSAL")
                                        .and(col("amount").gt(lit(100000))),
                                lit("HIGH_VALUE_REVERSAL_REVIEW_REQUIRED"))
                        .when(col("ledgerStatus").equalTo("RECONCILED"), lit("RECONCILED"))
                        .otherwise(lit("OK")))
                .withColumn("reconciledAtEpochMillis", lit(System.currentTimeMillis()))
                .drop("priorAmount", "priorTxnType");

        LOG.info("Reconciliation transformation applied. Physical plan (abbreviated):");
        reconciled.explain(false);

        // Aggregate business KPI view - exception rate by region, exposed as
        // a temp view so a downstream BI tool (e.g. Synapse Serverless SQL)
        // could query it directly against the curated Delta table.
        Dataset<Row> exceptionSummary = reconciled
                .groupBy("region", "reconciliationStatus")
                .agg(count("*").alias("txn_count"), sum("amount").alias("total_amount"))
                .orderBy(col("txn_count").desc());

        LOG.info("Exception summary by region (top skew indicator):");
        exceptionSummary.show(20, false);

        return reconciled;
    }
}
