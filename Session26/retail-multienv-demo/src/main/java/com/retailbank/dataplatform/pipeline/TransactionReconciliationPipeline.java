package com.retailbank.dataplatform.pipeline;

import com.retailbank.dataplatform.config.AppConfig;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Core reconciliation transformation: left-joins channel-side card transactions
 * against core-ledger postings on {@code transactionId} and classifies each
 * transaction as MATCHED / AMOUNT_MISMATCH / MISSING_LEDGER_ENTRY.
 *
 * <p>This class contains zero environment-specific logic — no file paths, no
 * secrets, no cluster URLs. It receives an already-fully-resolved
 * {@link AppConfig} and two DataFrames, and returns a DataFrame. That separation
 * is what makes the SAME compiled class correct in dev, test, and prod: only the
 * config injected from outside changes.</p>
 */
public final class TransactionReconciliationPipeline {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionReconciliationPipeline.class);

    private final SparkSession spark;
    private final AppConfig config;

    public TransactionReconciliationPipeline(SparkSession spark, AppConfig config) {
        this.spark = spark;
        this.config = config;
    }

    public Dataset<Row> reconcile(Dataset<Row> cardTransactions, Dataset<Row> ledgerEntries) {
        int thresholdCents = config.reconciliation().discrepancyThresholdCents();
        BigDecimalThreshold threshold = BigDecimalThreshold.fromCents(thresholdCents);

        Dataset<Row> joined = cardTransactions.alias("ch")
                .join(ledgerEntries.alias("lg"),
                        cardTransactions.col("transactionId").equalTo(ledgerEntries.col("transactionId")),
                        "left_outer");

        Column discrepancy = functions.when(
                functions.col("lg.postedAmount").isNull(),
                functions.lit(null).cast("decimal(18,2)")
        ).otherwise(
                functions.col("ch.amount").minus(functions.col("lg.postedAmount"))
        );

        Column status = functions.when(functions.col("lg.postedAmount").isNull(), functions.lit("MISSING_LEDGER_ENTRY"))
                .when(functions.abs(functions.col("ch.amount").minus(functions.col("lg.postedAmount")))
                                .geq(functions.lit(threshold.decimalValue())),
                        functions.lit("AMOUNT_MISMATCH"))
                .otherwise(functions.lit("MATCHED"));

        Dataset<Row> result = joined.select(
                functions.col("ch.transactionId").as("transactionId"),
                functions.col("ch.accountId").as("accountId"),
                functions.col("ch.branchCode").as("branchCode"),
                functions.col("ch.amount").as("channelAmount"),
                functions.col("lg.postedAmount").as("ledgerAmount"),
                discrepancy.as("discrepancyAmount"),
                status.as("status"),
                functions.lit(config.environmentName()).as("environment")
        );

        int targetPartitions = config.spark().shufflePartitions();
        result = result.repartition(targetPartitions);

        logSummary(result);
        return result;
    }

    private void logSummary(Dataset<Row> result) {
        Dataset<Row> statusCounts = result.groupBy("status").count();
        LOG.info("==== Reconciliation summary (environment='{}') ====", config.environmentName());
        statusCounts.collectAsList().forEach(row ->
                LOG.info("  {} = {}", row.getString(0), row.getLong(1)));
        LOG.info("====================================================");
    }

    /**
     * Small helper converting an integer "cents" threshold into a BigDecimal
     * comparable against 2-decimal-scale currency columns.
     */
    private record BigDecimalThreshold(java.math.BigDecimal decimalValue) {
        static BigDecimalThreshold fromCents(int cents) {
            return new BigDecimalThreshold(
                    java.math.BigDecimal.valueOf(cents, 2));
        }
    }
}
