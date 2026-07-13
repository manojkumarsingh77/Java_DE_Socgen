package com.npunext.bank.fraud.job;

import com.npunext.bank.fraud.generator.SyntheticDataGenerator;
import com.npunext.bank.fraud.model.Transaction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * Retail Banking Fraud Pre-Processor.
 *
 * <p>Stage 1  — Initialization: build a {@link SparkSession}, portable between
 *               local IntelliJ execution and {@code spark-submit --master k8s://...}.
 * <p>Stage 2  — Synthetic Data Generation: build a self-contained banking
 *               transaction dataset via {@link SyntheticDataGenerator}.
 * <p>Stage 3  — Core Transformation: risk-score every transaction and
 *               aggregate flagged volume per account.
 * <p>Stage 4  — Egress: persist the flagged output and print a summary +
 *               physical execution plan to stdout (captured by the driver
 *               pod log in AKS, or the IntelliJ console locally).
 */
public final class FraudPreProcessorJob {

    private static final String MASTER_OVERRIDE_PROPERTY = "spark.master.override";

    private FraudPreProcessorJob() {
    }

    public static void main(String[] args) {
        int recordCount = args.length > 0 ? Integer.parseInt(args[0]) : 50_000;
        String outputPath = args.length > 1 ? args[1] : "/tmp/fraud-preprocessor-output";

        SparkSession spark = buildSparkSession();
        try {
            run(spark, recordCount, outputPath);
        } finally {
            spark.stop();
        }
    }

    /**
     * Builds the SparkSession. When run locally inside IntelliJ, the VM option
     * {@code -Dspark.master.override=local[*]} sets the master explicitly.
     * When launched via {@code spark-submit} on Kubernetes, no override is
     * present — the master supplied on the spark-submit command line
     * (k8s://https://...) takes effect instead, so the code never hardcodes it.
     */
    private static SparkSession buildSparkSession() {
        SparkSession.Builder builder = SparkSession.builder()
                .appName("RetailBankFraudPreProcessor");

        String masterOverride = System.getProperty(MASTER_OVERRIDE_PROPERTY);
        if (masterOverride != null && !masterOverride.isBlank()) {
            builder = builder.master(masterOverride);
        }

        return builder
                .config("spark.sql.shuffle.partitions", "8")
                .config("spark.ui.showConsoleProgress", "true")
                .getOrCreate();
    }

    private static void run(SparkSession spark, int recordCount, String outputPath) {
        Dataset<Row> transactions = loadSyntheticTransactions(spark, recordCount);
        transactions.createOrReplaceTempView("transactions");

        // --- Core Transformation: rule-based risk scoring ---------------
        // A transaction is flagged HIGH risk when it combines a large
        // foreign-currency amount with an unusual local hour (00:00-04:59) —
        // a classic card-not-present fraud signature. MEDIUM risk covers
        // large domestic outliers. Everything else is LOW.
        Dataset<Row> scored = spark.sql(
                "SELECT *, " +
                "  hour(timestamp_millis(transaction_epoch_millis)) AS txn_hour, " +
                "  CASE " +
                "    WHEN foreign_transaction = true AND amount > 5000 " +
                "         AND hour(timestamp_millis(transaction_epoch_millis)) < 5 THEN 'HIGH' " +
                "    WHEN amount > 3000 AND channel = 'ATM_WITHDRAWAL' THEN 'MEDIUM' " +
                "    WHEN amount > 4000 THEN 'MEDIUM' " +
                "    ELSE 'LOW' " +
                "  END AS risk_level " +
                "FROM transactions"
        );
        scored.createOrReplaceTempView("scored_transactions");
        scored.cache();

        // --- Aggregation: flagged volume and exposure per account -------
        Dataset<Row> accountRisk = spark.sql(
                "SELECT account_id, " +
                "       COUNT(*) AS total_transactions, " +
                "       SUM(CASE WHEN risk_level = 'HIGH' THEN 1 ELSE 0 END) AS high_risk_count, " +
                "       SUM(CASE WHEN risk_level = 'HIGH' THEN amount ELSE 0 END) AS high_risk_amount " +
                "FROM scored_transactions " +
                "GROUP BY account_id " +
                "HAVING high_risk_count > 0 " +
                "ORDER BY high_risk_amount DESC"
        );

        System.out.println("=== Physical Execution Plan (accountRisk) ===");
        accountRisk.explain(true);

        System.out.println("=== Top 20 Accounts by Flagged High-Risk Exposure ===");
        accountRisk.show(20, false);

        long totalCount = scored.count();
        long highRiskCount = scored.filter("risk_level = 'HIGH'").count();
        System.out.printf(
                "=== Summary: %d transactions processed, %d flagged HIGH risk (%.3f%%) ===%n",
                totalCount, highRiskCount, (highRiskCount * 100.0) / totalCount
        );

        // --- Egress -------------------------------------------------------
        scored.filter("risk_level != 'LOW'")
                .coalesce(1)
                .write()
                .mode(SaveMode.Overwrite)
                .partitionBy("risk_level")
                .parquet(outputPath);

        System.out.println("Flagged transactions written to: " + outputPath);
    }

    /**
     * Converts the in-memory synthetic {@link Transaction} records into a
     * Spark {@link Dataset} of {@link Row} against an explicit
     * {@link StructType}. An explicit schema (rather than reflective bean
     * encoding) guarantees stable column types and names regardless of the
     * Java record's internal representation.
     */
    private static Dataset<Row> loadSyntheticTransactions(SparkSession spark, int recordCount) {
        SyntheticDataGenerator generator = new SyntheticDataGenerator(
                42L,          // fixed seed -> reproducible demo runs
                recordCount,
                0.02          // 2% injected anomaly rate
        );
        List<Transaction> synthetic = generator.generate();

        List<Row> rows = new ArrayList<>(synthetic.size());
        for (Transaction t : synthetic) {
            rows.add(RowFactory.create(
                    t.transactionId(),
                    t.accountId(),
                    t.customerId(),
                    t.amount(),
                    t.currency(),
                    t.merchantCategory(),
                    t.channel(),
                    t.city(),
                    t.country(),
                    t.transactionEpochMillis(),
                    t.foreignTransaction(),
                    t.deviceId()
            ));
        }

        StructType schema = new StructType(new StructField[]{
                new StructField("transaction_id", DataTypes.StringType, false, Metadata.empty()),
                new StructField("account_id", DataTypes.StringType, false, Metadata.empty()),
                new StructField("customer_id", DataTypes.StringType, false, Metadata.empty()),
                new StructField("amount", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("currency", DataTypes.StringType, false, Metadata.empty()),
                new StructField("merchant_category", DataTypes.StringType, false, Metadata.empty()),
                new StructField("channel", DataTypes.StringType, false, Metadata.empty()),
                new StructField("city", DataTypes.StringType, false, Metadata.empty()),
                new StructField("country", DataTypes.StringType, false, Metadata.empty()),
                new StructField("transaction_epoch_millis", DataTypes.LongType, false, Metadata.empty()),
                new StructField("foreign_transaction", DataTypes.BooleanType, false, Metadata.empty()),
                new StructField("device_id", DataTypes.StringType, false, Metadata.empty())
        });

        return spark.createDataFrame(rows, schema);
    }
}
