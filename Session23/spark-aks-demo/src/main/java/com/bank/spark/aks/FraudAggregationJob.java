package com.bank.spark.aks;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.apache.spark.sql.functions.*;

/**
 * Retail Banking — Real-Time Fraud Score Aggregation on Spark-on-Kubernetes (AKS).
 *
 * Demonstrates:
 *  1. Kubernetes-native SparkSession configuration (driver as K8s controller).
 *  2. Dynamic executor allocation + shuffle tracking (no external shuffle service pods).
 *  3. Synthetic high-fidelity retail-banking dataset generation (no external I/O required).
 *  4. Account-level velocity aggregation (Catalyst partial-aggregate physical plan).
 *  5. Parquet egress partitioned by risk tier.
 */
public final class FraudAggregationJob {

    private static final int NUM_ACCOUNTS = 500;
    private static final int NUM_TRANSACTIONS = 50_000;
    private static final double HIGH_RISK_VELOCITY_THRESHOLD = 8.0; // txns/hour trailing window
    private static final double HIGH_RISK_AMOUNT_THRESHOLD = 25_000.0;

    /** Immutable synthetic transaction record — Java 17 record replaces Lombok/POJO boilerplate. */
    public record BankTransaction(
            String transactionId,
            String accountId,
            long eventEpochSeconds,
            double amount,
            String merchantCategory,
            String channel,   // POS, ONLINE, ATM, WIRE
            String countryCode
    ) implements Serializable {}

    public static void main(String[] args) {

        // ---------- 1. CONFIGURATION / INITIALIZATION ----------
        SparkSession spark = buildSparkSession();
        spark.sparkContext().setLogLevel("WARN");

        try {
            // ---------- 2. SYNTHETIC DATA GENERATION ----------
            Dataset<Row> transactions = generateSyntheticTransactions(spark, NUM_TRANSACTIONS, NUM_ACCOUNTS);
            transactions.cache();

            System.out.println("=== Synthetic Dataset Sample ===");
            transactions.show(10, false);
            System.out.println("Total synthetic transactions generated: " + transactions.count());

            // ---------- 3. CORE TRANSFORMATION / PROCESSING PIPELINE ----------
            Dataset<Row> accountRiskProfile = computeAccountVelocityFeatures(transactions);

            System.out.println("=== Account Risk Profile (Top 20 by risk score) ===");
            accountRiskProfile.orderBy(col("riskScore").desc()).show(20, false);

            // ---------- 4. WRITE / EGRESS STAGE ----------
            String outputPath = resolveLocalOutputPath();
            accountRiskProfile
                    .withColumn("riskTier",
                            when(col("riskScore").geq(0.75), lit("HIGH"))
                                    .when(col("riskScore").geq(0.4), lit("MEDIUM"))
                                    .otherwise(lit("LOW")))
                    .write()
                    .mode(SaveMode.Overwrite)
                    .partitionBy("riskTier")
                    .parquet(outputPath);

            System.out.println("Fraud risk profiles written to: " + outputPath);

        } finally {
            spark.stop();
        }
    }

    /**
     * Builds the SparkSession. When run inside IntelliJ (spark.master unset via VM options
     * defaults to local[*]), this executes as a single-JVM local job. When submitted via
     * spark-submit with --master k8s://<API_SERVER>, the identical application code — with
     * zero changes — is scheduled as pods on AKS by the driver's Kubernetes scheduler backend.
     */
    private static SparkSession buildSparkSession() {
        return SparkSession.builder()
                .appName("retail-banking-fraud-aggregation")
                .config("spark.sql.shuffle.partitions", "8")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                // Kubernetes dynamic allocation: relies on shuffle-tracking (not an external
                // shuffle service DaemonSet) to safely scale executor pods down mid-job.
                .config("spark.dynamicAllocation.enabled", "true")
                .config("spark.dynamicAllocation.shuffleTracking.enabled", "true")
                .config("spark.dynamicAllocation.minExecutors", "1")
                .config("spark.dynamicAllocation.maxExecutors", "6")
                .config("spark.kubernetes.allocation.batch.size", "3")
                .getOrCreate();
    }

    private static String resolveLocalOutputPath() {
        String os = System.getProperty("os.name").toLowerCase();
        String base = os.contains("win")
                ? System.getProperty("java.io.tmpdir") + "spark-aks-demo\\fraud-output"
                : "/tmp/spark-aks-demo/fraud-output";
        return base;
    }

    /**
     * Generates a realistic retail-banking transaction dataset entirely in-memory —
     * no external file or database dependency, so the demo runs immediately on click.
     */
    private static Dataset<Row> generateSyntheticTransactions(SparkSession spark, int numTxns, int numAccounts) {
        Random random = new Random(42L);
        String[] merchantCategories = {"GROCERY", "ELECTRONICS", "TRAVEL", "FUEL", "DINING", "UTILITIES", "JEWELRY"};
        String[] channels = {"POS", "ONLINE", "ATM", "WIRE"};
        String[] countryCodes = {"IN", "US", "GB", "AE", "SG"};

        List<String> accountIds = new ArrayList<>(numAccounts);
        for (int i = 0; i < numAccounts; i++) {
            accountIds.add(String.format("ACC-%06d", i));
        }

        long nowEpoch = Instant.now().getEpochSecond();
        long windowStartEpoch = Instant.now().minus(24, ChronoUnit.HOURS).getEpochSecond();

        List<BankTransaction> records = new ArrayList<>(numTxns);
        for (int i = 0; i < numTxns; i++) {
            String accountId = accountIds.get(random.nextInt(numAccounts));
            long eventTime = windowStartEpoch + (long) (random.nextDouble() * (nowEpoch - windowStartEpoch));

            // Inject a "velocity fraud" pattern into ~5% of accounts: burst of rapid,
            // high-value transactions clustered in a tight time window.
            boolean isBurstAccount = accountId.hashCode() % 20 == 0;
            double amount = isBurstAccount
                    ? 5_000 + random.nextDouble() * 30_000
                    : 5 + random.nextDouble() * 2_000;

            records.add(new BankTransaction(
                    "TXN-" + String.format("%08d", i),
                    accountId,
                    eventTime,
                    Math.round(amount * 100.0) / 100.0,
                    merchantCategories[random.nextInt(merchantCategories.length)],
                    channels[random.nextInt(channels.length)],
                    countryCodes[random.nextInt(countryCodes.length)]
            ));
        }

        StructType schema = new StructType(new StructField[]{
                new StructField("transactionId", DataTypes.StringType, false, Metadata.empty()),
                new StructField("accountId", DataTypes.StringType, false, Metadata.empty()),
                new StructField("eventEpochSeconds", DataTypes.LongType, false, Metadata.empty()),
                new StructField("amount", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("merchantCategory", DataTypes.StringType, false, Metadata.empty()),
                new StructField("channel", DataTypes.StringType, false, Metadata.empty()),
                new StructField("countryCode", DataTypes.StringType, false, Metadata.empty())
        });

        List<Row> rows = new ArrayList<>(records.size());
        for (BankTransaction t : records) {
            rows.add(RowFactory.create(
                    t.transactionId(), t.accountId(), t.eventEpochSeconds(),
                    t.amount(), t.merchantCategory(), t.channel(), t.countryCode()));
        }

        return spark.createDataFrame(rows, schema);
    }

    /**
     * Computes per-account velocity and monetary aggregation features used as
     * fraud-model inputs: transaction count, total amount, max single amount,
     * distinct-country fan-out (card-testing indicator), and a composite risk score.
     */
    private static Dataset<Row> computeAccountVelocityFeatures(Dataset<Row> transactions) {
        Dataset<Row> agg = transactions.groupBy(col("accountId"))
                .agg(
                        count("transactionId").alias("txnCount"),
                        sum("amount").alias("totalAmount"),
                        max("amount").alias("maxAmount"),
                        countDistinct("countryCode").alias("distinctCountries"),
                        countDistinct("channel").alias("distinctChannels")
                );

        return agg.withColumn("riskScore",
                least(
                        lit(1.0),
                        (col("txnCount").cast("double").divide(lit(HIGH_RISK_VELOCITY_THRESHOLD))
                                .multiply(lit(0.5)))
                                .plus(when(col("maxAmount").geq(lit(HIGH_RISK_AMOUNT_THRESHOLD)), lit(0.3)).otherwise(lit(0.0)))
                                .plus(when(col("distinctCountries").geq(lit(3)), lit(0.2)).otherwise(lit(0.0)))
                )
        );
    }
}
