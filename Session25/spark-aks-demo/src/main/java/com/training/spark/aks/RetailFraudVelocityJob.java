package com.training.spark.aks;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * RetailFraudVelocityJob
 *
 * Enterprise-grade transaction velocity fraud detection job designed to run seamlessly
 * across local development environments (IntelliJ) and Azure Kubernetes Service (AKS).
 */
public final class RetailFraudVelocityJob {

    private static final Logger LOG = LoggerFactory.getLogger(RetailFraudVelocityJob.class);

    private static final int VELOCITY_COUNT_THRESHOLD = 5;
    private static final double VELOCITY_AMOUNT_THRESHOLD = 10_000.00;

    private static final String[] MERCHANT_CATEGORIES = {
            "GROCERY", "FUEL", "ELECTRONICS", "TRAVEL", "RESTAURANT",
            "ATM_WITHDRAWAL", "ONLINE_RETAIL", "UTILITIES", "PHARMACY", "JEWELRY"
    };
    private static final String[] CHANNELS = {"POS", "ONLINE", "ATM", "MOBILE_APP"};
    private static final String[] STATE_CODES = {
            "NY", "CA", "TX", "FL", "IL", "WA", "MA", "NJ", "GA", "OH"
    };

    public static void main(String[] args) {
        // Determine a safe default path if no arguments are provided.
        // Avoids absolute root paths on macOS/Windows local environments.
        String defaultPath = "/opt/spark-apps/output";
        String os = System.getProperty("os.name").toLowerCase();
        if (os.contains("mac") || os.contains("win")) {
            defaultPath = System.getProperty("user.dir") + File.separator + "target" + File.separator + "output";
        }

        String outputBasePath = args.length > 0 ? args[0] : defaultPath;
        int recordCount = args.length > 1 ? Integer.parseInt(args[1]) : 50_000;

        try (SparkSession spark = SparkSession.builder()
                .appName("RetailFraudVelocityJob")
                .getOrCreate()) {

            LOG.info("Spark version: {}", spark.version());
            LOG.info("Output Destination: {}", outputBasePath);
            LOG.info("Generating {} synthetic transactions...", recordCount);

            Dataset<Row> transactions = generateSyntheticTransactions(spark, recordCount);
            transactions.cache();

            long totalTxns = transactions.count();
            long distinctAccounts = transactions.select("account_id").distinct().count();
            LOG.info("Generated {} transactions across {} accounts", totalTxns, distinctAccounts);

            transactions.createOrReplaceTempView("transactions");

            Dataset<Row> velocityFlags = spark.sql(
                    "SELECT account_id, "
                            + "       window(event_ts, '5 minutes') AS txn_window, "
                            + "       COUNT(*) AS txn_count, "
                            + "       ROUND(SUM(amount_usd), 2) AS total_amount, "
                            + "       ROUND(AVG(amount_usd), 2) AS avg_amount, "
                            + "       COLLECT_SET(merchant_category) AS merchant_categories "
                            + "FROM transactions "
                            + "GROUP BY account_id, window(event_ts, '5 minutes') "
                            + "HAVING COUNT(*) >= " + VELOCITY_COUNT_THRESHOLD
                            + "    OR SUM(amount_usd) >= " + VELOCITY_AMOUNT_THRESHOLD + " "
                            + "ORDER BY txn_count DESC, total_amount DESC"
            );

            velocityFlags.cache();
            long flaggedCount = velocityFlags.count();
            LOG.info("Flagged {} account/window combinations for review", flaggedCount);

            System.out.println("=== TOP FLAGGED ACCOUNT WINDOWS (velocity or spend threshold breach) ===");
            velocityFlags.show(20, false);

            System.out.println("=== SUMMARY BY MERCHANT CATEGORY ===");
            transactions.groupBy("merchant_category")
                    .count()
                    .orderBy(org.apache.spark.sql.functions.col("count").desc())
                    .show(false);

            // Ensure output structure writes safely across storage systems
            transactions.write()
                    .mode(SaveMode.Overwrite)
                    .partitionBy("state_code")
                    .parquet(outputBasePath + "/raw-transactions");

            velocityFlags.coalesce(1)
                    .write()
                    .mode(SaveMode.Overwrite)
                    .json(outputBasePath + "/velocity-flags");

            LOG.info("Wrote raw transactions to {}/raw-transactions", outputBasePath);
            LOG.info("Wrote velocity flags to {}/velocity-flags", outputBasePath);
            LOG.info("Job complete.");
        } catch (Exception e) {
            LOG.error("Fatal error during job execution pipeline", e);
            System.exit(1);
        }
    }

    private static Dataset<Row> generateSyntheticTransactions(SparkSession spark, int recordCount) {
        StructType schema = new StructType(new StructField[]{
                new StructField("transaction_id", DataTypes.StringType, false, Metadata.empty()),
                new StructField("account_id", DataTypes.StringType, false, Metadata.empty()),
                new StructField("amount_usd", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("merchant_category", DataTypes.StringType, false, Metadata.empty()),
                new StructField("channel", DataTypes.StringType, false, Metadata.empty()),
                new StructField("event_ts", DataTypes.TimestampType, false, Metadata.empty()),
                new StructField("state_code", DataTypes.StringType, false, Metadata.empty())
        });

        Random random = new Random(42L);
        long nowEpochSeconds = Instant.now().getEpochSecond();

        int accountPoolSize = Math.max(200, recordCount / 100);
        int burstyAccountCount = Math.max(5, accountPoolSize / 40);

        List<Row> rows = new ArrayList<>(recordCount);

        for (int i = 0; i < recordCount; i++) {
            boolean isBurstyAccount = i % accountPoolSize < burstyAccountCount;
            String accountId = String.format("ACC-%06d", i % accountPoolSize);

            double amount;
            long eventTimeEpoch;

            if (isBurstyAccount && random.nextDouble() < 0.6) {
                amount = 200 + random.nextDouble() * 2500;
                eventTimeEpoch = nowEpochSeconds - random.nextInt(240);
            } else {
                amount = 5 + random.nextDouble() * 450;
                eventTimeEpoch = nowEpochSeconds - random.nextInt(24 * 60 * 60);
            }

            String merchantCategory = MERCHANT_CATEGORIES[random.nextInt(MERCHANT_CATEGORIES.length)];
            String channel = CHANNELS[random.nextInt(CHANNELS.length)];
            String stateCode = STATE_CODES[random.nextInt(STATE_CODES.length)];

            rows.add(RowFactory.create(
                    "TXN-" + String.format("%010d", i),
                    accountId,
                    Math.round(amount * 100.0) / 100.0,
                    merchantCategory,
                    channel,
                    Timestamp.from(Instant.ofEpochSecond(eventTimeEpoch)),
                    stateCode
            ));
        }

        return spark.createDataFrame(rows, schema);
    }

    private RetailFraudVelocityJob() {
        // Entry-point utility class
    }
}