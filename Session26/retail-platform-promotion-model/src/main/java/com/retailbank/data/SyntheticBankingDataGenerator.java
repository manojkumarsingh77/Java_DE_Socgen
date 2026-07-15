package com.retailbank.data;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.sql.Date;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Generates a deterministic, realistic-looking retail banking transaction
 * dataset so the demo runs immediately with no external data source,
 * regardless of which environment it is executed against. Row count scales
 * with environment so dev stays fast and test/prod exercise real
 * partitioning behaviour.
 */
public final class SyntheticBankingDataGenerator {

    private static final long SEED = 42L;

    private static final String[] MERCHANT_CATEGORIES = {
            "GROCERY", "FUEL", "ELECTRONICS", "DINING", "TRAVEL", "UTILITIES", "PHARMACY", "APPAREL"
    };
    private static final String[] CHANNELS = {"POS", "ONLINE", "ATM", "MOBILE_WALLET"};
    private static final String[] REGIONS = {"NORTH", "SOUTH", "EAST", "WEST", "CENTRAL"};
    private static final String[] LOYALTY_TIERS = {"STANDARD", "SILVER", "GOLD", "PLATINUM"};
    private static final String[] CUSTOMER_SEGMENTS = {"MASS_MARKET", "AFFLUENT", "PRIVATE_BANKING", "STUDENT"};

    private SyntheticBankingDataGenerator() {
    }

    public static StructType schema() {
        return new StructType(new StructField[]{
                new StructField("transactionId", DataTypes.StringType, false, Metadata.empty()),
                new StructField("customerId", DataTypes.StringType, false, Metadata.empty()),
                new StructField("accountId", DataTypes.StringType, false, Metadata.empty()),
                new StructField("transactionDate", DataTypes.DateType, false, Metadata.empty()),
                new StructField("transactionAmount", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("merchantCategory", DataTypes.StringType, false, Metadata.empty()),
                new StructField("channel", DataTypes.StringType, false, Metadata.empty()),
                new StructField("region", DataTypes.StringType, false, Metadata.empty()),
                new StructField("loyaltyTier", DataTypes.StringType, false, Metadata.empty()),
                new StructField("customerSegment", DataTypes.StringType, false, Metadata.empty()),
                new StructField("previousPromotionResponse", DataTypes.BooleanType, false, Metadata.empty()),
                new StructField("riskScore", DataTypes.IntegerType, false, Metadata.empty())
        });
    }

    /**
     * @param spark      active session
     * @param rowCount   number of synthetic transactions to generate
     * @param numAccounts number of distinct customer accounts to spread transactions across
     */
    public static Dataset<Row> generate(SparkSession spark, int rowCount, int numAccounts) {
        Random random = new Random(SEED);
        List<Row> rows = new ArrayList<>(rowCount);
        LocalDate baseDate = LocalDate.of(2026, 1, 1);

        for (int i = 0; i < rowCount; i++) {
            int accountIdx = random.nextInt(numAccounts);
            String customerId = "CUST-" + String.format("%06d", accountIdx);
            String accountId = "ACCT-" + String.format("%06d", accountIdx);
            String transactionId = "TXN-" + String.format("%09d", i);

            LocalDate txnDate = baseDate.plusDays(random.nextInt(180));
            double amount = round2(5.0 + random.nextDouble() * 9995.0);

            String category = MERCHANT_CATEGORIES[random.nextInt(MERCHANT_CATEGORIES.length)];
            String channel = CHANNELS[random.nextInt(CHANNELS.length)];
            String region = REGIONS[random.nextInt(REGIONS.length)];
            String loyaltyTier = LOYALTY_TIERS[weightedTierIndex(random)];
            String segment = CUSTOMER_SEGMENTS[random.nextInt(CUSTOMER_SEGMENTS.length)];
            boolean previousResponse = random.nextDouble() < 0.30;
            int riskScore = 1 + random.nextInt(100);

            rows.add(RowFactory.create(
                    transactionId,
                    customerId,
                    accountId,
                    Date.valueOf(txnDate),
                    amount,
                    category,
                    channel,
                    region,
                    loyaltyTier,
                    segment,
                    previousResponse,
                    riskScore
            ));
        }

        return spark.createDataFrame(rows, schema());
    }

    /** Skews the distribution so STANDARD/SILVER customers are the majority, GOLD/PLATINUM rarer. */
    private static int weightedTierIndex(Random random) {
        double r = random.nextDouble();
        if (r < 0.50) return 0; // STANDARD
        if (r < 0.80) return 1; // SILVER
        if (r < 0.95) return 2; // GOLD
        return 3;               // PLATINUM
    }

    private static double round2(double value) {
        return Math.round(value * 100.0) / 100.0;
    }
}
