package com.npunext.bank.fraud.generator;

import com.npunext.bank.fraud.model.Transaction;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Generates a deterministic (seeded) synthetic retail-banking transaction
 * dataset so the demo is fully self-contained — no external files, no
 * network calls, no sample-data downloads required.
 *
 * ~2% of records are deliberately injected as high-risk anomalies
 * (large foreign-currency spend at unusual local hours) so the fraud
 * pre-processing stage in {@code FraudPreProcessorJob} has real signal
 * to detect.
 */
public final class SyntheticDataGenerator {

    private static final String[] MERCHANT_CATEGORIES = {
            "GROCERY", "ELECTRONICS", "TRAVEL", "FUEL", "DINING",
            "ATM_WITHDRAWAL", "ONLINE_RETAIL", "UTILITIES", "JEWELRY", "PHARMACY"
    };

    private static final String[] CHANNELS = {"POS", "ATM", "ONLINE", "MOBILE_APP", "WIRE"};

    private static final String[][] CITY_COUNTRY = {
            {"Bengaluru", "IN"}, {"Mumbai", "IN"}, {"Delhi", "IN"}, {"Chennai", "IN"},
            {"Singapore", "SG"}, {"Dubai", "AE"}, {"London", "GB"}, {"New York", "US"},
            {"Frankfurt", "DE"}, {"Sydney", "AU"}
    };

    private final Random random;
    private final int recordCount;
    private final double anomalyRate;

    public SyntheticDataGenerator(long seed, int recordCount, double anomalyRate) {
        this.random = new Random(seed);
        this.recordCount = recordCount;
        this.anomalyRate = anomalyRate;
    }

    /**
     * Produces the full in-memory synthetic dataset.
     */
    public List<Transaction> generate() {
        List<Transaction> transactions = new ArrayList<>(recordCount);
        int accountPoolSize = Math.max(500, recordCount / 40);

        for (int i = 0; i < recordCount; i++) {
            boolean injectAnomaly = random.nextDouble() < anomalyRate;
            transactions.add(buildTransaction(i, accountPoolSize, injectAnomaly));
        }
        return transactions;
    }

    private Transaction buildTransaction(int index, int accountPoolSize, boolean anomaly) {
        String transactionId = "TXN-" + String.format("%09d", index);
        int accountBucket = random.nextInt(accountPoolSize);
        String accountId = "ACC-" + String.format("%06d", accountBucket);
        String customerId = "CUST-" + String.format("%06d", accountBucket);

        String[] cityCountry = CITY_COUNTRY[random.nextInt(CITY_COUNTRY.length)];
        String city = cityCountry[0];
        String country = cityCountry[1];

        String merchantCategory = MERCHANT_CATEGORIES[random.nextInt(MERCHANT_CATEGORIES.length)];
        String channel = CHANNELS[random.nextInt(CHANNELS.length)];
        String deviceId = "DEV-" + String.format("%05d", random.nextInt(20000));

        double amount;
        boolean foreignTransaction;
        long epochMillis;

        if (anomaly) {
            // High-risk pattern: large amount, foreign transaction, unusual local hour (00:00-04:59)
            amount = 5000 + random.nextDouble() * 45000;
            foreignTransaction = true;
            epochMillis = randomTimestampAtHour(random.nextInt(5)); // 0-4 AM
        } else {
            amount = 5 + random.nextDouble() * 2500;
            foreignTransaction = random.nextDouble() < 0.08; // 8% legitimate cross-border spend
            epochMillis = randomTimestampAtHour(5 + random.nextInt(19)); // 05:00-23:59
        }

        String currency = foreignTransaction ? "USD" : "INR";

        return new Transaction(
                transactionId, accountId, customerId, round2(amount), currency,
                merchantCategory, channel, city, country, epochMillis, foreignTransaction, deviceId
        );
    }

    private long randomTimestampAtHour(int hour) {
        int dayOffset = random.nextInt(30); // spread across a rolling 30-day window
        ZonedDateTime base = ZonedDateTime.now(ZoneOffset.UTC)
                .minusDays(dayOffset)
                .withHour(hour)
                .withMinute(random.nextInt(60))
                .withSecond(random.nextInt(60))
                .withNano(0);
        return Instant.from(base).toEpochMilli();
    }

    private double round2(double value) {
        return Math.round(value * 100.0) / 100.0;
    }
}
