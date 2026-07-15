package com.retailbank.dataplatform.data;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * Generates a realistic, self-contained retail-banking dataset so the demo runs
 * immediately without any external data feed. Produces two related datasets:
 *
 * <ul>
 *   <li>{@code cardTransactions} — channel-side transaction records (POS/ATM/MOBILE/ACH)</li>
 *   <li>{@code ledgerEntries} — the corresponding core-ledger postings, with a
 *       deliberately injected error rate (missing entries + amount mismatches)
 *       so the reconciliation pipeline has real discrepancies to find</li>
 * </ul>
 *
 * <p>Generation happens driver-side into a {@code List<Row>} then parallelized via
 * {@link SparkSession#createDataFrame(List, StructType)} — appropriate at the
 * record counts used by this demo (tens of thousands to low millions). For
 * genuinely large synthetic volumes you would instead generate via
 * {@code spark.range(n).mapPartitions(...)} to distribute the generation itself;
 * that variant is noted in the LAB_GUIDE.md "scaling this demo" section.</p>
 */
public final class SyntheticBankingDataGenerator {

    private static final String[] CHANNELS = {"POS", "ATM", "MOBILE", "ACH"};
    private static final String[] BRANCH_CODES = {
            "BLR-001", "BLR-002", "MUM-014", "DEL-007", "HYD-003", "PUN-009", "CHN-005"
    };
    private static final String CURRENCY = "INR";

    private final SparkSession spark;
    private final long recordCount;
    private final long seed;

    public SyntheticBankingDataGenerator(SparkSession spark, long recordCount, long seed) {
        this.spark = spark;
        this.recordCount = recordCount;
        this.seed = seed;
    }

    /** Result bundle: the two correlated DataFrames produced by one generation pass. */
    public record GeneratedDataset(Dataset<Row> cardTransactions, Dataset<Row> ledgerEntries) { }

    public GeneratedDataset generate() {
        Random random = new Random(seed);

        List<Row> transactionRows = new ArrayList<>((int) Math.min(recordCount, Integer.MAX_VALUE - 8));
        List<Row> ledgerRows = new ArrayList<>((int) Math.min(recordCount, Integer.MAX_VALUE - 8));

        Instant windowStart = Instant.now().minusSeconds(6L * 3600); // last 6 hours of activity

        for (long i = 0; i < recordCount; i++) {
            String transactionId = "TXN-" + UUID.randomUUID();
            String accountId = "ACC-" + String.format("%08d", random.nextInt(200_000));
            String branchCode = BRANCH_CODES[random.nextInt(BRANCH_CODES.length)];
            String channel = CHANNELS[random.nextInt(CHANNELS.length)];

            BigDecimal amount = BigDecimal.valueOf(5 + random.nextDouble() * 45_000)
                    .setScale(2, RoundingMode.HALF_UP);

            Timestamp txnTimestamp = Timestamp.from(
                    windowStart.plusSeconds(random.nextInt(6 * 3600)));

            transactionRows.add(RowFactory.create(
                    transactionId, accountId, branchCode, channel,
                    amount, CURRENCY, txnTimestamp));

            // ---- Inject realistic ledger discrepancies ----
            double errorRoll = random.nextDouble();
            if (errorRoll < 0.02) {
                // 2% of transactions: ledger posting never arrived (late batch / dropped event)
                continue;
            } else if (errorRoll < 0.05) {
                // next 3%: ledger amount mismatches the channel amount (rounding / FX / fee bug)
                BigDecimal mismatchAmount = amount
                        .add(BigDecimal.valueOf((random.nextBoolean() ? 1 : -1) * (1 + random.nextInt(500)) / 100.0))
                        .setScale(2, RoundingMode.HALF_UP);
                ledgerRows.add(RowFactory.create(
                        transactionId, accountId, mismatchAmount, CURRENCY,
                        Timestamp.from(txnTimestamp.toInstant().plusSeconds(random.nextInt(120)))));
            } else {
                // 95%: clean, matching ledger posting
                ledgerRows.add(RowFactory.create(
                        transactionId, accountId, amount, CURRENCY,
                        Timestamp.from(txnTimestamp.toInstant().plusSeconds(random.nextInt(120)))));
            }
        }

        StructType transactionSchema = new StructType(new StructField[]{
                new StructField("transactionId", DataTypes.StringType, false, Metadata.empty()),
                new StructField("accountId", DataTypes.StringType, false, Metadata.empty()),
                new StructField("branchCode", DataTypes.StringType, false, Metadata.empty()),
                new StructField("channel", DataTypes.StringType, false, Metadata.empty()),
                new StructField("amount", DataTypes.createDecimalType(18, 2), false, Metadata.empty()),
                new StructField("currency", DataTypes.StringType, false, Metadata.empty()),
                new StructField("transactionTimestamp", DataTypes.TimestampType, false, Metadata.empty())
        });

        StructType ledgerSchema = new StructType(new StructField[]{
                new StructField("transactionId", DataTypes.StringType, false, Metadata.empty()),
                new StructField("accountId", DataTypes.StringType, false, Metadata.empty()),
                new StructField("postedAmount", DataTypes.createDecimalType(18, 2), false, Metadata.empty()),
                new StructField("currency", DataTypes.StringType, false, Metadata.empty()),
                new StructField("postedTimestamp", DataTypes.TimestampType, false, Metadata.empty())
        });

        Dataset<Row> cardTransactions = spark.createDataFrame(transactionRows, transactionSchema);
        Dataset<Row> ledgerEntries = spark.createDataFrame(ledgerRows, ledgerSchema);

        return new GeneratedDataset(cardTransactions, ledgerEntries);
    }
}
