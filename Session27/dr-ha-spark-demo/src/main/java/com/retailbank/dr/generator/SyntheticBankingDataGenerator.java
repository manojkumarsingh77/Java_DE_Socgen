package com.retailbank.dr.generator;

import com.retailbank.dr.config.AppConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * Generates a high-fidelity synthetic retail-banking transaction stream, one
 * micro-batch at a time, so the whole DR demo is self-contained and runs
 * immediately without any external data source or network dependency.
 *
 * The generator is deterministic per batch (seeded), which matters for a DR
 * drill: we need to be able to prove exactly *which* transactionIds were lost
 * during the simulated outage window.
 */
public final class SyntheticBankingDataGenerator {

    private static final String[] TXN_TYPES =
            {"DEBIT", "CREDIT", "TRANSFER", "ATM_WITHDRAWAL", "POS_PAYMENT"};
    private static final String[] CURRENCIES = {"USD", "USD", "USD", "EUR", "GBP"};
    private static final String[] BRANCHES =
            {"BR-NYC-001", "BR-CHI-014", "BR-SFO-027", "BR-DAL-009", "BR-ATL-033"};

    public static final StructType SCHEMA = new StructType(new StructField[]{
            field("transactionId", DataTypes.StringType),
            field("accountId", DataTypes.StringType),
            field("customerId", DataTypes.StringType),
            field("branchId", DataTypes.StringType),
            field("region", DataTypes.StringType),
            field("transactionType", DataTypes.StringType),
            field("amount", DataTypes.DoubleType),
            field("currency", DataTypes.StringType),
            field("status", DataTypes.StringType),
            field("eventTimestamp", DataTypes.TimestampType),
            field("ingestTimestamp", DataTypes.TimestampType),
            field("batchId", DataTypes.IntegerType)
    });

    private final AppConfig config;
    private final SparkSession spark;

    public SyntheticBankingDataGenerator(AppConfig config, SparkSession spark) {
        this.config = config;
        this.spark = spark;
    }

    /**
     * Produces exactly one synthetic micro-batch of banking transactions, tagged
     * with the origin region and a real ingestTimestamp captured at generation
     * time (this becomes the PRIMARY commit clock used later for RPO math).
     */
    public Dataset<Row> generateBatch(int batchId) {
        // Seed derived from batchId => reproducible, independent batches (no shared RNG state
        // across executors/partitions if this were parallelized).
        Random rnd = new Random(config.randomSeed() + batchId);
        List<Row> rows = new ArrayList<>(config.recordsPerBatch());

        Instant ingestTs = Instant.now();

        for (int i = 0; i < config.recordsPerBatch(); i++) {
            String txnId = "TXN-" + batchId + "-" + UUID.randomUUID();
            String accountId = "ACC-" + String.format("%08d", rnd.nextInt(2_000_000));
            String customerId = "CUST-" + String.format("%07d", rnd.nextInt(500_000));
            String branch = BRANCHES[rnd.nextInt(BRANCHES.length)];
            String type = TXN_TYPES[rnd.nextInt(TXN_TYPES.length)];
            String currency = CURRENCIES[rnd.nextInt(CURRENCIES.length)];
            double amount = round2(5 + rnd.nextDouble() * 9_995);
            String status = rnd.nextDouble() < 0.02 ? "PENDING" : "POSTED";

            // Event time slightly precedes ingest time (transport/processing latency)
            Instant eventTs = ingestTs.minusMillis(rnd.nextInt(400));

            rows.add(RowFactory.create(
                    txnId,
                    accountId,
                    customerId,
                    branch,
                    config.primaryRegionName(),
                    type,
                    amount,
                    currency,
                    status,
                    Timestamp.from(eventTs),
                    Timestamp.from(ingestTs),
                    batchId
            ));
        }

        return spark.createDataFrame(rows, SCHEMA);
    }

    private static double round2(double v) {
        return Math.round(v * 100.0) / 100.0;
    }

    private static StructField field(String name, org.apache.spark.sql.types.DataType type) {
        return new StructField(name, type, false, Metadata.empty());
    }
}
