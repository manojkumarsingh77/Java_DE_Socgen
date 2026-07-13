package com.frauddetection.generator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.frauddetection.config.PipelineConfig;
import com.frauddetection.model.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * FRAUD DETECTION PIPELINE - Transaction Data Generator
 *
 * ┌─────────────────────────────────────────────────────────────────────────┐
 * │  WHAT THIS TEACHES:                                                     │
 * │                                                                         │
 * │  1. LATE EVENT STRATEGY                                                 │
 * │     We generate 5% of events with timestamps 1-8 minutes in the past.  │
 * │     Spark's watermark (10 min) allows these to be processed correctly.  │
 * │     Events older than 10 min are DROPPED (watermark expired).           │
 * │                                                                         │
 * │  2. SOURCE FOR STRUCTURED STREAMING                                     │
 * │     We write JSON files to a directory.                                 │
 * │     Spark's file source is replayable = exactly-once semantics.        │
 * │     Each file = one batch of transactions.                              │
 * │                                                                         │
 * │  3. BACKPRESSURE SIMULATION                                             │
 * │     We periodically burst 3x normal volume to simulate peak load.      │
 * │     Spark's maxFilesPerTrigger limits how much we process per batch.   │
 * └─────────────────────────────────────────────────────────────────────────┘
 *
 * DEMO NOTE: In production, this would be replaced by:
 *   - Apache Kafka source (kafka:// connector)
 *   - Amazon Kinesis source
 *   - Event Hub source
 * The pipeline logic remains IDENTICAL — only the source changes.
 */
public class TransactionGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionGenerator.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Random RANDOM = new Random(42); // Seeded for reproducibility

    private final String outputDirectory;
    private final ScheduledExecutorService scheduler;
    private final AtomicLong totalGenerated = new AtomicLong(0);
    private final AtomicLong lateEventsGenerated = new AtomicLong(0);
    private final AtomicLong fraudEventsGenerated = new AtomicLong(0);
    private volatile boolean running = false;

    // Simulated customer pool (realistic: bank has finite customers)
    private static final String[] CUSTOMER_IDS = generateCustomerIds(200);
    private static final String[] CITIES = {
            "New York", "London", "Singapore", "Mumbai", "Lagos",
            "Bucharest", "Sydney", "Tokyo", "Dubai", "Frankfurt"
    };

    public TransactionGenerator(String outputDirectory) {
        this.outputDirectory = outputDirectory;
        this.scheduler = Executors.newScheduledThreadPool(2);
    }

    /**
     * Start generating transactions.
     * Writes JSON files every second — Spark reads them as a streaming source.
     */
    public void start() {
        running = true;
        try {
            Files.createDirectories(Paths.get(outputDirectory));
        } catch (IOException e) {
            throw new RuntimeException("Failed to create output directory: " + outputDirectory, e);
        }

        LOG.info("╔══════════════════════════════════════════════════════════════╗");
        LOG.info("║         TRANSACTION GENERATOR STARTED                       ║");
        LOG.info("║  Rate: {} txn/sec | Fraud Rate: {}% | Late: {}%             ║",
                PipelineConfig.TRANSACTIONS_PER_SECOND,
                (int)(PipelineConfig.FRAUD_RATE * 100),
                (int)(PipelineConfig.LATE_EVENT_RATE * 100));
        LOG.info("║  Output: {}                                                  ║", outputDirectory);
        LOG.info("╚══════════════════════════════════════════════════════════════╝");

        // ─── Normal transaction batch every 1 second ───
        scheduler.scheduleAtFixedRate(
                this::generateNormalBatch,
                0, 1, TimeUnit.SECONDS
        );

        // ─── Burst simulation every 15 seconds (backpressure demo) ───
        scheduler.scheduleAtFixedRate(
                this::generateBurstBatch,
                10, 15, TimeUnit.SECONDS
        );

        // ─── Late event injection every 5 seconds ───
        scheduler.scheduleAtFixedRate(
                this::generateLateEvents,
                3, 5, TimeUnit.SECONDS
        );

        // ─── Stats reporting every 10 seconds ───
        scheduler.scheduleAtFixedRate(
                this::reportStats,
                5, 10, TimeUnit.SECONDS
        );
    }

    public void stop() {
        running = false;
        scheduler.shutdown();
        try {
            scheduler.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        LOG.info("Transaction Generator stopped. Total generated: {}", totalGenerated.get());
    }

    // ─────────────────────────────────────────────────────────────
    //  NORMAL BATCH: Steady-state transaction stream
    // ─────────────────────────────────────────────────────────────
    private void generateNormalBatch() {
        if (!running) return;
        int count = PipelineConfig.TRANSACTIONS_PER_SECOND;
        writeTransactionFile("batch", generateTransactions(count, false));
    }

    // ─────────────────────────────────────────────────────────────
    //  BURST BATCH: 3x load spike (demonstrates backpressure)
    //  Spark UI will show increased processing time for this batch
    // ─────────────────────────────────────────────────────────────
    private void generateBurstBatch() {
        if (!running) return;
        int count = PipelineConfig.TRANSACTIONS_PER_SECOND * 3;
        LOG.info("🌊 BURST EVENT: Generating {} transactions (3x normal load — watch Spark UI!)", count);
        writeTransactionFile("burst", generateTransactions(count, false));
    }

    // ─────────────────────────────────────────────────────────────
    //  LATE EVENTS: Arrive after delay (demonstrates watermark)
    //  These have event timestamps 1-8 minutes in the past
    //  Spark's 10-min watermark will ACCEPT these
    //  Events older than 10 min would be DROPPED (expired watermark)
    // ─────────────────────────────────────────────────────────────
    private void generateLateEvents() {
        if (!running) return;
        int count = 5 + RANDOM.nextInt(10);
        Transaction[] lateTransactions = generateTransactions(count, true);
        writeTransactionFile("late", lateTransactions);
        lateEventsGenerated.addAndGet(count);
        LOG.info("⏰ LATE EVENTS: Injected {} late transactions (timestamps {}-{} min ago)",
                count, 1, PipelineConfig.MAX_LATE_DELAY_MINUTES);
    }

    // ─────────────────────────────────────────────────────────────
    //  CORE TRANSACTION GENERATION
    // ─────────────────────────────────────────────────────────────
    private Transaction[] generateTransactions(int count, boolean makeLate) {
        Transaction[] transactions = new Transaction[count];
        long now = System.currentTimeMillis();

        for (int i = 0; i < count; i++) {
            boolean isFraud = RANDOM.nextDouble() < PipelineConfig.FRAUD_RATE;

            // Compute event timestamp
            long eventTimestamp;
            boolean isLate = makeLate || (!isFraud && RANDOM.nextDouble() < PipelineConfig.LATE_EVENT_RATE);

            if (isLate) {
                // Late events: 1 to MAX_LATE_DELAY_MINUTES minutes in the past
                int minutesLate = 1 + RANDOM.nextInt(PipelineConfig.MAX_LATE_DELAY_MINUTES);
                eventTimestamp = now - (minutesLate * 60 * 1000L);
            } else {
                // Normal events: within the last 2 seconds
                eventTimestamp = now - (RANDOM.nextInt(2000));
            }

            String customerId = CUSTOMER_IDS[RANDOM.nextInt(CUSTOMER_IDS.length)];
            Transaction txn;

            if (isFraud) {
                txn = generateFraudTransaction(customerId, eventTimestamp);
                fraudEventsGenerated.incrementAndGet();
            } else {
                txn = generateLegitTransaction(customerId, eventTimestamp);
            }

            txn.setLateEvent(isLate);
            transactions[i] = txn;
        }

        totalGenerated.addAndGet(count);
        return transactions;
    }

    /**
     * Generate a FRAUDULENT transaction.
     * Mimics real fraud patterns:
     *   - High-value amounts
     *   - High-risk merchant categories
     *   - High-risk countries
     *   - Rapid successive transactions (velocity fraud — detected by window aggregation)
     */
    private Transaction generateFraudTransaction(String customerId, long eventTimestamp) {
        int fraudType = RANDOM.nextInt(4);
        String txnId = "TXN-FRAUD-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        String cardNum = "****" + (1000 + RANDOM.nextInt(9000));

        return switch (fraudType) {
            case 0 -> {
                // HIGH VALUE FRAUD: Large transfer to crypto exchange
                double amount = 5000 + RANDOM.nextDouble() * 15000;
                String merchant = PipelineConfig.HIGH_RISK_MERCHANTS[RANDOM.nextInt(PipelineConfig.HIGH_RISK_MERCHANTS.length)];
                String country  = PipelineConfig.HIGH_RISK_COUNTRIES[RANDOM.nextInt(PipelineConfig.HIGH_RISK_COUNTRIES.length)];
                yield new Transaction(txnId, customerId, cardNum, amount, merchant, country,
                        CITIES[RANDOM.nextInt(CITIES.length)], eventTimestamp, "ONLINE");
            }
            case 1 -> {
                // GEO ANOMALY: Customer normally in US, now in high-risk country
                double amount = 500 + RANDOM.nextDouble() * 3000;
                String country  = PipelineConfig.HIGH_RISK_COUNTRIES[RANDOM.nextInt(PipelineConfig.HIGH_RISK_COUNTRIES.length)];
                yield new Transaction(txnId, customerId, cardNum, amount, "ATM_WITHDRAWAL", country,
                        CITIES[RANDOM.nextInt(CITIES.length)], eventTimestamp, "ATM");
            }
            case 2 -> {
                // VELOCITY FRAUD: Many small transactions in rapid succession
                // These cluster for the same customer — window aggregation catches it
                double amount = 50 + RANDOM.nextDouble() * 200;
                yield new Transaction(txnId, customerId, cardNum, amount, "RETAIL",
                        "US", "New York", eventTimestamp, "POS");
            }
            default -> {
                // CARD TESTING: Small amounts to test stolen cards
                double amount = 1 + RANDOM.nextDouble() * 10;
                String merchant = PipelineConfig.HIGH_RISK_MERCHANTS[RANDOM.nextInt(PipelineConfig.HIGH_RISK_MERCHANTS.length)];
                yield new Transaction(txnId, customerId, cardNum, amount, merchant,
                        "US", "New York", eventTimestamp, "ONLINE");
            }
        };
    }

    /**
     * Generate a LEGITIMATE transaction.
     * Normal customer behaviour: grocery, gas, restaurant, etc.
     */
    private Transaction generateLegitTransaction(String customerId, long eventTimestamp) {
        String txnId = "TXN-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        String cardNum = "****" + (1000 + RANDOM.nextInt(9000));
        double amount = 5 + RANDOM.nextDouble() * 500;
        String merchant = PipelineConfig.NORMAL_MERCHANTS[RANDOM.nextInt(PipelineConfig.NORMAL_MERCHANTS.length)];
        String country  = PipelineConfig.NORMAL_COUNTRIES[RANDOM.nextInt(PipelineConfig.NORMAL_COUNTRIES.length)];
        String channel  = new String[]{"POS", "ONLINE", "ATM", "MOBILE"}[RANDOM.nextInt(4)];

        return new Transaction(txnId, customerId, cardNum, amount, merchant, country,
                CITIES[RANDOM.nextInt(CITIES.length)], eventTimestamp, channel);
    }

    // ─────────────────────────────────────────────────────────────
    //  FILE WRITER: Atomic write to avoid partial-read by Spark
    //  Uses temp file + rename pattern (atomicity guarantee)
    // ─────────────────────────────────────────────────────────────
    private void writeTransactionFile(String prefix, Transaction[] transactions) {
        if (!running || transactions.length == 0) return;

        String timestamp = String.valueOf(System.currentTimeMillis());
        String tempFileName   = outputDirectory + "/." + prefix + "-" + timestamp + ".json.tmp";
        String finalFileName  = outputDirectory + "/" + prefix + "-" + timestamp + ".json";

        try (PrintWriter writer = new PrintWriter(new FileWriter(tempFileName))) {
            for (Transaction txn : transactions) {
                writer.println(MAPPER.writeValueAsString(txn));
            }
        } catch (IOException e) {
            LOG.error("Failed to write transaction file", e);
            return;
        }

        // Atomic rename: Spark only reads completed files
        File tempFile  = new File(tempFileName);
        File finalFile = new File(finalFileName);
        if (!tempFile.renameTo(finalFile)) {
            LOG.warn("Could not rename temp file to final: {}", finalFileName);
        }
    }

    private void reportStats() {
        LOG.info("📊 GENERATOR STATS: Total={} | Fraud={} | LateEvents={}",
                totalGenerated.get(), fraudEventsGenerated.get(), lateEventsGenerated.get());
    }

    private static String[] generateCustomerIds(int count) {
        String[] ids = new String[count];
        for (int i = 0; i < count; i++) {
            ids[i] = String.format("CUST-%05d", i + 1);
        }
        return ids;
    }

    public long getTotalGenerated() { return totalGenerated.get(); }
    public long getLateEventsGenerated() { return lateEventsGenerated.get(); }
    public long getFraudEventsGenerated() { return fraudEventsGenerated.get(); }
}
