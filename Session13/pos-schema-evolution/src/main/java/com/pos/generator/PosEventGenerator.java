package com.pos.generator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pos.config.PipelineConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * POS EVENT GENERATOR
 *
 * Simulates a realistic POS terminal estate where:
 * ┌─────────────────────────────────────────────────────────────────────┐
 * │  10% — V1 events (legacy terminals not yet upgraded)               │
 * │  70% — V2 events (current standard firmware)                       │
 * │  10% — V3 events (canary terminals testing new firmware)           │
 * │   5% — CORRUPT events (missing required fields, bad values)        │
 * │   5% — INVALID events (valid JSON, fails business rules)           │
 * └─────────────────────────────────────────────────────────────────────┘
 *
 * TEACHING NOTE on SCHEMA EVOLUTION:
 * This mix is intentional. In a real bank or retailer:
 *  - You can NEVER force all terminals to upgrade simultaneously
 *  - Some stores run on 10-year-old hardware
 *  - You deploy new firmware region-by-region (canary deployment)
 *  - Your pipeline MUST handle ALL versions at once
 *  - This is WHY backward compatibility is non-negotiable
 */
public class PosEventGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(PosEventGenerator.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Random RNG = new Random(42);

    private static final String[] TERMINALS = {
            "TERM0001","TERM0002","TERM0003","TERM0004","TERM0005",
            "TERM0006","TERM0007","TERM0008","TERM0009","TERM0010"
    };
    private static final String[] REGIONS   = {"NORTH","SOUTH","EAST","WEST","CENTRAL"};
    private static final String[] PAYMENTS  = {"CASH","CARD","CONTACTLESS","MOBILE_PAY"};
    private static final String[] CASHIERS  = {"EMP001","EMP002","EMP003","EMP004","EMP005"};

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private final AtomicLong v1Count  = new AtomicLong();
    private final AtomicLong v2Count  = new AtomicLong();
    private final AtomicLong v3Count  = new AtomicLong();
    private final AtomicLong badCount = new AtomicLong();
    private volatile boolean running  = false;

    public void start() {
        running = true;
        try { Files.createDirectories(Paths.get(PipelineConfig.SOURCE_DIR)); }
        catch (IOException e) { throw new RuntimeException(e); }

        // Normal batch every 2 seconds
        scheduler.scheduleAtFixedRate(this::writeBatch,
                0, PipelineConfig.BATCH_INTERVAL_SEC, TimeUnit.SECONDS);

        // Stats every 10 seconds
        scheduler.scheduleAtFixedRate(this::logStats, 8, 10, TimeUnit.SECONDS);

        LOG.info("╔══════════════════════════════════════════════════════════╗");
        LOG.info("║  POS EVENT GENERATOR STARTED                            ║");
        LOG.info("║  Mix: 10% v1 | 70% v2 | 10% v3 | 10% invalid/corrupt  ║");
        LOG.info("╚══════════════════════════════════════════════════════════╝");
    }

    public void stop() {
        running = false;
        scheduler.shutdown();
        try { scheduler.awaitTermination(3, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    private void writeBatch() {
        if (!running) return;
        List<Map<String, Object>> events = new ArrayList<>();
        int n = PipelineConfig.EVENTS_PER_BATCH;

        for (int i = 0; i < n; i++) {
            int roll = RNG.nextInt(100);
            Map<String, Object> event;
            if      (roll < 10)  { event = makeV1();      v1Count.incrementAndGet(); }
            else if (roll < 80)  { event = makeV2();      v2Count.incrementAndGet(); }
            else if (roll < 90)  { event = makeV3();      v3Count.incrementAndGet(); }
            else if (roll < 95)  { event = makeCorrupt(); badCount.incrementAndGet(); }
            else                 { event = makeInvalid(); badCount.incrementAndGet(); }
            events.add(event);
        }

        writeFile(events);
    }

    // ── V1: legacy terminals — original 6-field schema ─────────────────────
    private Map<String, Object> makeV1() {
        var m = new LinkedHashMap<String, Object>();
        m.put("transactionId",  uuid());
        m.put("terminalId",     terminal());
        m.put("amount",         amount(1, 500));
        m.put("eventTimestamp", Instant.now().toEpochMilli());
        m.put("itemCount",      RNG.nextInt(20) + 1);
        m.put("cashierId",      cashier());
        // NO loyaltyCardId, paymentMethod, taxAmount, storeRegion, receiptEmail, selfCheckout
        return m;
    }

    // ── V2: current standard — 10-field schema ──────────────────────────────
    private Map<String, Object> makeV2() {
        double amt = amount(1, 2000);
        double tax = Math.round(amt * 0.08 * 100.0) / 100.0;
        var m = new LinkedHashMap<String, Object>();
        m.put("transactionId",  uuid());
        m.put("terminalId",     terminal());
        m.put("amount",         amt);
        m.put("eventTimestamp", Instant.now().toEpochMilli());
        m.put("itemCount",      RNG.nextInt(50) + 1);
        m.put("cashierId",      cashier());
        // v2 optional fields (present in most, absent in some — tests nullable handling)
        if (RNG.nextBoolean()) m.put("loyaltyCardId", "LOYAL-" + RNG.nextInt(99999));
        m.put("paymentMethod", PAYMENTS[RNG.nextInt(PAYMENTS.length)]);
        m.put("taxAmount",     tax);
        m.put("storeRegion",   REGIONS[RNG.nextInt(REGIONS.length)]);
        return m;
    }

    // ── V3: canary terminals — 12-field schema ──────────────────────────────
    private Map<String, Object> makeV3() {
        var m = makeV2(); // extends v2
        // v3 optional additions
        if (RNG.nextBoolean()) m.put("receiptEmail", "customer" + RNG.nextInt(9999) + "@email.com");
        m.put("selfCheckout", RNG.nextBoolean());
        return m;
    }

    // ── CORRUPT: missing required field ─────────────────────────────────────
    // These will be caught by RULE 1 (COMPLETENESS) and quarantined
    private Map<String, Object> makeCorrupt() {
        var m = makeV2();
        int corruption = RNG.nextInt(4);
        switch (corruption) {
            case 0 -> m.remove("transactionId");   // missing required field
            case 1 -> m.remove("amount");           // missing required field
            case 2 -> m.put("terminalId", "BAD");  // wrong format (not 8 chars) — RULE 3
            case 3 -> m.put("amount", -5.00);       // negative amount — RULE 2
        }
        return m;
    }

    // ── INVALID: valid JSON, fails business rules ────────────────────────────
    private Map<String, Object> makeInvalid() {
        var m = makeV2();
        int type = RNG.nextInt(2);
        switch (type) {
            // Tax > amount (rule 4 — consistency)
            case 0 -> { double amt = amount(1, 100); m.put("amount", amt); m.put("taxAmount", amt * 2); }
            // Absurdly high item count
            case 1 -> m.put("itemCount", 999);
        }
        return m;
    }

    private void writeFile(List<Map<String, Object>> events) {
        String ts   = String.valueOf(System.currentTimeMillis());
        String tmp  = PipelineConfig.SOURCE_DIR + "/.batch-" + ts + ".json.tmp";
        String done = PipelineConfig.SOURCE_DIR + "/batch-"  + ts + ".json";
        try (var pw = new PrintWriter(new FileWriter(tmp))) {
            for (var e : events) pw.println(MAPPER.writeValueAsString(e));
        } catch (IOException ex) { LOG.error("Write error", ex); return; }
        new File(tmp).renameTo(new File(done));
    }

    private void logStats() {
        LOG.info("📊 GENERATOR: v1={} | v2={} | v3={} | invalid/corrupt={}",
                v1Count.get(), v2Count.get(), v3Count.get(), badCount.get());
    }

    public long getV1Count()  { return v1Count.get();  }
    public long getV2Count()  { return v2Count.get();  }
    public long getV3Count()  { return v3Count.get();  }
    public long getBadCount() { return badCount.get(); }

    private String uuid()     { return UUID.randomUUID().toString().substring(0, 12).toUpperCase(); }
    private String terminal() { return TERMINALS[RNG.nextInt(TERMINALS.length)]; }
    private String cashier()  { return CASHIERS[RNG.nextInt(CASHIERS.length)]; }
    private double amount(double min, double max) {
        return Math.round((min + RNG.nextDouble() * (max - min)) * 100.0) / 100.0;
    }
}
