package com.bank.resilience.pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * BulkheadPattern — Isolate Services Behind Separate Semaphore Pools
 *
 * THE SHIP ANALOGY:
 * A ship has watertight compartments (bulkheads). If compartment 3 floods,
 * compartments 1, 2, 4 remain sealed. Without bulkheads, one breach sinks all.
 *
 * THE BANKING PROBLEM:
 * Without bulkhead: All services share 200 threads.
 *   Flash sale → 200 fraud checks fill all 200 threads → balance check blocked
 *   → card network blocked → ALL payments fail.
 *
 * WITH bulkhead: Each service has its own semaphore.
 *   Fraud pool (10 threads) exhausted → ONLY fraud checks fail.
 *   Balance pool (20 threads) completely unaffected → payments continue.
 */
public class BulkheadPattern {

    private static final Logger log = LoggerFactory.getLogger(BulkheadPattern.class);

    /** Config for one bulkhead partition */
    public record BulkheadConfig(String name, int maxConcurrent, long acquireTimeoutMs) {
        public static BulkheadConfig fraud()        { return new BulkheadConfig("FRAUD_SERVICE",     10, 500);  }
        public static BulkheadConfig balance()      { return new BulkheadConfig("BALANCE_SERVICE",   20, 1000); }
        public static BulkheadConfig cardNetwork()  { return new BulkheadConfig("CARD_NETWORK",      15, 2000); }
        public static BulkheadConfig notification() { return new BulkheadConfig("NOTIFICATION_SVC",   5, 200);  }
        public static BulkheadConfig ledger()       { return new BulkheadConfig("LEDGER_SERVICE",    25, 1500); }
    }

    /** Real-time stats per bulkhead */
    public record BulkheadStats(String name, int maxConcurrent, int currentlyActive,
                                  long totalCalls, long rejectedCalls) {
        public double utilizationPct() {
            return maxConcurrent > 0 ? (double) currentlyActive / maxConcurrent * 100.0 : 0;
        }
        @Override public String toString() {
            return String.format("Bulkhead[%s]: active=%d/%d (%.0f%%), rejected=%d/%d",
                name, currentlyActive, maxConcurrent, utilizationPct(), rejectedCalls, totalCalls);
        }
    }

    /** Typed result from execute() */
    public static class BulkheadResult<T> {
        public enum Kind { EXECUTED, REJECTED }
        private final Kind   kind;
        private final T      value;
        private final String reason;
        private final String name;

        private BulkheadResult(Kind kind, T value, String reason, String name) {
            this.kind = kind; this.value = value; this.reason = reason; this.name = name;
        }

        public static <T> BulkheadResult<T> executed(T value)              { return new BulkheadResult<>(Kind.EXECUTED, value, null,   null);  }
        public static <T> BulkheadResult<T> rejected(String reason, String n) { return new BulkheadResult<>(Kind.REJECTED, null,  reason, n); }

        public boolean isExecuted() { return kind == Kind.EXECUTED; }
        public boolean isRejected() { return kind == Kind.REJECTED; }
        public T       getValue()   { return value;  }
        public String  getReason()  { return reason; }
        public String  getName()    { return name;   }
    }

    // ── Per-bulkhead state ────────────────────────────────────────────────────
    private final Map<String, Semaphore>    semaphores    = new ConcurrentHashMap<>();
    private final Map<String, BulkheadConfig> configs     = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong>   totalCallsMap = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong>   rejectedMap   = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong>   activeMap     = new ConcurrentHashMap<>();

    // ── Registration ─────────────────────────────────────────────────────────
    public void register(BulkheadConfig config) {
        semaphores.put(config.name(),    new Semaphore(config.maxConcurrent(), true));
        configs.put(config.name(),       config);
        totalCallsMap.put(config.name(), new AtomicLong(0));
        rejectedMap.put(config.name(),   new AtomicLong(0));
        activeMap.put(config.name(),     new AtomicLong(0));
        log.info("Bulkhead [{}] registered: maxConcurrent={}, acquireTimeout={}ms",
            config.name(), config.maxConcurrent(), config.acquireTimeoutMs());
    }

    /** Register all standard banking service bulkheads at startup */
    public static BulkheadPattern banking() {
        var bh = new BulkheadPattern();
        bh.register(BulkheadConfig.fraud());
        bh.register(BulkheadConfig.balance());
        bh.register(BulkheadConfig.cardNetwork());
        bh.register(BulkheadConfig.notification());
        bh.register(BulkheadConfig.ledger());
        return bh;
    }

    // ── Core execute ──────────────────────────────────────────────────────────
    /**
     * Execute an operation through the named bulkhead.
     * If the semaphore is exhausted → REJECTED immediately (fail fast).
     */
    public <T> BulkheadResult<T> execute(String bulkheadName, Supplier<T> operation) {
        Semaphore sem         = semaphores.get(bulkheadName);
        BulkheadConfig config = configs.get(bulkheadName);
        if (sem == null || config == null) throw new IllegalArgumentException("Unknown bulkhead: " + bulkheadName);

        totalCallsMap.get(bulkheadName).incrementAndGet();

        boolean acquired = false;
        try {
            acquired = sem.tryAcquire(config.acquireTimeoutMs(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        if (!acquired) {
            rejectedMap.get(bulkheadName).incrementAndGet();
            int active = config.maxConcurrent() - sem.availablePermits();
            log.warn("Bulkhead [{}] REJECTED: {}/{} threads active. Request dropped.",
                bulkheadName, active, config.maxConcurrent());
            return BulkheadResult.rejected(
                String.format("Bulkhead [%s] full (%d/%d threads active)",
                    bulkheadName, active, config.maxConcurrent()),
                bulkheadName
            );
        }

        activeMap.get(bulkheadName).incrementAndGet();
        log.info("Bulkhead [{}] permit acquired. Active: {}/{}",
            bulkheadName, config.maxConcurrent() - sem.availablePermits(), config.maxConcurrent());

        try {
            T result = operation.get();
            return BulkheadResult.executed(result);
        } finally {
            sem.release();
            activeMap.get(bulkheadName).decrementAndGet();
        }
    }

    // ── Observability ─────────────────────────────────────────────────────────
    public BulkheadStats getStats(String name) {
        BulkheadConfig cfg = configs.get(name);
        Semaphore sem       = semaphores.get(name);
        if (cfg == null || sem == null) throw new IllegalArgumentException("Unknown: " + name);
        int active = cfg.maxConcurrent() - sem.availablePermits();
        return new BulkheadStats(name, cfg.maxConcurrent(), active,
            totalCallsMap.get(name).get(), rejectedMap.get(name).get());
    }

    public void printAllStats() {
        log.info("== BULKHEAD STATS ==================================");
        configs.keySet().forEach(name -> log.info("  {}", getStats(name)));
        log.info("====================================================");
    }
}
