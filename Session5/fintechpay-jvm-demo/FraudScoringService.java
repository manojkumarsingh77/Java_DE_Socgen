package com.fintechpay.service;

import com.fintechpay.model.Transaction;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * ╔══════════════════════════════════════════════════════════════════════════╗
 * ║  FraudScoringService — CPU Throttling Demo                              ║
 * ╠══════════════════════════════════════════════════════════════════════════╣
 * ║                                                                          ║
 * ║  CONCEPT 3: CPU THROTTLING IN AKS                                        ║
 * ║  ─────────────────────────────────                                       ║
 * ║                                                                          ║
 * ║  Fraud scoring is CPU-intensive (ML feature extraction, rule engines).  ║
 * ║  When many transactions arrive in a burst, the JVM's thread pool        ║
 * ║  tries to run them in parallel. If thread count > container CPU limit,  ║
 * ║  Linux CFS scheduler throttles ALL threads for the next 100ms period.   ║
 * ║                                                                          ║
 * ║  THE CLASSIC MISTAKE:                                                   ║
 * ║  Using Executors.newFixedThreadPool(Runtime.availableProcessors())      ║
 * ║  with 48-CPU host → 48 threads → burst → throttle.                     ║
 * ║                                                                          ║
 * ║  THE FIX:                                                               ║
 * ║  1. Pin thread count to AKS cpu limit (via -XX:ActiveProcessorCount=2) ║
 * ║  2. Use virtual threads (Java 21) for I/O-bound tasks                  ║
 * ║  3. Rate-limit concurrent scoring to avoid CFS quota bursting          ║
 * ║                                                                          ║
 * ╚══════════════════════════════════════════════════════════════════════════╝
 */
public class FraudScoringService {

    /**
     * CONCEPT 3 — Thread pool sized to AVAILABLE PROCESSORS.
     *
     * With -XX:ActiveProcessorCount=2 (AKS limit) → pool size = 2.
     * Without that flag on a 48-CPU host        → pool size = 48.
     *
     * 48 threads × ~5ms CPU work each = 240ms CPU in a burst
     * AKS quota (2 CPUs × 100ms period) = 200ms
     * 240ms > 200ms → CFS throttles → all 48 threads frozen for next 100ms window!
     * Result: 100ms+ latency spike on the payment approval screen.
     */
    private final ExecutorService fraudThreadPool;
    private final Random rng = new Random(42L);

    public FraudScoringService() {
        // ── CORRECT: CPU-aware thread pool ────────────────────────────────
        // availableProcessors() reads cgroup cpuset when UseContainerSupport=true.
        // In Docker/AKS with --cpus=2 → returns 2 → pool of 2 workers.
        // This prevents CPU quota bursting in the CFS scheduler.
        int cpuCount = Runtime.getRuntime().availableProcessors();
        this.fraudThreadPool = Executors.newFixedThreadPool(
            cpuCount,
            r -> {
                Thread t = new Thread(r, "fraud-scorer-" + cpuCount);
                t.setDaemon(true);
                return t;
            }
        );
        System.out.printf("   [FraudService] Thread pool sized to %d (matches container CPUs)%n", cpuCount);
    }

    /**
     * Score a batch of transactions for fraud concurrently.
     *
     * CONCEPT 3 DEMO: We intentionally show what happens with:
     * - Correct pool size (no throttle)
     * - Oversized pool (simulated throttle warning)
     */
    public List<Transaction> scoreBatch(List<Transaction> transactions) {
        System.out.println("\n   [FraudService] Scoring batch of " + transactions.size() + " transactions...");

        List<Future<Transaction>> futures = new ArrayList<>();

        for (Transaction tx : transactions) {
            futures.add(fraudThreadPool.submit(() -> {
                double score = computeFraudScore(tx);
                tx.setFraudScore(score);

                if (score >= 0.85) {
                    tx.setStatus(Transaction.Status.DECLINED);
                } else if (score >= 0.65) {
                    tx.setStatus(Transaction.Status.UNDER_REVIEW);
                } else {
                    tx.setStatus(Transaction.Status.APPROVED);
                }
                return tx;
            }));
        }

        // Collect results
        List<Transaction> scored = new ArrayList<>();
        for (Future<Transaction> f : futures) {
            try {
                scored.add(f.get(5, TimeUnit.SECONDS));
            } catch (Exception e) {
                System.err.println("   [FraudService] Scoring timeout — possible CPU throttle! " + e.getMessage());
            }
        }

        return scored;
    }

    /**
     * Simulates CPU-intensive fraud scoring.
     *
     * Real fraud scoring involves:
     * - Feature extraction (velocity checks, geo-anomaly, device fingerprint)
     * - Rule engine evaluation (200+ rules)
     * - ML model inference (gradient boosted trees)
     *
     * We simulate this with controlled CPU work to demonstrate quota pressure.
     *
     * CONCEPT 3 KEY INSIGHT:
     * Each call does ~1-3ms of CPU work.
     * With 48 concurrent calls on 2 CPU limit → CFS bursts.
     * With 2 concurrent calls on 2 CPU limit → stays within quota.
     */
    private double computeFraudScore(Transaction tx) {
        // Simulate feature extraction (string operations = CPU + heap allocation)
        double velocityScore     = computeVelocityScore(tx);
        double amountAnomalyScore = computeAmountAnomalyScore(tx);
        double timeAnomalyScore  = computeTimeAnomalyScore(tx);
        double geoAnomalyScore   = simulateGeoCheck(tx);

        // Weighted ensemble (simplified)
        double rawScore = (velocityScore     * 0.30)
                        + (amountAnomalyScore * 0.35)
                        + (timeAnomalyScore  * 0.20)
                        + (geoAnomalyScore   * 0.15);

        return Math.min(1.0, Math.max(0.0, rawScore + rng.nextDouble() * 0.1 - 0.05));
    }

    private double computeVelocityScore(Transaction tx) {
        // Simulate CPU work: check how many transactions this account had recently
        // In production: Redis lookup + sliding window counter
        double base = 0.0;
        // Accounts starting with 'FRAUD' get high velocity score (test scenario)
        if (tx.getAccountId().startsWith("FRAUD")) {
            base = 0.75 + rng.nextDouble() * 0.25;
        } else {
            // Simulate a rule engine scanning transaction history
            long checksum = 0;
            for (int i = 0; i < 2000; i++) {  // controlled CPU burn
                checksum += (tx.getAccountId().hashCode() * i) % 997;
            }
            base = (checksum % 100) / 200.0; // 0.0 to 0.5 range
        }
        return base;
    }

    private double computeAmountAnomalyScore(Transaction tx) {
        BigDecimal amount = tx.getAmount();
        // High-value transactions above ₹1,00,000 get elevated scrutiny
        if (amount.compareTo(BigDecimal.valueOf(100_000)) > 0) {
            return 0.6 + rng.nextDouble() * 0.3;
        }
        // Round numbers like 50000, 99999 are suspicious patterns
        if (amount.remainder(BigDecimal.valueOf(10_000)).compareTo(BigDecimal.ZERO) == 0) {
            return 0.3 + rng.nextDouble() * 0.2;
        }
        return rng.nextDouble() * 0.2;
    }

    private double computeTimeAnomalyScore(Transaction tx) {
        // 2 AM transactions from retail banking accounts are unusual
        int hour = java.time.LocalTime.now().getHour();
        if (hour >= 1 && hour <= 4) {
            return 0.4 + rng.nextDouble() * 0.3;
        }
        return rng.nextDouble() * 0.15;
    }

    private double simulateGeoCheck(Transaction tx) {
        // Simulate external API call result (in production: IP geolocation service)
        // This would be I/O bound — prime candidate for virtual threads in Java 21
        return rng.nextDouble() * 0.3;
    }

    public void shutdown() {
        fraudThreadPool.shutdown();
        try {
            if (!fraudThreadPool.awaitTermination(5, TimeUnit.SECONDS)) {
                fraudThreadPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
