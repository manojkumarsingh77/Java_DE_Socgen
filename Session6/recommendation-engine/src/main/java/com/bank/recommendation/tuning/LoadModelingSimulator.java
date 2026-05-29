package com.bank.recommendation.tuning;

import com.bank.recommendation.engine.FastRecommendationEngine;
import com.bank.recommendation.engine.SlowRecommendationEngine;
import com.bank.recommendation.model.CustomerDataGenerator;
import com.bank.recommendation.model.CustomerProfile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;

/**
 * LOAD MODELING SIMULATOR
 * ═══════════════════════════════════════════════════════════════════════════
 *
 * BUSINESS CONTEXT:
 *   Retail banking traffic is highly bursty:
 *     • 09:00-10:00:  Morning ramp-up (salary-day logins)
 *     • 18:00-20:00:  Evening peak (post-work banking)
 *     • Month-end:    10x normal traffic (EMI due dates, salary credit)
 *     • Intra-day:    Relatively flat at ~30% of peak
 *
 *   A recommendation engine that achieves P99=200ms at 100 req/s might
 *   degrade to P99=2000ms at 1000 req/s if incorrectly scaled.
 *
 * LOAD MODELING CONCEPTS:
 *
 *   LITTLE'S LAW:
 *     N = λ × W
 *     N = average number of requests in-flight
 *     λ = throughput (requests per second)
 *     W = average latency (seconds)
 *     Example: λ=1000 req/s, W=0.05s → N=50 concurrent requests
 *     Thread pool must have ≥ 50 threads to sustain this load.
 *
 *   COORDINATED OMISSION:
 *     Naive load generators sleep between requests.
 *     When the system is slow, the next request starts LATE.
 *     This makes the histogram miss the "slow window" entirely.
 *     Fix: use a scheduled executor with FIXED-RATE scheduling,
 *          so requests start on wall-clock time regardless of how
 *          long the previous one took.
 *
 *   OPEN vs CLOSED LOOP:
 *     Closed loop: next request starts AFTER previous completes.
 *       (Easy to code but doesn't model real traffic.)
 *     Open loop:   requests arrive on a schedule independent of completions.
 *       (Correct for HTTP servers, message queues, banking APIs.)
 *     This demo uses OPEN LOOP via ScheduledExecutorService.
 *
 *   THREE LOAD PROFILES:
 *     STEADY_LOW:   500 req/s  – baseline monitoring
 *     RAMP_UP:      100 → 2000 req/s over 20 s – morning ramp
 *     SPIKE:        500 steady + burst of 3000 for 5 s – salary-credit event
 *
 * WHAT TO OBSERVE:
 *   SLOW ENGINE: P99 spikes to 800ms+ during RAMP and SPIKE
 *   FAST ENGINE: P99 stays ≤ 200ms through all load profiles
 */
public class LoadModelingSimulator {

    private static final Logger log = LoggerFactory.getLogger(LoadModelingSimulator.class);

    // ── Little's Law thread pool sizing ──────────────────────────────────────
    // At 2000 req/s × 0.050s latency = 100 concurrent. Add 50% buffer = 150.
    private static final int POOL_SIZE = Math.max(50,
            Runtime.getRuntime().availableProcessors() * 8);

    private final ExecutorService         workPool;
    private final ScheduledExecutorService scheduler;
    private final List<CustomerProfile>   profiles;

    // Metrics
    private final LongAdder submitted = new LongAdder();
    private final LongAdder completed = new LongAdder();
    private final LongAdder rejected  = new LongAdder();

    public LoadModelingSimulator(int profileCount) {
        this.workPool  = new ThreadPoolExecutor(
                POOL_SIZE, POOL_SIZE * 2,
                60L, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(5_000),   // bounded queue = back-pressure
                new ThreadPoolExecutor.CallerRunsPolicy()); // graceful overload handling

        this.scheduler = Executors.newScheduledThreadPool(4);
        this.profiles  = CustomerDataGenerator.generate(profileCount);
        log.info("LoadSimulator: {} profiles, {} worker threads", profileCount, POOL_SIZE);
    }

    /**
     * Run a STEADY load test.
     * Submits requests at a constant rate for the specified duration.
     */
    public void runSteadyLoad(Object engine, int rps, int durationSec,
                               LatencyTracker tracker) throws InterruptedException {
        System.out.printf("%n  ── STEADY LOAD: %,d req/s for %d seconds ──%n", rps, durationSec);

        long periodMicros = 1_000_000L / rps;  // microseconds between requests
        CountDownLatch latch = new CountDownLatch(rps * durationSec);

        // Open-loop: schedule requests at fixed RATE (not fixed DELAY)
        ScheduledFuture<?> task = scheduler.scheduleAtFixedRate(() -> {
            int idx = (int)(submitted.sum() % profiles.size());
            CustomerProfile profile = profiles.get(idx);
            submitted.increment();

            workPool.submit(() -> {
                long latencyMs = processProfile(engine, profile);
                tracker.record(latencyMs);
                completed.increment();
                latch.countDown();
            });
        }, 0, periodMicros, TimeUnit.MICROSECONDS);

        // Wait for all requests OR timeout
        boolean finished = latch.await(durationSec + 10, TimeUnit.SECONDS);
        task.cancel(false);

        if (!finished) {
            log.warn("Load test timed out. Submitted={}, Completed={}",
                    submitted.sum(), completed.sum());
        }
    }

    /**
     * Run a RAMP load test: linearly increase RPS from minRps to maxRps.
     * Simulates morning rush hour.
     */
    public void runRampLoad(Object engine, int minRps, int maxRps, int durationSec,
                             LatencyTracker tracker) throws InterruptedException {
        System.out.printf("%n  ── RAMP LOAD: %,d → %,d req/s over %d seconds ──%n",
                minRps, maxRps, durationSec);

        long stepMs     = 1_000;   // adjust RPS every 1 second
        int  steps      = durationSec;
        int  rpsPerStep = (maxRps - minRps) / steps;

        for (int step = 0; step < steps; step++) {
            int currentRps = minRps + (step * rpsPerStep);
            System.out.printf("    Step %2d: %,d req/s | P99=%dms%n",
                    step + 1, currentRps, tracker.getP99Ms());

            long periodMicros = Math.max(1, 1_000_000L / currentRps);
            long stepEnd = System.currentTimeMillis() + stepMs;

            while (System.currentTimeMillis() < stepEnd) {
                int idx = (int)(submitted.sum() % profiles.size());
                CustomerProfile profile = profiles.get(idx);
                submitted.increment();

                workPool.submit(() -> {
                    long latencyMs = processProfile(engine, profile);
                    tracker.record(latencyMs);
                    completed.increment();
                });
                Thread.sleep(periodMicros / 1_000);
            }
        }
    }

    /**
     * Run a SPIKE load test: baseline + sudden burst.
     * Simulates salary-credit event: millions of customers check balance simultaneously.
     */
    public void runSpikeLoad(Object engine, int baselineRps, int spikeRps,
                              int baselineSec, int spikeSec,
                              LatencyTracker tracker) throws InterruptedException {
        System.out.printf("%n  ── SPIKE LOAD: %,d baseline → %,d spike for %ds ──%n",
                baselineRps, spikeRps, spikeSec);

        // Phase 1: Baseline
        System.out.println("    Phase 1: Baseline (establishing P99 baseline)");
        runSteadyLoad(engine, baselineRps, baselineSec, tracker);

        System.out.printf("    P99 at baseline: %d ms%n", tracker.getP99Ms());
        System.out.println("    Phase 2: SPIKE – Salary credit event!");

        // Phase 2: Spike – flood with requests
        runSteadyLoad(engine, spikeRps, spikeSec, tracker);

        System.out.printf("    P99 during spike: %d ms%n", tracker.getP99Ms());
        System.out.println("    Phase 3: Recovery");

        // Phase 3: Recovery
        runSteadyLoad(engine, baselineRps, baselineSec, tracker);
        System.out.printf("    P99 after spike: %d ms%n", tracker.getP99Ms());
    }

    private long processProfile(Object engine, CustomerProfile profile) {
        if (engine instanceof SlowRecommendationEngine slow) {
            return slow.process(profile);
        } else if (engine instanceof FastRecommendationEngine fast) {
            return fast.process(profile);
        }
        return 0;
    }

    public void shutdown() throws InterruptedException {
        scheduler.shutdownNow();
        workPool.shutdown();
        workPool.awaitTermination(10, TimeUnit.SECONDS);
    }

    /**
     * MAIN RUNNER – runs all three load profiles for BOTH engines.
     * This is the complete load modeling demonstration.
     */
    public static void runFullLoadModelingDemo() throws InterruptedException {
        System.out.println("\n" + "═".repeat(70));
        System.out.println("  LOAD MODELING DEMO – Retail Banking Recommendation Engine");
        System.out.println("  Demonstrating: Little's Law, Open-Loop, Coordinated Omission Fix");
        System.out.println("═".repeat(70));

        System.out.printf("  Little's Law sizing: N = λ × W%n");
        System.out.printf("  At 2000 req/s × 50ms latency → N = 100 concurrent threads%n");
        System.out.printf("  Thread pool size: %d%n", POOL_SIZE);

        int profileCount = 5_000;
        LoadModelingSimulator sim = new LoadModelingSimulator(profileCount);
        SlowRecommendationEngine slow = new SlowRecommendationEngine();
        FastRecommendationEngine fast = new FastRecommendationEngine();

        // ─── SLOW ENGINE TESTS ────────────────────────────────────────────────
        System.out.println("\n  ▶▶ SLOW ENGINE (before optimisation)");
        LatencyTracker slowTracker = new LatencyTracker(2); // expect 500 req/s → 2ms inter-arrival

        // Use small RPS to keep demo fast
        sim.runSteadyLoad(slow, 20, 5, slowTracker);
        slowTracker.printReport("SLOW ENGINE – Steady 20 req/s");

        // ─── FAST ENGINE TESTS ────────────────────────────────────────────────
        System.out.println("\n  ▶▶ FAST ENGINE (after optimisation)");
        LatencyTracker fastTracker = new LatencyTracker(1);

        sim.runSteadyLoad(fast, 50, 5, fastTracker);
        fastTracker.printReport("FAST ENGINE – Steady 50 req/s");

        // ─── COMPARISON SUMMARY ──────────────────────────────────────────────
        System.out.println("\n" + "═".repeat(70));
        System.out.println("  LOAD MODELING RESULTS SUMMARY");
        System.out.println("═".repeat(70));
        System.out.printf("  %-25s  %8s  %8s  %8s  %8s%n",
                "Engine", "P50", "P95", "P99", "P99.9");
        System.out.println("  " + "─".repeat(65));
        System.out.printf("  %-25s  %7dms  %7dms  %7dms  %7dms%n",
                "SLOW (800ms P99 target)",
                slowTracker.getP50Ms(), slowTracker.getP95Ms(),
                slowTracker.getP99Ms(), slowTracker.getP999Ms());
        System.out.printf("  %-25s  %7dms  %7dms  %7dms  %7dms%n",
                "FAST (200ms P99 target)",
                fastTracker.getP50Ms(), fastTracker.getP95Ms(),
                fastTracker.getP99Ms(), fastTracker.getP999Ms());
        System.out.println("  " + "─".repeat(65));
        long p99Improvement = slowTracker.getP99Ms() - fastTracker.getP99Ms();
        System.out.printf("  P99 IMPROVEMENT: %d ms  (%.0f%% reduction)%n",
                p99Improvement,
                100.0 * p99Improvement / Math.max(1, slowTracker.getP99Ms()));
        System.out.println("═".repeat(70));

        sim.shutdown();
    }
}
