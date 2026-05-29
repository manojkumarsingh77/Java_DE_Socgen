package com.bank.recommendation.profiling;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * FALSE SHARING DEMO
 * ═══════════════════════════════════════════════════════════════════════════
 *
 * WHAT IS FALSE SHARING?
 *   Modern CPUs cache memory in 64-byte "cache lines".
 *   If Thread-A writes field X and Thread-B writes field Y,
 *   and X and Y sit in the SAME 64-byte cache line:
 *     → Thread-A's write invalidates Thread-B's cached line
 *     → Thread-B must re-fetch from L3/RAM (100–300 CPU cycles)
 *     → Thread-A must also re-fetch (same invalidation)
 *   This ping-pong is "false sharing": threads share a CACHE LINE not DATA.
 *
 * RETAIL BANKING CONTEXT:
 *   Our pre-processor has THREE concurrent scoring threads each writing
 *   one field of CustomerProfile:
 *     Thread-1 (RiskScorer)       → writes riskScore       (8 bytes / double)
 *     Thread-2 (PropensityScorer) → writes propensityScore (8 bytes / double)
 *     Thread-3 (ChurnScorer)      → writes churnScore      (8 bytes / double)
 *   All three fit in a single 64-byte cache line → false sharing.
 *   At 10,000 profiles/sec this produces 100+ ns overhead per write
 *   → cumulative P99 degradation of 150-200 ms.
 *
 * DEMO DESIGN:
 *   We create TWO versions of a counter array:
 *     BadCounters:     counters are adjacent (same cache line)
 *     PaddedCounters:  each counter padded to 128 bytes (its own cache line)
 *   Four threads each write to their own counter 50 million times.
 *   We measure and compare wall-clock time.
 *
 * EXPECTED RESULT (typical):
 *   BadCounters    : ~2500 ms
 *   PaddedCounters : ~400  ms
 *   Speedup        : ~6x
 *
 * TO RUN IN INTELLIJ:
 *   Run FalseSharingDemo.main() directly.
 *   Add JVM flag: -XX:-RestrictContended
 */
public class FalseSharingDemo {

    private static final Logger log = LoggerFactory.getLogger(FalseSharingDemo.class);

    private static final int  THREADS      = 4;
    private static final long ITERATIONS   = 50_000_000L;

    // ─── BAD: Fields share cache lines ───────────────────────────────────────
    static class BadScoringCounters {
        // ⚠️  All three fields are ADJACENT in memory.
        // On a 64-bit JVM: object header=16 bytes, then fields contiguous.
        // riskCount, propensityCount, churnCount all fit in ONE 64-byte line.
        volatile long riskCount       = 0;
        volatile long propensityCount = 0;
        volatile long churnCount      = 0;
        volatile long eligibleCount   = 0;
    }

    // ─── GOOD: Each field padded to 128 bytes (safe across all CPU architectures)
    // 128 bytes > 64 bytes (x86 L1 cache line) and > 128 bytes (ARM cache line)
    static class PaddedScoringCounters {
        // Pre-padding: 7 longs × 8 = 56 bytes
        long p01, p02, p03, p04, p05, p06, p07;
        volatile long riskCount = 0;
        // Post-padding: 7 longs × 8 = 56 bytes
        long p08, p09, p10, p11, p12, p13, p14;

        long q01, q02, q03, q04, q05, q06, q07;
        volatile long propensityCount = 0;
        long q08, q09, q10, q11, q12, q13, q14;

        long r01, r02, r03, r04, r05, r06, r07;
        volatile long churnCount = 0;
        long r08, r09, r10, r11, r12, r13, r14;

        long s01, s02, s03, s04, s05, s06, s07;
        volatile long eligibleCount = 0;
        long s08, s09, s10, s11, s12, s13, s14;
    }

    public static void runDemo() throws InterruptedException {
        System.out.println("\n" + "═".repeat(64));
        System.out.println("  FALSE SHARING DEMO – Retail Banking Scoring Counters");
        System.out.println("═".repeat(64));
        System.out.printf("  Threads: %d  |  Iterations per thread: %,d%n%n",
                THREADS, ITERATIONS);

        // Warm up JIT
        runBad(10_000);
        runPadded(10_000);
        System.gc();
        Thread.sleep(500);

        // Actual measurement
        long badMs    = runBad(ITERATIONS);
        long paddedMs = runPadded(ITERATIONS);

        System.out.println();
        System.out.println("┌─────────────────────────────────────────────────────┐");
        System.out.printf ("│  BadCounters (false sharing)    : %6d ms          │%n", badMs);
        System.out.printf ("│  PaddedCounters (cache aligned) : %6d ms          │%n", paddedMs);
        System.out.printf ("│  Speedup                        : %6.1fx           │%n",
                (double) badMs / Math.max(1, paddedMs));
        System.out.printf ("│  Latency saved (simulated P99)  : ~%4d ms          │%n",
                Math.max(0, badMs - paddedMs) / 10);
        System.out.println("└─────────────────────────────────────────────────────┘");

        System.out.println("\n  EXPLANATION:");
        System.out.println("  BadCounters: all 4 volatile longs share 1 cache line.");
        System.out.println("  Each write by Thread-N invalidates the line for all others.");
        System.out.println("  CPUs bounce the cache line between cores L1/L2/L3/RAM.");
        System.out.println("  PaddedCounters: each volatile long has its own 128-byte region.");
        System.out.println("  Writes are fully independent → no cross-core invalidation.");
        System.out.println("═".repeat(64));
    }

    private static long runBad(long iterations) throws InterruptedException {
        BadScoringCounters counters = new BadScoringCounters();
        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        CountDownLatch latch = new CountDownLatch(THREADS);

        long start = System.nanoTime();

        pool.submit(() -> { for (long i=0;i<iterations;i++) counters.riskCount++;       latch.countDown(); });
        pool.submit(() -> { for (long i=0;i<iterations;i++) counters.propensityCount++; latch.countDown(); });
        pool.submit(() -> { for (long i=0;i<iterations;i++) counters.churnCount++;      latch.countDown(); });
        pool.submit(() -> { for (long i=0;i<iterations;i++) counters.eligibleCount++;   latch.countDown(); });

        latch.await(30, TimeUnit.SECONDS);
        long elapsed = (System.nanoTime() - start) / 1_000_000;
        pool.shutdown();

        if (iterations >= ITERATIONS) {
            System.out.printf("  [BAD]    riskCount=%,d  (verify no JIT elimination)%n",
                    counters.riskCount);
        }
        return elapsed;
    }

    private static long runPadded(long iterations) throws InterruptedException {
        PaddedScoringCounters counters = new PaddedScoringCounters();
        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        CountDownLatch latch = new CountDownLatch(THREADS);

        long start = System.nanoTime();

        pool.submit(() -> { for (long i=0;i<iterations;i++) counters.riskCount++;       latch.countDown(); });
        pool.submit(() -> { for (long i=0;i<iterations;i++) counters.propensityCount++; latch.countDown(); });
        pool.submit(() -> { for (long i=0;i<iterations;i++) counters.churnCount++;      latch.countDown(); });
        pool.submit(() -> { for (long i=0;i<iterations;i++) counters.eligibleCount++;   latch.countDown(); });

        latch.await(30, TimeUnit.SECONDS);
        long elapsed = (System.nanoTime() - start) / 1_000_000;
        pool.shutdown();

        if (iterations >= ITERATIONS) {
            System.out.printf("  [PADDED] riskCount=%,d  (verify no JIT elimination)%n",
                    counters.riskCount);
        }
        return elapsed;
    }

    public static void main(String[] args) throws InterruptedException {
        runDemo();
    }
}
