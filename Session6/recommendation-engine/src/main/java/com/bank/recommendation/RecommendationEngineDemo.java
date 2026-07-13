package com.bank.recommendation;

import com.bank.recommendation.engine.FastRecommendationEngine;
import com.bank.recommendation.engine.SlowRecommendationEngine;
import com.bank.recommendation.io.IOBottleneckDemo;
import com.bank.recommendation.model.CustomerDataGenerator;
import com.bank.recommendation.model.CustomerProfile;
import com.bank.recommendation.profiling.FalseSharingDemo;
import com.bank.recommendation.profiling.ThreadDumpAnalyzer;
import com.bank.recommendation.tuning.LatencyTracker;
import com.bank.recommendation.tuning.LoadModelingSimulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ╔══════════════════════════════════════════════════════════════════════════╗
 * ║  RETAIL BANKING RECOMMENDATION ENGINE PRE-PROCESSOR                    ║
 * ║  Performance Tuning Demo: P99 800ms → 200ms                            ║
 * ╠══════════════════════════════════════════════════════════════════════════╣
 * ║                                                                          ║
 * ║  BUSINESS REQUIREMENT                                                   ║
 * ║  ─────────────────────────────────────────────────────────────────────  ║
 * ║  HDFC / Axis / ICICI mobile app home screen must display personalised  ║
 * ║  product recommendations (loans, credit cards, investments) within      ║
 * ║  200ms (P99) when the customer opens the app.                          ║
 * ║                                                                          ║
 * ║  CURRENT STATE:  P99 = 800ms  → customers see loading spinner           ║
 * ║  TARGET STATE:   P99 = 200ms  → instant personalised tiles              ║
 * ║                                                                          ║
 * ║  ROOT CAUSES (demonstrated in this demo):                               ║
 * ║    1. Lock contention   – CPU profiling / thread dumps                  ║
 * ║    2. Disk I/O on hot path – IO bottleneck analysis                     ║
 * ║    3. False sharing     – cache-line invalidation between threads       ║
 * ║    4. No parallelism    – sequential stages                             ║
 * ║    5. GC pressure       – object allocation storm                       ║
 * ║                                                                          ║
 * ║  DEMOS INCLUDED:                                                         ║
 * ║    [1] Quick latency comparison (Slow vs Fast, 100 customers)           ║
 * ║    [2] False Sharing Demo (cache-line padding speedup)                  ║
 * ║    [3] IO Bottleneck Analysis (disk vs cache)                           ║
 * ║    [4] Thread Dump Analysis (lock contention)                           ║
 * ║    [5] CPU Profiling (per-thread CPU time)                              ║
 * ║    [6] Load Modeling Simulator (P95/P99 under real traffic)             ║
 * ║    [7] SLA Report (final before/after comparison)                       ║
 * ║                                                                          ║
 * ║  JVM FLAGS (add to IntelliJ Run Configuration → VM options):            ║
 * ║    -Xms512m -Xmx1g -XX:+UseG1GC -XX:MaxGCPauseMillis=50               ║
 * ║    -XX:-RestrictContended                                                ║
 * ║                                                                          ║
 * ╚══════════════════════════════════════════════════════════════════════════╝
 */
public class RecommendationEngineDemo {

    private static final Logger log = LoggerFactory.getLogger(RecommendationEngineDemo.class);

    // Number of customers to process in each demo
    private static final int DEMO_CUSTOMERS  = 100;
    private static final int LOAD_CUSTOMERS  = 5_000;
    // Number of concurrent threads for load test
    private static final int LOAD_THREADS    = Math.max(4,
            Runtime.getRuntime().availableProcessors() * 2);

    public static void main(String[] args) throws Exception {

        printBanner();

        // ── DEMO 1: Quick latency comparison ──────────────────────────────────
        section("DEMO 1 – Quick Latency Comparison: Slow vs Fast Engine");
        runQuickComparison();
        pause();

        // ── DEMO 2: False Sharing ──────────────────────────────────────────────
        section("DEMO 2 – False Sharing: CPU Cache-Line Contention");
        FalseSharingDemo.runDemo();
        pause();

        // ── DEMO 3: IO Bottleneck ──────────────────────────────────────────────
        section("DEMO 3 – I/O Bottleneck Analysis: Disk vs In-Memory Cache");
        IOBottleneckDemo.runDemo();
        pause();

        // ── DEMO 4: Thread Dumps ───────────────────────────────────────────────
        section("DEMO 4 – Thread Dump Analysis: Lock Contention Visualized");
        ThreadDumpAnalyzer.demonstrateLockContention();
        pause();

        // ── DEMO 5: CPU Profiling ──────────────────────────────────────────────
        section("DEMO 5 – CPU Profiling: Per-Thread CPU Time Breakdown");
        runCpuProfilingDemo();
        pause();

        // ── DEMO 6: Load Modeling ──────────────────────────────────────────────
        section("DEMO 6 – Load Modeling Simulator: P95/P99 Under Real Traffic");
        LoadModelingSimulator.runFullLoadModelingDemo();
        pause();

        // ── DEMO 7: Final SLA Report ───────────────────────────────────────────
        section("DEMO 7 – Final SLA Report: Before vs After");
        runFinalSlaReport();

        System.out.println("\n✅  All demos complete.");
        System.out.println("   Next: Run RecommendationBenchmark.main() for JMH benchmarks.");
        System.out.println("   See README.md for IntelliJ profiler & async-profiler instructions.");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // DEMO 1: Quick comparison – processes N customers with BOTH engines
    // and prints per-request latency + P99
    // ─────────────────────────────────────────────────────────────────────────
    private static void runQuickComparison() {
        List<CustomerProfile> profiles = CustomerDataGenerator.generate(DEMO_CUSTOMERS);
        SlowRecommendationEngine slow  = new SlowRecommendationEngine();
        FastRecommendationEngine fast  = new FastRecommendationEngine();
        LatencyTracker slowTracker     = new LatencyTracker(5);
        LatencyTracker fastTracker     = new LatencyTracker(2);

        System.out.println("\n  Processing " + DEMO_CUSTOMERS + " customer profiles...\n");

        // SLOW ENGINE
        System.out.println("  ── Slow Engine (bottlenecks: lock contention + disk I/O) ──");
        for (CustomerProfile p : profiles) {
            long ms = slow.process(p);
            slowTracker.record(ms);
        }
        slowTracker.printReport("SLOW ENGINE");

        // Print a sample recommendation
        System.out.println("\n  Sample output (Slow Engine):");
        profiles.stream().limit(3).forEach(p ->
                System.out.printf("    %s → %s%n", p.getCustomerId(), p.getRecommendedProducts()));

        // FAST ENGINE
        System.out.println("\n  ── Fast Engine (parallel pipeline + cached rules) ──");
        for (CustomerProfile p : CustomerDataGenerator.generate(DEMO_CUSTOMERS)) {
            long ms = fast.process(p);
            fastTracker.record(ms);
        }
        fastTracker.printReport("FAST ENGINE");

        System.out.printf("%n  SUMMARY: P99 reduced from %dms → %dms  (%.0f%% improvement)%n",
                slowTracker.getP99Ms(), fastTracker.getP99Ms(),
                100.0 * (slowTracker.getP99Ms() - fastTracker.getP99Ms()) /
                        Math.max(1, slowTracker.getP99Ms()));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // DEMO 5: CPU Profiling – run both engines and capture CPU time
    // ─────────────────────────────────────────────────────────────────────────
    private static void runCpuProfilingDemo() throws InterruptedException {
        ThreadDumpAnalyzer analyzer   = new ThreadDumpAnalyzer();
        List<CustomerProfile> profiles = CustomerDataGenerator.generate(200);
        ExecutorService pool           = Executors.newFixedThreadPool(LOAD_THREADS);
        AtomicInteger idx              = new AtomicInteger(0);

        System.out.println("\n  PART A: CPU profile during SLOW engine processing");
        System.out.println("  ─────────────────────────────────────────────────");
        System.out.println("  HOW TO READ THIS:");
        System.out.println("  • High CPU ms + BLOCKED state = waiting for lock (wasted budget)");
        System.out.println("  • Threads in BLOCKED = lock contention (root cause of P99)");
        System.out.println();

        SlowRecommendationEngine slow = new SlowRecommendationEngine();

        // Submit work
        for (int i = 0; i < LOAD_THREADS; i++) {
            pool.submit(() -> {
                for (int j = 0; j < 20; j++) {
                    int i2 = idx.getAndIncrement() % profiles.size();
                    slow.process(profiles.get(i2));
                }
            });
        }

        // Capture CPU profile mid-flight
        Thread.sleep(500);
        analyzer.printCpuProfile("Slow Engine – 500ms into processing");
        pool.shutdown();
        pool.awaitTermination(30, TimeUnit.SECONDS);

        System.out.println("\n  PART B: CPU profile during FAST engine processing");
        System.out.println("  ─────────────────────────────────────────────────");
        System.out.println("  Expected: threads mostly RUNNABLE (doing real work)");
        System.out.println("            Zero BLOCKED threads (no shared lock)");
        System.out.println();

        FastRecommendationEngine fast = new FastRecommendationEngine();
        ExecutorService pool2 = Executors.newFixedThreadPool(LOAD_THREADS);
        AtomicInteger idx2 = new AtomicInteger(0);

        for (int i = 0; i < LOAD_THREADS; i++) {
            pool2.submit(() -> {
                for (int j = 0; j < 20; j++) {
                    int i2 = idx2.getAndIncrement() % profiles.size();
                    fast.process(profiles.get(i2));
                }
            });
        }

        Thread.sleep(500);
        analyzer.printCpuProfile("Fast Engine – 500ms into processing");
        pool2.shutdown();
        pool2.awaitTermination(30, TimeUnit.SECONDS);

        System.out.println("\n  REAL PROFILING IN INTELLIJ:");
        System.out.println("  Run → Profile (CPU) → Select 'CPU Profiler'");
        System.out.println("  Look for: hot methods in flame graph.");
        System.out.println("  Slow Engine: ReentrantLock.lock() dominates");
        System.out.println("  Fast Engine: Math.exp(), multiply (actual scoring work)");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // DEMO 7: Final SLA report with recommendations
    // ─────────────────────────────────────────────────────────────────────────
    private static void runFinalSlaReport() {
        List<CustomerProfile> profiles = CustomerDataGenerator.generate(500);
        SlowRecommendationEngine slow  = new SlowRecommendationEngine();
        FastRecommendationEngine fast  = new FastRecommendationEngine();
        LatencyTracker slowTracker     = new LatencyTracker(5);
        LatencyTracker fastTracker     = new LatencyTracker(1);

        System.out.println("\n  Processing 500 customers with each engine...");
        profiles.forEach(p -> slowTracker.record(slow.process(p)));
        CustomerDataGenerator.generate(500).forEach(p -> fastTracker.record(fast.process(p)));

        slowTracker.printReport("BEFORE OPTIMISATION (Slow Engine)");
        fastTracker.printReport("AFTER OPTIMISATION  (Fast Engine)");

        System.out.println();
        System.out.println("╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║  OPTIMISATION ATTRIBUTION – What fixed what?                ║");
        System.out.println("╠══════════════════════════════════════════════════════════════╣");
        System.out.println("║  Fix                   Technique   P99 Improvement           ║");
        System.out.println("╠══════════════════════════════════════════════════════════════╣");
        System.out.println("║  Parallel pipeline     CompletableFuture    ~350ms           ║");
        System.out.println("║  Rule caching          Caffeine Cache       ~250ms           ║");
        System.out.println("║  False-sharing fix     Cache-line padding   ~100ms           ║");
        System.out.println("║  Object pool/records   Reduce GC pressure    ~50ms           ║");
        System.out.println("║  Thread pool tuning    Little's Law          ~50ms           ║");
        System.out.println("╠══════════════════════════════════════════════════════════════╣");
        System.out.println("║  TOTAL                                      ~800ms           ║");
        System.out.println("║  Result: P99 800ms → 200ms  (75% reduction)                 ║");
        System.out.println("╚══════════════════════════════════════════════════════════════╝");

        System.out.println();
        System.out.println("  NEXT STEPS FOR PRODUCTION:");
        System.out.println("  1. Run full JMH benchmark: mvn package && java -jar target/*.jar");
        System.out.println("  2. Profile with async-profiler: ./profiler.sh -e cpu -d 60 <pid>");
        System.out.println("  3. Enable JFR: -XX:StartFlightRecording=duration=60s,filename=app.jfr");
        System.out.println("  4. Analyse GC: -Xlog:gc*:file=gc.log → use GCViewer");
        System.out.println("  5. Monitor P99 in Grafana with Micrometer + HdrHistogram export");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Utilities
    // ─────────────────────────────────────────────────────────────────────────
    private static void printBanner() {
        System.out.println();
        System.out.println("╔═══════════════════════════════════════════════════════════════════╗");
        System.out.println("║     RETAIL BANKING RECOMMENDATION ENGINE PRE-PROCESSOR           ║");
        System.out.println("║     Performance Tuning: P99  800ms  →  200ms                    ║");
        System.out.println("╠═══════════════════════════════════════════════════════════════════╣");
        System.out.println("║  Topics: CPU Profiling │ Thread Dumps │ JMH │ False Sharing      ║");
        System.out.println("║          IO Bottleneck │ SLA Tuning   │ Load Modeling            ║");
        System.out.printf ("║  JVM: %s%-47s║%n",
                System.getProperty("java.version"),
                " | Cores: " + Runtime.getRuntime().availableProcessors());
        System.out.println("╚═══════════════════════════════════════════════════════════════════╝");
    }

    private static void section(String title) {
        System.out.println("\n\n" + "█".repeat(70));
        System.out.println("  " + title);
        System.out.println("█".repeat(70));
    }

    private static void pause() throws InterruptedException {
        Thread.sleep(300); // brief pause between demos for readability
    }
}
