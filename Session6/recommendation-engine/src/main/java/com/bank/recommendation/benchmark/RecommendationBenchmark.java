package com.bank.recommendation.benchmark;

import com.bank.recommendation.engine.FastRecommendationEngine;
import com.bank.recommendation.engine.SlowRecommendationEngine;
import com.bank.recommendation.model.CustomerDataGenerator;
import com.bank.recommendation.model.CustomerProfile;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * JMH BENCHMARK – Recommendation Engine Pre-Processor
 * ═══════════════════════════════════════════════════════════════════════════
 *
 * WHY JMH (Java Microbenchmark Harness)?
 *   Writing benchmarks with System.nanoTime() loops is deceptively dangerous:
 *     • JIT compilation: code gets FASTER during the loop (not stable)
 *     • Dead code elimination: JIT may REMOVE code with no side effects
 *     • Constant folding: JIT may pre-compute results at compile time
 *     • Measurement overhead: nanoTime() itself takes 20–100 ns
 *     • GC interference: a GC pause during a short loop skews results
 *   JMH solves ALL of these systematically.
 *
 * JMH KEY CONCEPTS DEMONSTRATED:
 *
 *   @State(Scope.Thread):
 *     Each benchmark thread gets its OWN instance of BenchmarkState.
 *     Prevents contention on shared state from polluting measurements.
 *     For our engine: each thread has its own CustomerProfile to process.
 *
 *   @Warmup:
 *     JMH runs the benchmark for warmup iterations FIRST (discarded).
 *     This lets the JIT compiler fully optimise the hot path before measurement.
 *     Without warmup, early iterations are 5-50x slower than steady state.
 *
 *   @Measurement:
 *     The actual measurement phase after JIT is warm.
 *     5 iterations × 2 seconds each = statistically robust result.
 *
 *   @Fork:
 *     Each fork = a fresh JVM process.
 *     Eliminates JIT state pollution between benchmarks.
 *     @Fork(2) = run twice, average results.
 *
 *   Blackhole:
 *     JMH's anti-dead-code-elimination device.
 *     consume() tells JIT "this value IS used; don't optimise it away."
 *     Without Blackhole, JIT may eliminate the entire engine call!
 *
 *   @BenchmarkMode(Mode.AverageTime):
 *     Reports average time per operation in specified TimeUnit.
 *     Other modes: Throughput, SampleTime (for percentiles), SingleShotTime.
 *
 *   @OutputTimeUnit(TimeUnit.MILLISECONDS):
 *     Report in milliseconds (matches our SLA targets).
 *
 * HOW TO RUN IN INTELLIJ:
 *   Option A (Easy): Run RecommendationBenchmark.main() directly.
 *   Option B (Full): mvn package → java -jar target/recommendation-engine-benchmarks.jar
 *   Option C: Install "JMH Plugin" from IntelliJ Marketplace, then click ▶ next to @Benchmark
 *
 * INTERPRETING RESULTS:
 *   Score column = average ms per operation (lower is better)
 *   Error column = ± standard deviation (small error = stable benchmark)
 *   Compare "slow" vs "fast" rows directly.
 *   SLA target: score ≤ 200 ms (P99 proxy from AverageTime).
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)              // ONE engine shared across all threads
@Warmup(iterations = 2, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 3, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1, jvmArgs = {
        "-Xms512m", "-Xmx1g",
        "-XX:+UseG1GC",
        "-XX:MaxGCPauseMillis=50"    // SLA-driven GC tuning: max 50ms GC pause
})
public class RecommendationBenchmark {

    // ── Shared state (set up once per benchmark, not per iteration) ───────────
    private SlowRecommendationEngine slowEngine;
    private FastRecommendationEngine fastEngine;
    private List<CustomerProfile>    customerProfiles;
    private final AtomicInteger      profileIndex = new AtomicInteger(0);

    @Setup(Level.Trial)   // called ONCE before all iterations
    public void setup() {
        slowEngine       = new SlowRecommendationEngine();
        fastEngine       = new FastRecommendationEngine();
        customerProfiles = CustomerDataGenerator.generate(10_000);
        System.out.println("[JMH] Setup complete: " + customerProfiles.size() + " profiles loaded");
    }

    @TearDown(Level.Trial) // called ONCE after all iterations
    public void tearDown() {
        System.out.println("[JMH] FastEngine cache stats: " + fastEngine.getCacheStats());
        System.out.println("[JMH] FastEngine avg latency: " + fastEngine.getAvgLatencyMs() + " ms");
    }

    /**
     * BENCHMARK 1: Slow engine (all bottlenecks present)
     * Expected: ~50-80 ms average (bottlenecks compound at low concurrency in benchmark)
     * Expected P99 under real load: ~800 ms
     */
    @Benchmark
    public void slowEngine_singleRequest(Blackhole bh) {
        // Round-robin through profiles so we don't always hit the same one
        int idx = profileIndex.getAndIncrement() % customerProfiles.size();
        CustomerProfile profile = customerProfiles.get(idx);
        long latencyMs = slowEngine.process(profile);
        bh.consume(latencyMs);  // Blackhole prevents dead-code elimination
        bh.consume(profile.getRecommendedProducts());
    }

    /**
     * BENCHMARK 2: Fast engine (all optimisations applied)
     * Expected: ~25-35 ms average (parallel stages, cached rules)
     * Expected P99 under real load: ~180-200 ms
     */
    @Benchmark
    public void fastEngine_singleRequest(Blackhole bh) {
        int idx = profileIndex.getAndIncrement() % customerProfiles.size();
        CustomerProfile profile = customerProfiles.get(idx);
        long latencyMs = fastEngine.process(profile);
        bh.consume(latencyMs);
        bh.consume(profile.getRecommendedProducts());
    }

    /**
     * BENCHMARK 3: Fast engine – eligibility rule parsing only
     * Isolates the I/O fix contribution (cache vs disk).
     * Use this to attribute latency savings specifically to FIX-2.
     */
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void fastEngine_throughput(Blackhole bh) {
        int idx = profileIndex.getAndIncrement() % customerProfiles.size();
        CustomerProfile profile = customerProfiles.get(idx);
        long latencyMs = fastEngine.process(profile);
        bh.consume(latencyMs);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // RUNNER – launches JMH programmatically from main()
    // Useful for IntelliJ "Run" button (no mvn package needed)
    // ─────────────────────────────────────────────────────────────────────────
    public static void main(String[] args) throws RunnerException {
        System.out.println("╔═══════════════════════════════════════════════════════╗");
        System.out.println("║  JMH BENCHMARK – Retail Banking Recommendation Engine ║");
        System.out.println("║  Target SLA: P99 ≤ 200ms                             ║");
        System.out.println("╚═══════════════════════════════════════════════════════╝");

        Options opts = new OptionsBuilder()
                .include(RecommendationBenchmark.class.getSimpleName())
                .warmupIterations(2)
                .warmupTime(TimeValue.seconds(2))
                .measurementIterations(3)
                .measurementTime(TimeValue.seconds(3))
                .forks(1)
                .threads(1)                              // single-threaded to measure latency
                .resultFormat(ResultFormatType.TEXT)
                .result("jmh-results.txt")              // save results to file
                .jvmArgs(
                    "-Xms512m", "-Xmx1g",
                    "-XX:+UseG1GC",
                    "-XX:MaxGCPauseMillis=50",
                    "-XX:+PrintGCDetails",               // see GC pauses in output
                    "-Xlog:gc:gc.log"                    // GC log for analysis
                )
                .build();

        System.out.println("\nRunning JMH benchmarks... (this takes ~3 minutes)");
        System.out.println("TIP: While running, take a thread dump in IntelliJ to");
        System.out.println("     see the difference in thread states between slow/fast.\n");

        new Runner(opts).run();

        System.out.println("\nResults saved to: jmh-results.txt");
        System.out.println("Open gc.log with GCViewer to analyse GC pause contribution to P99");
    }
}
