package com.bank.recommendation.tuning;

import org.HdrHistogram.Histogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * SLA-DRIVEN TUNING – LatencyTracker with HdrHistogram
 * ═══════════════════════════════════════════════════════════════════════════
 *
 * BUSINESS SLA (Retail Banking):
 *   P50  ≤  50 ms   (median – customer perceived "snappy")
 *   P95  ≤ 150 ms   (95 out of 100 customers – regulatory reporting)
 *   P99  ≤ 200 ms   (SLA contractual target – penalty beyond this)
 *   P999 ≤ 500 ms   (extreme outliers – GC pauses, cold starts)
 *   MAX  ≤   2 s    (hard timeout – app shows cached fallback)
 *
 * WHY HdrHistogram vs System.nanoTime() simple array?
 *   • HdrHistogram is lock-free for recording (uses CAS internally)
 *   • It stores compressed high-dynamic-range data: 100ns–10min in 3 sig-fig
 *   • ArrayList<Long> at 10,000 req/s = 80 MB/s just for latency data
 *   • HdrHistogram: constant memory (fixed-size buckets), ~30 KB total
 *   • Coordinated omission correction: accounts for requests that never
 *     started because the system was overloaded (regular histograms MISS these)
 *
 * COORDINATED OMISSION (P99 LIES without this fix):
 *   If a GC pause freezes your app for 200ms, requests during that window
 *   never start.  Simple histograms only record completed requests.
 *   HdrHistogram.recordValueWithExpectedInterval() injects phantom samples
 *   for the "missed" requests, giving TRUE P99.
 *
 * SLA BREACH DETECTION:
 *   Every 1000 requests we check P99.  If P99 > 200ms we log a WARNING.
 *   This drives the circuit-breaker in a real production system.
 */
public class LatencyTracker {

    private static final Logger log = LoggerFactory.getLogger(LatencyTracker.class);

    // SLA thresholds (milliseconds)
    public static final long SLA_P50_MS   =   50;
    public static final long SLA_P95_MS   =  150;
    public static final long SLA_P99_MS   =  200;
    public static final long SLA_P999_MS  =  500;

    // HdrHistogram: tracks values from 1µs to 10min with 3 significant figures
    // significantValueDigits=3 means precision to 0.1% (adequate for SLA reporting)
    private final Histogram histogram;

    // Expected interval for coordinated-omission correction
    // At 1000 req/s, expected inter-arrival = 1 ms
    private final long expectedIntervalMs;

    // Counters
    private final LongAdder totalRequests = new LongAdder();
    private final LongAdder slaBreaches   = new LongAdder();  // P99 exceeded

    private long windowStartMs = System.currentTimeMillis();

    public LatencyTracker(long expectedIntervalMs) {
        this.expectedIntervalMs = expectedIntervalMs;
        // Track values from 1ms to 10,000ms (10 seconds)
        this.histogram = new Histogram(
                TimeUnit.MILLISECONDS.toMicros(1),       // lowest discernible value: 1µs
                TimeUnit.SECONDS.toMicros(10),            // highest trackable value: 10s
                3                                         // significant value digits
        );
    }

    /**
     * Record a latency sample (milliseconds).
     *
     * recordValueWithExpectedInterval: if latencyMs > expectedIntervalMs,
     * automatically inserts phantom samples to correct for coordinated omission.
     */
    public void record(long latencyMs) {
        long latencyUs = TimeUnit.MILLISECONDS.toMicros(latencyMs);
        histogram.recordValueWithExpectedInterval(
                latencyUs,
                TimeUnit.MILLISECONDS.toMicros(expectedIntervalMs)
        );
        totalRequests.increment();

        // SLA breach detection every 100 requests
        long count = totalRequests.sum();
        if (count % 100 == 0) {
            checkSla();
        }
    }

    /**
     * Check current P99 against SLA target.
     * In production this would trigger an alert / circuit-breaker.
     */
    public void checkSla() {
        long p99Ms = getP99Ms();
        long p95Ms = getP95Ms();

        if (p99Ms > SLA_P99_MS) {
            slaBreaches.increment();
            log.warn("⚠️  SLA BREACH: P99={}ms exceeds target {}ms | P95={}ms | requests={}",
                    p99Ms, SLA_P99_MS, p95Ms, totalRequests.sum());
        }
    }

    // ── Percentile accessors ──────────────────────────────────────────────────

    public long getP50Ms() {
        return TimeUnit.MICROSECONDS.toMillis(histogram.getValueAtPercentile(50.0));
    }
    public long getP95Ms() {
        return TimeUnit.MICROSECONDS.toMillis(histogram.getValueAtPercentile(95.0));
    }
    public long getP99Ms() {
        return TimeUnit.MICROSECONDS.toMillis(histogram.getValueAtPercentile(99.0));
    }
    public long getP999Ms() {
        return TimeUnit.MICROSECONDS.toMillis(histogram.getValueAtPercentile(99.9));
    }
    public long getMaxMs() {
        return TimeUnit.MICROSECONDS.toMillis(histogram.getMaxValue());
    }
    public long getMeanMs() {
        return (long) TimeUnit.MICROSECONDS.toMillis((long) histogram.getMean());
    }

    public long getTotalRequests() { return totalRequests.sum();  }
    public long getSlaBreaches()   { return slaBreaches.sum();    }

    /**
     * Print a formatted SLA report to stdout.
     * Shows each percentile vs its target with PASS/FAIL indicator.
     */
    public void printReport(String engineName) {
        long elapsedMs  = System.currentTimeMillis() - windowStartMs;
        double throughput = totalRequests.sum() * 1000.0 / Math.max(1, elapsedMs);

        System.out.println();
        System.out.println("╔══════════════════════════════════════════════════════════╗");
        System.out.printf ("║  LATENCY REPORT  ─  %-36s║%n", engineName);
        System.out.println("╠══════════════════════════════════════════════════════════╣");
        System.out.printf ("║  Requests processed : %-34d║%n", totalRequests.sum());
        System.out.printf ("║  Throughput         : %-30.1f req/s ║%n", throughput);
        System.out.printf ("║  SLA breaches (P99) : %-34d║%n", slaBreaches.sum());
        System.out.println("╠══════════════════════════════════════════════════════════╣");
        System.out.printf ("║  %-10s  %8s  %8s  %8s                ║%n",
                "Percentile", "Actual", "Target", "Status");
        System.out.println("╠══════════════════════════════════════════════════════════╣");
        printRow("P50",   getMeanMs(), SLA_P50_MS);
        printRow("P95",   getP95Ms(),  SLA_P95_MS);
        printRow("P99",   getP99Ms(),  SLA_P99_MS);
        printRow("P99.9", getP999Ms(), SLA_P999_MS);
        System.out.printf ("║  %-10s  %7dms  %8s  %8s                ║%n",
                "MAX", getMaxMs(), "—", "");
        System.out.println("╚══════════════════════════════════════════════════════════╝");
    }

    private void printRow(String label, long actualMs, long targetMs) {
        String status = actualMs <= targetMs ? "✅ PASS" : "❌ FAIL";
        System.out.printf("║  %-10s  %7dms  %7dms  %-8s              ║%n",
                label, actualMs, targetMs, status);
    }

    public void reset() {
        histogram.reset();
        totalRequests.reset();
        slaBreaches.reset();
        windowStartMs = System.currentTimeMillis();
    }
}
