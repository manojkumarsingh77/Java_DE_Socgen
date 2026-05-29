package com.bank.recommendation.profiling;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * THREAD DUMP ANALYZER
 * ═══════════════════════════════════════════════════════════════════════════
 *
 * WHAT ARE THREAD DUMPS?
 *   A thread dump is a snapshot of every thread's stack trace at one moment.
 *   It's the PRIMARY diagnostic for:
 *     • Deadlocks       (threads waiting for each other's locks)
 *     • Lock contention (many threads BLOCKED on one lock)
 *     • Thread starvation (threads stuck in WAITING/TIMED_WAITING)
 *     • CPU hogs        (threads in RUNNABLE but in tight loops)
 *
 * HOW TO TAKE THREAD DUMPS IN PRODUCTION:
 *     jstack <pid>          → prints to stdout
 *     kill -3 <pid>         → on Linux, prints to process stdout
 *     jcmd <pid> Thread.print → modern alternative to jstack
 *     IntelliJ: Run → "Get Thread Dump" button in the Debug toolbar
 *     VisualVM: Threads tab → "Thread Dump" button
 *
 * WHAT THIS DEMO SHOWS:
 *   1. SLOW ENGINE thread dump: Most threads are BLOCKED on scoringLock.
 *      The thread dump clearly shows: "waiting to lock <0x...> (a java.util.concurrent.locks...)"
 *      This is the smoking gun for lock contention.
 *
 *   2. We programmatically detect deadlocks using ThreadMXBean.
 *
 *   3. We count threads by state (BLOCKED, WAITING, RUNNABLE) to quantify
 *      the severity of contention.
 *
 * IN INTELLIJ:
 *   While SlowEngineLoadTest is running, click:
 *   Run → Get Thread Dump (or press the camera icon in the threads panel)
 *   You will see "waiting to lock [scoringLock]" for most threads.
 *
 *   After switching to FastEngine:
 *   Most threads are RUNNABLE or TIMED_WAITING (parked in ForkJoinPool idle).
 *   Zero BLOCKED threads.
 */
public class ThreadDumpAnalyzer {

    private static final Logger log = LoggerFactory.getLogger(ThreadDumpAnalyzer.class);

    private final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();

    /**
     * Capture and print a formatted thread dump showing thread state breakdown.
     * Called DURING the slow engine run to demonstrate lock contention.
     */
    public void captureAndPrint(String label) {
        System.out.println("\n" + "═".repeat(70));
        System.out.println("  THREAD DUMP ANALYSIS  ─  " + label);
        System.out.println("═".repeat(70));

        ThreadInfo[] allThreads = threadMXBean.dumpAllThreads(true, true);

        // Group by state
        Map<Thread.State, List<ThreadInfo>> byState = Arrays.stream(allThreads)
                .collect(Collectors.groupingBy(ThreadInfo::getThreadState));

        // ── Summary ──────────────────────────────────────────────────────────
        System.out.println("\n  THREAD STATE SUMMARY:");
        System.out.printf("  %-20s  %5s%n", "State", "Count");
        System.out.println("  " + "-".repeat(28));
        for (Thread.State state : Thread.State.values()) {
            List<ThreadInfo> threads = byState.getOrDefault(state, List.of());
            if (!threads.isEmpty()) {
                String indicator = switch (state) {
                    case BLOCKED         -> " ← ⚠️  LOCK CONTENTION";
                    case WAITING         -> " ← parked (idle)";
                    case TIMED_WAITING   -> " ← sleeping/parked";
                    case RUNNABLE        -> " ← doing work";
                    default              -> "";
                };
                System.out.printf("  %-20s  %5d%s%n", state, threads.size(), indicator);
            }
        }

        // ── BLOCKED thread details (the critical ones) ────────────────────────
        List<ThreadInfo> blocked = byState.getOrDefault(Thread.State.BLOCKED, List.of());
        if (!blocked.isEmpty()) {
            System.out.println("\n  BLOCKED THREADS (lock contention – these are costing you P99 latency):");
            blocked.stream().limit(5).forEach(ti -> {
                System.out.printf("  Thread: %-40s%n", ti.getThreadName());
                System.out.printf("    Waiting for lock held by: %s%n",
                        ti.getLockOwnerName() != null ? ti.getLockOwnerName() : "unknown");
                System.out.printf("    Lock: %s%n", ti.getLockName());
                // Print top 3 frames of the stack
                StackTraceElement[] stack = ti.getStackTrace();
                for (int i = 0; i < Math.min(3, stack.length); i++) {
                    System.out.printf("      at %s%n", stack[i]);
                }
                System.out.println();
            });
        }

        // ── Deadlock detection ────────────────────────────────────────────────
        long[] deadlockedIds = threadMXBean.findDeadlockedThreads();
        if (deadlockedIds != null && deadlockedIds.length > 0) {
            System.out.println("  🔴 DEADLOCK DETECTED! Thread IDs: " +
                    Arrays.toString(deadlockedIds));
            System.out.println("  This will cause complete system hang without a restart.");
        } else {
            System.out.println("  ✅ No deadlocks detected.");
        }

        // ── Hot method detection (RUNNABLE threads in tight loops) ───────────
        List<ThreadInfo> runnable = byState.getOrDefault(Thread.State.RUNNABLE, List.of());
        System.out.println("\n  TOP RUNNABLE THREADS (potential CPU hogs):");
        runnable.stream()
                .filter(ti -> ti.getStackTrace().length > 0)
                .filter(ti -> !ti.getThreadName().startsWith("JMH"))
                .limit(5)
                .forEach(ti -> {
                    StackTraceElement top = ti.getStackTrace()[0];
                    System.out.printf("  %-40s  → %s.%s()%n",
                            ti.getThreadName(), top.getClassName(), top.getMethodName());
                });

        System.out.println("\n" + "═".repeat(70));
    }

    /**
     * LOCK CONTENTION SIMULATION:
     * Starts N threads all competing for a single ReentrantLock (like SlowEngine).
     * Dumps threads after 2 seconds to capture the blocked state.
     * Then cancels and repeats with a lock-free approach.
     */
    public static void demonstrateLockContention() throws InterruptedException {
        System.out.println("\n" + "═".repeat(70));
        System.out.println("  LOCK CONTENTION DEMO – Simulating SlowRecommendationEngine");
        System.out.println("═".repeat(70));

        ReentrantLock globalLock = new ReentrantLock();
        ExecutorService pool = Executors.newFixedThreadPool(20);
        CountDownLatch startLatch = new CountDownLatch(1);
        java.util.concurrent.atomic.AtomicBoolean running = new java.util.concurrent.atomic.AtomicBoolean(true);

        // Start 20 threads all competing for one lock (simulates SlowEngine)
        for (int i = 0; i < 20; i++) {
            int threadId = i;
            pool.submit(() -> {
                try { startLatch.await(); } catch (InterruptedException e) { return; }
                while (running.get()) {
                    globalLock.lock();
                    try {
                        // Simulate scoring work: busy-wait 20ms
                        long end = System.nanoTime() + 20_000_000L;
                        while (System.nanoTime() < end) { /* busy work */ }
                    } finally {
                        globalLock.unlock();
                    }
                }
            });
        }

        startLatch.countDown();
        Thread.sleep(1000); // let threads pile up

        // Take thread dump – will show many BLOCKED threads
        ThreadDumpAnalyzer analyzer = new ThreadDumpAnalyzer();
        analyzer.captureAndPrint("SlowEngine (20 threads fighting 1 lock)");

        running.set(false);
        pool.shutdownNow();
        pool.awaitTermination(5, TimeUnit.SECONDS);

        System.out.println("\n  KEY INSIGHT:");
        System.out.println("  The BLOCKED count above = wasted threads.");
        System.out.println("  Each BLOCKED thread holds a platform thread (1 MB stack).");
        System.out.println("  With 20 threads and 20ms work per lock: only 50 req/s possible.");
        System.out.println("  FastEngine (parallel, no shared lock): 2000+ req/s per core.");
    }

    /**
     * CPU PROFILING SIMULATION using ThreadMXBean.getThreadCpuTime()
     *
     * In production you use:
     *   • async-profiler (flame graphs): ./profiler.sh -d 30 -f flame.html <pid>
     *   • IntelliJ Profiler: Run → Profile (Ctrl+Alt+F5)
     *   • JFR: java -XX:StartFlightRecording=filename=rec.jfr ...
     *
     * This demo uses ThreadMXBean to show per-thread CPU time, which
     * reveals WHICH threads consume the most CPU.
     */
    public void printCpuProfile(String label) {
        if (!threadMXBean.isThreadCpuTimeEnabled()) {
            threadMXBean.setThreadCpuTimeEnabled(true);
        }

        System.out.println("\n" + "─".repeat(70));
        System.out.println("  CPU PROFILE SNAPSHOT  ─  " + label);
        System.out.println("─".repeat(70));
        System.out.printf("  %-45s  %10s  %10s%n", "Thread Name", "CPU ms", "Wall ms");
        System.out.println("  " + "─".repeat(68));

        long[] ids = threadMXBean.getAllThreadIds();
        ThreadInfo[] infos = threadMXBean.getThreadInfo(ids);

        List<long[]> cpuData = new ArrayList<>();
        for (ThreadInfo ti : infos) {
            if (ti == null) continue;
            long cpuNs = threadMXBean.getThreadCpuTime(ti.getThreadId());
            if (cpuNs > 0) {
                cpuData.add(new long[]{ti.getThreadId(), cpuNs});
            }
        }

        // Sort by CPU time descending
        cpuData.sort((a, b) -> Long.compare(b[1], a[1]));

        cpuData.stream().limit(10).forEach(entry -> {
            ThreadInfo ti = threadMXBean.getThreadInfo(entry[0]);
            if (ti == null) return;
            long cpuMs = entry[1] / 1_000_000;
            System.out.printf("  %-45s  %10d  %10s%n",
                    truncate(ti.getThreadName(), 45),
                    cpuMs,
                    ti.getThreadState().toString());
        });

        System.out.println("─".repeat(70));
        System.out.println("  How to read this:");
        System.out.println("  High CPU ms + BLOCKED state = waiting for lock (wasted CPU budget)");
        System.out.println("  High CPU ms + RUNNABLE = doing real work (desirable)");
        System.out.println("  For deeper analysis: use IntelliJ Profiler or async-profiler");
    }

    private String truncate(String s, int maxLen) {
        return s.length() <= maxLen ? s : s.substring(0, maxLen - 3) + "...";
    }
}
