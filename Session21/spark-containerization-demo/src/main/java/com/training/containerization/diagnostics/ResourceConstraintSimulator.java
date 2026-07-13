package com.training.containerization.diagnostics;

import com.training.containerization.config.JobConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * === TOPIC: Resource Constraints ===
 * <p>
 * THE PROBLEM this class demonstrates:
 * A data platform job that is never tested under a resource ceiling behaves
 * unpredictably in production the moment Kubernetes/Docker enforces one - it may be
 * OOMKilled mid-shuffle, or silently throttled to a crawl by a CPU quota, with no
 * warning during development because a laptop has "unlimited" resources.
 * <p>
 * THE SOLUTION this class demonstrates:
 * Two controlled, observable simulations you re-run under different
 * `--memory` / `--cpus` values (see scripts/run-with-constraints.sh and
 * docker-compose.yml) so the class of failure becomes visible and discussable
 * BEFORE it happens unexpectedly with a real Spark shuffle at 2am in production.
 */
public class ResourceConstraintSimulator {

    /**
     * Progressively allocates memory and reports how much was reachable before
     * hitting the configured stop threshold OR a real OutOfMemoryError - whichever
     * comes first. Run this with a small container --memory limit and a matching
     * small -Xmx (via JAVA_OPTS) to see it stop gracefully; run it with a small
     * --memory limit but NO corresponding -Xmx cap to see the mismatch that leads
     * to a container-level OOMKill instead of a catchable JVM OutOfMemoryError.
     */
    public static void simulateMemoryPressure(JobConfig config) {
        long maxMemoryBytes = Runtime.getRuntime().maxMemory();
        long stopAtBytes = (long) (maxMemoryBytes * (config.memStressStopPercent / 100.0));
        int stepBytes = config.memStressStepMb * 1024 * 1024;

        System.out.println("\n================= MEMORY PRESSURE SIMULATION =================");
        System.out.printf("JVM heap ceiling (-Xmx effective): %.1f MB%n", maxMemoryBytes / (1024.0 * 1024));
        System.out.printf("Will stop deliberately at        : %.0f%% (%.1f MB) to avoid a hard crash%n",
                config.memStressStopPercent, stopAtBytes / (1024.0 * 1024));
        System.out.printf("Allocation step                  : %d MB%n", config.memStressStepMb);

        List<byte[]> ballast = new ArrayList<>();
        long allocated = 0;
        int step = 0;
        try {
            while (allocated < stopAtBytes) {
                ballast.add(new byte[stepBytes]);
                allocated += stepBytes;
                step++;
                long free = Runtime.getRuntime().freeMemory();
                System.out.printf("  step %-3d allocated=%6.1f MB   jvm.freeMemory()=%7.1f MB%n",
                        step, allocated / (1024.0 * 1024), free / (1024.0 * 1024));
                Thread.sleep(150); // slow down so it is visible in `docker stats`
            }
            System.out.println("Reached configured safety threshold WITHOUT an OutOfMemoryError.");
            System.out.println("=> Heap (-Xmx via MaxRAMPercentage) and container --memory are well aligned.");
        } catch (OutOfMemoryError oom) {
            System.out.println("!! Caught java.lang.OutOfMemoryError after allocating "
                    + String.format("%.1f MB", allocated / (1024.0 * 1024)));
            System.out.println("=> This is the GOOD outcome: the JVM heap limit was hit first, so the");
            System.out.println("   application got a catchable exception instead of being silently");
            System.out.println("   OOMKilled by the container runtime (which gives you no stack trace).");
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        } finally {
            ballast.clear();
        }
        System.out.println("==================================================================\n");
    }

    /**
     * Busy-loops on N threads for a fixed duration. Open `docker stats` (or
     * Activity Monitor / Task Manager) in a second terminal while this runs, and
     * compare CPU% under different `--cpus` values with CPU_STRESS_THREADS set
     * above and below the container's effective processor count.
     */
    public static void simulateCpuPressure(JobConfig config) {
        int threads = Math.max(1, config.cpuStressThreads);
        int durationSec = config.cpuStressDurationSec;

        System.out.println("\n================= CPU PRESSURE SIMULATION =================");
        System.out.println("JVM-visible availableProcessors() : " + Runtime.getRuntime().availableProcessors());
        System.out.println("Threads launched for this test    : " + threads);
        System.out.println("Duration                          : " + durationSec + "s");
        System.out.println("Watch `docker stats` in another terminal now ...");

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);
        long endAtMillis = System.currentTimeMillis() + (durationSec * 1000L);

        for (int t = 0; t < threads; t++) {
            pool.submit(() -> {
                long spins = 0;
                double x = 0.0001d;
                while (System.currentTimeMillis() < endAtMillis) {
                    // deliberately CPU-bound math, no I/O, no allocation to keep this a pure CPU test
                    x = Math.sqrt(x * x + 1.0000001);
                    spins++;
                }
                latch.countDown();
            });
        }

        try {
            latch.await(durationSec + 10L, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            pool.shutdownNow();
        }
        System.out.println("CPU pressure simulation complete.");
        System.out.println("=> If wall-clock time to finish grew noticeably as --cpus was lowered below");
        System.out.println("   the thread count, that is the CFS CPU quota throttling this container -");
        System.out.println("   the same mechanism that silently slows down under-provisioned Spark executors.");
        System.out.println("==================================================================\n");
    }
}
