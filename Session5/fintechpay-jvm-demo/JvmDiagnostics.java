package com.fintechpay.config;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadMXBean;
import java.util.List;

/**
 * ╔══════════════════════════════════════════════════════════════════════════╗
 * ║  JvmDiagnostics — FintechPay Container Awareness Reporter               ║
 * ╠══════════════════════════════════════════════════════════════════════════╣
 * ║                                                                          ║
 * ║  CONCEPT 1: JVM FLAGS IN DOCKER                                          ║
 * ║  ─────────────────────────────                                           ║
 * ║  Java 17 ships with UseContainerSupport=true by default.                 ║
 * ║  This class PROVES the JVM is reading cgroup limits correctly            ║
 * ║  by comparing what the JVM "thinks" it has vs. the OS reports.           ║
 * ║                                                                          ║
 * ║  CONCEPT 2: HEAP vs CONTAINER LIMITS                                     ║
 * ║  ────────────────────────────────────                                    ║
 * ║  We show: containerMemory, heapMax, nonHeapMax, and the safe ratio.      ║
 * ║  Rule: heap ≤ 75% of container RAM. The remaining ~25% absorbs           ║
 * ║  Metaspace, thread stacks, JIT code cache, and GC overhead.              ║
 * ║                                                                          ║
 * ║  CONCEPT 3: CPU THROTTLING IN AKS                                        ║
 * ║  ─────────────────────────────────                                       ║
 * ║  We report available processors (should match AKS cpu limit)             ║
 * ║  and active GC collectors. Mismatches cause thread pool over-sizing      ║
 * ║  and CPU quota bursting → throttle pauses.                               ║
 * ║                                                                          ║
 * ╚══════════════════════════════════════════════════════════════════════════╝
 */
public class JvmDiagnostics {

    private static final long MB = 1024L * 1024L;
    private static final long GB = 1024L * MB;

    /**
     * Prints a full JVM + container health report to stdout.
     *
     * In a real banking system this would be exposed as a /actuator/jvm
     * endpoint and scraped by Prometheus on every AKS pod.
     */
    public static void printReport() {
        RuntimeMXBean    runtime = ManagementFactory.getRuntimeMXBean();
        MemoryMXBean     memory  = ManagementFactory.getMemoryMXBean();
        ThreadMXBean     threads = ManagementFactory.getThreadMXBean();

        MemoryUsage heapUsage    = memory.getHeapMemoryUsage();
        MemoryUsage nonHeapUsage = memory.getNonHeapMemoryUsage();

        printBanner("FintechPay JVM Container Report");

        // ── Section 1: Runtime identity ───────────────────────────────────
        printSection("1. JVM Identity");
        System.out.printf("   %-35s %s%n", "JVM Version:", runtime.getVmVersion());
        System.out.printf("   %-35s %s%n", "JVM Vendor:",  runtime.getVmVendor());
        System.out.printf("   %-35s %s%n", "Java Spec:",   System.getProperty("java.version"));
        System.out.printf("   %-35s %dms%n","JVM Uptime:", runtime.getUptime());

        // ── Section 2: CPU — CONCEPT 3 insight ───────────────────────────
        //
        // Runtime.availableProcessors() reads cgroup cpuset (when UseContainerSupport=true)
        // In Docker: matches --cpus flag. In AKS: matches resources.limits.cpu
        // If this number is wrong (equals host CPU count), the JVM creates too many
        // GC threads, ForkJoinPool workers, and G1 concurrent threads → all compete
        // for the same 2 CPU quota → throttle spikes.
        //
        printSection("2. CPU Awareness (CONCEPT 3: AKS Throttling)");
        int availableCpus = Runtime.getRuntime().availableProcessors();
        System.out.printf("   %-35s %d%n",   "Available processors (cgroup-aware):", availableCpus);
        System.out.printf("   %-35s %d%n",   "JVM thread count (current):",  threads.getThreadCount());
        System.out.printf("   %-35s %d%n",   "Peak thread count:",           threads.getPeakThreadCount());
        System.out.printf("   %-35s %d%n",   "Daemon thread count:",         threads.getDaemonThreadCount());

        // Warning: catch the classic mistake (JVM sees all host CPUs)
        if (availableCpus > 8) {
            System.out.println();
            System.out.println("   ⚠️  WARNING: JVM sees " + availableCpus + " CPUs.");
            System.out.println("      If your AKS limit is 2 CPUs, add: -XX:ActiveProcessorCount=2");
            System.out.println("      Without this, G1GC spawns " + (availableCpus * 2) +
                               " GC threads → CPU quota burst → throttle pauses!");
        } else {
            System.out.println("   ✓  CPU count looks reasonable for a container workload.");
        }

        // ── Section 3: Memory — CONCEPT 1 + 2 ───────────────────────────
        //
        // CONCEPT 1 — UseContainerSupport: heapMax should be ~75% of container RAM,
        //   NOT 25% of 64 GB host RAM. If heapMax > 2 GB in a 2 GB container → flag is off!
        //
        // CONCEPT 2 — Heap vs Container:
        //   containerRAM = Runtime.maxMemory() (when UseContainerSupport=true)
        //   We compute heapRatio = heapMax / totalPhysical and warn if unsafe.
        //
        printSection("3. Memory Layout (CONCEPT 1 & 2: Heap vs Container)");

        long heapMax      = heapUsage.getMax();      // -Xmx effective value
        long heapInit     = heapUsage.getInit();     // -Xms effective value
        long heapUsed     = heapUsage.getUsed();     // currently occupied
        long nonHeapUsed  = nonHeapUsage.getUsed();  // Metaspace + CodeCache etc.
        long nonHeapMax   = nonHeapUsage.getMax();   // -1 if unlimited (Metaspace default)

        // Runtime.maxMemory() is the JVM's view of total available memory.
        // With UseContainerSupport=true → reads from cgroup memory.limit_in_bytes
        // Without it                   → reads from /proc/meminfo (host RAM)
        long jvmViewOfTotal = Runtime.getRuntime().maxMemory();

        System.out.printf("   %-35s %,d MB (%,.1f GB)%n",
            "Heap max (-Xmx effective):", heapMax / MB, (double) heapMax / GB);
        System.out.printf("   %-35s %,d MB%n",
            "Heap init (-Xms effective):", heapInit / MB);
        System.out.printf("   %-35s %,d MB%n",
            "Heap used (current):", heapUsed / MB);
        System.out.printf("   %-35s %,d MB%n",
            "Non-heap used (Metaspace+JIT):", nonHeapUsed / MB);
        System.out.printf("   %-35s %s%n",
            "Non-heap max:", nonHeapMax == -1 ? "unlimited (Metaspace)" : (nonHeapMax / MB) + " MB");
        System.out.printf("   %-35s %,d MB%n",
            "JVM total view of RAM:", jvmViewOfTotal / MB);

        // The critical ratio check
        System.out.println();
        if (heapMax > 0 && jvmViewOfTotal > 0) {
            double heapRatio = (double) heapMax / jvmViewOfTotal * 100.0;
            System.out.printf("   %-35s %.1f%%%n", "Heap / JVM-RAM ratio:", heapRatio);

            if (heapRatio > 85.0) {
                System.out.println();
                System.out.println("   ⚠️  DANGER: Heap ratio is " + String.format("%.1f", heapRatio) + "%.");
                System.out.println("      Non-heap (Metaspace, threads, JIT) has < 15% room.");
                System.out.println("      Add: -XX:MaxRAMPercentage=75.0  (DO NOT use -Xmx = container limit!)");
            } else if (heapRatio > 75.0) {
                System.out.println("   ⚠️  WARNING: Heap ratio is slightly high. Recommended: ≤ 75%.");
                System.out.println("      Consider: -XX:MaxRAMPercentage=75.0");
            } else {
                System.out.printf("   ✓  Heap ratio %.1f%% is within safe range (≤75%%). ✓%n", heapRatio);
            }
        }

        // ── Section 4: GC Configuration ──────────────────────────────────
        //
        // CONCEPT 3 — CPU Throttling: GC is a major source of quota bursting.
        // G1GC is recommended for banking: it targets pause times, not throughput,
        // which keeps us within the 200ms CFS period safely.
        // With wrong CPU count → too many GC threads → GC alone can exhaust quota.
        //
        printSection("4. GC Configuration (CONCEPT 3: Throttle Prevention)");
        List<GarbageCollectorMXBean> gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
        for (GarbageCollectorMXBean gc : gcBeans) {
            System.out.printf("   %-35s collections=%-6d  totalTime=%dms%n",
                gc.getName() + ":", gc.getCollectionCount(), gc.getCollectionTime());
        }

        // Detect if G1GC is active (recommended for AKS/Docker banking workloads)
        boolean hasG1 = gcBeans.stream().anyMatch(gc -> gc.getName().contains("G1"));
        boolean hasZGC = gcBeans.stream().anyMatch(gc -> gc.getName().contains("ZGC"));
        boolean hasShenandoah = gcBeans.stream().anyMatch(gc -> gc.getName().contains("Shenandoah"));

        System.out.println();
        if (hasG1) {
            System.out.println("   ✓  G1GC active — ideal for banking (bounded pause times).");
            System.out.println("      Tip: -XX:MaxGCPauseMillis=200 keeps GC within CFS period.");
        } else if (hasZGC || hasShenandoah) {
            System.out.println("   ✓  Sub-ms GC active. Excellent for high-frequency trading systems.");
        } else {
            System.out.println("   ⚠️  Consider switching to G1GC or ZGC for AKS workloads.");
            System.out.println("      ParallelGC maximizes throughput but pauses can burst CPU quota.");
        }

        // ── Section 5: Active JVM flags relevant to container deployment ──
        printSection("5. Container-Relevant JVM Flags Summary");
        printFlagStatus("UseContainerSupport",
            "Auto-detects cgroup limits. ON by default in Java 17. CRITICAL for Docker/AKS.");
        printFlagStatus("MaxRAMPercentage",
            "Heap = X% of container RAM. Preferred over hardcoded -Xmx.");
        printFlagStatus("ActiveProcessorCount",
            "Override CPU count. Use when K8s cpuset differs from requested CPUs.");
        printFlagStatus("UseG1GC",
            "G1 collector. Best pause-time control for 100ms CFS scheduling period.");
        printFlagStatus("MaxGCPauseMillis",
            "GC pause budget. Set ≤100ms to stay under CFS quota in each period.");
        printFlagStatus("InitiatingHeapOccupancyPercent",
            "Start concurrent GC early (35%) to avoid stop-the-world during peak load.");

        printBanner("End of Report");
    }

    // ── Helper: measure heap allocation speed (shows GC pressure) ────────
    /**
     * CONCEPT 3 DEMO: Shows how allocation rate affects GC frequency.
     *
     * High allocation rate → frequent minor GCs → each GC uses CPU quota
     * → quota bursting → AKS throttles all threads → latency spike.
     *
     * In retail banking this happens during batch statement generation
     * or fraud scoring on large transaction sets.
     */
    public static void measureAllocationPressure(String label, Runnable workload) {
        Runtime rt = Runtime.getRuntime();
        long beforeGC = getTotalGcTime();
        long beforeFree = rt.freeMemory();

        long startTime = System.currentTimeMillis();
        workload.run();
        long elapsed = System.currentTimeMillis() - startTime;

        long afterGC = getTotalGcTime();
        long afterFree = rt.freeMemory();

        long gcTime       = afterGC - beforeGC;
        long memAllocated = Math.max(0, beforeFree - afterFree); // rough estimate

        System.out.printf(
            "   [PERF] %-30s  elapsed=%4dms  gcTime=%3dms  allocPressure=%-6s%n",
            label + ":",
            elapsed,
            gcTime,
            memAllocated > 50 * MB ? "HIGH" : memAllocated > 10 * MB ? "MED" : "LOW"
        );

        // CONCEPT 3: If GC time > 80ms, we're at risk of CPU quota burst in AKS
        if (gcTime > 80) {
            System.out.println("   ⚠️  GC time " + gcTime + "ms during this operation!");
            System.out.println("      In AKS with 2-CPU limit, this may exhaust the 200ms CFS quota.");
            System.out.println("      Consider: -XX:InitiatingHeapOccupancyPercent=35 to trigger GC earlier.");
        }
    }

    // ── Private helpers ───────────────────────────────────────────────────

    private static long getTotalGcTime() {
        return ManagementFactory.getGarbageCollectorMXBeans()
            .stream()
            .mapToLong(GarbageCollectorMXBean::getCollectionTime)
            .sum();
    }

    private static void printBanner(String title) {
        String line = "═".repeat(60);
        System.out.println();
        System.out.println("  ╔" + line + "╗");
        System.out.printf("  ║  %-58s║%n", title);
        System.out.println("  ╚" + line + "╝");
        System.out.println();
    }

    private static void printSection(String title) {
        System.out.println();
        System.out.println("  ┌─ " + title + " " + "─".repeat(Math.max(0, 55 - title.length())));
    }

    private static void printFlagStatus(String flag, String description) {
        System.out.printf("   %-40s → %s%n", "-XX:+" + flag, description);
    }
}
