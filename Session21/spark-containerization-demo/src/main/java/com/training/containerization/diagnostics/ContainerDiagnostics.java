package com.training.containerization.diagnostics;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * === TOPIC: JVM Tuning in Containers ===
 * <p>
 * THE PROBLEM this class demonstrates:
 * Historically (pre JDK 10, and even today if the flag is disabled), the JVM reads
 * CPU count / memory size from the HOST kernel (/proc/cpuinfo, /proc/meminfo), not
 * from the cgroup limits Docker/Kubernetes applied to the container. Result: the JVM
 * sizes its default heap and common-fork-join-pool as if it owns the whole machine,
 * then gets OOM-killed by the container runtime the moment it actually tries to use
 * that memory - even though "java -Xmx" was never explicitly exceeded.
 * <p>
 * THE SOLUTION this class demonstrates:
 * 1. Java 17 ships -XX:+UseContainerSupport ENABLED by default, so
 *    Runtime.availableProcessors() and default heap sizing already respect cgroup
 *    limits out of the box.
 * 2. For predictable behaviour under real workloads (Spark included) we still
 *    explicitly set -XX:MaxRAMPercentage / -XX:InitialRAMPercentage /
 *    -XX:ActiveProcessorCount rather than relying purely on defaults - this method
 *    prints the active flags so learners can SEE those settings take effect when
 *    docker-compose.yml changes JAVA_OPTS.
 * <p>
 * Run this with different `docker run --memory=` values (see scripts/run-with-constraints.sh)
 * and re-observe the output to make the effect concrete.
 */
public class ContainerDiagnostics {

    public static void printReport() {
        System.out.println("\n================= CONTAINER / JVM DIAGNOSTICS =================");
        printRuntimeView();
        printActiveJvmFlags();
        printCgroupView();
        printContainerDetection();
        System.out.println("==================================================================\n");
    }

    /** What the JVM itself believes it has to work with. */
    private static void printRuntimeView() {
        Runtime rt = Runtime.getRuntime();
        System.out.println("-- JVM's own view (Runtime) --");
        System.out.println("  Java version               : " + Runtime.version());
        System.out.println("  availableProcessors()       : " + rt.availableProcessors());
        System.out.printf ("  maxMemory() (i.e. -Xmx eff.): %.1f MB%n", rt.maxMemory() / (1024.0 * 1024));
        System.out.printf ("  totalMemory() (heap now)    : %.1f MB%n", rt.totalMemory() / (1024.0 * 1024));
        System.out.printf ("  freeMemory()                : %.1f MB%n", rt.freeMemory() / (1024.0 * 1024));

        Object osBean = ManagementFactory.getOperatingSystemMXBean();
        if (osBean instanceof com.sun.management.OperatingSystemMXBean sunOsBean) {
            System.out.printf("  Total physical memory (OS) : %.1f MB%n",
                    sunOsBean.getTotalMemorySize() / (1024.0 * 1024));
            System.out.printf("  Free physical memory (OS)  : %.1f MB%n",
                    sunOsBean.getFreeMemorySize() / (1024.0 * 1024));
            System.out.printf("  Process CPU load           : %.2f%%%n", sunOsBean.getProcessCpuLoad() * 100);
        }
    }

    /** The JVM flags actually in effect for this process - proves JAVA_OPTS was applied. */
    private static void printActiveJvmFlags() {
        System.out.println("\n-- Active JVM input arguments (from JAVA_OPTS / run config) --");
        RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        List<String> args = runtimeMXBean.getInputArguments();
        if (args.isEmpty()) {
            System.out.println("  (none set - JVM is running on 100% ergonomic defaults)");
        } else {
            args.forEach(a -> System.out.println("  " + a));
        }
    }

    /** What the LINUX CGROUP actually enforces - the ground truth Docker/K8s applies. */
    private static void printCgroupView() {
        System.out.println("\n-- Cgroup enforced limits (ground truth on Linux containers) --");

        // cgroup v2 (modern Docker Desktop / most current Linux distros)
        String v2Mem = readFirstLine("/sys/fs/cgroup/memory.max");
        String v2Cpu = readFirstLine("/sys/fs/cgroup/cpu.max");
        if (v2Mem != null || v2Cpu != null) {
            System.out.println("  cgroup version              : v2");
            System.out.println("  memory.max                  : " + describeMemLimit(v2Mem));
            System.out.println("  cpu.max                     : " + (v2Cpu != null ? v2Cpu : "n/a"));
            return;
        }

        // cgroup v1 (older Docker / some CI runners)
        String v1Mem = readFirstLine("/sys/fs/cgroup/memory/memory.limit_in_bytes");
        String v1CpuQuota = readFirstLine("/sys/fs/cgroup/cpu/cpu.cfs_quota_us");
        String v1CpuPeriod = readFirstLine("/sys/fs/cgroup/cpu/cpu.cfs_period_us");
        if (v1Mem != null) {
            System.out.println("  cgroup version               : v1");
            System.out.println("  memory.limit_in_bytes        : " + describeMemLimit(v1Mem));
            System.out.println("  cpu.cfs_quota_us/period_us   : " + v1CpuQuota + " / " + v1CpuPeriod);
            return;
        }

        System.out.println("  No cgroup memory/cpu files found.");
        System.out.println("  Expected on: bare host machine, or macOS/Windows running Docker Desktop");
        System.out.println("  (Docker Desktop runs containers inside a hidden Linux VM - cgroup files");
        System.out.println("   exist INSIDE that VM's containers, not on the host JVM you launch from IntelliJ).");
    }

    private static String describeMemLimit(String raw) {
        if (raw == null) return "n/a";
        if (raw.equals("max") || raw.equals("-1")) return raw + "  (== unlimited, no container memory cap applied)";
        try {
            long bytes = Long.parseLong(raw.trim());
            return String.format("%s bytes  (%.1f MB)", raw, bytes / (1024.0 * 1024));
        } catch (NumberFormatException e) {
            return raw;
        }
    }

    private static void printContainerDetection() {
        boolean dockerEnvFile = Files.exists(Path.of("/.dockerenv"));
        boolean cgroupMentionsContainer = false;
        String cgroupContents = readFirstLine("/proc/1/cgroup");
        if (cgroupContents != null) {
            cgroupMentionsContainer = cgroupContents.contains("docker") || cgroupContents.contains("kubepods");
        }
        System.out.println("\n-- Are we actually inside a container? --");
        System.out.println("  /.dockerenv present          : " + dockerEnvFile);
        System.out.println("  /proc/1/cgroup mentions docker/kubepods : " + cgroupMentionsContainer);
        System.out.println("  => Running inside a container: " + (dockerEnvFile || cgroupMentionsContainer));
    }

    private static String readFirstLine(String path) {
        try {
            List<String> lines = Files.readAllLines(Path.of(path));
            return lines.isEmpty() ? null : lines.get(0);
        } catch (IOException | RuntimeException e) {
            return null;
        }
    }
}
