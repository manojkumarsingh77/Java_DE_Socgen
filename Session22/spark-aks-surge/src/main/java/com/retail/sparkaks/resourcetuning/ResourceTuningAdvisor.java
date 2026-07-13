package com.retail.sparkaks.resourcetuning;

/**
 * Encapsulates the resource-tuning MATH that a Spark-on-K8s operator has to
 * get right before Black Friday, so the arithmetic lives in one place
 * instead of being hidden inside a demo script.
 *
 * TEACHING POINTS driven by this class:
 *  - {@link #memoryOverheadMb(int)}: spark.executor.memoryOverhead default
 *    formula = max(384Mi, 0.10 * executorMemory).
 *  - {@link #executorsPerNode(int, int, int, int)}: the classic "5 cores per
 *    executor" rule of thumb and why huge executors (all node cores in one
 *    JVM) hurt you (long GC pauses, poor HDFS/ADLS throughput parallelism).
 *  - {@link #totalPodMemoryMb(int)}: request/limit the SCHEDULER sees, which
 *    is what actually decides whether the pod fits + what the OOMKiller
 *    enforces (not JVM -Xmx alone).
 */
public final class ResourceTuningAdvisor {

    private ResourceTuningAdvisor() {}

    /** spark.executor.memoryOverhead default: max(384Mi, executorMemory * 0.10). */
    public static int memoryOverheadMb(int executorHeapMb) {
        return Math.max(384, (int) Math.round(executorHeapMb * 0.10));
    }

    /** What the pod actually requests/limits from Kubernetes. */
    public static int totalPodMemoryMb(int executorHeapMb) {
        return executorHeapMb + memoryOverheadMb(executorHeapMb);
    }

    /**
     * How many executors fit per node for a given per-executor core count,
     * reserving a slice of the node for the OS/kubelet/daemonsets.
     */
    public static int executorsPerNode(int nodeMilliCpu, int nodeMemoryMb,
                                       int coresPerExecutor, int executorHeapMb) {
        int reserveMilliCpu = 500;     // kubelet/system-reserved headroom
        int reserveMemoryMb = 1024;
        int usableCpu = nodeMilliCpu - reserveMilliCpu;
        int usableMem = nodeMemoryMb - reserveMemoryMb;
        int byCpu = usableCpu / (coresPerExecutor * 1000);
        int byMem = usableMem / totalPodMemoryMb(executorHeapMb);
        return Math.max(0, Math.min(byCpu, byMem));
    }

    /**
     * Returns a human-readable warning if the given sizing violates common
     * Spark tuning guidance, or null if the sizing looks healthy.
     */
    public static String sizingWarning(int coresPerExecutor, int executorsPerNode) {
        if (coresPerExecutor >= 8) {
            return "coresPerExecutor=" + coresPerExecutor
                    + " is too high -> long GC pauses, poor HDFS client throughput "
                    + "(rule of thumb: keep 4-5 cores/executor)";
        }
        if (executorsPerNode == 0) {
            return "executorsPerNode=0 -> requested pod size does not fit on this node at all";
        }
        if (coresPerExecutor == 1) {
            return "coresPerExecutor=1 -> too many tiny JVMs, overhead-heavy, low task parallelism per executor";
        }
        return null;
    }
}
