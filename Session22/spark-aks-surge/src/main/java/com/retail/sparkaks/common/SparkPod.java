package com.retail.sparkaks.common;

/**
 * A Spark-on-Kubernetes pod: either the DRIVER or one EXECUTOR.
 *
 * Mirrors real spark-submit resource knobs:
 *   spark.driver.memory / spark.driver.cores
 *   spark.executor.memory / spark.executor.cores
 *   spark.executor.memoryOverhead (default = max(384Mi, 10% of executor.memory))
 *
 * requestMemoryMb (what the pod asks the scheduler for) therefore =
 *   executorMemory + memoryOverhead  (+ optional off-heap / pyspark mem, omitted for clarity)
 *
 * limitMemoryMb is normally set equal to the request in Spark's own pod
 * templates (Guaranteed QoS) - going over the LIMIT is what triggers an
 * OOMKill, not going over the JVM heap.
 */
public class SparkPod {

    public enum Role { DRIVER, EXECUTOR }

    private final String name;
    private final Role role;
    private final int executorId;             // 0 for driver
    private final int requestMilliCpu;
    private final int heapMemoryMb;            // -Xmx equivalent (spark.executor/driver.memory)
    private final int overheadMemoryMb;        // spark.executor.memoryOverhead
    private String nodeName;                   // null == Pending
    private boolean oomKilled = false;
    private int restartCount = 0;

    public SparkPod(String name, Role role, int executorId,
                    int requestMilliCpu, int heapMemoryMb, int overheadMemoryMb) {
        this.name = name;
        this.role = role;
        this.executorId = executorId;
        this.requestMilliCpu = requestMilliCpu;
        this.heapMemoryMb = heapMemoryMb;
        this.overheadMemoryMb = overheadMemoryMb;
    }

    /** Total memory REQUEST/LIMIT the scheduler sees = heap + overhead (Guaranteed QoS). */
    public int getRequestMemoryMb() { return heapMemoryMb + overheadMemoryMb; }
    public int getRequestMilliCpu() { return requestMilliCpu; }

    /**
     * Simulate feeding this pod a working-set size (e.g. a skewed shuffle
     * partition). If workingSetMb exceeds the pod's memory LIMIT, the
     * kubelet OOMKills the container - exactly like a real executor dying
     * on a data-skewed stage.
     */
    public boolean simulateMemoryLoad(int workingSetMb) {
        if (workingSetMb > getRequestMemoryMb()) {
            oomKilled = true;
            restartCount++;
            nodeName = null; // evicted; will need rescheduling
            return false;
        }
        return true;
    }

    public void clearOom() { oomKilled = false; }

    public boolean isPending() { return nodeName == null; }

    public String getName()          { return name; }
    public Role getRole()            { return role; }
    public int getExecutorId()       { return executorId; }
    public int getHeapMemoryMb()     { return heapMemoryMb; }
    public int getOverheadMemoryMb() { return overheadMemoryMb; }
    public String getNodeName()      { return nodeName; }
    public void setNodeName(String n){ this.nodeName = n; }
    public boolean isOomKilled()     { return oomKilled; }
    public int getRestartCount()     { return restartCount; }

    @Override
    public String toString() {
        String status = oomKilled ? "OOMKilled(restart#" + restartCount + ")"
                : nodeName == null ? "PENDING" : "Running@" + nodeName;
        return "%-16s role=%-8s cpu=%4dm heap=%5dMi ovh=%4dMi -> %s".formatted(
                name, role, requestMilliCpu, heapMemoryMb, overheadMemoryMb, status);
    }
}
