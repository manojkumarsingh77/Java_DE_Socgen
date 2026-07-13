package com.retail.sparkaks.resourcetuning;

import com.retail.sparkaks.common.AksNode;
import com.retail.sparkaks.common.Banner;
import com.retail.sparkaks.common.SparkPod;
import com.retail.sparkaks.sparkpods.SparkJob;

import java.util.ArrayList;
import java.util.List;

/**
 * TOPIC 2: RESOURCE TUNING
 *
 * Retail story: "order-fraud-scoring" batch job joins Black-Friday orders
 * against a customer-history table. One customer segment (gift-card resale
 * fraud ring) skews one partition to 6x normal size. Under-sized executors
 * die with OOMKilled; correctly-sized ones absorb the skew.
 */
public class ResourceTuningDemo {

    public static void run() {
        Banner.topic(2, "RESOURCE TUNING - surviving a skewed Black Friday join");

        Banner.problem(
                "'order-fraud-scoring' was tuned once, months ago, for average-day volume:",
                "- 16 executors x 1 core x 2GB heap ('more executors = more parallelism', they said).",
                "- On Black Friday, one join key (gift-card fraud ring) skews to 4x the average",
                "  partition size -> that task's working set blows past the pod memory limit.",
                "- Executors get OOMKilled, Spark retries the stage, it OOMs again, job stalls.");

        Banner.requirement(
                "R1: Executor sizing must follow a defensible formula, not folklore.",
                "R2: spark.executor.memoryOverhead must be budgeted, not forgotten.",
                "R3: Sizing must tolerate a realistic skew factor for the busiest partitions.",
                "R4: Avoid pathological executor shapes (1 core JVMs, or 1 giant JVM per node).");

        Banner.solution(
                "Use ResourceTuningAdvisor to compute sizing BEFORE submit, then verify with a",
                "simulated OOM test using real skew numbers:",
                "  ResourceTuningAdvisor.memoryOverheadMb()   -> max(384Mi, 10% heap)",
                "  ResourceTuningAdvisor.totalPodMemoryMb()   -> what the scheduler/OOMKiller enforce",
                "  ResourceTuningAdvisor.executorsPerNode()   -> cores/memory packing per node",
                "  ResourceTuningAdvisor.sizingWarning()      -> flags anti-patterns automatically",
                "  SparkPod.simulateMemoryLoad(workingSetMb)  -> models the OOMKill itself");

        Banner.demo("Node shape: Standard_D16s_v5 (16 vCPU, 64GiB) x 3 nodes.");
        int nodeMilliCpu = 16000, nodeMemoryMb = 64000;
        List<AksNode> cluster = new ArrayList<>();
        for (int i = 1; i <= 3; i++) cluster.add(new AksNode("spark-node-" + i, nodeMilliCpu, nodeMemoryMb));

        int averagePartitionMb = 1800; // "typical" shuffle partition on a normal day
        int skewedPartitionMb = averagePartitionMb * 4; // Black Friday fraud-ring skew (4x)

        Banner.step("Config A (as inherited): 16 executors x 1 core x 2048Mi heap.");
        runConfig(cluster, "order-fraud-scoring-A", 1, 2048, 16, skewedPartitionMb, averagePartitionMb);

        Banner.step("Config B (tuned): 4 executors x 4 cores x 8192Mi heap - same total resources.");
        runConfig(cluster, "order-fraud-scoring-B", 4, 8192, 4, skewedPartitionMb, averagePartitionMb);

        Banner.keyTakeaway(
                "Same TOTAL cluster resources, radically different resilience: fewer/bigger",
                "executors give each task more headroom to absorb a skewed partition.",
                "Always budget memoryOverhead explicitly; the scheduler/OOMKiller enforces",
                "heap+overhead, not your -Xmx alone.",
                "Rule of thumb: 4-5 cores/executor; avoid 1-core JVMs and single-JVM-per-node.",
                "Long-term fix for skew itself is salting/AQE skew-join, not just bigger pods -",
                "but right-sizing buys you the time to ship that fix without an outage.");
        Banner.pause();
    }

    private static void runConfig(List<AksNode> cluster, String appName,
                                  int coresPerExecutor, int heapMb, int totalExecutors,
                                  int skewedPartitionMb, int averagePartitionMb) {

        cluster.forEach(n -> n.getPods().clear());

        int overhead = ResourceTuningAdvisor.memoryOverheadMb(heapMb);
        int podMem = ResourceTuningAdvisor.totalPodMemoryMb(heapMb);
        int perNode = ResourceTuningAdvisor.executorsPerNode(
                cluster.get(0).getAllocatableMilliCpu(), cluster.get(0).getAllocatableMemoryMb(),
                coresPerExecutor, heapMb);

        Banner.out("memoryOverhead = %dMi  =>  pod request/limit = %dMi".formatted(overhead, podMem));
        Banner.out("executorsPerNode (packing check) = " + perNode);
        String warning = ResourceTuningAdvisor.sizingWarning(coresPerExecutor, perNode);
        if (warning != null) Banner.warn(warning); else Banner.out("Sizing looks healthy (no anti-pattern flagged).");

        SparkJob job = new SparkJob(appName, 1000, 2048, 384,
                coresPerExecutor * 1000, heapMb, overhead);
        job.schedule(job.createDriverPod(), cluster);
        List<SparkPod> execs = job.createExecutorPods(totalExecutors);
        execs.forEach(e -> job.schedule(e, cluster));

        Banner.out("Feeding average partitions (%dMi) to all but one executor...".formatted(averagePartitionMb));
        for (int i = 0; i < execs.size() - 1; i++) execs.get(i).simulateMemoryLoad(averagePartitionMb);

        Banner.out("Feeding the SKEWED fraud-ring partition (%dMi) to the last executor...".formatted(skewedPartitionMb));
        SparkPod hot = execs.get(execs.size() - 1);
        boolean survived = hot.simulateMemoryLoad(skewedPartitionMb);

        Banner.out(survived
                ? "SURVIVED: " + hot.getName() + " absorbed the skewed partition within its memory limit."
                : "OOMKILLED: " + hot.getName() + " exceeded pod memory limit (" + podMem + "Mi) and was evicted.");
        Banner.out("Job status: " + (job.isApplicationFailed() ? "FAILED" : "stage retried / still healthy"));
    }
}
