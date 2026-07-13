package com.retail.sparkaks.sparkpods;

import com.retail.sparkaks.common.AksNode;
import com.retail.sparkaks.common.Banner;
import com.retail.sparkaks.common.SparkPod;

import java.util.ArrayList;
import java.util.List;

/**
 * TOPIC 1: DRIVER / EXECUTOR PODS
 *
 * Retail story: the "checkout-events-aggregator" Spark Structured Streaming
 * job reads Black-Friday clickstream/checkout events from Kafka and writes
 * rolling metrics to the dashboard. Ops keeps treating it like a normal
 * microservice Deployment and gets burned.
 */
public class DriverExecutorDemo {

    public static void run() {
        Banner.topic(1, "SPARK DRIVER / EXECUTOR PODS - how Spark-on-K8s really schedules");

        Banner.problem(
                "Ops rolled 'checkout-events-aggregator' like any other Deployment:",
                "- They killed the pod named '...-exec-3' during a routine node drain,",
                "  assuming it was stateless like a web pod - the WHOLE JOB stayed healthy.",
                "- Next drain hit the pod named '...-driver' instead - the ENTIRE Spark",
                "  application died, even though 11 other executor pods were fine.",
                "- Nobody could explain why losing 1 of 12 pods was sometimes a non-event",
                "  and sometimes a full outage during Black Friday.");

        Banner.requirement(
                "R1: Team must understand there is exactly ONE driver pod per Spark app,",
                "    created first, and it owns the Spark session / DAG scheduler / UI.",
                "R2: Executors are created BY the driver (needs RBAC), not by a Deployment.",
                "R3: Losing an executor => task retry on another executor (cheap).",
                "R4: Losing the driver => entire application fails (expensive) - the driver",
                "    pod must be treated as a stateful, single-point-of-failure workload.");

        Banner.solution(
                "Model the asymmetric lifecycle explicitly instead of a uniform pod list:",
                "  SparkJob.createDriverPod()      -> pod #1, must reach Running first",
                "  SparkJob.createExecutorPods(n)   -> driver calls K8s API to add pod 2..n+1",
                "  SparkJob.onDriverLost()          -> models the driver-loss blast radius",
                "AKS operational guidance this maps to: pin the driver with a higher",
                "priorityClass / a dedicated small pool, and NEVER let voluntary disruption",
                "(node drain, spot eviction) target it without a PodDisruptionBudget (Topic 3).");

        Banner.demo("Submitting checkout-events-aggregator (spark-submit --master k8s://...)");

        List<AksNode> cluster = new ArrayList<>();
        for (int i = 1; i <= 4; i++) cluster.add(new AksNode("spark-node-" + i, 4000, 16000));

        SparkJob job = new SparkJob("checkout-events-aggregator",
                1000, 2048, 384,     // driver: 1 vCPU, 2GB heap, 384Mi overhead
                2000, 4096, 512);    // executor: 2 vCPU, 4GB heap, 512Mi overhead

        Banner.step("Step 1: create + schedule the DRIVER pod (must land before anything else).");
        SparkPod driverPod = job.createDriverPod();
        job.schedule(driverPod, cluster);
        Banner.out(driverPod.toString());

        Banner.step("Step 2: driver is Running -> it now requests 6 EXECUTOR pods from the API server.");
        List<SparkPod> execs = job.createExecutorPods(6);
        execs.forEach(e -> job.schedule(e, cluster));
        job.printState();

        Banner.step("Scenario A: routine node drain evicts executor 'checkout-events-aggregator-exec-3'.");
        SparkPod victimExec = job.getExecutors().stream()
                .filter(e -> e.getName().endsWith("exec-3")).findFirst().orElseThrow();
        cluster.forEach(n -> n.remove(victimExec));
        Banner.out("Evicted: " + victimExec.getName());
        List<SparkPod> replacement = job.requestExecutors(6, cluster); // driver notices and refills to target
        replacement.forEach(e -> job.schedule(e, cluster));
        Banner.out("Driver detected the loss and requested a replacement executor automatically.");
        Banner.out("Application status: " + (job.isApplicationFailed() ? "FAILED" : "HEALTHY - job kept running throughout"));

        Banner.step("Scenario B: same drain instead evicts the DRIVER pod (no PDB in place).");
        cluster.forEach(n -> n.remove(driverPod));
        job.onDriverLost();
        job.printState();
        Banner.warn("Every executor is now orphaned - Spark has no driver HA on vanilla Kubernetes.");
        Banner.out("This is exactly the outage the retail team hit. Topic 3 (PDB) fixes this class of problem.");

        Banner.keyTakeaway(
                "Driver pod = created first, hosts the Spark UI + DAG scheduler = SPOF.",
                "Executor pods = created BY the driver, disposable, tasks simply retry elsewhere.",
                "Treat the driver as a 'pet': dedicated priority/pool + PDB + no spot eviction.",
                "Treat executors as 'cattle': fine on spot, fine under aggressive autoscaling.");
        Banner.pause();
    }
}
