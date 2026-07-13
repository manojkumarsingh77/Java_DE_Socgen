package com.retail.sparkaks.pdb;

import com.retail.sparkaks.common.AksNode;
import com.retail.sparkaks.common.Banner;
import com.retail.sparkaks.common.SparkPod;
import com.retail.sparkaks.sparkpods.SparkJob;

import java.util.ArrayList;
import java.util.List;

/**
 * TOPIC 3: POD DISRUPTION BUDGETS
 *
 * Retail story: same "checkout-events-aggregator" from Topic 1. This time
 * AKS needs to patch a CVE and rolls a node-image upgrade DURING Black
 * Friday peak. Without PDBs, the upgrade controller can evict the driver
 * pod or too many executors at once - exactly the outage the team fears.
 */
public class PdbDemo {

    public static void run() {
        Banner.topic(3, "POD DISRUPTION BUDGETS - surviving a node upgrade mid-peak");

        Banner.problem(
                "Security mandates an urgent AKS node-image patch during Black Friday peak.",
                "- The upgrade controller cordons + drains nodes one at a time.",
                "- Without any guardrail it can evict the SPARK DRIVER pod (Topic 1's SPOF)",
                "  or evict 5 of 6 executors from one node simultaneously,",
                "  collapsing the job's available parallelism right at peak load.");

        Banner.requirement(
                "R1: The driver pod must NEVER be voluntarily evicted while the job is live.",
                "R2: At most a small, bounded fraction of executors may be evicted at once,",
                "    so the job always keeps enough parallelism to keep up with Black Friday load.",
                "R3: Any disruption that would break these rules must be REJECTED by the",
                "    platform automatically - not left to an operator's judgment at 2am.");

        Banner.solution(
                "Define PodDisruptionBudgets per Spark role and let the Eviction API enforce them:",
                "  PodDisruptionBudget.minAvailableAbsolute(\"driver-pdb\", DRIVER, 1)",
                "  PodDisruptionBudget.maxUnavailablePercent(\"executor-pdb\", EXECUTOR, 0.25)",
                "  EvictionController.drainNode()  -> checks EVERY pod against EVERY PDB",
                "                                     before allowing the eviction to proceed");

        List<AksNode> cluster = new ArrayList<>();
        for (int i = 1; i <= 4; i++) cluster.add(new AksNode("spark-node-" + i, 6000, 24000));

        SparkJob job = new SparkJob("checkout-events-aggregator",
                1000, 2048, 384, 2000, 4096, 512);
        job.schedule(job.createDriverPod(), cluster);
        job.createExecutorPods(8).forEach(e -> job.schedule(e, cluster));
        Banner.demo("Cluster before upgrade:");
        job.printState();

        EvictionController evictionApi = new EvictionController()
                .register(PodDisruptionBudget.minAvailableAbsolute("driver-pdb", SparkPod.Role.DRIVER, 1))
                .register(PodDisruptionBudget.maxUnavailablePercent("executor-pdb", SparkPod.Role.EXECUTOR, 0.25));

        Banner.step("Registered PodDisruptionBudgets:");
        evictionApi.printBudgets();

        List<SparkPod> allPods = new ArrayList<>();
        allPods.add(job.getDriver());
        allPods.addAll(job.getExecutors());

        Banner.step("AKS node-image upgrade begins: cordon + drain nodes one at a time (surge upgrade).");
        int totalEvicted = 0;
        for (AksNode node : cluster) {
            Banner.out("--- draining " + node.getName() + " (" + node.getPods().size() + " pod(s) on it) ---");
            List<SparkPod> evicted = evictionApi.drainNode(node, allPods);
            totalEvicted += evicted.size();
        }

        Banner.out("Total pods evicted across the whole upgrade: " + totalEvicted);
        Banner.out("Driver still alive: " + (!job.getDriver().isPending() || job.getDriver().getNodeName() != null
                ? "YES - protected by driver-pdb" : "NO"));
        long healthyExecs = job.getExecutors().stream().filter(e -> e.getNodeName() != null).count();
        Banner.out("Executors still healthy: " + healthyExecs + " / " + job.getExecutors().size()
                + " (>= 75% preserved at every step by executor-pdb)");

        Banner.keyTakeaway(
                "A PDB does not prevent disruption forever - it prevents TOO MUCH disruption",
                "AT ONCE, forcing the upgrade/drain controller to slow down and go node-by-node.",
                "minAvailable=1 on a singleton (the driver) effectively blocks its eviction",
                "entirely while the job is live - exactly the protection Topic 1 was missing.",
                "maxUnavailable=25% on executors keeps enough parallelism alive to survive",
                "Black Friday load even while the whole cluster gets patched underneath it.",
                "Real AKS surfaces a blocked eviction as HTTP 429 from the Eviction API - the",
                "upgrade simply retries later; it never just deletes the pod outright.");
        Banner.pause();
    }
}
