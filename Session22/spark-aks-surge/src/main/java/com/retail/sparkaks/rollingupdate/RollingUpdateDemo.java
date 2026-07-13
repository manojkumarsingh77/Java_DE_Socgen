package com.retail.sparkaks.rollingupdate;

import com.retail.sparkaks.common.AksNode;
import com.retail.sparkaks.common.Banner;
import com.retail.sparkaks.common.SparkPod;
import com.retail.sparkaks.pdb.EvictionController;
import com.retail.sparkaks.pdb.PodDisruptionBudget;
import com.retail.sparkaks.sparkpods.SparkJob;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * TOPIC 4: ROLLING UPDATES
 *
 * Retail story: the season's final act - AKS must roll out a Kubernetes
 * minor-version upgrade to the whole "spark" node pool WHILE
 * checkout-events-aggregator keeps serving Black Friday traffic, using
 * everything taught in Topics 1-3 together.
 */
public class RollingUpdateDemo {

    public static void run() {
        Banner.topic(4, "ROLLING UPDATES - upgrading the cluster under Black Friday load");

        Banner.problem(
                "The 'spark' node pool must move to a new AKS node image / K8s minor version",
                "during the Black Friday change freeze exception window:",
                "- A naive in-place upgrade takes nodes down 1-by-1, temporarily shrinking",
                "  total cluster capacity right when checkout-events-aggregator needs it most.",
                "- Done carelessly, it also re-creates the Topic 1 and Topic 3 failure modes:",
                "  the driver pod or too many executors at once could be pulled offline.");

        Banner.requirement(
                "R1: Total usable capacity must never drop below today's footprint mid-upgrade.",
                "R2: Every node drain during the upgrade must still respect the PDBs from Topic 3.",
                "R3: Pods evicted from an upgrading node must land on already-ready new nodes",
                "    before the old node is deleted - zero-gap handover.",
                "R4: The whole rollout should be resumable: a temporarily-blocked node just",
                "    waits for its protected pods to become evictable, it never forces the issue.");

        Banner.solution(
                "AKS surge upgrade strategy + the SAME PDB-aware eviction path from Topic 3:",
                "  RollingUpgradeSimulator.upgradePool(oldNodes, newNodeFactory, maxSurge, job, api)",
                "    1. Provision maxSurge extra new-image nodes BEFORE draining anything",
                "    2. Cordon the old node (stop new scheduling onto it)",
                "    3. Drain it via EvictionController.drainNode() -> PDBs from Topic 3 apply",
                "    4. Reschedule evicted pods onto the new nodes",
                "    5. Delete the old node only once fully drained");

        List<AksNode> pool = new ArrayList<>();
        for (int i = 1; i <= 4; i++) pool.add(new AksNode("spark-node-" + i + "-v1", 6000, 24000));

        SparkJob job = new SparkJob("checkout-events-aggregator",
                1000, 2048, 384, 2000, 4096, 512);
        job.schedule(job.createDriverPod(), pool);
        job.createExecutorPods(8).forEach(e -> job.schedule(e, pool));

        Banner.demo("Cluster before upgrade (image v1, Black Friday traffic live):");
        job.printState();

        EvictionController evictionApi = new EvictionController()
                .register(PodDisruptionBudget.minAvailableAbsolute("driver-pdb", SparkPod.Role.DRIVER, 1))
                .register(PodDisruptionBudget.maxUnavailablePercent("executor-pdb", SparkPod.Role.EXECUTOR, 0.25));

        Banner.step("Starting surge upgrade: maxSurge=1 extra node allowed above the normal 4.");
        AtomicInteger seq = new AtomicInteger(1);
        RollingUpgradeSimulator upgrader = new RollingUpgradeSimulator();
        List<AksNode> after = upgrader.upgradePool(
                pool,
                () -> new AksNode("spark-node-new-" + seq.getAndIncrement() + "-v2", 6000, 24000),
                1,
                job,
                evictionApi);

        Banner.step("Cluster after upgrade pass:");
        after.forEach(n -> System.out.println("    " + n));
        job.printState();

        long healthyExecs = job.getExecutors().stream().filter(e -> e.getNodeName() != null).count();
        long stillPending = job.getExecutors().size() - healthyExecs;
        Banner.out("Executors still healthy throughout the upgrade: " + healthyExecs + " / " + job.getExecutors().size());
        Banner.out("Driver node during upgrade: " + job.getDriver().getNodeName());
        if (stillPending > 0) {
            Banner.warn(stillPending + " executor(s) ended this pass Pending: maxSurge=1 gave only one");
            Banner.warn("extra node's worth of landing space, and cordoned old nodes cannot accept them");
            Banner.warn("back (cordoned = unschedulable, by design). The upgrade controller simply");
            Banner.warn("retries this last node on its NEXT pass once more capacity frees up -");
            Banner.warn("in practice you would raise maxSurge (e.g. to 2) for a workload this size.");
        }

        Banner.keyTakeaway(
                "Rolling/surge upgrade = Topic 1 (know your driver/executor blast radius)",
                "+ Topic 2 (right-sized pods actually fit on the new nodes)",
                "+ Topic 3 (PDBs bound how much can be evicted at once) composed together.",
                "maxSurge trades a short burst of extra node cost for zero capacity dip -",
                "the correct trade during Black Friday; you'd use maxSurge=0 in a cost-",
                "sensitive dev/test environment instead (Multi-env isolation topic).",
                "This is exactly how az aks upgrade / az aks nodepool upgrade behaves in AKS,",
                "and why your PDBs must be correct BEFORE you ever click upgrade in peak season.");
        Banner.pause();
    }
}
