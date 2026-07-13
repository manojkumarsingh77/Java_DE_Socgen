package com.retail.sparkaks.rollingupdate;

import com.retail.sparkaks.common.AksNode;
import com.retail.sparkaks.common.Banner;
import com.retail.sparkaks.common.SparkPod;
import com.retail.sparkaks.pdb.EvictionController;
import com.retail.sparkaks.sparkpods.SparkJob;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * Simulates an AKS node pool rolling ("surge") upgrade - the mechanism AKS
 * uses for node-image upgrades, Kubernetes minor-version upgrades, and
 * scale-set model changes.
 *
 * TEACHING POINTS driven by this class:
 *  - {@link #upgradePool(List, Supplier, int, SparkJob, EvictionController)}:
 *    maxSurge extra (temporary) nodes are added FIRST so cluster CAPACITY
 *    never drops below 100% during the upgrade - critical during Black Friday.
 *  - Each old node is cordoned (no new pods) then drained through the SAME
 *    {@link EvictionController} used in Topic 3 - PDBs are respected here too.
 *  - Evicted pods are rescheduled onto the new (already-provisioned) nodes
 *    before the old node is deleted - never the other way around.
 */
public class RollingUpgradeSimulator {

    /**
     * @param oldNodes      the node pool BEFORE upgrade (mutated in place: old nodes removed)
     * @param newNodeFactory supplies a freshly-imaged replacement node on demand
     * @param maxSurge      how many extra nodes may exist temporarily above the pool's normal size
     * @param job           the Spark job whose pods live on this pool (for rescheduling + reporting)
     * @param evictionApi   shared PDB-aware eviction controller from Topic 3
     * @return the final, fully-upgraded list of nodes
     */
    public List<AksNode> upgradePool(List<AksNode> oldNodes, Supplier<AksNode> newNodeFactory,
                                     int maxSurge, SparkJob job, EvictionController evictionApi) {

        List<AksNode> live = new ArrayList<>(oldNodes);
        List<AksNode> upgraded = new ArrayList<>();
        int originalSize = oldNodes.size();

        Banner.out("Upgrade plan: " + originalSize + " node(s) total, maxSurge=" + maxSurge
                + " -> capacity never drops below " + originalSize + " nodes during the rollout.");

        int surgeBudgetRemaining = maxSurge;
        for (AksNode oldNode : new ArrayList<>(oldNodes)) {

            // 1. Surge: bring up a freshly-imaged node BEFORE touching the old one.
            AksNode freshNode = null;
            if (surgeBudgetRemaining > 0) {
                freshNode = newNodeFactory.get();
                live.add(freshNode);
                surgeBudgetRemaining--;
                Banner.out("Surge-provisioned " + freshNode.getName() + " (new image) - capacity now "
                        + live.size() + " nodes.");
            }

            // 2. Cordon the old node: unschedulable for new pods, existing pods untouched yet.
            oldNode.setCordoned(true);
            Banner.out("Cordoned " + oldNode.getName() + " (unschedulable for new pods).");

            // 3. Drain: evict pods through the PDB-aware eviction controller (Topic 3 logic reused).
            List<SparkPod> allPods = new ArrayList<>();
            allPods.add(job.getDriver());
            allPods.addAll(job.getExecutors());
            List<SparkPod> evicted = evictionApi.drainNode(oldNode, allPods);

            // 4. Reschedule evicted pods onto surviving/new nodes with room.
            for (SparkPod pod : evicted) {
                live.stream().filter(n -> n != oldNode && n.canFit(pod)).findFirst()
                        .ifPresentOrElse(
                                target -> { target.assign(pod); Banner.out("Rescheduled " + pod.getName() + " -> " + target.getName()); },
                                () -> Banner.warn("No capacity to reschedule " + pod.getName() + " yet - stays Pending"));
            }

            // 5. Only delete the old node once it is empty (or its remaining pods are protected/blocked).
            if (oldNode.getPods().isEmpty()) {
                live.remove(oldNode);
                upgraded.add(oldNode);
                Banner.out("Deleted old node " + oldNode.getName() + " (fully drained).");
            } else {
                Banner.warn(oldNode.getName() + " still has " + oldNode.getPods().size()
                        + " PDB-protected pod(s) - left cordoned, upgrade will retry it next pass.");
            }
        }

        Banner.out("Upgrade pass complete. Live nodes: " + live.size() + " (target size " + originalSize + ").");
        return live;
    }
}
