package com.retail.sparkaks.pdb;

import com.retail.sparkaks.common.AksNode;
import com.retail.sparkaks.common.Banner;
import com.retail.sparkaks.common.SparkPod;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Simulates the Kubernetes Eviction API (what `kubectl drain` and AKS's
 * node-image upgrade controller both call under the hood) checking every
 * registered {@link PodDisruptionBudget} before evicting each pod.
 *
 * TEACHING POINT: eviction is tried pod-by-pod; a single blocked pod does
 * NOT abort the whole drain - it is retried later (here: reported back so
 * the operator/upgrade controller can wait and retry), while every pod that
 * IS safe to evict proceeds immediately.
 */
public class EvictionController {

    private final List<PodDisruptionBudget> budgets = new ArrayList<>();

    public EvictionController register(PodDisruptionBudget pdb) { budgets.add(pdb); return this; }

    /**
     * Attempt to drain (evict every pod from) a node.
     * @param allPods the full fleet, needed to compute "healthy count" per role for each PDB
     * @return list of pods that were actually evicted
     */
    public List<SparkPod> drainNode(AksNode node, List<SparkPod> allPods) {
        List<SparkPod> evicted = new ArrayList<>();
        for (SparkPod pod : new ArrayList<>(node.getPods())) {
            List<SparkPod> sameRole = allPods.stream().filter(p -> p.getRole() == pod.getRole()).toList();

            boolean allowed = budgets.stream().allMatch(pdb -> pdb.allowsEviction(pod, sameRole));
            if (allowed) {
                node.remove(pod);
                evicted.add(pod);
                Banner.out("EVICTED  " + pod.getName() + " from " + node.getName());
            } else {
                Banner.warn("BLOCKED  evicting " + pod.getName()
                        + " would violate its PodDisruptionBudget -> API server returns 429 Too Many Requests");
            }
        }
        return evicted;
    }

    public void printBudgets() {
        budgets.forEach(b -> System.out.println("    PDB " + b.describe()));
    }
}
