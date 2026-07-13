package com.retail.sparkaks.pdb;

import com.retail.sparkaks.common.SparkPod;

import java.util.List;

/**
 * Models a Kubernetes PodDisruptionBudget and the eviction-admission check
 * the API server performs for every VOLUNTARY disruption (kubectl drain,
 * AKS node-image rolling upgrade, cluster-autoscaler scale-down; a spot
 * reclaim is INVOLUNTARY and bypasses this - an important distinction for
 * the class).
 *
 * A PDB guards a set of pods (matched by role/label) with EITHER:
 *   - minAvailable: absolute number or percentage that must remain Running
 *   - maxUnavailable: percentage allowed to be evicted at once
 * Exactly one of the two is set, matching real Kubernetes semantics.
 */
public class PodDisruptionBudget {

    private final String name;
    private final SparkPod.Role targetRole;
    private final Integer minAvailableAbs;
    private final Double minAvailablePct;
    private final Double maxUnavailablePct;

    private PodDisruptionBudget(String name, SparkPod.Role targetRole,
                                Integer minAvailableAbs, Double minAvailablePct, Double maxUnavailablePct) {
        this.name = name;
        this.targetRole = targetRole;
        this.minAvailableAbs = minAvailableAbs;
        this.minAvailablePct = minAvailablePct;
        this.maxUnavailablePct = maxUnavailablePct;
    }

    public static PodDisruptionBudget minAvailableAbsolute(String name, SparkPod.Role role, int minAvailable) {
        return new PodDisruptionBudget(name, role, minAvailable, null, null);
    }

    public static PodDisruptionBudget minAvailablePercent(String name, SparkPod.Role role, double pct) {
        return new PodDisruptionBudget(name, role, null, pct, null);
    }

    public static PodDisruptionBudget maxUnavailablePercent(String name, SparkPod.Role role, double pct) {
        return new PodDisruptionBudget(name, role, null, null, pct);
    }

    /**
     * The exact check the K8s eviction API performs: would evicting `candidate`
     * violate this budget, given the CURRENT set of healthy pods it protects?
     */
    public boolean allowsEviction(SparkPod candidate, List<SparkPod> allPodsOfRole) {
        if (candidate.getRole() != targetRole) return true; // this PDB doesn't cover this pod

        long totalDesired = allPodsOfRole.size();
        long healthyNow = allPodsOfRole.stream().filter(p -> !p.isPending() && !p.isOomKilled()).count();
        long healthyAfterEviction = healthyNow - 1;

        if (minAvailableAbs != null) {
            return healthyAfterEviction >= minAvailableAbs;
        }
        if (minAvailablePct != null) {
            return healthyAfterEviction >= Math.ceil(totalDesired * minAvailablePct);
        }
        if (maxUnavailablePct != null) {
            long maxUnavailable = (long) Math.floor(totalDesired * maxUnavailablePct);
            long alreadyUnavailable = totalDesired - healthyNow;
            return (alreadyUnavailable + 1) <= maxUnavailable;
        }
        return true;
    }

    public String describe() {
        if (minAvailableAbs != null)   return name + ": role=" + targetRole + " minAvailable=" + minAvailableAbs;
        if (minAvailablePct != null)   return name + ": role=" + targetRole + " minAvailable=" + (int) (minAvailablePct * 100) + "%";
        if (maxUnavailablePct != null) return name + ": role=" + targetRole + " maxUnavailable=" + (int) (maxUnavailablePct * 100) + "%";
        return name;
    }

    public String getName()              { return name; }
    public SparkPod.Role getTargetRole() { return targetRole; }
}
