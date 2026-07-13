package com.retail.sparkaks.common;

import java.util.ArrayList;
import java.util.List;

/**
 * A simplified AKS worker node hosting Spark driver/executor pods.
 * Tracks allocatable capacity, current pods, cordon state (used during
 * rolling/cluster upgrades) and node age (used by surge upgrade logic).
 */
public class AksNode {

    private final String name;
    private final int allocatableMilliCpu;
    private final int allocatableMemoryMb;
    private final List<SparkPod> pods = new ArrayList<>();
    private boolean cordoned = false;   // true => unschedulable (new pods rejected)
    private boolean draining = false;   // true => being evicted during upgrade

    public AksNode(String name, int allocatableMilliCpu, int allocatableMemoryMb) {
        this.name = name;
        this.allocatableMilliCpu = allocatableMilliCpu;
        this.allocatableMemoryMb = allocatableMemoryMb;
    }

    public int requestedMilliCpu() { return pods.stream().mapToInt(SparkPod::getRequestMilliCpu).sum(); }
    public int requestedMemoryMb() { return pods.stream().mapToInt(SparkPod::getRequestMemoryMb).sum(); }
    public int freeMilliCpu()      { return allocatableMilliCpu - requestedMilliCpu(); }
    public int freeMemoryMb()      { return allocatableMemoryMb - requestedMemoryMb(); }

    public boolean canFit(SparkPod pod) {
        return !cordoned
                && freeMilliCpu() >= pod.getRequestMilliCpu()
                && freeMemoryMb() >= pod.getRequestMemoryMb();
    }

    public void assign(SparkPod pod) { pods.add(pod); pod.setNodeName(name); }
    public boolean remove(SparkPod pod) { pod.setNodeName(null); return pods.remove(pod); }

    public String getName()              { return name; }
    public int getAllocatableMilliCpu()  { return allocatableMilliCpu; }
    public int getAllocatableMemoryMb()  { return allocatableMemoryMb; }
    public List<SparkPod> getPods()      { return pods; }
    public boolean isCordoned()          { return cordoned; }
    public void setCordoned(boolean c)   { this.cordoned = c; }
    public boolean isDraining()          { return draining; }
    public void setDraining(boolean d)   { this.draining = d; }

    @Override
    public String toString() {
        return "%s%s [cpu %dm/%dm, mem %dMi/%dMi, pods=%d]".formatted(
                name, cordoned ? " (CORDONED)" : "",
                requestedMilliCpu(), allocatableMilliCpu,
                requestedMemoryMb(), allocatableMemoryMb, pods.size());
    }
}
