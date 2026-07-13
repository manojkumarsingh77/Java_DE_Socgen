package com.retail.sparkaks.sparkpods;

import com.retail.sparkaks.common.AksNode;
import com.retail.sparkaks.common.SparkPod;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Models ONE Spark application running on Kubernetes (spark-submit
 * --master k8s://...): exactly one DRIVER pod plus a variable number of
 * EXECUTOR pods that the driver itself requests from the API server.
 *
 * TEACHING POINTS driven by this class:
 *  - {@link #submit(List)}: the driver pod is created FIRST; only once it is
 *    Running does it create executor pods (asymmetry vs. a plain Deployment).
 *  - {@link #requestExecutors(int, List)}: models spark.dynamicAllocation.*:
 *    the driver asks the cluster for more/fewer executors as stages need them.
 *  - Losing the DRIVER pod kills the whole application (single point of
 *    failure) - losing an EXECUTOR just triggers a retry of its tasks.
 */
public class SparkJob {

    private final String appName;
    private SparkPod driver;
    private final List<SparkPod> executors = new ArrayList<>();
    private final int driverMilliCpu;
    private final int driverHeapMb;
    private final int driverOverheadMb;
    private final int executorMilliCpu;
    private final int executorHeapMb;
    private final int executorOverheadMb;
    private int nextExecutorId = 1;
    private boolean applicationFailed = false;

    public SparkJob(String appName,
                    int driverMilliCpu, int driverHeapMb, int driverOverheadMb,
                    int executorMilliCpu, int executorHeapMb, int executorOverheadMb) {
        this.appName = appName;
        this.driverMilliCpu = driverMilliCpu;
        this.driverHeapMb = driverHeapMb;
        this.driverOverheadMb = driverOverheadMb;
        this.executorMilliCpu = executorMilliCpu;
        this.executorHeapMb = executorHeapMb;
        this.executorOverheadMb = executorOverheadMb;
    }

    /** Step 1 of Spark-on-K8s: create & schedule the DRIVER pod. Everything else waits on this. */
    public SparkPod createDriverPod() {
        driver = new SparkPod(appName + "-driver", SparkPod.Role.DRIVER, 0,
                driverMilliCpu, driverHeapMb, driverOverheadMb);
        return driver;
    }

    /**
     * Step 2: once the driver is Running, it calls the K8s API itself to
     * create N executor pods (this is why Spark needs an RBAC ServiceAccount
     * with pod-create permission - unlike normal app pods created by a
     * Deployment controller).
     */
    public List<SparkPod> createExecutorPods(int count) {
        if (driver == null || driver.isPending()) {
            throw new IllegalStateException("Driver must be Running before executors can be requested");
        }
        List<SparkPod> created = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            SparkPod ex = new SparkPod(appName + "-exec-" + nextExecutorId,
                    SparkPod.Role.EXECUTOR, nextExecutorId,
                    executorMilliCpu, executorHeapMb, executorOverheadMb);
            nextExecutorId++;
            executors.add(ex);
            created.add(ex);
        }
        return created;
    }

    /**
     * spark.dynamicAllocation.enabled simulation: driver requests executors
     * up to `target`, or releases idle ones down to `target`, honoring
     * minExecutors/maxExecutors bounds passed by the caller.
     */
    public List<SparkPod> requestExecutors(int target, List<AksNode> cluster) {
        List<SparkPod> delta = new ArrayList<>();
        int current = (int) activeExecutors().count();
        if (target > current) {
            delta.addAll(createExecutorPods(target - current));
        } else if (target < current) {
            int toRemove = current - target;
            List<SparkPod> idle = activeExecutors().toList();
            for (int i = 0; i < toRemove && i < idle.size(); i++) {
                SparkPod victim = idle.get(idle.size() - 1 - i);
                cluster.forEach(n -> n.remove(victim));
                executors.remove(victim);
            }
        }
        return delta;
    }

    /** Very small first-fit scheduler shared by every topic's demo. */
    public Optional<AksNode> schedule(SparkPod pod, List<AksNode> cluster) {
        Optional<AksNode> chosen = cluster.stream().filter(n -> n.canFit(pod)).findFirst();
        chosen.ifPresent(n -> n.assign(pod));
        return chosen;
    }

    /** Losing the driver = application failure; Spark has no driver-HA on plain K8s. */
    public void onDriverLost() {
        applicationFailed = true;
        executors.forEach(e -> e.setNodeName(null));
    }

    public java.util.stream.Stream<SparkPod> activeExecutors() {
        return executors.stream().filter(e -> !e.isOomKilled());
    }

    public String getAppName()         { return appName; }
    public SparkPod getDriver()        { return driver; }
    public List<SparkPod> getExecutors() { return executors; }
    public boolean isApplicationFailed() { return applicationFailed; }

    public void printState() {
        System.out.println("    APP " + appName + (applicationFailed ? "  ** FAILED (driver lost) **" : ""));
        System.out.println("      " + driver);
        executors.forEach(e -> System.out.println("      " + e));
    }
}
