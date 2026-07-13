package com.bankcorp.dataeng.pipeline;

import com.bankcorp.dataeng.config.AppConfig;
import com.bankcorp.dataeng.config.EnvironmentProfile;
import com.bankcorp.dataeng.data.SyntheticBankingDataGenerator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TOPIC: Autoscaler Internals.
 *
 * Two DISTINCT autoscalers cooperate on AKS and this class exercises the
 * signal path for both:
 *
 *  (A) SPARK DYNAMIC RESOURCE ALLOCATION (application-level, JVM control loop)
 *      Controlled by spark.dynamicAllocation.* conf. Spark's
 *      ExecutorAllocationManager polls pending task backlog every
 *      `spark.dynamicAllocation.schedulerBacklogTimeout` (default 1s) and
 *      requests `spark.dynamicAllocation.executorAllocationRatio` new
 *      executors when `numRunningTasks < numPendingTasks`. On K8s this
 *      translates 1:1 to new Executor Pod objects submitted to the API
 *      server via the KubernetesClusterSchedulerBackend.
 *
 *  (B) AKS CLUSTER AUTOSCALER (infrastructure-level, node control loop)
 *      Watches for Pods in `Pending` state with `FailedScheduling` events
 *      (insufficient CPU/memory on existing nodes). Every
 *      `--scan-interval` (default 10s) it simulates scheduling the pending
 *      pod against a hypothetical NEW node from the node pool's VMSS
 *      template; if it would fit, it calls the Azure VMSS API to add a
 *      node (scale-up latency: ~3-5 min for a new VM to become Ready).
 *      Scale-DOWN triggers when a node's utilization stays below
 *      `--scale-down-utilization-threshold` (default 0.5) for
 *      `--scale-down-unneeded-time` (default 10m), and requires ALL pods on
 *      that node to be safely evictable (no local storage, respects
 *      PodDisruptionBudget).
 *
 * This class increases data volume across batches to grow the shuffle
 * partition count beyond available parallelism, forcing Spark's dynamic
 * allocation to request more executors than fit on current nodes - which
 * is precisely the FailedScheduling condition that wakes the Cluster
 * Autoscaler in the real AKS deployment (see k8s/05-cluster-autoscaler-config.yaml).
 */
public final class AutoscalerLoadSimulator {

    private static final Logger LOG = LoggerFactory.getLogger(AutoscalerLoadSimulator.class);

    private AutoscalerLoadSimulator() {
    }

    public static void runProgressiveLoadWaves(SparkSession spark, EnvironmentProfile envProfile) {
        int baseRows = AppConfig.SYNTHETIC_ROW_COUNT;
        int waves = AppConfig.SYNTHETIC_BATCH_COUNT;

        LOG.info("========== AUTOSCALER LOAD SIMULATION START ({} waves) ==========", waves);
        LOG.info("Dynamic Allocation config -> minExecutors={}, maxExecutors={}, backlogTimeout={}s",
                envProfile.minExecutors(), envProfile.maxExecutors(),
                spark.conf().get("spark.dynamicAllocation.schedulerBacklogTimeout", "1s"));

        for (int wave = 1; wave <= waves; wave++) {
            int waveRowCount = baseRows * wave;
            // numSlices grows super-linearly with wave number to deliberately
            // outpace static executor count - this is the exact backlog
            // condition ExecutorAllocationManager.updateAndSyncNumExecutorsTarget()
            // reacts to.
            int numSlices = Math.max(4, wave * 8);

            long startNanos = System.nanoTime();
            Dataset<Row> wavePayload = SyntheticBankingDataGenerator.generate(
                    spark, waveRowCount, AppConfig.RANDOM_SEED + wave, numSlices);

            long rowsProcessed = wavePayload.count(); // materializes the job -> triggers task scheduling
            long elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000;

            int activeExecutors = spark.sparkContext().getExecutorMemoryStatus().size();

            LOG.info("[WAVE {}/{}] rows={} partitions={} activeExecutors(incl.driver in local mode)={} elapsedMs={}",
                    wave, waves, rowsProcessed, numSlices, activeExecutors, elapsedMillis);

            if (numSlices > envProfile.maxExecutors() * 2) {
                LOG.warn("[WAVE {}] Partition count ({}) exceeds 2x maxExecutors ({}) for env '{}'. " +
                        "On real AKS this wave would produce PENDING executor pods and a FailedScheduling " +
                        "event, waking the Cluster Autoscaler scan loop.",
                        wave, numSlices, envProfile.maxExecutors(), envProfile.envName());
            }
        }
        LOG.info("========== AUTOSCALER LOAD SIMULATION END ==========");
    }
}
