package com.bankcorp.dataeng.pipeline;

import com.bankcorp.dataeng.config.EnvironmentProfile;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TOPIC: Node Pools.
 *
 * An AKS cluster used for this POC is provisioned with FOUR distinct node
 * pools (see k8s/04-nodepools-azcli.sh for the exact `az aks nodepool add`
 * commands). Each pool exists for a specific mechanical reason:
 *
 *   1. systempool   - VM Size: Standard_D4s_v5, mode=System, tainted
 *                     CriticalAddonsOnly=true:NoSchedule. Hosts CoreDNS,
 *                     metrics-server, the Key Vault CSI driver daemonset,
 *                     and the AKS-managed components ONLY. Spark pods are
 *                     never scheduled here because they lack the matching
 *                     toleration.
 *
 *   2. driverpool   - VM Size: Standard_D8s_v5, mode=User. Hosts ONLY Spark
 *                     driver pods (nodeSelector: role=spark-driver). Kept
 *                     separate from executors so a runaway executor OOM-kill
 *                     never evicts the driver (which would fail the whole
 *                     job) - this is THE most common AKS-Spark production
 *                     incident root cause.
 *
 *   3. userpool (per-env: dev-userpool / staging-userpool) - VM Size:
 *                     Standard_D8s_v5, mode=User, autoscaling enabled
 *                     (min=1,max=8 for dev; min=2,max=8 for staging).
 *                     Hosts Spark EXECUTOR pods for non-prod environments.
 *
 *   4. prodhighmem  - VM Size: Standard_E16s_v5 (memory-optimized),
 *                     mode=User, autoscaling enabled (min=4,max=32),
 *                     taint: workload=prod-spark:NoSchedule. Hosts prod
 *                     executor pods only; the memory-optimized SKU absorbs
 *                     the region-skewed shuffle partitions (APAC-MUMBAI
 *                     hotspot) without executor OOM.
 *
 * Locally (IntelliJ, local[*] master) there is no real node pool - this
 * class instead VALIDATES that the Spark conf keys which map to
 * nodeSelector/toleration equivalents in the SparkApplication CRD are set
 * correctly, so the SAME jar fails fast if misconfigured before it ever
 * reaches spark-submit on AKS.
 */
public final class NodePoolTopologyAdvisor {

    private static final Logger LOG = LoggerFactory.getLogger(NodePoolTopologyAdvisor.class);

    private NodePoolTopologyAdvisor() {
    }

    public static void reportPlannedTopology(SparkConf conf, EnvironmentProfile envProfile) {
        LOG.info("========== NODE POOL PLACEMENT PLAN ({}) ==========", envProfile.envName());
        LOG.info("Driver target node pool   : driverpool          (nodeSelector: role=spark-driver)");
        LOG.info("Executor target node pool : {}   (nodeSelector: {})",
                resolveExecutorPoolName(envProfile), envProfile.executorNodeSelector());
        LOG.info("Executor min/max replicas : {} / {}", envProfile.minExecutors(), envProfile.maxExecutors());
        LOG.info("Toleration required       : {}={}:NoSchedule", envProfile.nodePoolTaintKey(), envProfile.nodePoolTaintValue());

        int configuredExecutorCores = Integer.parseInt(conf.get("spark.executor.cores", "4"));
        String configuredExecutorMemory = conf.get("spark.executor.memory", "8g");
        LOG.info("Requested executor shape  : {} cores / {} memory (must fit within node pool VM SKU allocatable capacity)",
                configuredExecutorCores, configuredExecutorMemory);

        if (envProfile.envName().equals("prod") && configuredExecutorCores < 8) {
            LOG.warn("PROD environment detected but spark.executor.cores={} is below the recommended 8 " +
                    "for the memory-optimized E16s_v5 prodhighmem pool - executor packing will be suboptimal " +
                    "(2 executors/node instead of the designed 1 executor/node isolation).", configuredExecutorCores);
        }

        LOG.info("=====================================================");
    }

    private static String resolveExecutorPoolName(EnvironmentProfile envProfile) {
        return switch (envProfile.envName()) {
            case "dev" -> "dev-userpool";
            case "staging" -> "staging-userpool";
            case "prod" -> "prodhighmem";
            default -> "unknown-pool";
        };
    }
}
