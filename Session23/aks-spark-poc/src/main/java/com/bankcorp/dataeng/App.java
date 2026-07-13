package com.bankcorp.dataeng;

import com.bankcorp.dataeng.config.AppConfig;
import com.bankcorp.dataeng.config.EnvironmentProfile;
import com.bankcorp.dataeng.data.SyntheticBankingDataGenerator;
import com.bankcorp.dataeng.egress.BlobRawIngestor;
import com.bankcorp.dataeng.egress.DeltaLakeWriter;
import com.bankcorp.dataeng.pipeline.AutoscalerLoadSimulator;
import com.bankcorp.dataeng.pipeline.LedgerReconciliationProcessor;
import com.bankcorp.dataeng.pipeline.MultiEnvIsolationGuard;
import com.bankcorp.dataeng.pipeline.NetworkPolicyValidator;
import com.bankcorp.dataeng.pipeline.NodePoolTopologyAdvisor;
import com.bankcorp.dataeng.security.SecretsResolver;
import io.delta.tables.DeltaTable;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ============================================================================
 *  AKS ARCHITECTURE DEEP DIVE - RETAIL BANKING POC
 *  Topics covered end-to-end in a single execution:
 *    1. Node Pools              -> NodePoolTopologyAdvisor
 *    2. Autoscaler Internals    -> AutoscalerLoadSimulator
 *    3. Network Policies        -> NetworkPolicyValidator
 *    4. Secrets Management      -> SecretsResolver
 *    5. Multi-Env Isolation     -> MultiEnvIsolationGuard, EnvironmentProfile
 * ============================================================================
 *
 * Business scenario: Core Ledger Reconciliation for a Retail Bank.
 *
 * Flow:  Synthetic ledger generation -> RAW zone (Blob/local JSON)
 *        -> Reconciliation transform -> CURATED zone (Delta/ADLS or local)
 *        -> business KPI summary printed to console.
 *
 * Runs identically:
 *   (a) inside IntelliJ IDEA on macOS M1 Max or Windows 11 (local[*] master)
 *   (b) via spark-submit --master k8s://... on AKS (see k8s/06-sparkapplication-*.yaml)
 */
public final class App {

    private static final Logger LOG = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) {
        EnvironmentProfile envProfile = AppConfig.ENV_PROFILE;

        LOG.info("############################################################");
        LOG.info("# AKS ARCHITECTURE DEEP DIVE POC - environment = {}", envProfile.envName());
        LOG.info("# AZURE_MODE = {}", AppConfig.AZURE_MODE);
        LOG.info("############################################################");

        // ---------------------------------------------------------------
        // TOPIC 5: MULTI-ENV ISOLATION - fail fast before any data moves
        // ---------------------------------------------------------------
        MultiEnvIsolationGuard.enforce(envProfile);

        // ---------------------------------------------------------------
        // TOPIC 4: SECRETS MANAGEMENT - resolve storage credentials
        // ---------------------------------------------------------------
        SecretsResolver secretsResolver = new SecretsResolver(AppConfig.AZURE_KEYVAULT_URI);
        String storageAccountKey = secretsResolver.resolve(
                envProfile.keyVaultSecretPrefix() + "adls-storage-key",
                "local-dev-placeholder-key-not-used-in-local-mode");

        SparkConf sparkConf = buildSparkConf(envProfile, storageAccountKey);

        // ---------------------------------------------------------------
        // TOPIC 3: NETWORK POLICIES - pre-flight connectivity intent check
        // ---------------------------------------------------------------
        NetworkPolicyValidator.validateExpectedTrafficMatrix();

        try (SparkSession spark = SparkSession.builder()
                .config(sparkConf)
                .getOrCreate()) {

            spark.sparkContext().setLogLevel("WARN");
            spark.conf().set("spark.sql.shuffle.partitions", String.valueOf(AppConfig.SHUFFLE_PARTITIONS));

            // -----------------------------------------------------------
            // TOPIC 1: NODE POOLS - report/validate planned placement
            // -----------------------------------------------------------
            NodePoolTopologyAdvisor.reportPlannedTopology(sparkConf, envProfile);

            // -----------------------------------------------------------
            // TOPIC 2: AUTOSCALER INTERNALS - progressive load waves
            // -----------------------------------------------------------
            AutoscalerLoadSimulator.runProgressiveLoadWaves(spark, envProfile);

            // -----------------------------------------------------------
            // BUSINESS SCENARIO: CORE LEDGER RECONCILIATION
            // -----------------------------------------------------------
            LOG.info("Generating primary synthetic ledger dataset for reconciliation run...");
            Dataset<Row> syntheticLedger = SyntheticBankingDataGenerator.generate(
                    spark, AppConfig.SYNTHETIC_ROW_COUNT, AppConfig.RANDOM_SEED, AppConfig.SHUFFLE_PARTITIONS);

            LOG.info("Landing synthetic ledger into RAW zone...");
            BlobRawIngestor.landInRawZone(syntheticLedger);

            LOG.info("Reading back RAW zone for reconciliation processing...");
            Dataset<Row> rawLedger = BlobRawIngestor.readFromRawZone(spark);

            LOG.info("Executing core ledger reconciliation transformation...");
            Dataset<Row> reconciled = LedgerReconciliationProcessor.reconcile(spark, rawLedger);

            LOG.info("Upserting reconciled output into CURATED Delta zone...");
            DeltaLakeWriter.upsertCuratedLedger(spark, reconciled);

            LOG.info("Verifying curated Delta table via DeltaTable.forPath + history()...");
            String curatedPath = AppConfig.curatedDeltaPath();
            if (DeltaTable.isDeltaTable(spark, curatedPath)) {
                DeltaTable curated = DeltaTable.forPath(spark, curatedPath);
                curated.history(5).show(false);
            }

            LOG.info("############################################################");
            LOG.info("# POC RUN COMPLETE - all 5 topics executed successfully.");
            LOG.info("# RAW zone     : {}", AppConfig.rawZonePath());
            LOG.info("# CURATED zone : {}", AppConfig.curatedDeltaPath());
            LOG.info("############################################################");
        }
    }

    /**
     * Central Spark configuration - the SAME conf object is used whether the
     * job later runs local[*] in IntelliJ or is submitted to AKS via
     * spark-submit --master k8s://<api-server>. K8s-specific keys
     * (spark.kubernetes.*) are silently ignored by local[*] master, so no
     * conditional branching is required.
     */
    private static SparkConf buildSparkConf(EnvironmentProfile envProfile, String storageAccountKey) {
        SparkConf conf = new SparkConf()
                .setAppName("retail-banking-ledger-reconciliation-" + envProfile.envName())
                .setMaster(AppConfig.SPARK_MASTER)

                // Delta Lake catalog + SQL extensions
                .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

                // Dynamic Allocation (Topic 2: Autoscaler Internals - Spark-side control loop)
                .set("spark.dynamicAllocation.enabled", "true")
                .set("spark.dynamicAllocation.shuffleTracking.enabled", "true")
                .set("spark.dynamicAllocation.minExecutors", String.valueOf(envProfile.minExecutors()))
                .set("spark.dynamicAllocation.maxExecutors", String.valueOf(envProfile.maxExecutors()))
                .set("spark.dynamicAllocation.schedulerBacklogTimeout", "1s")
                .set("spark.dynamicAllocation.sustainedSchedulerBacklogTimeout", "5s")
                .set("spark.dynamicAllocation.executorIdleTimeout", "60s")

                // Executor sizing (Topic 1: Node Pools - must fit target VM SKU)
                .set("spark.executor.cores", "4")
                .set("spark.executor.memory", "8g")
                .set("spark.executor.memoryOverhead", "2g")

                // Kubernetes scheduler backend settings (no-op under local[*],
                // required when spark-submit targets k8s:// on AKS)
                .set("spark.kubernetes.namespace", envProfile.k8sNamespace())
                .set("spark.kubernetes.driver.label.role", "spark-driver")
                .set("spark.kubernetes.executor.label.role", "spark-executor")
                .set("spark.kubernetes.node.selector.agentpool",
                        envProfile.executorNodeSelector().split("=")[1])
                .set("spark.kubernetes.executor.podTemplateFile", "/opt/spark/conf/executor-pod-template.yaml")

                // Networking safety: disable EPOLL native transport so the
                // identical jar runs on both macOS ARM64 (M1 Max) and
                // Windows x64 local drivers without native lib mismatches.
                .set("spark.shuffle.io.mode", "NIO");

        if (AppConfig.AZURE_MODE) {
            conf.set("fs.azure.account.key." + AppConfig.AZURE_STORAGE_ACCOUNT_NAME + ".dfs.core.windows.net",
                    storageAccountKey);
            conf.set("fs.azure.account.key." + AppConfig.AZURE_STORAGE_ACCOUNT_NAME + ".blob.core.windows.net",
                    storageAccountKey);
        }

        return conf;
    }
}
