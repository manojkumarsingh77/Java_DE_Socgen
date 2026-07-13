package com.training.gitops;

import com.training.gitops.build.BuildInfo;
import com.training.gitops.config.PipelineConfig;
import com.training.gitops.deployment.BlueGreenDeploymentManager;
import com.training.gitops.deployment.CanaryReleaseManager;
import com.training.gitops.job.InventoryAnalyticsJob;
import com.training.gitops.pipeline.CDPipeline;
import com.training.gitops.pipeline.CIPipeline;
import com.training.gitops.pipeline.CIResult;
import com.training.gitops.registry.ArtifactRegistry;

/**
 * Single entry point for every mode in this CI/CD & GitOps training demo.
 * <p>
 * Usage: java -jar app.jar [ci | cd | pipeline | version | job | bluegreen-demo | canary-demo | reset]
 * <p>
 *   ci             - runs ONLY the CI half: build/smoke-test, version, scan, push, auto-deploy Dev
 *   cd             - runs ONLY the CD half: promote Dev -> Stage (blue/green) -> Prod (canary)
 *   pipeline       - runs ci then cd back-to-back (the full GitOps flow in one command)
 *   version        - prints BuildInfo for the current registry version
 *   job            - runs the underlying Spark job directly, no pipeline
 *   bluegreen-demo - standalone deep-dive: deploy + switch + rollback
 *   canary-demo    - standalone deep-dive: progressive wave rollout + auto-rollback
 *   reset          - clears .registry/ so the demo can be restarted from a clean slate
 */
public class App {

    public static void main(String[] args) {
        String mode = args.length > 0 ? args[0] : "pipeline";
        PipelineConfig config = PipelineConfig.fromEnv();

        System.out.println("############################################################");
        System.out.println("# CI/CD & GitOps for Spark - Training Demo");
        System.out.println("# mode = " + mode);
        System.out.println("############################################################");

        switch (mode) {
            case "ci" -> new CIPipeline(config).run();
            case "cd" -> new CDPipeline(config).run();
            case "pipeline" -> {
                CIResult ci = new CIPipeline(config).run();
                if (ci.success) {
                    new CDPipeline(config).run();
                } else {
                    System.out.println("[pipeline] CD skipped because CI did not succeed.");
                }
            }
            case "version" -> printVersion(config);
            case "job" -> new InventoryAnalyticsJob().runSmokeTest("manual", false);
            case "bluegreen-demo" -> runBlueGreenDeepDive(config);
            case "canary-demo" -> runCanaryDeepDive();
            case "reset" -> resetRegistry(config);
            default -> {
                System.err.println("Unknown mode '" + mode + "'. Valid: ci | cd | pipeline | version | job | bluegreen-demo | canary-demo | reset");
                System.exit(1);
            }
        }
    }

    private static void printVersion(PipelineConfig config) {
        ArtifactRegistry registry = new ArtifactRegistry(config.registryDir);
        String version = registry.readOrInitVersion();
        BuildInfo info = BuildInfo.capture(version);
        System.out.println(info);
        System.out.println("image tag equivalent: " + info.asImageTag("myacr.azurecr.io/inventory-analytics"));
    }

    private static void runBlueGreenDeepDive(PipelineConfig config) {
        BlueGreenDeploymentManager manager = new BlueGreenDeploymentManager(config.registryDir);
        System.out.println("\n--- Blue/Green deep dive ---");
        System.out.println("Starting active slot: " + manager.activeSlot());
        manager.deployAndSwitch("demo-1.0.0", false);
        System.out.println("Active slot after healthy deploy: " + manager.activeSlot());
        manager.deployAndSwitch("demo-1.1.0-broken", true);
        System.out.println("Active slot after FAILED deploy (should be unchanged): " + manager.activeSlot());
        manager.rollback();
        System.out.println("Active slot after manual rollback: " + manager.activeSlot());
    }

    private static void runCanaryDeepDive() {
        System.out.println("\n--- Canary deep dive: healthy candidate ---");
        new CanaryReleaseManager().rollout("demo-1.0.0", false);
        System.out.println("\n--- Canary deep dive: candidate with injected failures (watch auto-rollback) ---");
        new CanaryReleaseManager().rollout("demo-1.1.0-broken", true);
    }

    private static void resetRegistry(PipelineConfig config) {
        java.io.File dir = new java.io.File(config.registryDir);
        java.io.File[] files = dir.listFiles();
        int deleted = 0;
        if (files != null) {
            for (java.io.File f : files) {
                if (!f.getName().equals(".gitkeep") && f.delete()) deleted++;
            }
        }
        System.out.println("[reset] cleared " + deleted + " file(s) from " + config.registryDir
                + " - demo state is now fresh (version restarts at 1.0.0)");
    }
}
