package com.training.gitops.pipeline;

import com.training.gitops.config.PipelineConfig;
import com.training.gitops.deployment.BlueGreenDeploymentManager;
import com.training.gitops.deployment.CanaryReleaseManager;
import com.training.gitops.registry.ArtifactRegistry;

/**
 * === THE "CD" HALF OF THIS DEMO ===
 * <p>
 * === TOPIC: Dev -> Stage -> Prod Promotion ===
 * <p>
 * THE PROBLEM this class demonstrates:
 * Promoting straight from Dev to Prod skips the two things that make
 * promotion safe: (a) a human approval gate at each higher-risk environment,
 * and (b) a deployment STRATEGY appropriate to that environment's blast
 * radius. Different environments carry different risk, so they get different
 * strategies - Dev is cheap to break, Prod is not.
 * <p>
 * THE SOLUTION this class demonstrates:
 * The SAME immutable, already-scanned artifact (built once by CIPipeline) is
 * promoted through three environments, each with an explicit gate and its own
 * strategy:
 * <p>
 *   DEV   -> already deployed automatically at the end of CI (no gate, "recreate" strategy)
 *   STAGE -> requires APPROVE_STAGE=true (simulates a GitHub/Azure DevOps
 *            "environment protection rule" / required reviewer)
 *            strategy = BLUE/GREEN ({@link BlueGreenDeploymentManager})
 *   PROD  -> requires APPROVE_PROD=true (a second, stricter approval gate)
 *            strategy = CANARY ({@link CanaryReleaseManager})
 * <p>
 * Run in isolation via: java -jar app.jar cd
 * (Requires a prior successful `ci` run so a version exists in the registry.)
 */
public class CDPipeline {

    private final PipelineConfig config;
    private final ArtifactRegistry registry;
    private final BlueGreenDeploymentManager blueGreen;
    private final CanaryReleaseManager canary;

    public CDPipeline(PipelineConfig config) {
        this.config = config;
        this.registry = new ArtifactRegistry(config.registryDir);
        this.blueGreen = new BlueGreenDeploymentManager(config.registryDir);
        this.canary = new CanaryReleaseManager();
    }

    public void run() {
        System.out.println("\n================= CD PIPELINE =================");

        String devVersion = registry.currentVersionInEnvironment("dev");
        if (devVersion == null || !registry.manifestExists(devVersion)) {
            System.out.println("[CD] No artifact found in Dev. Run mode 'ci' first to build, scan and push a version.");
            System.out.println("=================================================\n");
            return;
        }
        System.out.println("[CD] Promoting version " + devVersion + " (already built + scanned once in CI)");

        System.out.println("\n[CD 1/2] STAGE - gate: APPROVE_STAGE=" + config.approveStage);
        if (!config.approveStage) {
            System.out.println("  [stage] BLOCKED - waiting for approval. Re-run with APPROVE_STAGE=true.");
            System.out.println("  (this simulates a required-reviewer / environment protection rule)");
            printCurrentState();
            System.out.println("=================================================\n");
            return;
        }
        System.out.println("  [stage] approved -> deploying via BLUE/GREEN strategy");
        boolean stageOk = blueGreen.deployAndSwitch(devVersion, config.blueGreenInjectFailure);
        if (!stageOk) {
            System.out.println("[CD] STOPPED - Stage deployment failed its health check. Prod promotion blocked.");
            printCurrentState();
            System.out.println("=================================================\n");
            return;
        }
        registry.promote(devVersion, "stage");

        System.out.println("\n[CD 2/2] PROD - gate: APPROVE_PROD=" + config.approveProd);
        if (!config.approveProd) {
            System.out.println("  [prod] BLOCKED - waiting for approval. Re-run with APPROVE_STAGE=true APPROVE_PROD=true.");
            System.out.println("  (Stage succeeded and is now live - Prod is untouched until approved)");
            printCurrentState();
            System.out.println("=================================================\n");
            return;
        }
        System.out.println("  [prod] approved -> deploying via CANARY strategy");
        CanaryReleaseManager.CanaryOutcome outcome = canary.rollout(devVersion, config.canaryInjectFailure);
        if (!outcome.success) {
            System.out.println("[CD] Prod canary rollout FAILED at " + outcome.reachedPercent
                    + "% - reason: " + outcome.reason);
            System.out.println("[CD] Prod remains on its previous stable version. No user-facing impact.");
            printCurrentState();
            System.out.println("=================================================\n");
            return;
        }
        registry.promote(devVersion, "prod");

        System.out.println("\n[CD] SUCCESS - version " + devVersion + " is now live in Dev, Stage AND Prod.");
        printCurrentState();
        System.out.println("=================================================\n");
    }

    private void printCurrentState() {
        System.out.println("\n  --- current promotion state ---");
        System.out.println("  dev   -> " + registry.currentVersionInEnvironment("dev"));
        System.out.println("  stage -> " + registry.currentVersionInEnvironment("stage"));
        System.out.println("  prod  -> " + registry.currentVersionInEnvironment("prod"));
        System.out.println("  blue/green active slot -> " + blueGreen.activeSlot());
    }
}
