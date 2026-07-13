package com.training.gitops.pipeline;

import com.training.gitops.build.BuildInfo;
import com.training.gitops.build.SemanticVersionCalculator;
import com.training.gitops.config.PipelineConfig;
import com.training.gitops.job.InventoryAnalyticsJob;
import com.training.gitops.registry.ArtifactRegistry;
import com.training.gitops.security.ScanResult;
import com.training.gitops.security.SecurityScanner;

/**
 * === THE "CI" HALF OF THIS DEMO ===
 * <p>
 * Orchestrates, in exactly this order, the same stages a real GitHub Actions
 * `ci.yml` / Azure DevOps build stage would run (see .github/workflows/ci.yml
 * for the 1:1 YAML mapping):
 * <p>
 *   1. BUILD    - compiling already happened via `mvn package` before this JVM
 *                 even started; here we run a unit-test-equivalent SMOKE TEST
 *                 of the actual Spark job to prove the artifact works at all.
 *   2. VERSION  - {@link SemanticVersionCalculator} computes the next semver.
 *   3. SCAN     - {@link SecurityScanner} gates the pipeline (ACR Security
 *                 Scanning topic) - a failed scan stops EVERYTHING below it.
 *   4. PUSH     - {@link ArtifactRegistry#push} publishes the versioned,
 *                 scanned artifact (simulates `docker push` to ACR) and
 *                 immediately auto-deploys it to Dev (no approval needed for
 *                 Dev - see EnvironmentPromoter).
 * <p>
 * Run in isolation via: java -jar app.jar ci
 */
public class CIPipeline {

    private final PipelineConfig config;
    private final ArtifactRegistry registry;

    public CIPipeline(PipelineConfig config) {
        this.config = config;
        this.registry = new ArtifactRegistry(config.registryDir);
    }

    public CIResult run() {
        System.out.println("\n================= CI PIPELINE =================");

        System.out.println("[CI 1/4] BUILD - running smoke test against the compiled artifact ...");
        boolean buildOk = new InventoryAnalyticsJob().runSmokeTest("ci-build", false);
        if (!buildOk) {
            System.out.println("[CI] FAILED at BUILD stage - stopping pipeline.");
            return new CIResult(false, null, "Build/smoke-test failed");
        }

        System.out.println("\n[CI 2/4] VERSION - computing next semantic version ...");
        String currentVersion = registry.readOrInitVersion();
        SemanticVersionCalculator.BumpType bump = SemanticVersionCalculator.bumpTypeFromEnv();
        String nextVersion = SemanticVersionCalculator.next(currentVersion, bump);
        BuildInfo buildInfo = BuildInfo.capture(nextVersion);
        System.out.println("  current=" + currentVersion + "  bump=" + bump + "  next=" + nextVersion);
        System.out.println("  " + buildInfo);

        System.out.println("\n[CI 3/4] SCAN - running (simulated) ACR / Trivy vulnerability scan ...");
        ScanResult scanResult = new SecurityScanner().scan(config.scanReportPath);
        scanResult.findings.forEach(f -> System.out.println("    " + f));
        System.out.println("  Critical=" + scanResult.criticalCount + "  High=" + scanResult.highCount
                + "  Medium=" + scanResult.mediumCount + "  => " + (scanResult.passed ? "PASS" : "BLOCKED"));
        System.out.println("  reason: " + scanResult.reason);

        if (!scanResult.passed) {
            System.out.println("[CI] FAILED at SCAN stage - artifact will NOT be pushed to the registry.");
            System.out.println("[CI] (this is the ACR security-scanning gate working as intended)");
            return new CIResult(false, nextVersion, scanResult.reason);
        }

        System.out.println("\n[CI 4/4] PUSH - publishing scanned artifact to registry, then auto-deploying to Dev ...");
        registry.push(buildInfo, scanResult.passed, scanResult.criticalCount, scanResult.highCount);
        registry.persistVersion(nextVersion);
        registry.promote(nextVersion, "dev"); // Dev = no approval gate, deploy-on-push, simplest strategy (recreate)
        boolean devHealthy = new InventoryAnalyticsJob().runSmokeTest("dev", false);
        System.out.println("  [dev] smoke test after auto-deploy: " + (devHealthy ? "HEALTHY" : "UNHEALTHY"));

        System.out.println("\n[CI] SUCCESS - version " + nextVersion + " built, scanned, pushed, deployed to Dev.");
        System.out.println("[CI] Run mode 'cd' next to promote this exact artifact to Stage and Prod.");
        System.out.println("=================================================\n");
        return new CIResult(true, nextVersion, "CI succeeded");
    }
}
