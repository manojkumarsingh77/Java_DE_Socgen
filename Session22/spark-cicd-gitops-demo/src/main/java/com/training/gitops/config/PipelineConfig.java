package com.training.gitops.config;

/**
 * Every pipeline knob externalized as an environment variable - mirrors how a
 * real GitHub Actions / Azure DevOps pipeline passes configuration via
 * env:/variables: blocks without ever touching code. See DEMO-GUIDE.md for the
 * full list and what each one demonstrates.
 */
public class PipelineConfig {

    public final String registryDir;
    public final String scanReportPath;
    public final boolean approveStage;
    public final boolean approveProd;
    public final boolean blueGreenInjectFailure;
    public final boolean canaryInjectFailure;

    private PipelineConfig(String registryDir, String scanReportPath, boolean approveStage,
                            boolean approveProd, boolean blueGreenInjectFailure, boolean canaryInjectFailure) {
        this.registryDir = registryDir;
        this.scanReportPath = scanReportPath;
        this.approveStage = approveStage;
        this.approveProd = approveProd;
        this.blueGreenInjectFailure = blueGreenInjectFailure;
        this.canaryInjectFailure = canaryInjectFailure;
    }

    public static PipelineConfig fromEnv() {
        return new PipelineConfig(
                env("REGISTRY_DIR", ".registry"),
                env("SCAN_REPORT_PATH", "config/vulnerability-findings.json"),
                envBool("APPROVE_STAGE", false),
                envBool("APPROVE_PROD", false),
                envBool("BLUEGREEN_INJECT_FAILURE", false),
                envBool("CANARY_INJECT_FAILURE", false)
        );
    }

    private static String env(String key, String def) {
        String v = System.getenv(key);
        return (v == null || v.isBlank()) ? def : v;
    }

    private static boolean envBool(String key, boolean def) {
        String v = System.getenv(key);
        return (v == null || v.isBlank()) ? def : Boolean.parseBoolean(v.trim());
    }
}
