package com.fraud.app;

import org.json.JSONObject;

/**
 * Build/release metadata, sourced from environment variables that the
 * Dockerfile bakes in via --build-arg at image-build time (see Dockerfile
 * ARG/ENV APP_VERSION, GIT_COMMIT, BUILD_DATE) and from environment
 * variables set at deploy time (APP_ENV, DEPLOY_SLOT).
 *
 * Exposing this at runtime via GET /version is what makes Blue/Green and
 * Canary rollouts verifiable during the demo: curl the load balancer
 * repeatedly and watch which version/slot answers as traffic weights shift.
 */
public class VersionInfo {

    private final String appVersion;
    private final String gitCommit;
    private final String buildDate;
    private final String appEnv;
    private final String deploySlot;

    public VersionInfo() {
        this.appVersion = env("APP_VERSION", "0.0.0-local");
        this.gitCommit = env("GIT_COMMIT", "nogit");
        this.buildDate = env("BUILD_DATE", "unknown");
        this.appEnv = env("APP_ENV", "local");
        this.deploySlot = env("DEPLOY_SLOT", "n/a");
    }

    private static String env(String key, String fallback) {
        String v = System.getenv(key);
        return (v == null || v.isBlank()) ? fallback : v;
    }

    public JSONObject toJson() {
        JSONObject json = new JSONObject();
        json.put("appVersion", appVersion);
        json.put("gitCommit", gitCommit);
        json.put("buildDate", buildDate);
        json.put("environment", appEnv);
        json.put("deploySlot", deploySlot);
        return json;
    }

    public String banner() {
        return "Fraud Scoring Service | version=" + appVersion
                + " gitCommit=" + gitCommit
                + " env=" + appEnv
                + " slot=" + deploySlot
                + " builtAt=" + buildDate;
    }
}
