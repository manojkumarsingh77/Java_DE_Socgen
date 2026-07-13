package com.training.gitops.pipeline;

public class CIResult {
    public final boolean success;
    public final String version;
    public final String reason;

    public CIResult(boolean success, String version, String reason) {
        this.success = success;
        this.version = version;
        this.reason = reason;
    }
}
