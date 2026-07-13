package com.training.gitops.security;

import java.util.List;

public class ScanResult {
    public final boolean passed;
    public final int criticalCount;
    public final int highCount;
    public final int mediumCount;
    public final List<String> findings;
    public final String reason;

    public ScanResult(boolean passed, int criticalCount, int highCount, int mediumCount,
                       List<String> findings, String reason) {
        this.passed = passed;
        this.criticalCount = criticalCount;
        this.highCount = highCount;
        this.mediumCount = mediumCount;
        this.findings = findings;
        this.reason = reason;
    }
}
