package com.training.gitops.deployment;

import com.training.gitops.job.InventoryAnalyticsJob;

import java.util.Random;

/**
 * === TOPIC: Canary Release ===
 * <p>
 * THE PROBLEM this class demonstrates:
 * Even after passing a health check in an idle Blue/Green slot, a new version
 * can still misbehave under REAL production traffic and load patterns. Prod
 * is the highest-risk environment - an all-at-once cutover there risks 100%
 * of users hitting a bad release simultaneously.
 * <p>
 * THE SOLUTION this class demonstrates:
 * Progressive traffic shifting in waves (10% -> 25% -> 50% -> 100% by
 * default). At each wave, real (simulated) requests are split between the
 * current stable version and the new candidate; the candidate's error rate is
 * measured; if it exceeds a threshold, the rollout automatically halts and
 * reverts the candidate's traffic share to 0% - all BEFORE 100% of users were
 * ever exposed. This is the risk-managed strategy this demo uses specifically
 * for the Prod environment (see EnvironmentPromoter).
 */
public class CanaryReleaseManager {

    private static final int[] DEFAULT_WAVES_PERCENT = {10, 25, 50, 100};
    private static final int REQUESTS_PER_WAVE = 50;

    public CanaryOutcome rollout(String candidateVersion, boolean injectFailure) {
        double errorThresholdPercent = envDouble("CANARY_ERROR_THRESHOLD_PERCENT", 15.0);
        int[] waves = DEFAULT_WAVES_PERCENT;

        System.out.println("  [canary] rolling out candidate version " + candidateVersion
                + " across waves " + java.util.Arrays.toString(waves) + "% "
                + "(auto-rollback if candidate error rate > " + errorThresholdPercent + "%)");

        // One smoke test up-front proves the candidate can even start correctly.
        boolean bootHealthy = new InventoryAnalyticsJob().runSmokeTest("canary-boot-check", injectFailure);
        if (!bootHealthy) {
            System.out.println("  [canary] candidate failed its BOOT health check -> rollout never started (0% traffic exposure)");
            return new CanaryOutcome(false, 0, "Boot health check failed");
        }

        Random random = new Random(123L);
        double candidateFailureProbability = injectFailure ? 0.35 : 0.01; // 35% vs a normal ~1% baseline noise

        for (int wavePercent : waves) {
            int candidateRequests = Math.round(REQUESTS_PER_WAVE * (wavePercent / 100.0f));
            int stableRequests = REQUESTS_PER_WAVE - candidateRequests;
            int candidateErrors = 0;
            for (int i = 0; i < candidateRequests; i++) {
                if (random.nextDouble() < candidateFailureProbability) candidateErrors++;
            }
            double candidateErrorRate = candidateRequests == 0 ? 0.0 : (candidateErrors * 100.0 / candidateRequests);

            System.out.printf("  [canary] wave %3d%%  ->  candidate reqs=%3d (errors=%2d, %.1f%%)   stable reqs=%3d%n",
                    wavePercent, candidateRequests, candidateErrors, candidateErrorRate, stableRequests);

            if (candidateErrorRate > errorThresholdPercent) {
                System.out.println("  [canary] error rate " + String.format("%.1f", candidateErrorRate)
                        + "% EXCEEDS threshold " + errorThresholdPercent + "% at the " + wavePercent
                        + "% wave -> AUTO-ROLLBACK: candidate traffic share reverted to 0%");
                return new CanaryOutcome(false, wavePercent,
                        "Error rate " + String.format("%.1f", candidateErrorRate) + "% exceeded threshold at " + wavePercent + "% wave");
            }
        }

        System.out.println("  [canary] all waves completed within error threshold -> candidate PROMOTED to 100% (new stable)");
        return new CanaryOutcome(true, 100, "All waves passed");
    }

    private static double envDouble(String key, double def) {
        String v = System.getenv(key);
        if (v == null || v.isBlank()) return def;
        try { return Double.parseDouble(v.trim()); } catch (NumberFormatException e) { return def; }
    }

    public static class CanaryOutcome {
        public final boolean success;
        public final int reachedPercent;
        public final String reason;

        public CanaryOutcome(boolean success, int reachedPercent, String reason) {
            this.success = success;
            this.reachedPercent = reachedPercent;
            this.reason = reason;
        }
    }
}
