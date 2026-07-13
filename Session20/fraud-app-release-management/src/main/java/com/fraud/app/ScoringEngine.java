package com.fraud.app;

import java.util.Map;

/**
 * Pure, deterministic fraud-scoring rules engine.
 *
 * Deliberately free of Spark / HTTP / I/O so it is trivial to unit test -
 * this class is what the CI/CD pipeline's "build & test" stage exercises
 * (see src/test/java/.../ScoringEngineTest.java and .github/workflows/ci-cd.yml).
 *
 * The merchantRiskLookup map is pre-computed once at startup by Spark
 * (see FraudScoringService) from historical transaction data, then handed
 * to this engine so that each HTTP request scores in-memory with no
 * per-request Spark/cluster overhead.
 */
public class ScoringEngine {

    public static final double CRITICAL_THRESHOLD = 70.0;
    public static final double HIGH_THRESHOLD = 50.0;
    public static final double MEDIUM_THRESHOLD = 25.0;

    private final Map<String, Double> merchantRiskLookup;

    public ScoringEngine(Map<String, Double> merchantRiskLookup) {
        this.merchantRiskLookup = merchantRiskLookup;
    }

    /**
     * Computes a 0-100 style risk score for a transaction using simple,
     * explainable weighted rules (deliberately not a black-box ML model,
     * so the logic is easy to teach and unit test).
     */
    public double computeRiskScore(double amount, String merchantCategory,
                                    String regionCode, double deviceTrustScore) {
        double score = 0.0;

        // Rule 1: transaction amount tiers
        if (amount > 10000) {
            score += 40;
        } else if (amount > 3000) {
            score += 20;
        } else if (amount > 1000) {
            score += 10;
        }

        // Rule 2: historical merchant-category / region risk (from Spark aggregation)
        String key = merchantKey(merchantCategory, regionCode);
        double merchantRisk = merchantRiskLookup.getOrDefault(key, 0.0);
        score += merchantRisk;

        // Rule 3: device trust
        if (deviceTrustScore < 0.3) {
            score += 25;
        } else if (deviceTrustScore < 0.6) {
            score += 10;
        }

        return Math.min(100.0, Math.round(score * 100.0) / 100.0);
    }

    public String riskBand(double score) {
        if (score >= CRITICAL_THRESHOLD) {
            return "CRITICAL";
        } else if (score >= HIGH_THRESHOLD) {
            return "HIGH";
        } else if (score >= MEDIUM_THRESHOLD) {
            return "MEDIUM";
        }
        return "LOW";
    }

    public boolean isSuspectedFraud(double score) {
        return score >= HIGH_THRESHOLD;
    }

    public static String merchantKey(String merchantCategory, String regionCode) {
        return merchantCategory.toUpperCase() + "|" + regionCode.toUpperCase();
    }
}
