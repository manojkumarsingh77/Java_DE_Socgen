package com.fraud.app;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the pure scoring rules. This is what
 * ".github/workflows/ci-cd.yml" (build-and-test job) runs via `mvn test`
 * BEFORE any Docker image is built - the classic "shift-left" CI gate.
 */
class ScoringEngineTest {

    private ScoringEngine engine;

    @BeforeEach
    void setUp() {
        Map<String, Double> lookup = Map.of(
                "CRYPTO_EXCHANGE|RGN-06", 30.0,
                "GROCERY|RGN-01", 0.0
        );
        engine = new ScoringEngine(lookup);
    }

    @Test
    void lowRiskGroceryTransactionScoresLow() {
        double score = engine.computeRiskScore(45.00, "GROCERY", "RGN-01", 0.92);
        assertEquals("LOW", engine.riskBand(score));
        assertFalse(engine.isSuspectedFraud(score));
    }

    @Test
    void largeAmountIncreasesScore() {
        double smallScore = engine.computeRiskScore(500, "GROCERY", "RGN-01", 0.9);
        double largeScore = engine.computeRiskScore(15000, "GROCERY", "RGN-01", 0.9);
        assertTrue(largeScore > smallScore);
    }

    @Test
    void lowDeviceTrustIncreasesScore() {
        double trustedScore = engine.computeRiskScore(500, "GROCERY", "RGN-01", 0.9);
        double untrustedScore = engine.computeRiskScore(500, "GROCERY", "RGN-01", 0.1);
        assertTrue(untrustedScore > trustedScore);
    }

    @Test
    void highRiskCryptoCombinationIsFlaggedCritical() {
        double score = engine.computeRiskScore(22000, "CRYPTO_EXCHANGE", "RGN-06", 0.15);
        assertEquals("CRITICAL", engine.riskBand(score));
        assertTrue(engine.isSuspectedFraud(score));
    }

    @Test
    void unknownMerchantKeyDefaultsToZeroRisk() {
        double score = engine.computeRiskScore(100, "UNKNOWN_CATEGORY", "RGN-99", 0.9);
        assertTrue(score < ScoringEngine.MEDIUM_THRESHOLD);
    }

    @Test
    void scoreIsCappedAt100() {
        double score = engine.computeRiskScore(50000, "CRYPTO_EXCHANGE", "RGN-06", 0.05);
        assertTrue(score <= 100.0);
    }
}
