package com.bank.gcdemo;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class FraudScoringEngine {

    private final AllocationPressureGenerator pressureGenerator =
            new AllocationPressureGenerator();

    private final HumongousObjectSimulator humongousSimulator =
            new HumongousObjectSimulator();

    private final Random random = new Random();

    public boolean score(PaymentRequest request) {

        pressureGenerator.createShortLivedGarbage();

        List<byte[]> featureVectors = new ArrayList<>();

        for (int i = 0; i < 20; i++) {
            featureVectors.add(new byte[256 * 1024]);
        }

        if (random.nextInt(1000) < 5) {
            humongousSimulator.simulateLargePayload();
        }

        return request.getAmount() < 50000;
    }
}