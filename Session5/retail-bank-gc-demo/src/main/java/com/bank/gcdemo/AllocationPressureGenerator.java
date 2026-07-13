package com.bank.gcdemo;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class AllocationPressureGenerator {

    public void createShortLivedGarbage() {

        List<String> tempObjects = new ArrayList<>();

        for (int i = 0; i < 2000; i++) {
            tempObjects.add(UUID.randomUUID().toString());
        }
    }
}