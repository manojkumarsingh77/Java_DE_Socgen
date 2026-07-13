package com.bank.gcdemo;

public class HumongousObjectSimulator {

    public void simulateLargePayload() {

        byte[] payload = new byte[8 * 1024 * 1024];

        for (int i = 0; i < payload.length; i += 4096) {
            payload[i] = 1;
        }
    }
}