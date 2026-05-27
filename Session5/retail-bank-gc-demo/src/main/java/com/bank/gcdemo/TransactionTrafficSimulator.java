package com.bank.gcdemo;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class TransactionTrafficSimulator implements Runnable {

    private final AuthorizationService authorizationService =
            new AuthorizationService();

    private final AtomicLong counter;
    private final Random random = new Random();

    public TransactionTrafficSimulator(AtomicLong counter) {
        this.counter = counter;
    }

    @Override
    public void run() {

        while (true) {

            PaymentRequest request =
                    new PaymentRequest(
                            counter.incrementAndGet(),
                            random.nextLong(100000),
                            random.nextDouble(100000),
                            "M-" + random.nextInt(5000)
                    );

            authorizationService.authorize(request);
        }
    }
}