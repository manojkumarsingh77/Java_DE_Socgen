package com.bank.gcdemo;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class CardAuthorizationApplication {

    public static void main(String[] args) throws Exception {

        int workers = 8;

        AtomicLong counter = new AtomicLong();

        ExecutorService executor =
                Executors.newFixedThreadPool(workers);

        MetricsPrinter metricsPrinter = new MetricsPrinter();

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < workers; i++) {
            executor.submit(new TransactionTrafficSimulator(counter));
        }

        while (true) {
            Thread.sleep(5000);
            metricsPrinter.printMetrics(counter.get(), startTime);
        }
    }
}