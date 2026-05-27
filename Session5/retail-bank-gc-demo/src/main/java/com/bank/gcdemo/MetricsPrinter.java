package com.bank.gcdemo;

public class MetricsPrinter {

    public void printMetrics(long txnsProcessed, long startTime) {

        Runtime runtime = Runtime.getRuntime();

        long usedHeapMb =
                (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024);

        long totalHeapMb =
                runtime.totalMemory() / (1024 * 1024);

        long maxHeapMb =
                runtime.maxMemory() / (1024 * 1024);

        long uptimeSeconds =
                (System.currentTimeMillis() - startTime) / 1000;

        System.out.println(
                "Processed=" + txnsProcessed +
                        " | UsedHeapMB=" + usedHeapMb +
                        " | TotalHeapMB=" + totalHeapMb +
                        " | MaxHeapMB=" + maxHeapMb +
                        " | UptimeSec=" + uptimeSeconds
        );
    }
}