package com.acme.retail.analytics;

import java.util.function.Supplier;

public final class JobTimer {

    private JobTimer() {
    }

    public static <T> T time(String label, Supplier<T> supplier) {
        long start = System.nanoTime();
        T result = supplier.get();
        long end = System.nanoTime();
        double seconds = (end - start) / 1_000_000_000.0;
        System.out.printf("%s took %.2f seconds%n", label, seconds);
        return result;
    }
}