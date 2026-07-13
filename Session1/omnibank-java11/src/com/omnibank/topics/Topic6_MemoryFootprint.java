package com.omnibank.topics;

import com.omnibank.model.Purchase;
import com.omnibank.util.DataFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

public class Topic6_MemoryFootprint {

    private static long usedMemoryMB() {
        Runtime rt = Runtime.getRuntime();
        return (rt.totalMemory() - rt.freeMemory()) / (1024 * 1024);
    }

    @SuppressWarnings("CallToSystemGC")
    private static void cleanGc() {
        for (int i = 0; i < 3; i++) {
            System.gc();
            try { Thread.sleep(50); } catch (InterruptedException ignored) {}
        }
    }

    public static void run() {
        System.out.println("\n=========== TOPIC 6: MEMORY FOOTPRINT ANALYSIS ===========");
        final int N = 1_000_000;

        cleanGc(); long before = usedMemoryMB();
        List<Long> boxed = new ArrayList<>(N);
        for (int i = 0; i < N; i++) boxed.add((long) i);
        long boxedUsed = usedMemoryMB() - before;
        System.out.println("Boxed List<Long>     (" + N + "): ~" + boxedUsed + " MB");
        boxed = null; cleanGc();

        before = usedMemoryMB();
        long[] primitive = new long[N];
        for (int i = 0; i < N; i++) primitive[i] = i;
        long primUsed = usedMemoryMB() - before;
        System.out.println("Primitive long[]     (" + N + "): ~" + primUsed + " MB");
        primitive = null; cleanGc();

        before = usedMemoryMB();
        List<Integer> grown = new ArrayList<>();
        for (int i = 0; i < N; i++) grown.add(i);
        long grownUsed = usedMemoryMB() - before;
        System.out.println("ArrayList (no size hint): ~" + grownUsed + " MB");
        grown = null; cleanGc();

        before = usedMemoryMB();
        List<Integer> presized = new ArrayList<>(N);
        for (int i = 0; i < N; i++) presized.add(i);
        long preUsed = usedMemoryMB() - before;
        System.out.println("ArrayList (pre-sized) : ~" + preUsed + " MB");
        presized = null; cleanGc();

        before = usedMemoryMB();
        List<Purchase> all = DataFactory.generatePurchases(N, 99);
        long holdAll = usedMemoryMB() - before;
        BigDecimal total = all.stream().map(Purchase::amount)
                .reduce(BigDecimal.ZERO, BigDecimal::add);
        System.out.println("Hold all " + N + " purchases: ~" + holdAll + " MB, total=" + total);
        all = null; cleanGc();

        before = usedMemoryMB();
        BigDecimal streamedTotal = IntStream.range(0, N)
                .mapToObj(i -> DataFactory.generatePurchases(1, i).get(0))
                .map(Purchase::amount)
                .reduce(BigDecimal.ZERO, BigDecimal::add);
        long streamed = usedMemoryMB() - before;
        System.out.println("Streamed aggregate    : ~" + streamed + " MB, total=" + streamedTotal);

        cleanGc(); before = usedMemoryMB();
        List<String> nonInterned = new ArrayList<>(N);
        for (int i = 0; i < N; i++) nonInterned.add(new String("GROCERY"));
        long ni = usedMemoryMB() - before;
        nonInterned = null; cleanGc();

        before = usedMemoryMB();
        List<String> interned = new ArrayList<>(N);
        for (int i = 0; i < N; i++) interned.add("GROCERY".intern());
        long ix = usedMemoryMB() - before;
        System.out.println("\nStrings non-interned: ~" + ni + " MB");
        System.out.println("Strings interned    : ~" + ix + " MB  (shared char[])");
        interned = null; cleanGc();

        System.out.println("\nTakeaway: prefer primitives, pre-size, stream don't hoard, intern repeated codes.");
    }
}
