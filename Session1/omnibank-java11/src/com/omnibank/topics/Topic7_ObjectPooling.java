package com.omnibank.topics;

import java.text.NumberFormat;
import java.util.Locale;
import java.util.concurrent.*;
import java.util.stream.IntStream;

public class Topic7_ObjectPooling {

    private static final ThreadLocal<NumberFormat> CURRENCY_TL =
            ThreadLocal.withInitial(() -> NumberFormat.getCurrencyInstance(new Locale("en", "IN")));

    static class FormatterPool {
        private final BlockingQueue<NumberFormat> pool;

        FormatterPool(int size) {
            pool = new ArrayBlockingQueue<>(size);
            for (int i = 0; i < size; i++) {
                pool.offer(NumberFormat.getCurrencyInstance(new Locale("en", "IN")));
            }
        }
        NumberFormat borrow() throws InterruptedException { return pool.take(); }
        void release(NumberFormat fmt) { pool.offer(fmt); }
        int available() { return pool.size(); }
    }

    public static void run() throws Exception {
        System.out.println("\n=========== TOPIC 7: OBJECT POOLING ===========");
        final int WORK = 200_000;

        long t1 = System.nanoTime();
        long checksumA = IntStream.range(0, WORK).parallel().mapToLong(i -> {
            NumberFormat f = NumberFormat.getCurrencyInstance(new Locale("en", "IN"));
            return f.format(i * 1.5).length();
        }).sum();
        long noPoolMs = (System.nanoTime() - t1) / 1_000_000;

        long t2 = System.nanoTime();
        long checksumB = IntStream.range(0, WORK).parallel().mapToLong(i -> {
            NumberFormat f = CURRENCY_TL.get();
            return f.format(i * 1.5).length();
        }).sum();
        long tlMs = (System.nanoTime() - t2) / 1_000_000;

        FormatterPool pool = new FormatterPool(Runtime.getRuntime().availableProcessors());
        long t3 = System.nanoTime();
        long checksumC = IntStream.range(0, WORK).parallel().mapToLong(i -> {
            NumberFormat f = null;
            try {
                f = pool.borrow();
                return f.format(i * 1.5).length();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return 0;
            } finally {
                if (f != null) pool.release(f);
            }
        }).sum();
        long bqMs = (System.nanoTime() - t3) / 1_000_000;

        System.out.println("Formats per run: " + WORK + "  (results should match)");
        System.out.println("  No pool         : " + noPoolMs + " ms   checksum=" + checksumA);
        System.out.println("  ThreadLocal pool: " + tlMs     + " ms   checksum=" + checksumB);
        System.out.println("  Bounded queue   : " + bqMs     + " ms   checksum=" + checksumC);
        System.out.println("  Pool inventory after run: " + pool.available());

        CURRENCY_TL.remove();
        System.out.println("\nPro tip: always remove() ThreadLocal in finally blocks of pooled threads.");
    }
}
