package com.omnibank.topics;

import com.omnibank.model.Transaction;
import com.omnibank.util.DataFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Topic3_SpliteratorMechanics {

    static class BatchSpliterator<T> implements Spliterator<T> {
        private final List<T> source;
        private int start;
        private final int end;
        private final int batchSize;

        BatchSpliterator(List<T> source, int start, int end, int batchSize) {
            this.source = source;
            this.start = start;
            this.end = end;
            this.batchSize = batchSize;
        }

        @Override
        public boolean tryAdvance(Consumer<? super T> action) {
            if (start < end) {
                action.accept(source.get(start++));
                return true;
            }
            return false;
        }

        @Override
        public Spliterator<T> trySplit() {
            int remaining = end - start;
            if (remaining <= batchSize) return null;

            int mid = start + batchSize;
            BatchSpliterator<T> prefix =
                    new BatchSpliterator<>(source, start, mid, batchSize);
            this.start = mid;
            return prefix;
        }

        @Override public long estimateSize() { return end - start; }

        @Override
        public int characteristics() {
            return ORDERED | SIZED | SUBSIZED | IMMUTABLE | NONNULL;
        }
    }

    public static void run() {
        System.out.println("\n=========== TOPIC 3: SPLITERATOR MECHANICS ===========");

        List<Transaction> txns = DataFactory.generateTransactions(1000, 11);

        Spliterator<Transaction> defaultSpl = txns.spliterator();
        System.out.println("Default spliterator size: " + defaultSpl.estimateSize());
        System.out.println("Has SIZED?    " + defaultSpl.hasCharacteristics(Spliterator.SIZED));
        System.out.println("Has ORDERED?  " + defaultSpl.hasCharacteristics(Spliterator.ORDERED));

        BatchSpliterator<Transaction> custom =
                new BatchSpliterator<>(txns, 0, txns.size(), 250);

        List<Spliterator<Transaction>> parts = new ArrayList<>();
        parts.add(custom);
        boolean changed = true;
        while (changed) {
            changed = false;
            List<Spliterator<Transaction>> next = new ArrayList<>();
            for (Spliterator<Transaction> s : parts) {
                Spliterator<Transaction> p = s.trySplit();
                if (p != null) { next.add(p); changed = true; }
                next.add(s);
            }
            parts = next;
        }
        System.out.println("Custom spliterator produced " + parts.size() + " chunks of ~250");

        BatchSpliterator<Transaction> spl =
                new BatchSpliterator<>(txns, 0, txns.size(), 250);
        BigDecimal totalParallel = StreamSupport.stream(spl, true)
                .map(Transaction::getAmount)
                .reduce(BigDecimal.ZERO, BigDecimal::add);
        System.out.println("Parallel total via custom spliterator: " + totalParallel);

        BigDecimal totalSeq = txns.stream()
                .map(Transaction::getAmount)
                .reduce(BigDecimal.ZERO, BigDecimal::add);
        System.out.println("Sequential total:                      " + totalSeq);
        System.out.println("Match?                                 " + totalSeq.equals(totalParallel));
    }
}
