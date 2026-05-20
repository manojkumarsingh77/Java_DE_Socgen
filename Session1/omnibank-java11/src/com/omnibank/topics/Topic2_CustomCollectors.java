package com.omnibank.topics;

import com.omnibank.model.Transaction;
import com.omnibank.util.DataFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.stream.Collector;

public class Topic2_CustomCollectors {

    /** Result holder - Java 11 compatible (was a record). */
    public static final class AccountStats {
        private final int count;
        private final BigDecimal total;
        private final BigDecimal avg;
        private final BigDecimal max;

        public AccountStats(int count, BigDecimal total, BigDecimal avg, BigDecimal max) {
            this.count = count; this.total = total; this.avg = avg; this.max = max;
        }
        public int count()       { return count; }
        public BigDecimal total(){ return total; }
        public BigDecimal avg()  { return avg; }
        public BigDecimal max()  { return max; }

        @Override public boolean equals(Object o) {
            if (!(o instanceof AccountStats)) return false;
            AccountStats a = (AccountStats) o;
            return count == a.count && total.equals(a.total)
                && avg.equals(a.avg) && max.equals(a.max);
        }
        @Override public int hashCode() { return Objects.hash(count, total, avg, max); }

        @Override public String toString() {
            return String.format("AccountStats[count=%d, total=%s, avg=%s, max=%s]",
                    count, total, avg, max);
        }
    }

    static class StatsAccumulator {
        int count = 0;
        BigDecimal sum = BigDecimal.ZERO;
        BigDecimal max = BigDecimal.ZERO;

        void add(Transaction t) {
            count++;
            sum = sum.add(t.getAmount());
            if (t.getAmount().compareTo(max) > 0) max = t.getAmount();
        }

        StatsAccumulator merge(StatsAccumulator other) {
            this.count += other.count;
            this.sum = this.sum.add(other.sum);
            if (other.max.compareTo(this.max) > 0) this.max = other.max;
            return this;
        }

        AccountStats toResult() {
            BigDecimal avg = count == 0
                    ? BigDecimal.ZERO
                    : sum.divide(BigDecimal.valueOf(count), 2, RoundingMode.HALF_UP);
            return new AccountStats(count, sum, avg, max);
        }
    }

    public static Collector<Transaction, StatsAccumulator, AccountStats> toAccountStats() {
        return Collector.of(
                StatsAccumulator::new,
                StatsAccumulator::add,
                StatsAccumulator::merge,
                StatsAccumulator::toResult,
                Collector.Characteristics.UNORDERED
        );
    }

    public static Collector<Transaction, ?, List<Transaction>> toTopN(int n) {
        return Collector.of(
                () -> new PriorityQueue<Transaction>(
                        Comparator.comparing(Transaction::getAmount)),
                (heap, t) -> {
                    heap.offer(t);
                    if (heap.size() > n) heap.poll();
                },
                (h1, h2) -> { h2.forEach(t -> {
                    h1.offer(t);
                    if (h1.size() > n) h1.poll();
                }); return h1; },
                heap -> {
                    List<Transaction> list = new ArrayList<>(heap);
                    list.sort(Comparator.comparing(Transaction::getAmount).reversed());
                    return Collections.unmodifiableList(list);
                }
        );
    }

    public static void run() {
        System.out.println("\n=========== TOPIC 2: CUSTOM COLLECTORS ===========");

        List<Transaction> txns = DataFactory.generateTransactions(1000, 7);

        AccountStats stats = txns.stream().collect(toAccountStats());
        System.out.println("Sequential Stats: " + stats);

        AccountStats parallelStats = txns.parallelStream().collect(toAccountStats());
        System.out.println("Parallel   Stats: " + parallelStats);
        System.out.println("Same result?      " + stats.equals(parallelStats));

        List<Transaction> top5 = txns.stream().collect(toTopN(5));
        System.out.println("Top 5 by amount:");
        top5.forEach(t -> System.out.println("  " + t));
    }
}
