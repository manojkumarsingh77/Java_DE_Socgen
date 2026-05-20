package com.omnibank.topics;

import com.omnibank.model.Transaction;
import com.omnibank.util.DataFactory;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Topic4_StreamFusion {

    public static void run() {
        System.out.println("\n=========== TOPIC 4: STREAM FUSION ===========");

        List<Transaction> txns = DataFactory.generateTransactions(1_000_000, 13);

        AtomicInteger filterCalls = new AtomicInteger();
        AtomicInteger mapCalls    = new AtomicInteger();

        long count = txns.stream()
                .filter(t -> { filterCalls.incrementAndGet();
                               return t.getType() == Transaction.Type.WITHDRAWAL; })
                .map(t   -> { mapCalls.incrementAndGet();
                              return t.getAmount(); })
                .filter(a -> a.compareTo(new BigDecimal("50000")) > 0)
                .count();

        System.out.println("Result count:           " + count);
        System.out.println("filter() invocations:   " + filterCalls.get() + " (= input size)");
        System.out.println("map()    invocations:   " + mapCalls.get() +
                " (only those that passed filter1)");
        System.out.println("--> Single pass, no intermediate List allocated.");

        AtomicInteger seen = new AtomicInteger();
        Optional<Transaction> first = txns.stream()
                .peek(t -> seen.incrementAndGet())
                .filter(t -> t.getAmount().compareTo(new BigDecimal("99000")) > 0)
                .findFirst();
        System.out.println("\nShort-circuit findFirst:");
        System.out.println("  Found?           " + first.isPresent());
        System.out.println("  Elements seen:   " + seen.get() + " (out of 1,000,000)");

        AtomicInteger sideEffect = new AtomicInteger();
        Stream<Transaction> pipeline = txns.stream().peek(t -> sideEffect.incrementAndGet());
        System.out.println("\nBuilt pipeline, no terminal op yet.");
        System.out.println("  peek() invocations so far: " + sideEffect.get() + " (must be 0)");
        long c = pipeline.count();
        System.out.println("  After terminal count():    " + sideEffect.get());

        long t1 = System.nanoTime();
        long fused = txns.stream()
                .filter(t -> t.getType() == Transaction.Type.DEPOSIT)
                .mapToLong(t -> t.getAmount().longValue())
                .filter(a -> a > 50_000)
                .sum();
        long fusedNs = System.nanoTime() - t1;

        long t2 = System.nanoTime();
        List<Transaction> step1 = txns.stream()
                .filter(t -> t.getType() == Transaction.Type.DEPOSIT)
                .collect(Collectors.toList());
        List<Long> step2 = step1.stream()
                .map(t -> t.getAmount().longValue())
                .collect(Collectors.toList());
        long naive = step2.stream().filter(a -> a > 50_000).mapToLong(Long::longValue).sum();
        long naiveNs = System.nanoTime() - t2;

        System.out.println("\nFusion vs naive multi-pass (same result expected):");
        System.out.println("  Fused result: " + fused + "  time: " + (fusedNs/1_000_000) + " ms");
        System.out.println("  Naive result: " + naive + "  time: " + (naiveNs/1_000_000) + " ms");
        System.out.println("  Naive allocates 2 intermediate Lists; fused allocates 0.");
    }
}
