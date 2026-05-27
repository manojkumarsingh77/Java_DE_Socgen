package com.fintechpay.service;

import com.fintechpay.model.Transaction;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * ╔══════════════════════════════════════════════════════════════════════════╗
 * ║  TransactionService — Heap Allocation Pressure Demo                     ║
 * ╠══════════════════════════════════════════════════════════════════════════╣
 * ║                                                                          ║
 * ║  CONCEPT 2: HEAP vs CONTAINER LIMITS                                    ║
 * ║  ─────────────────────────────────────                                   ║
 * ║                                                                          ║
 * ║  This service demonstrates TWO allocation patterns:                     ║
 * ║                                                                          ║
 * ║  A) processTransactions()  — normal streaming processing.               ║
 * ║     Allocates modest objects, short-lived, stays in Eden space.        ║
 * ║     Low GC pressure. Heap stays well below Xmx.                        ║
 * ║                                                                          ║
 * ║  B) generateMonthlyStatements() — batch processing with large objects. ║
 * ║     Allocates large String/List objects, long-lived (Tenured space).   ║
 * ║     Can trigger Full GC. This is where heap exhaustion is most likely  ║
 * ║     if MaxRAMPercentage is set too high.                                ║
 * ║                                                                          ║
 * ║  KEY LESSON:                                                            ║
 * ║  Batch workloads spike heap usage. Leave buffer between Xmx             ║
 * ║  and container limit to avoid OOMKilled during batch runs.              ║
 * ║                                                                          ║
 * ╚══════════════════════════════════════════════════════════════════════════╝
 */
public class TransactionService {

    private final Random rng = new Random(99L);

    // Simulated account IDs for a retail bank branch in Mumbai
    private static final String[] ACCOUNTS = {
        "MUM-SAV-001", "MUM-SAV-002", "MUM-CUR-003",
        "MUM-SAV-004", "DEL-SAV-005", "FRAUD-TRY-001"
    };

    /**
     * Generates a realistic batch of retail banking transactions.
     *
     * HEAP INSIGHT: Each Transaction object is ~200 bytes on heap.
     * 1,000 transactions = ~200 KB — minimal heap pressure.
     * 1,000,000 transactions = ~200 MB — significant, monitor carefully.
     */
    public List<Transaction> generateTransactionBatch(int count) {
        List<Transaction> batch = new ArrayList<>(count);

        for (int i = 0; i < count; i++) {
            String account = ACCOUNTS[rng.nextInt(ACCOUNTS.length)];
            BigDecimal amount = BigDecimal.valueOf(
                100 + rng.nextInt(500_000)  // ₹100 to ₹5,00,000
            );

            Transaction.Type type = switch (rng.nextInt(4)) {
                case 0 -> Transaction.Type.CREDIT;
                case 1 -> Transaction.Type.DEBIT;
                case 2 -> Transaction.Type.TRANSFER;
                default -> Transaction.Type.FRAUD_CHECK;
            };

            batch.add(new Transaction(account, amount, type));
        }

        return batch;
    }

    /**
     * Processes transactions in a streaming fashion.
     *
     * CONCEPT 2 — LOW HEAP PRESSURE PATH:
     * Objects are created, processed, and become eligible for GC quickly.
     * This is the "healthy" allocation pattern — Eden space handles it.
     * With G1GC: minor collections reclaim Eden efficiently.
     */
    public void processTransactions(List<Transaction> transactions) {
        System.out.println("\n   [TxService] Processing " + transactions.size() + " transactions (stream mode)...");

        // Stream processing: objects created in method scope → GC-eligible after use
        long approved    = 0;
        long declined    = 0;
        long underReview = 0;

        for (Transaction tx : transactions) {
            if (tx.getStatus() == Transaction.Status.APPROVED) approved++;
            else if (tx.getStatus() == Transaction.Status.DECLINED) declined++;
            else if (tx.getStatus() == Transaction.Status.UNDER_REVIEW) underReview++;
        }

        System.out.printf("   [TxService] Results → Approved: %d | Declined: %d | Review: %d%n",
            approved, declined, underReview);
    }

    /**
     * Generates monthly account statements.
     *
     * CONCEPT 2 — HIGH HEAP PRESSURE PATH:
     * This is the DANGEROUS operation from a heap perspective.
     *
     * What happens in production:
     * - Bank runs this at month-end for all 2 million accounts
     * - Each statement = large StringBuilder (~50 KB string)
     * - 1000 concurrent statement generations = 50 MB of live objects
     * - These objects survive multiple minor GCs → promoted to Old Gen
     * - Old Gen fills up → G1 Mixed GC triggered → latency spike
     * - If MaxRAMPercentage=90% → barely any headroom → OOM under load
     *
     * THE FIX: MaxRAMPercentage=75% gives 500 MB buffer for Old Gen promotion.
     */
    public String generateMonthlyStatement(String accountId, List<Transaction> transactions) {
        // HEAP ALLOCATION: StringBuilder grows with each transaction line
        // Each line ~150 chars → 1000 tx × 150 chars = 150 KB per statement
        StringBuilder sb = new StringBuilder(transactions.size() * 150);

        sb.append("══════════════════════════════════════════════════\n");
        sb.append("  FintechPay Retail Bank — Monthly Statement\n");
        sb.append("  Account: ").append(accountId).append("\n");
        sb.append("  Period: ").append(java.time.YearMonth.now()).append("\n");
        sb.append("══════════════════════════════════════════════════\n\n");
        sb.append(String.format("  %-8s %-12s %-8s %12s %-12s%n",
            "TX-ID", "TYPE", "STATUS", "AMOUNT (₹)", "FRAUD SCORE"));
        sb.append("  " + "─".repeat(58) + "\n");

        BigDecimal totalCredits = BigDecimal.ZERO;
        BigDecimal totalDebits  = BigDecimal.ZERO;

        for (Transaction tx : transactions) {
            // Each string concat here allocates new String objects on heap
            sb.append(String.format("  %-8s %-12s %-8s %,12.2f  %.3f%n",
                tx.getId(),
                tx.getType(),
                tx.getStatus(),
                tx.getAmount(),
                tx.getFraudScore()
            ));

            if (tx.getType() == Transaction.Type.CREDIT) {
                totalCredits = totalCredits.add(tx.getAmount());
            } else {
                totalDebits = totalDebits.add(tx.getAmount());
            }
        }

        sb.append("  " + "─".repeat(58) + "\n");
        sb.append(String.format("  Total Credits:  ₹%,14.2f%n", totalCredits));
        sb.append(String.format("  Total Debits:   ₹%,14.2f%n", totalDebits));
        sb.append(String.format("  Net Balance:    ₹%,14.2f%n", totalCredits.subtract(totalDebits)));
        sb.append("\n══════════════════════════════════════════════════\n");

        // CONCEPT 2: This string now lives in Tenured/Old Gen until the caller releases it
        // If generating thousands of these concurrently → heap pressure → GC thrash
        return sb.toString();
    }

    /**
     * Demonstrates the memory footprint of different workload sizes.
     *
     * CONCEPT 1 + 2: UseContainerSupport ensures maxMemory() reflects
     * the CONTAINER limit, not the host. MaxRAMPercentage ensures we
     * never allocate more than 75% of that container limit as heap.
     */
    public static void printMemorySnapshot(String label) {
        Runtime rt = Runtime.getRuntime();
        long total = rt.totalMemory();
        long free  = rt.freeMemory();
        long max   = rt.maxMemory();
        long used  = total - free;

        double usedPct  = (double) used  / max * 100;
        double totalPct = (double) total / max * 100;

        System.out.printf(
            "   [MEM-SNAP] %-30s used=%,5d MB  total=%,5d MB  max=%,5d MB  utilization=%.1f%%%n",
            label + ":",
            used  / (1024 * 1024),
            total / (1024 * 1024),
            max   / (1024 * 1024),
            usedPct
        );

        // CONCEPT 2: Warn if we're approaching the 75% safety threshold
        if (usedPct > 80.0) {
            System.out.println("   ⚠️  Heap utilization > 80%! Risk of GC pressure and OOM.");
            System.out.println("      Check: Is MaxRAMPercentage set correctly in this container?");
        }
    }
}
