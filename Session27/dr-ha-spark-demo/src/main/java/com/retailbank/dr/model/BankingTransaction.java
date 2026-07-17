package com.retailbank.dr.model;

import java.io.Serializable;
import java.time.Instant;

/**
 * Immutable DTO representing a single retail-banking ledger event.
 * Modeled as a Java 17 record: compact, immutable, and directly mappable
 * to a Spark Row via reflection-based Encoders.bean(...) or an explicit StructType.
 *
 * This is the atomic unit whose journey between the PRIMARY region table and the
 * SECONDARY (replica) region table is what we measure for RPO (how many/which of
 * these were lost) and RTO (how long until reads/writes resume after failover).
 */
public record BankingTransaction(
        String transactionId,
        String accountId,
        String customerId,
        String branchId,
        String region,              // origin region at write time, e.g. "us-east" (primary)
        String transactionType,     // DEBIT, CREDIT, TRANSFER, ATM_WITHDRAWAL, POS_PAYMENT
        double amount,
        String currency,
        String status,              // POSTED, PENDING, REVERSED
        Instant eventTimestamp,     // business event time (when the transaction occurred)
        Instant ingestTimestamp,    // when the primary region committed it to the ledger
        int batchId                 // synthetic micro-batch id used to simulate streaming ingestion
) implements Serializable {

    /** Compact canonical constructor performing defensive validation. */
    public BankingTransaction {
        if (transactionId == null || transactionId.isBlank()) {
            throw new IllegalArgumentException("transactionId must not be blank");
        }
        if (amount < 0 && !"REVERSED".equals(status)) {
            throw new IllegalArgumentException("Negative amount only allowed for REVERSED transactions");
        }
    }
}
