package com.retailbank.dataplatform.model;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Timestamp;

/**
 * Immutable domain records shared across the pipeline.
 * Kept in a single file so the demo stays self-contained; in a production
 * mono-repo these would live as separate top-level types.
 */
public final class DomainModels {

    private DomainModels() {
        // no instances — namespace holder for the nested records
    }

    /**
     * A card/POS/ACH transaction as captured at the channel edge (POS terminal,
     * mobile app, ATM). This is the "source of truth" the merchant/channel reports.
     */
    public record CardTransaction(
            String transactionId,
            String accountId,
            String branchCode,
            String channel,          // POS, ATM, MOBILE, ACH
            BigDecimal amount,
            String currency,
            Timestamp transactionTimestamp
    ) implements Serializable { }

    /**
     * The corresponding core-ledger posting for that same transaction, as recorded
     * by the core banking ledger. In a healthy system every CardTransaction has
     * exactly one matching LedgerEntry with an identical amount.
     */
    public record LedgerEntry(
            String transactionId,
            String accountId,
            BigDecimal postedAmount,
            String currency,
            Timestamp postedTimestamp
    ) implements Serializable { }

    /**
     * Output of the reconciliation pipeline: one row per transaction, flagged
     * with whether the channel record and the ledger record agree.
     */
    public record ReconciliationResult(
            String transactionId,
            String accountId,
            String branchCode,
            BigDecimal channelAmount,
            BigDecimal ledgerAmount,
            BigDecimal discrepancyAmount,
            String status,           // MATCHED, AMOUNT_MISMATCH, MISSING_LEDGER_ENTRY
            String environment
    ) implements Serializable { }
}
