package com.fintechpay.model;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Represents a retail banking transaction in FintechPay.
 *
 * Intentionally simple — the focus of this demo is JVM behavior,
 * not domain complexity.
 */
public class Transaction {

    public enum Type { CREDIT, DEBIT, TRANSFER, FRAUD_CHECK }
    public enum Status { PENDING, APPROVED, DECLINED, UNDER_REVIEW }

    private final String id;
    private final String accountId;
    private final BigDecimal amount;
    private final Type type;
    private Status status;
    private final Instant timestamp;
    private double fraudScore;

    public Transaction(String accountId, BigDecimal amount, Type type) {
        this.id        = UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        this.accountId = accountId;
        this.amount    = amount;
        this.type      = type;
        this.status    = Status.PENDING;
        this.timestamp = Instant.now();
        this.fraudScore = 0.0;
    }

    // ── Getters & setters ─────────────────────────────────────────────────

    public String getId()             { return id; }
    public String getAccountId()      { return accountId; }
    public BigDecimal getAmount()     { return amount; }
    public Type getType()             { return type; }
    public Status getStatus()         { return status; }
    public Instant getTimestamp()     { return timestamp; }
    public double getFraudScore()     { return fraudScore; }

    public void setStatus(Status status)      { this.status = status; }
    public void setFraudScore(double score)   { this.fraudScore = score; }

    @Override
    public String toString() {
        return String.format(
            "TX[%s | acct=%-12s | %s | %8.2f INR | score=%.2f | %s]",
            id, accountId, type, amount, fraudScore, status
        );
    }
}
