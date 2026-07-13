package com.omnibank.model;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * IMMUTABLE Transaction DTO - Banking domain (Java 11 compatible).
 * Demonstrates: Immutable DTO design + Defensive copying.
 */
public final class Transaction {

    public enum Type { DEPOSIT, WITHDRAWAL, TRANSFER, PURCHASE }

    private final String txnId;
    private final String accountId;
    private final BigDecimal amount;
    private final LocalDateTime timestamp;
    private final Type type;
    private final List<String> tags;

    public Transaction(String txnId, String accountId, BigDecimal amount,
                       LocalDateTime timestamp, Type type, List<String> tags) {
        this.txnId = Objects.requireNonNull(txnId);
        this.accountId = Objects.requireNonNull(accountId);
        this.amount = Objects.requireNonNull(amount);
        this.timestamp = Objects.requireNonNull(timestamp);
        this.type = Objects.requireNonNull(type);
        // Defensive copy IN - Java 11 way (no List.copyOf needed, but works since 10+)
        this.tags = Collections.unmodifiableList(new ArrayList<>(Objects.requireNonNull(tags)));
    }

    public String getTxnId()            { return txnId; }
    public String getAccountId()        { return accountId; }
    public BigDecimal getAmount()       { return amount; }
    public LocalDateTime getTimestamp() { return timestamp; }
    public Type getType()               { return type; }

    public List<String> getTags() {
        return tags;   // already unmodifiable
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Transaction)) return false;
        Transaction t = (Transaction) o;
        return txnId.equals(t.txnId);
    }

    @Override public int hashCode() { return txnId.hashCode(); }

    @Override
    public String toString() {
        return String.format("Txn[%s|%s|%s|%s|%s]",
                txnId, accountId, type, amount, timestamp);
    }
}
