package com.npunext.bank.fraud.model;

/**
 * Immutable DTO representing a single retail-banking card transaction.
 *
 * Implemented as a Java 17 record: the compiler generates the canonical
 * constructor, accessors, equals()/hashCode()/toString(), and — because all
 * components are final — the instance is safe to share across Spark's
 * task-serialization boundary without defensive copying.
 */
public record Transaction(
        String transactionId,
        String accountId,
        String customerId,
        double amount,
        String currency,
        String merchantCategory,
        String channel,
        String city,
        String country,
        long transactionEpochMillis,
        boolean foreignTransaction,
        String deviceId
) {
}
