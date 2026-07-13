package com.bank.resilience.model;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * PaymentOrder — immutable record representing one payment to authorize.
 *
 * THE KEY FIELD: idempotencyKey
 * Generated ONCE when customer clicks "Pay". Stable across all retries and
 * all saga steps. The card network uses this to avoid double-charging.
 */
public record PaymentOrder(
    String orderId,
    String customerId,
    String customerName,
    String merchantId,
    String merchantName,
    BigDecimal amount,
    String currency,
    String cardLastFour,
    String cardNetwork,
    PaymentChannel channel,
    Instant createdAt,
    String idempotencyKey
) {
    public enum PaymentChannel { ONLINE, POS, UPI, ATM }

    public static PaymentOrder of(String customerId, String customerName,
                                   String merchantId, String merchantName,
                                   BigDecimal amount, String cardLastFour,
                                   String cardNetwork, PaymentChannel channel) {
        String orderId = "ORD-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        return new PaymentOrder(
            orderId, customerId, customerName, merchantId, merchantName,
            amount, "INR", cardLastFour, cardNetwork, channel,
            Instant.now(), UUID.randomUUID().toString()
        );
    }

    public String amountFormatted() {
        return String.format("Rs. %,.2f", amount);
    }

    @Override
    public String toString() {
        return String.format(
            "PaymentOrder[id=%s, customer=%s, merchant=%s, amount=%s, card=****%s]",
            orderId, customerName, merchantName, amountFormatted(), cardLastFour
        );
    }
}
