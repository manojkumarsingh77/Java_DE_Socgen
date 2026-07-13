package com.bank.resilience.model;

import java.time.Instant;

/**
 * AuthResult — represents the outcome of a payment authorization attempt.
 *
 * WHY NOT SEALED INTERFACE?
 * Sealed interfaces with pattern-matching switch require --enable-preview in Java 17.
 * We use a plain class with a Status enum instead — same expressiveness,
 * zero IntelliJ configuration needed. Java 21 makes pattern matching standard.
 *
 * Three outcomes:
 *   AUTHORIZED — payment confirmed, customer can proceed
 *   DECLINED   — business rule rejected it (insufficient funds, fraud, etc.)
 *   FAILED     — technical failure (service down, timeout, circuit open)
 */
public class AuthResult {

    public enum Status { AUTHORIZED, DECLINED, FAILED }

    public enum DeclineReason {
        INSUFFICIENT_CREDIT, FRAUD_DETECTED, CARD_BLOCKED,
        LIMIT_EXCEEDED, INVALID_MERCHANT, EXPIRED_CARD
    }

    public enum FailureType {
        CIRCUIT_OPEN, BULKHEAD_FULL, TIMEOUT,
        SERVICE_UNAVAILABLE, SAGA_COMPENSATION_TRIGGERED, FRAUD_SERVICE_OVERLOADED
    }

    private final String      orderId;
    private final Status      status;
    private final String      authCode;        // set only for AUTHORIZED
    private final DeclineReason declineReason; // set only for DECLINED
    private final FailureType failureType;     // set only for FAILED
    private final String      message;
    private final boolean     retryable;
    private final Instant     timestamp;

    // Private constructor — use factory methods below
    private AuthResult(String orderId, Status status, String authCode,
                       DeclineReason declineReason, FailureType failureType,
                       String message, boolean retryable) {
        this.orderId       = orderId;
        this.status        = status;
        this.authCode      = authCode;
        this.declineReason = declineReason;
        this.failureType   = failureType;
        this.message       = message;
        this.retryable     = retryable;
        this.timestamp     = Instant.now();
    }

    // ── Factory methods ───────────────────────────────────────────────────────

    public static AuthResult authorized(String orderId, String authCode) {
        return new AuthResult(orderId, Status.AUTHORIZED, authCode,
            null, null, "Payment authorized", false);
    }

    public static AuthResult declined(String orderId, DeclineReason reason, String message) {
        return new AuthResult(orderId, Status.DECLINED, null,
            reason, null, message, false);
    }

    public static AuthResult failed(String orderId, FailureType type,
                                     String message, boolean retryable) {
        return new AuthResult(orderId, Status.FAILED, null,
            null, type, message, retryable);
    }

    // ── Accessors ─────────────────────────────────────────────────────────────

    public String        getOrderId()       { return orderId; }
    public Status        getStatus()        { return status; }
    public String        getAuthCode()      { return authCode; }
    public DeclineReason getDeclineReason() { return declineReason; }
    public FailureType   getFailureType()   { return failureType; }
    public String        getMessage()       { return message; }
    public boolean       isRetryable()      { return retryable; }
    public Instant       getTimestamp()     { return timestamp; }

    public boolean isAuthorized() { return status == Status.AUTHORIZED; }
    public boolean isDeclined()   { return status == Status.DECLINED;   }
    public boolean isFailed()     { return status == Status.FAILED;     }

    @Override
    public String toString() {
        return switch (status) {
            case AUTHORIZED -> String.format("AUTHORIZED[orderId=%s, authCode=%s]", orderId, authCode);
            case DECLINED   -> String.format("DECLINED[orderId=%s, reason=%s]", orderId, declineReason);
            case FAILED     -> String.format("FAILED[orderId=%s, type=%s, retryable=%b]",
                                    orderId, failureType, retryable);
        };
    }
}
