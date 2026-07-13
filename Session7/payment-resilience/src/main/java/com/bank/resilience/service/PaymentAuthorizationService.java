package com.bank.resilience.service;

import com.bank.resilience.model.AuthResult;
import com.bank.resilience.model.PaymentOrder;
import com.bank.resilience.model.SagaState;
import com.bank.resilience.pattern.BulkheadPattern;
import com.bank.resilience.pattern.CircuitBreaker;
import com.bank.resilience.saga.SagaOrchestrator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

/**
 * PaymentAuthorizationService — The Full Resilience Stack in Action.
 *
 * Wires ALL four patterns into one payment authorization flow:
 *   Layer 1: Bulkhead         — separate thread pool per service
 *   Layer 2: Circuit Breaker  — protect fraud service from cascade failure
 *   Layer 3: Saga Orchestrator— coordinate distributed transaction
 *   Layer 4: Compensating Txn — undo partial work on any saga failure
 *
 * NOTE: All switch statements use standard Java 17 (no preview features).
 *   CircuitResult  checked via .isSuccess() / .isCircuitOpen() / .isError()
 *   BulkheadResult checked via .isExecuted() / .isRejected()
 *   SagaState      checked via .getStatus() enum comparison
 *   AuthResult     checked via .isAuthorized() / .isDeclined() / .isFailed()
 */
public class PaymentAuthorizationService {

    private static final Logger log = LoggerFactory.getLogger(PaymentAuthorizationService.class);

    private final CircuitBreaker   fraudCircuitBreaker;
    private final CircuitBreaker   cardNetworkCB;
    private final BulkheadPattern  bulkhead;
    private final SagaOrchestrator sagaOrchestrator;
    private final Random           random = new Random();

    // Configurable failure rates for demo scenarios
    private double  fraudFailRate      = 0.0;
    private double  cardNetworkFailRate = 0.0;
    private boolean debitAlwaysFails   = false;

    public PaymentAuthorizationService() {
        this.fraudCircuitBreaker = CircuitBreaker.forFraudService();
        this.cardNetworkCB       = CircuitBreaker.forCardNetwork();
        this.bulkhead            = BulkheadPattern.banking();
        this.sagaOrchestrator    = new SagaOrchestrator();
        log.info("PaymentAuthorizationService initialized with all resilience patterns.");
    }

    // ── Main entry point ──────────────────────────────────────────────────────

    /**
     * Authorize a payment through the complete resilience-protected pipeline.
     */
    public AuthResult authorize(PaymentOrder order) {
        log.info("AUTH | Processing: {}", order);

        // Layer 1: Bulkhead — isolate this payment thread pool from others
        BulkheadPattern.BulkheadResult<AuthResult> bulkheadResult =
            bulkhead.execute(
                BulkheadPattern.BulkheadConfig.balance().name(),
                () -> authorizeWithCircuitBreakers(order)
            );

        if (bulkheadResult.isExecuted()) {
            return bulkheadResult.getValue();
        } else {
            log.warn("AUTH | Bulkhead rejected payment {}: {}", order.orderId(), bulkheadResult.getReason());
            return AuthResult.failed(
                order.orderId(),
                AuthResult.FailureType.BULKHEAD_FULL,
                "Payment service at capacity. Please retry in 30 seconds.",
                true
            );
        }
    }

    // ── Layer 2: Circuit Breaker protection ──────────────────────────────────

    private AuthResult authorizeWithCircuitBreakers(PaymentOrder order) {
        // Pre-check fraud service via circuit breaker BEFORE starting the saga.
        // If the CB is OPEN, we apply a safe fallback policy rather than failing entirely.
        CircuitBreaker.CircuitResult<FraudPreCheckResult> fraudCBResult =
            fraudCircuitBreaker.execute(
                () -> simulateFraudPreCheck(order),
                () -> FraudPreCheckResult.ALLOW_FALLBACK
            );

        FraudPreCheckResult preCheck;
        if (fraudCBResult.isSuccess()) {
            preCheck = fraudCBResult.getValue();
            log.info("AUTH | Fraud pre-check result: {}", preCheck);
        } else if (fraudCBResult.isCircuitOpen()) {
            log.warn("AUTH | Fraud CB OPEN — applying fallback risk policy for order {}",
                order.orderId());
            preCheck = FraudPreCheckResult.ALLOW_FALLBACK;
        } else {
            // isError()
            log.error("AUTH | Fraud service threw exception: {}",
                fraudCBResult.getError() != null ? fraudCBResult.getError().getMessage() : "unknown");
            preCheck = FraudPreCheckResult.ALLOW_FALLBACK;
        }

        // Hard block: explicit fraud signal
        if (preCheck == FraudPreCheckResult.BLOCK) {
            return AuthResult.declined(
                order.orderId(),
                AuthResult.DeclineReason.FRAUD_DETECTED,
                "Transaction blocked by fraud detection engine."
            );
        }

        // Layers 3 & 4: Saga with compensating transactions
        boolean fraudCircuitWasOpen = (preCheck == FraudPreCheckResult.ALLOW_FALLBACK);
        return authorizeViaSaga(order, fraudCircuitWasOpen);
    }

    // ── Layers 3 & 4: Saga + Compensating Transactions ───────────────────────

    private AuthResult authorizeViaSaga(PaymentOrder order, boolean fraudCircuitWasOpen) {
        boolean fraudFail = random.nextDouble() < fraudFailRate;
        boolean cardFail  = random.nextDouble() < cardNetworkFailRate;

        if (fraudCircuitWasOpen) {
            // Circuit was open: we already decided to allow this payment at the pre-check level.
            // Skip the in-saga fraud step to avoid redundant work.
            log.warn("AUTH | Fraud CB was OPEN — skipping in-saga ML check, using allow-list policy.");
            fraudFail = false;
        }

        var steps      = sagaOrchestrator.buildPaymentAuthorizationSteps(fraudFail, cardFail, debitAlwaysFails);
        var sagaState  = sagaOrchestrator.executePaymentSaga(order, steps);

        log.info("AUTH | Saga result: {}", sagaState);

        if (sagaState.getStatus() == SagaState.SagaStatus.COMMITTED) {
            String authCode = "AUTH-" + System.currentTimeMillis() % 1_000_000;
            log.info("AUTH | Payment AUTHORIZED: {} authCode={}", order.orderId(), authCode);
            return AuthResult.authorized(order.orderId(), authCode);

        } else if (sagaState.getStatus() == SagaState.SagaStatus.COMPENSATED) {
            log.warn("AUTH | Saga COMPENSATED for {}: {}", order.orderId(), sagaState.getFailureReason());
            printSagaAuditTrail(sagaState);
            return AuthResult.failed(
                order.orderId(),
                AuthResult.FailureType.SAGA_COMPENSATION_TRIGGERED,
                "Payment declined: " + sagaState.getFailureReason(),
                false
            );

        } else {
            return AuthResult.failed(
                order.orderId(),
                AuthResult.FailureType.SERVICE_UNAVAILABLE,
                "Unexpected saga state: " + sagaState.getStatus(),
                true
            );
        }
    }

    // ── Fraud pre-check simulation ────────────────────────────────────────────

    private FraudPreCheckResult simulateFraudPreCheck(PaymentOrder order) {
        if (random.nextDouble() < fraudFailRate) {
            throw new RuntimeException("FraudService timeout after 5000ms");
        }
        // Rule: amounts over Rs.1,00,000 get a hard block for further manual review
        if (order.amount().intValue() > 100_000) {
            return FraudPreCheckResult.BLOCK;
        }
        return FraudPreCheckResult.ALLOW;
    }

    private enum FraudPreCheckResult { ALLOW, BLOCK, ALLOW_FALLBACK }

    // ── Audit trail printer ───────────────────────────────────────────────────

    private void printSagaAuditTrail(SagaState state) {
        log.info("== SAGA AUDIT TRAIL [{}] ====================", state.getSagaId());
        log.info("  Status: {} | Failed at: {}", state.getStatus(), state.getFailedAtStep());
        log.info("  Completed steps ({}):", state.getCompletedSteps().size());
        state.getCompletedSteps().forEach(s -> log.info("    {}", s));
        log.info("  Compensated steps ({}):", state.getCompensatedSteps().size());
        state.getCompensatedSteps().forEach(s -> log.info("    {}", s));
        log.info("=============================================");
    }

    // ── Configuration setters for demo scenarios ──────────────────────────────

    public void setFraudFailRate(double rate)       { this.fraudFailRate = rate; }
    public void setCardNetworkFailRate(double rate)  { this.cardNetworkFailRate = rate; }
    public void setDebitAlwaysFails(boolean fail)    { this.debitAlwaysFails = fail; }

    // ── Accessors for observability in simulations ────────────────────────────

    public CircuitBreaker  getFraudCircuitBreaker() { return fraudCircuitBreaker; }
    public CircuitBreaker  getCardNetworkCB()       { return cardNetworkCB; }
    public BulkheadPattern getBulkhead()            { return bulkhead; }
}
