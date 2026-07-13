package com.bank.resilience.saga;

import com.bank.resilience.model.PaymentOrder;
import com.bank.resilience.model.SagaState;
import com.bank.resilience.model.SagaState.SagaStep;
import com.bank.resilience.model.SagaState.SagaStep.StepStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;

/**
 * SagaOrchestrator — Central coordinator for distributed payment transaction.
 *
 * ── ORCHESTRATION APPROACH ───────────────────────────────────────────────────
 * One central orchestrator knows ALL steps and ALL compensations.
 * It calls each service in order, records state in the saga log, and
 * triggers compensation in reverse order when any step fails.
 *
 *   PRO: Easy to understand. Single audit trail. Easy to debug.
 *        Compensations are explicitly coded in one place.
 *   CON: Orchestrator is a single point of coupling.
 *   Tools: Axon Framework, AWS Step Functions, Camunda BPM.
 *
 * ── COMPENSATING TRANSACTIONS ────────────────────────────────────────────────
 * When step N fails, execute compensation for steps N-1 ... 0 in REVERSE.
 * Compensations are NOT rollbacks — they are new forward-moving transactions
 * that semantically undo the effect (void Visa auth, release reservation).
 *
 * ── THE PROBLEM SOLVED ───────────────────────────────────────────────────────
 * Without saga: Visa authorizes Rs.45,000 (step 3), CBS debit fails (step 4).
 * Visa auth is live, customer limit locked, merchant expects settlement.
 * 40 phantom cases/day x 45 min each = 30 hours manual ops work per day.
 * With saga: CBS failure triggers automatic Visa void. Zero phantom auths.
 */
public class SagaOrchestrator {

    private static final Logger log = LoggerFactory.getLogger(SagaOrchestrator.class);

    /**
     * SagaStep — one step in the saga with a forward action and a compensating action.
     *
     * @param <T>          return type of the forward action
     * @param stepName     human-readable name (e.g. "Reserve credit limit")
     * @param serviceName  which microservice handles this step
     * @param action       forward action: what to do when executing this step
     * @param compensation compensating action: how to undo this step if a later step fails
     */
    public record SagaStep<T>(
        String stepName,
        String serviceName,
        Function<PaymentOrder, T>    action,
        Function<PaymentOrder, Void> compensation
    ) {}

    // ── Core saga execution ───────────────────────────────────────────────────

    /**
     * Execute a payment authorization saga.
     *
     * Steps:
     *   1. Execute each step in sequence, recording results in the saga log.
     *   2. If any step throws → trigger compensation for all previously
     *      completed steps in REVERSE order.
     *   3. Return SagaState with a complete audit trail of every action taken.
     *
     * @param order the payment order being authorized
     * @param steps ordered list of saga steps (build with buildPaymentAuthorizationSteps)
     * @return SagaState with status COMMITTED (all succeeded) or COMPENSATED (some rolled back)
     */
    public SagaState executePaymentSaga(PaymentOrder order, List<SagaStep<?>> steps) {
        String sagaId = "SAGA-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        List<SagaState.SagaStep> completed   = new ArrayList<>();
        List<SagaState.SagaStep> compensated = new ArrayList<>();

        log.info("SAGA [{}] Starting for order: {}", sagaId, order);
        log.info("SAGA [{}] Steps to execute: {}", sagaId,
            steps.stream().map(SagaStep::stepName).toList());

        for (int i = 0; i < steps.size(); i++) {
            SagaStep<?> step = steps.get(i);
            log.info("SAGA [{}] Step {}/{}: '{}' on {}",
                sagaId, i + 1, steps.size(), step.stepName(), step.serviceName());

            try {
                // Execute the forward action
                step.action().apply(order);

                // Record success
                completed.add(new SagaState.SagaStep(
                    step.stepName(), step.serviceName(),
                    StepStatus.COMPLETED, "Executed successfully", Instant.now()
                ));
                log.info("SAGA [{}] Step {} COMPLETED: '{}'", sagaId, i + 1, step.stepName());

            } catch (Exception e) {
                log.error("SAGA [{}] Step {} FAILED: '{}' — {}",
                    sagaId, i + 1, step.stepName(), e.getMessage());

                // Record the failure
                completed.add(new SagaState.SagaStep(
                    step.stepName(), step.serviceName(),
                    StepStatus.FAILED, "Failed: " + e.getMessage(), Instant.now()
                ));

                // ── Trigger compensation in REVERSE order ─────────────────────
                log.warn("SAGA [{}] Initiating COMPENSATION for {} previously completed steps...",
                    sagaId, i);

                for (int j = i - 1; j >= 0; j--) {
                    SagaStep<?> toCompensate = steps.get(j);
                    log.warn("SAGA [{}] Compensating step {}: '{}'",
                        sagaId, j + 1, toCompensate.stepName());
                    try {
                        toCompensate.compensation().apply(order);
                        compensated.add(new SagaState.SagaStep(
                            toCompensate.stepName(), toCompensate.serviceName(),
                            StepStatus.COMPENSATED, "Compensation executed", Instant.now()
                        ));
                        log.warn("SAGA [{}] Compensation for '{}' DONE.",
                            sagaId, toCompensate.stepName());
                    } catch (Exception ce) {
                        // Compensation itself failed — this is a "stuck saga"
                        // Production: alert ops via PagerDuty, persist to saga_stuck table
                        log.error("SAGA [{}] COMPENSATION FAILED for '{}': {} — ops alerted!",
                            sagaId, toCompensate.stepName(), ce.getMessage());
                    }
                }

                return new SagaState(sagaId, order.orderId(),
                    SagaState.SagaStatus.COMPENSATED,
                    List.copyOf(completed), List.copyOf(compensated),
                    step.stepName(), e.getMessage(),
                    Instant.now(), Instant.now()
                );
            }
        }

        // All steps succeeded — saga committed
        log.info("SAGA [{}] ALL {} steps COMPLETED. Saga COMMITTED.", sagaId, steps.size());
        return new SagaState(sagaId, order.orderId(),
            SagaState.SagaStatus.COMMITTED,
            List.copyOf(completed), List.of(),
            null, null, Instant.now(), Instant.now()
        );
    }

    // ── Step builder ──────────────────────────────────────────────────────────

    /**
     * Build the standard 5-step payment authorization saga.
     *
     * Each step has:
     *   action      — what the service does when the payment is being authorized
     *   compensation— what the service does to UNDO its action if a later step fails
     *
     * The compensation design is the most important engineering decision in a Saga.
     * Each compensation must be:
     *   - Idempotent: safe to call multiple times (in case of retries)
     *   - Semantically correct: undo the business effect, not just the DB row
     *
     * @param fraudShouldFail  if true, step 2 throws (fraud detected — demo)
     * @param cardNetworkFail  if true, step 3 throws (Visa timeout — demo)
     * @param debitFail        if true, step 4 throws (CBS unavailable — demo)
     */
    public List<SagaStep<?>> buildPaymentAuthorizationSteps(
            boolean fraudShouldFail, boolean cardNetworkFail, boolean debitFail) {

        return List.of(

            // ── STEP 1: Reserve credit limit ──────────────────────────────────
            // Forward:     Put a hold on Rs.45,000 of the customer's credit limit.
            // Compensate:  Release the hold — limit goes back to customer.
            new SagaStep<>(
                "Reserve credit limit", "HDFC Ledger Service",
                order -> {
                    log.info("  [LEDGER] Reserving {} for order {}",
                        order.amountFormatted(), order.orderId());
                    sleep(50);
                    return "RESERVED";
                },
                order -> {
                    log.warn("  [LEDGER] COMPENSATE: Releasing {} reservation for {}",
                        order.amountFormatted(), order.orderId());
                    sleep(30);
                    return null;
                }
            ),

            // ── STEP 2: Fraud screening ───────────────────────────────────────
            // Forward:     ML model scores transaction. Block if score > 80.
            // Compensate:  Record false-positive flag for model retraining.
            new SagaStep<>(
                "Fraud screening", "ML Fraud Engine",
                order -> {
                    log.info("  [FRAUD] ML check for {} amount {}",
                        order.customerName(), order.amountFormatted());
                    sleep(80);
                    if (fraudShouldFail) {
                        throw new RuntimeException(
                            "FRAUD_DETECTED: transaction pattern matches stolen card profile");
                    }
                    log.info("  [FRAUD] Score: 12/100 - LOW RISK. Approved.");
                    return "APPROVED";
                },
                order -> {
                    log.warn("  [FRAUD] COMPENSATE: Recording false-positive audit flag");
                    sleep(20);
                    return null;
                }
            ),

            // ── STEP 3: Card network authorization ────────────────────────────
            // Forward:     Request authorization from Visa/Mastercard.
            // Compensate:  Send void/reversal to cancel the authorization code.
            //              This is CRITICAL — without this, the customer's limit
            //              stays locked for up to 7 days.
            new SagaStep<>(
                "Card network auth", "Visa International",
                order -> {
                    log.info("  [VISA] Auth request for card ****{}", order.cardLastFour());
                    sleep(120);
                    if (cardNetworkFail) {
                        throw new RuntimeException(
                            "CARD_NETWORK_TIMEOUT: Visa gateway unresponsive after 5s");
                    }
                    String authCode = "VIS-" + (int) (Math.random() * 999999);
                    log.info("  [VISA] Auth code issued: {}", authCode);
                    return authCode;
                },
                order -> {
                    log.warn("  [VISA] COMPENSATE: Voiding authorization for card ****{}",
                        order.cardLastFour());
                    sleep(100);
                    return null;
                }
            ),

            // ── STEP 4: Debit core banking ledger ─────────────────────────────
            // Forward:     Actually deduct Rs.45,000 from the customer's account.
            // Compensate:  Credit Rs.45,000 back (refund). Triggers SMS to customer.
            new SagaStep<>(
                "Debit account", "Core Banking System",
                order -> {
                    log.info("  [CBS] Debiting {} from account", order.amountFormatted());
                    sleep(60);
                    if (debitFail) {
                        throw new RuntimeException(
                            "CBS_ERROR: core banking system temporarily unavailable");
                    }
                    log.info("  [CBS] Debit successful. Available limit updated.");
                    return "DEBITED";
                },
                order -> {
                    log.warn("  [CBS] COMPENSATE: Reversing debit - crediting {} back",
                        order.amountFormatted());
                    sleep(40);
                    return null;
                }
            ),

            // ── STEP 5: Customer notification ─────────────────────────────────
            // Forward:     Send SMS + push notification to customer.
            // Compensate:  Send "payment failed" notification.
            new SagaStep<>(
                "Notify customer", "SMS/Push Gateway",
                order -> {
                    log.info("  [NOTIF] Sending confirmation to {} for {}",
                        order.customerName(), order.amountFormatted());
                    sleep(30);
                    log.info("  [NOTIF] SMS: 'Payment of {} to {} confirmed.'",
                        order.amountFormatted(), order.merchantName());
                    return "NOTIFIED";
                },
                order -> {
                    log.warn("  [NOTIF] COMPENSATE: Sending failure SMS to {}",
                        order.customerName());
                    sleep(20);
                    return null;
                }
            )
        );
    }

    // ── Utility ───────────────────────────────────────────────────────────────

    /**
     * Compressed sleep for demo — real delays divided by 10 so demos run quickly.
     * In production remove this entirely; the actual network calls provide latency.
     */
    private static void sleep(long ms) {
        try {
            Thread.sleep(ms / 10);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
