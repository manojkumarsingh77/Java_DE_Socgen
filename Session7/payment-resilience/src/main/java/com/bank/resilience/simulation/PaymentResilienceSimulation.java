package com.bank.resilience.simulation;

import com.bank.resilience.model.AuthResult;
import com.bank.resilience.model.PaymentOrder;
import com.bank.resilience.model.SagaState;
import com.bank.resilience.pattern.BulkheadPattern;
import com.bank.resilience.pattern.CircuitBreaker;
import com.bank.resilience.saga.SagaOrchestrator;
import com.bank.resilience.service.PaymentAuthorizationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * PaymentResilienceSimulation — Vikram's Friday Night Purchase
 *
 * BUSINESS PROBLEM STATEMENT:
 * Bank Aadhara processes 8 million card payments per day touching 5 services.
 * During peak load (flash sales, salary day) three problems occur:
 *
 * PROBLEM 1 — Fraud service overload cascade:
 *   Flash sale → fraud ML engine saturated → 30s timeouts → all 200 shared
 *   threads occupied → balance + card network + CBS all blocked → full outage.
 *   FIX: Circuit breaker trips after 5 failures; remaining calls fast-fail in <1ms.
 *
 * PROBLEM 2 — Notification backlog kills payment speed:
 *   Diwali SMS queue: 20s/notification → all threads waiting for SMS ACK
 *   → payment auth blocked even though balance/card services are healthy.
 *   FIX: Bulkhead gives notification service its own 5-thread pool.
 *        Notification saturation CANNOT touch balance or card thread pools.
 *
 * PROBLEM 3 — Phantom authorizations (ghost debits):
 *   Visa authorizes Rs.45,000 (step 3). Core banking debit fails (step 4).
 *   Visa auth is live, customer limit locked, merchant expects settlement.
 *   40 such exceptions/day × 45 min each = manual ops nightmare.
 *   FIX: Saga + compensating transactions automatically void the Visa auth.
 *
 * HOW TO RUN:
 *   IntelliJ → File → Open → payment-resilience folder
 *   Right-click PaymentResilienceSimulation → Run main()
 *
 * NOTE: All switch statements use standard Java 17 enum switches.
 *   No --enable-preview needed. Runs out-of-the-box in IntelliJ.
 */
public class PaymentResilienceSimulation {

    private static final Logger log = LoggerFactory.getLogger(PaymentResilienceSimulation.class);

    private static final String VIKRAM_ID     = "CUST-VN-2024";
    private static final String VIKRAM_NAME   = "Vikram Nair";
    private static final String AMAZON_ID     = "MER-AMZ-IN";
    private static final String AMOUNT_LAPTOP = "45000";
    private static final String AMOUNT_COFFEE = "280";

    public static void main(String[] args) throws InterruptedException {
        printBanner();

        demo1_HappyPath();                    divider();
        demo2_CircuitBreakerFraudOverload();  divider();
        demo3_BulkheadIsolation();            divider();
        demo4_SagaOrchestratorHappyPath();    divider();
        demo5_SagaWithCompensation();         divider();
        demo6_SagaChoreographyContrast();     divider();
        demo7_FullResilienceUnderLoad();

        log.info("\n\n=== ALL DEMOS COMPLETE ===\n");
    }

    // =========================================================================
    // DEMO 1 — Happy path: all services healthy
    // =========================================================================
    static void demo1_HappyPath() throws InterruptedException {
        log.info("""

            +----------------------------------------------------------+
            |  DEMO 1 - Happy Path: Vikram buys laptop successfully    |
            |  All 5 services healthy. No failures. Full saga commit.  |
            +----------------------------------------------------------+
            """);

        var svc   = new PaymentAuthorizationService();
        var order = buildOrder(AMOUNT_LAPTOP);

        AuthResult result = svc.authorize(order);
        logAuthResult("DEMO1", result);
    }

    // =========================================================================
    // DEMO 2 — Circuit Breaker: fraud service trips and recovers
    // =========================================================================
    static void demo2_CircuitBreakerFraudOverload() throws InterruptedException {
        log.info("""

            +----------------------------------------------------------+
            |  DEMO 2 - Circuit Breaker: Fraud service overloaded      |
            |  Fraud service failing -> CB trips -> fast-fail -> probe |
            +----------------------------------------------------------+
            """);

        // Use a short 2-second reset timeout so demo runs quickly
        var fraudCB = new CircuitBreaker("FRAUD_SERVICE_DEMO", 3, 1, 2000);
        log.info("DEMO2 | Initial CB state: {}", fraudCB.getState());

        // Step A: Send 7 calls — all fail → CB trips after 3rd failure
        for (int i = 1; i <= 7; i++) {
            Thread.sleep(150);
            log.info("DEMO2 | Payment {} — CB state before call: {}", i, fraudCB.getState());

            CircuitBreaker.CircuitResult<String> result = fraudCB.execute(
                () -> { throw new RuntimeException("FraudService TIMEOUT after 5000ms"); },
                () -> "FALLBACK-ALLOW"
            );

            if (result.isSuccess()) {
                log.info("DEMO2 |   SUCCESS: {}", result.getValue());
            } else if (result.isCircuitOpen()) {
                log.warn("DEMO2 |   CB OPEN — fast-failed in <1ms. Fallback: {}", result.getReason());
            } else {
                log.error("DEMO2 |   Service error recorded: {}",
                    result.getError() != null ? result.getError().getMessage() : "unknown");
            }
        }

        log.info("DEMO2 | CB snapshot after 7 failures: {}", fraudCB.getSnapshot());

        // Step B: Wait for reset timeout → CB goes HALF_OPEN
        log.info("DEMO2 | Waiting 2.2s for reset timeout → HALF_OPEN...");
        Thread.sleep(2200);
        log.info("DEMO2 | CB state after wait: {} (should be HALF_OPEN)", fraudCB.getState());

        // Step C: Send a successful probe → CB closes
        CircuitBreaker.CircuitResult<String> probeResult =
            fraudCB.execute(() -> "fraud-check-ok", () -> "fallback");

        log.info("DEMO2 | Probe result kind: {} | CB state: {}",
            probeResult.getKind(), fraudCB.getState());

        log.info("""
            DEMO2 | KEY INSIGHT:
                    Without CB: 7 payments x 5s timeout = 35s thread occupation.
                    With CB:    3 failed normally, 4 fast-failed in <1ms each.
                    Thread starvation PREVENTED. System degraded, not collapsed.
            """);
    }

    // =========================================================================
    // DEMO 3 — Bulkhead isolation: notification pool flood doesn't kill payments
    // =========================================================================
    static void demo3_BulkheadIsolation() throws InterruptedException {
        log.info("""

            +----------------------------------------------------------+
            |  DEMO 3 - Bulkhead: Notification overload -> payment OK  |
            |  Flood notification pool, verify balance pool unaffected  |
            +----------------------------------------------------------+
            """);

        var bh = BulkheadPattern.banking();
        bh.printAllStats();

        AtomicInteger notifRejected   = new AtomicInteger(0);
        AtomicInteger balanceRejected = new AtomicInteger(0);

        // Flood the notification bulkhead (pool size = 5) with 8 slow calls
        log.info("DEMO3 | Flooding NOTIFICATION pool (5 threads) with 8 concurrent slow requests...");
        List<Thread> notifThreads = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            final int taskId = i + 1;
            Thread t = new Thread(() -> {
                BulkheadPattern.BulkheadResult<String> res =
                    bh.execute(BulkheadPattern.BulkheadConfig.notification().name(), () -> {
                        try { Thread.sleep(500); } catch (InterruptedException ex) {
                            Thread.currentThread().interrupt();
                        }
                        return "SMS sent #" + taskId;
                    });
                if (res.isRejected()) {
                    notifRejected.incrementAndGet();
                    log.warn("DEMO3 | Notification #{} REJECTED: {}", taskId, res.getReason());
                } else {
                    log.info("DEMO3 | Notification #{} executed normally.", taskId);
                }
            });
            notifThreads.add(t);
            t.start();
            Thread.sleep(30);
        }

        Thread.sleep(250); // Let notification pool saturate

        // While notification pool is flooded, balance service must be unaffected
        log.info("DEMO3 | Notification pool flooded. Testing BALANCE SERVICE now...");
        for (int i = 0; i < 3; i++) {
            BulkheadPattern.BulkheadResult<String> res =
                bh.execute(BulkheadPattern.BulkheadConfig.balance().name(),
                    () -> "balance-check-ok");
            if (res.isExecuted()) {
                log.info("DEMO3 | Balance check {} EXECUTED NORMALLY despite notification flood.", i + 1);
            } else {
                balanceRejected.incrementAndGet();
                log.error("DEMO3 | Balance check {} REJECTED — isolation FAILED!", i + 1);
            }
        }

        notifThreads.forEach(t -> {
            try { t.join(2000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        });

        log.info("DEMO3 | notif-rejected={}, balance-rejected={}", notifRejected.get(), balanceRejected.get());
        log.info("DEMO3 | Bulkhead isolation: {}", balanceRejected.get() == 0 ? "WORKS CORRECTLY" : "FAILED");
        bh.printAllStats();
    }

    // =========================================================================
    // DEMO 4 — Saga orchestrator: happy path, all 5 steps commit
    // =========================================================================
    static void demo4_SagaOrchestratorHappyPath() throws InterruptedException {
        log.info("""

            +----------------------------------------------------------+
            |  DEMO 4 - Saga Orchestrator: All 5 steps succeed         |
            |  Vikram's laptop purchase: full saga COMMITTED            |
            +----------------------------------------------------------+
            """);

        var orchestrator = new SagaOrchestrator();
        var order        = buildOrder(AMOUNT_LAPTOP);
        var steps        = orchestrator.buildPaymentAuthorizationSteps(false, false, false);
        SagaState saga   = orchestrator.executePaymentSaga(order, steps);

        log.info("DEMO4 | Saga outcome: {}", saga);
        log.info("DEMO4 | Steps completed: {}", saga.completedStepCount());
        saga.getCompletedSteps().forEach(s -> log.info("DEMO4 |   {}", s));
    }

    // =========================================================================
    // DEMO 5 — Saga with compensating transactions (card network fails at step 3)
    // =========================================================================
    static void demo5_SagaWithCompensation() throws InterruptedException {
        log.info("""

            +----------------------------------------------------------+
            |  DEMO 5 - Compensating Transactions: card network fails  |
            |  Steps 1+2 succeed. Step 3 fails. Compensations fire.    |
            +----------------------------------------------------------+
            """);

        var orchestrator = new SagaOrchestrator();
        var order        = buildOrder(AMOUNT_LAPTOP);

        // Inject card-network failure at step 3
        var steps      = orchestrator.buildPaymentAuthorizationSteps(false, true, false);
        SagaState saga = orchestrator.executePaymentSaga(order, steps);

        log.info("DEMO5 | Saga status:   {}", saga.getStatus());
        log.info("DEMO5 | Failed at:     {}", saga.getFailedAtStep());
        log.info("DEMO5 | Failure reason: {}", saga.getFailureReason());

        log.info("DEMO5 | Completed steps BEFORE failure:");
        saga.getCompletedSteps().forEach(s -> log.info("DEMO5 |   {}", s));

        log.info("DEMO5 | Compensated steps (executed in REVERSE order):");
        saga.getCompensatedSteps().forEach(s -> log.info("DEMO5 |   {}", s));

        log.info("""
            DEMO5 | KEY INSIGHT:
                    Without compensation: credit reserved, fraud approved,
                    but no card auth -> phantom reservation locked for 7 days.
                    With saga compensation: ALL completed steps UNDONE in reverse.
                    System returns to clean baseline. Zero phantom authorizations.
            """);
    }

    // =========================================================================
    // DEMO 6 — Orchestration vs Choreography: conceptual contrast
    // =========================================================================
    static void demo6_SagaChoreographyContrast() {
        log.info("""

            +----------------------------------------------------------+
            |  DEMO 6 - Orchestration vs Choreography contrast         |
            +----------------------------------------------------------+
            """);

        log.info("DEMO6 | ORCHESTRATION pattern:");
        log.info("DEMO6 |   One central orchestrator calls each service in sequence.");
        log.info("DEMO6 |   Steps: 1->2->3->4->5 (synchronous chain).");
        log.info("DEMO6 |   PRO: single audit trail, easy to debug, explicit compensations.");
        log.info("DEMO6 |   CON: orchestrator is a coupling point.");
        log.info("DEMO6 |   Tools: Axon Framework, AWS Step Functions, Camunda BPM.");
        log.info("");
        log.info("DEMO6 | CHOREOGRAPHY pattern:");
        log.info("DEMO6 |   No central coordinator. Services communicate via events.");
        log.info("DEMO6 |   PAYMENT_INITIATED -> FRAUD_CLEARED -> CARD_AUTHORIZED -> ...");
        log.info("DEMO6 |   PRO: decentralized, services scale independently.");
        log.info("DEMO6 |   CON: harder to trace — saga state scattered across services.");
        log.info("DEMO6 |   Tools: Apache Kafka, AWS EventBridge, Spring Cloud Stream.");
        log.info("");
        log.info("DEMO6 | WHEN TO USE WHICH:");
        log.info("DEMO6 |   Linear flow + audit required + small team -> ORCHESTRATION");
        log.info("DEMO6 |   Complex branching + 50+ services + autonomy -> CHOREOGRAPHY");
        log.info("DEMO6 |   Bank payment auth (5 ordered steps) -> ORCHESTRATION wins.");
        log.info("DEMO6 |   Bank event streaming (100+ consumers) -> CHOREOGRAPHY wins.");
    }

    // =========================================================================
    // DEMO 7 — Full resilience stack under 20 concurrent payments
    // =========================================================================
    static void demo7_FullResilienceUnderLoad() throws InterruptedException {
        log.info("""

            +----------------------------------------------------------+
            |  DEMO 7 - Full Stack: 20 concurrent payments under load  |
            |  Fraud service 60% fail rate. Circuit breaker active.    |
            +----------------------------------------------------------+
            """);

        var svc = new PaymentAuthorizationService();
        svc.setFraudFailRate(0.6);   // 60% of fraud calls will timeout

        int            paymentCount = 20;
        AtomicInteger  authorized   = new AtomicInteger(0);
        AtomicInteger  declined     = new AtomicInteger(0);
        AtomicInteger  failed       = new AtomicInteger(0);

        ExecutorService pool  = Executors.newFixedThreadPool(10);
        CountDownLatch  latch = new CountDownLatch(paymentCount);

        for (int i = 0; i < paymentCount; i++) {
            pool.submit(() -> {
                try {
                    AuthResult result = svc.authorize(buildOrder(AMOUNT_COFFEE));
                    if (result.isAuthorized()) {
                        authorized.incrementAndGet();
                    } else if (result.isDeclined()) {
                        declined.incrementAndGet();
                    } else {
                        failed.incrementAndGet();
                    }
                } finally {
                    latch.countDown();
                }
            });
            Thread.sleep(40);
        }

        latch.await(30, TimeUnit.SECONDS);
        pool.shutdown();

        log.info("DEMO7 | Results for {} payments:", paymentCount);
        log.info("DEMO7 |   Authorized : {}", authorized.get());
        log.info("DEMO7 |   Declined   : {}", declined.get());
        log.info("DEMO7 |   Failed/CB  : {}", failed.get());
        log.info("DEMO7 |   Fraud CB   : {}", svc.getFraudCircuitBreaker().getSnapshot());
        log.info("DEMO7 | Note: 'Failed/CB' = fast-failed by circuit breaker, not real timeouts.");
        log.info("DEMO7 |       System degraded gracefully — never fully stalled.");
    }

    // ── Shared helpers ────────────────────────────────────────────────────────

    static PaymentOrder buildOrder(String amount) {
        return PaymentOrder.of(
            VIKRAM_ID, VIKRAM_NAME,
            AMAZON_ID, "Amazon India",
            new BigDecimal(amount),
            "7291", "VISA",
            PaymentOrder.PaymentChannel.ONLINE
        );
    }

    static void logAuthResult(String prefix, AuthResult result) {
        if (result.isAuthorized()) {
            log.info("{} | AUTHORIZED: orderId={} authCode={}", prefix,
                result.getOrderId(), result.getAuthCode());
        } else if (result.isDeclined()) {
            log.warn("{} | DECLINED: orderId={} reason={}", prefix,
                result.getOrderId(), result.getDeclineReason());
        } else {
            log.error("{} | FAILED: orderId={} type={} retryable={}", prefix,
                result.getOrderId(), result.getFailureType(), result.isRetryable());
        }
    }

    static void divider() throws InterruptedException {
        Thread.sleep(500);
        log.info("\n{}", "=".repeat(60));
    }

    static void printBanner() {
        log.info("""

            +==========================================================+
            |                                                          |
            |   PAYMENT AUTHORIZATION FAILURE HANDLING                 |
            |   Resilience Patterns - Retail Banking (Java 17)         |
            |   "Vikram's Friday Night Laptop Purchase"                |
            |                                                          |
            |   Demo 1: Happy path - all services healthy              |
            |   Demo 2: Circuit Breaker - fraud service overloaded     |
            |   Demo 3: Bulkhead - notif flood isolated from payments  |
            |   Demo 4: Saga Orchestrator - 5-step authorization       |
            |   Demo 5: Compensating Transactions - card network fail  |
            |   Demo 6: Orchestration vs Choreography contrast         |
            |   Demo 7: Full stack under 20 concurrent payments        |
            |                                                          |
            +==========================================================+
            """);
    }
}
