package com.bank.retail.streaming.app3;

import com.bank.retail.streaming.model.PaymentOrderEvent;
import com.bank.retail.streaming.model.ProcessedPaymentEvent;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ThreadLocalRandom;

/**
 * ============================================================================
 *  PaymentGatewaySimulator - the heart of App 3's business logic
 * ============================================================================
 * BUSINESS PROBLEM THIS SOLVES:
 * Every incoming order must be (1) screened for fraud and (2) sent to a
 * payment gateway to actually move the money. Both steps are simulated here
 * with REALISTIC, RULE-BASED logic, including realistic FAILURE MODES
 * (occasional gateway errors, and - deliberately, for the demo - a slow
 * downstream dependency for ECOMMERCE transactions) so that App4/App5 have
 * a genuine incident to investigate, not a manufactured one.
 *
 * >>> THE METHOD THAT SOLVES THIS PROBLEM IS: processPayment() <<<
 * It is called once per incoming order, from inside the Spark map()
 * transformation in PaymentStreamProcessorApp.
 * ============================================================================
 */
public class PaymentGatewaySimulator {

    // static: a Log4j2 Logger is NOT part of any object's serialized state
    // (it lives on the class, not the instance), so it is completely safe
    // to use from inside a Spark lambda that Spark may serialize and ship
    // to another thread/executor.
    private static final Logger LOG = LogManager.getLogger(PaymentGatewaySimulator.class);

    // ---- Fraud rule thresholds (tunable "knobs" for the demo) ----
    private static final double FRAUD_AMOUNT_THRESHOLD = 75_000.0;

    // ---- SLA: any payment slower than this is a Service-Level breach ----
    public static final long SLA_THRESHOLD_MS = 1_000L;

    // ---- Random failure / slowness rates used to create a believable incident ----
    private static final double RANDOM_GATEWAY_FAILURE_RATE = 0.05; // 5% baseline gateway errors
    private static final double ECOMMERCE_SLOW_INCIDENT_RATE = 0.25; // 25% of ECOMMERCE calls hit the "incident"

    private static final DateTimeFormatter DATE_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneOffset.UTC);

    /**
     * >>> SOLUTION METHOD <<<
     * Runs fraud-check + simulated payment-gateway processing for one order
     * and returns the fully enriched record that gets written to Delta.
     */
    public ProcessedPaymentEvent processPayment(PaymentOrderEvent order) {

        // Push the correlationId into Log4j2's ThreadContext (the structured
        // logging "context map") BEFORE logging anything for this order.
        // Every log line emitted between put() and remove() will automatically
        // include correlationId=... per the pattern in log4j2.properties -
        // this is what lets an SRE later grep logs for ONE transaction.
        ThreadContext.put("correlationId", order.getCorrelationId());

        try {
            ProcessedPaymentEvent result = copyOrderFields(order);

            // ---- STEP 1: FRAUD CHECK ----
            FraudVerdict fraudVerdict = detectFraud(order);
            result.setFraudFlag(fraudVerdict.flagged);
            result.setFraudReason(fraudVerdict.reason);

            if (fraudVerdict.flagged) {
                result.setPaymentStatus("FRAUD_BLOCKED");
                result.setProcessingLatencyMs(0L); // blocked before we ever call the gateway
                LOG.warn("FRAUD_BLOCKED orderId={} customerId={} amount={} reason={}",
                        order.getOrderId(), order.getCustomerId(), order.getAmount(), fraudVerdict.reason);
            } else {
                // ---- STEP 2: PAYMENT GATEWAY CALL (simulated) ----
                long latencyMs = simulateGatewayLatency(order);
                boolean gatewayFailed = ThreadLocalRandom.current().nextDouble() < RANDOM_GATEWAY_FAILURE_RATE;

                // Actually "spend" the simulated latency. WHY a real sleep
                // and not just a fake number? Because we want Spark UI's
                // task timings and our own wall-clock logs to genuinely
                // reflect a slow call, exactly like a real slow downstream
                // dependency would look in production.
                sleepQuietly(latencyMs);

                result.setProcessingLatencyMs(latencyMs);

                if (gatewayFailed) {
                    result.setPaymentStatus("FAILED");
                    LOG.error("PAYMENT_FAILED orderId={} customerId={} channel={} latencyMs={} reason=gateway_error",
                            order.getOrderId(), order.getCustomerId(), order.getChannel(), latencyMs);
                } else {
                    result.setPaymentStatus("SUCCESS");
                    LOG.info("PAYMENT_SUCCESS orderId={} customerId={} channel={} latencyMs={}",
                            order.getOrderId(), order.getCustomerId(), order.getChannel(), latencyMs);
                }
            }

            boolean slaBreach = result.getProcessingLatencyMs() > SLA_THRESHOLD_MS;
            result.setSlaBreach(slaBreach);
            if (slaBreach) {
                LOG.warn("SLA_BREACH orderId={} latencyMs={} thresholdMs={} merchantCategory={}",
                        order.getOrderId(), result.getProcessingLatencyMs(), SLA_THRESHOLD_MS, order.getMerchantCategory());
            }

            result.setProcessedTimestamp(System.currentTimeMillis());
            result.setEventDate(DATE_FMT.format(Instant.ofEpochMilli(result.getProcessedTimestamp())));

            return result;
        } finally {
            // ALWAYS remove the context value once we're done with this
            // record - otherwise, on a reused thread (Spark reuses threads
            // across tasks), the NEXT order processed on this same thread
            // could accidentally inherit this order's correlationId in its
            // logs. This finally-block is the standard, correct way to use
            // a ThreadContext/MDC safely.
            ThreadContext.remove("correlationId");
        }
    }

    /**
     * Rule-based fraud check, modeling a simple real-world heuristic used by
     * retail banks: a large CARD payment from a device never seen before for
     * that customer is treated as suspicious and blocked for manual review.
     */
    private FraudVerdict detectFraud(PaymentOrderEvent order) {
        if ("CARD".equals(order.getChannel())
                && order.getAmount() > FRAUD_AMOUNT_THRESHOLD
                && order.isNewDevice()) {
            return new FraudVerdict(true,
                    "High-value CARD payment from a previously unseen device");
        }
        return new FraudVerdict(false, "");
    }

    /**
     * Produces the artificial-but-believable latency for this call. Most
     * calls are fast (normal production traffic). ECOMMERCE transactions
     * have an elevated chance of hitting a deliberately slow path - this is
     * OUR "injected incident" that App4 is designed to find and explain.
     */
    private long simulateGatewayLatency(PaymentOrderEvent order) {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        boolean isIncidentPath = "ECOMMERCE".equals(order.getMerchantCategory())
                && rnd.nextDouble() < ECOMMERCE_SLOW_INCIDENT_RATE;

        if (isIncidentPath) {
            // 2-5 seconds: clearly, dramatically over the 1 second SLA.
            return rnd.nextLong(2_000, 5_000);
        }
        // Normal traffic: 80ms-400ms, a realistic "fast" gateway round trip.
        return rnd.nextLong(80, 400);
    }

    private void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private ProcessedPaymentEvent copyOrderFields(PaymentOrderEvent order) {
        ProcessedPaymentEvent p = new ProcessedPaymentEvent();
        p.setCorrelationId(order.getCorrelationId());
        p.setOrderId(order.getOrderId());
        p.setCustomerId(order.getCustomerId());
        p.setCustomerName(order.getCustomerName());
        p.setAccountNumber(order.getAccountNumber());
        p.setIfscCode(order.getIfscCode());
        p.setBankName(order.getBankName());
        p.setChannel(order.getChannel());
        p.setMerchantCategory(order.getMerchantCategory());
        p.setMerchantName(order.getMerchantName());
        p.setAmount(order.getAmount());
        p.setCurrency(order.getCurrency());
        p.setDeviceId(order.getDeviceId());
        p.setNewDevice(order.isNewDevice());
        p.setCity(order.getCity());
        p.setOrderTimestamp(order.getOrderTimestamp());
        return p;
    }

    /** Tiny immutable value holder for a fraud decision + human-readable reason. */
    private static final class FraudVerdict {
        final boolean flagged;
        final String reason;
        FraudVerdict(boolean flagged, String reason) {
            this.flagged = flagged;
            this.reason = reason;
        }
    }
}
