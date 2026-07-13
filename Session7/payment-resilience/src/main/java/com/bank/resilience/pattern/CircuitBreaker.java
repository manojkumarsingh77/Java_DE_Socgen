package com.bank.resilience.pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * CircuitBreaker — Protect Fraud Service from Cascade Failure
 *
 * THREE STATES:
 *   CLOSED    → Normal. All calls pass through. Failure counter runs.
 *   OPEN      → Tripped. ALL calls fast-fail immediately. Timer runs.
 *   HALF_OPEN → Probe mode. ONE test call allowed through.
 *
 * THE STORY:
 * Friday 9PM flash sale. Fraud ML engine saturated. Response time: 30s.
 * WITHOUT circuit breaker: 200 threads × 30s wait = total payment outage.
 * WITH circuit breaker: after 5 failures → OPEN → fast-fail in <1ms.
 * Fraud service gets silence to recover. 60s later: probe → CLOSED.
 */
public class CircuitBreaker {

    private static final Logger log = LoggerFactory.getLogger(CircuitBreaker.class);

    public enum State {
        CLOSED    { @Override public boolean allowsCall()  { return true;  }
                    @Override public String  description() { return "CLOSED — all requests pass through"; } },
        OPEN      { @Override public boolean allowsCall()  { return false; }
                    @Override public String  description() { return "OPEN — requests fast-fail, service isolated"; } },
        HALF_OPEN { @Override public boolean allowsCall()  { return true;  }
                    @Override public String  description() { return "HALF_OPEN — probe request sent"; } };

        public abstract boolean allowsCall();
        public abstract String  description();
    }

    /** Plain wrapper returned from execute() — no sealed needed */
    public static class CircuitResult<T> {
        public enum Kind { SUCCESS, CIRCUIT_OPEN, SERVICE_ERROR }

        private final Kind      kind;
        private final T         value;
        private final String    reason;
        private final Exception error;

        private CircuitResult(Kind kind, T value, String reason, Exception error) {
            this.kind = kind; this.value = value; this.reason = reason; this.error = error;
        }

        public static <T> CircuitResult<T> success(T value)       { return new CircuitResult<>(Kind.SUCCESS,       value, null,   null);  }
        public static <T> CircuitResult<T> open(String reason)     { return new CircuitResult<>(Kind.CIRCUIT_OPEN,  null,  reason, null);  }
        public static <T> CircuitResult<T> error(Exception e)      { return new CircuitResult<>(Kind.SERVICE_ERROR, null,  null,   e);     }

        public boolean isSuccess()     { return kind == Kind.SUCCESS;       }
        public boolean isCircuitOpen() { return kind == Kind.CIRCUIT_OPEN;  }
        public boolean isError()       { return kind == Kind.SERVICE_ERROR; }

        public T         getValue()  { return value;  }
        public String    getReason() { return reason; }
        public Exception getError()  { return error;  }
        public Kind      getKind()   { return kind;   }

        @Override public String toString() {
            return switch (kind) {
                case SUCCESS      -> "SUCCESS(" + value + ")";
                case CIRCUIT_OPEN -> "CIRCUIT_OPEN(" + reason + ")";
                case SERVICE_ERROR -> "SERVICE_ERROR(" + error.getMessage() + ")";
            };
        }
    }

    public record CircuitSnapshot(String serviceName, State state,
                                   int consecutiveFailures, int totalCalls,
                                   int rejectedCalls, String description) {}

    // ── Configuration ─────────────────────────────────────────────────────────
    private final String serviceName;
    private final int    failureThreshold;
    private final int    successThresholdInHalfOpen;
    private final long   resetTimeoutMs;

    // ── Thread-safe state ─────────────────────────────────────────────────────
    private final AtomicReference<State> state            = new AtomicReference<>(State.CLOSED);
    private final AtomicInteger          consecutiveFails = new AtomicInteger(0);
    private final AtomicInteger          probeSuccesses   = new AtomicInteger(0);
    private final AtomicLong             openedAtMs       = new AtomicLong(0);
    private final AtomicInteger          totalCalls       = new AtomicInteger(0);
    private final AtomicInteger          rejectedCalls    = new AtomicInteger(0);

    public CircuitBreaker(String serviceName, int failureThreshold,
                           int successThresholdInHalfOpen, long resetTimeoutMs) {
        this.serviceName               = serviceName;
        this.failureThreshold          = failureThreshold;
        this.successThresholdInHalfOpen = successThresholdInHalfOpen;
        this.resetTimeoutMs            = resetTimeoutMs;
        log.info("CB [{}] Init: threshold={} failures, resetTimeout={}ms",
            serviceName, failureThreshold, resetTimeoutMs);
    }

    // ── Factory methods ───────────────────────────────────────────────────────
    public static CircuitBreaker forFraudService()       { return new CircuitBreaker("FRAUD_ML_ENGINE",     5, 2, 60_000); }
    public static CircuitBreaker forCardNetwork()        { return new CircuitBreaker("VISA_CARD_NETWORK",   3, 1, 30_000); }
    public static CircuitBreaker forNotificationService(){ return new CircuitBreaker("NOTIFICATION_GW",    10, 3, 15_000); }

    // ── Core execute ──────────────────────────────────────────────────────────
    /**
     * Execute an operation through the circuit breaker.
     * @param operation  the risky downstream call
     * @param fallback   what to return when circuit is OPEN
     */
    public <T> CircuitResult<T> execute(Supplier<T> operation, Supplier<T> fallback) {
        totalCalls.incrementAndGet();
        checkAndMaybeTransitionFromOpen();

        if (!state.get().allowsCall()) {
            rejectedCalls.incrementAndGet();
            log.warn("CB [{}] OPEN — fast-failing call #{}, returning fallback.", serviceName, totalCalls.get());
            return CircuitResult.open("Circuit OPEN for " + serviceName + ". Fallback applied.");
        }

        if (state.get() == State.HALF_OPEN) {
            log.info("CB [{}] HALF_OPEN — sending probe request.", serviceName);
        }

        try {
            T result = operation.get();
            recordSuccess();
            return CircuitResult.success(result);
        } catch (Exception e) {
            recordFailure(e);
            return CircuitResult.error(e);
        }
    }

    // ── State transitions ─────────────────────────────────────────────────────
    private void checkAndMaybeTransitionFromOpen() {
        if (state.get() == State.OPEN) {
            long elapsed = System.currentTimeMillis() - openedAtMs.get();
            if (elapsed >= resetTimeoutMs) {
                if (state.compareAndSet(State.OPEN, State.HALF_OPEN)) {
                    probeSuccesses.set(0);
                    log.info("CB [{}] OPEN → HALF_OPEN ({}ms elapsed). Sending probe.", serviceName, elapsed);
                }
            }
        }
    }

    private void recordSuccess() {
        State current = state.get();
        if (current == State.HALF_OPEN) {
            if (probeSuccesses.incrementAndGet() >= successThresholdInHalfOpen) {
                if (state.compareAndSet(State.HALF_OPEN, State.CLOSED)) {
                    consecutiveFails.set(0);
                    log.info("CB [{}] HALF_OPEN → CLOSED. Service recovered!", serviceName);
                }
            }
        } else {
            consecutiveFails.set(0);
        }
    }

    private void recordFailure(Exception e) {
        State current = state.get();
        if (current == State.HALF_OPEN) {
            if (state.compareAndSet(State.HALF_OPEN, State.OPEN)) {
                openedAtMs.set(System.currentTimeMillis());
                log.warn("CB [{}] HALF_OPEN → OPEN (probe failed: {}). Waiting {}ms.",
                    serviceName, e.getMessage(), resetTimeoutMs);
            }
            return;
        }
        int failures = consecutiveFails.incrementAndGet();
        log.warn("CB [{}] Failure #{}/{}: {}", serviceName, failures, failureThreshold, e.getMessage());
        if (failures >= failureThreshold && current == State.CLOSED) {
            if (state.compareAndSet(State.CLOSED, State.OPEN)) {
                openedAtMs.set(System.currentTimeMillis());
                log.error("CB [{}] CLOSED → OPEN! {} consecutive failures. Isolated for {}ms.",
                    serviceName, failures, resetTimeoutMs);
            }
        }
    }

    // ── Observability ─────────────────────────────────────────────────────────
    public void            forceOpen()   { state.set(State.OPEN);   openedAtMs.set(System.currentTimeMillis()); }
    public void            forceClose()  { state.set(State.CLOSED); consecutiveFails.set(0); }
    public State           getState()    { return state.get(); }
    public String          getServiceName() { return serviceName; }
    public CircuitSnapshot getSnapshot() {
        return new CircuitSnapshot(serviceName, state.get(), consecutiveFails.get(),
            totalCalls.get(), rejectedCalls.get(), state.get().description());
    }

    @Override public String toString() {
        return String.format("CB[%s, %s, fails=%d/%d, total=%d, rejected=%d]",
            serviceName, state.get(), consecutiveFails.get(), failureThreshold,
            totalCalls.get(), rejectedCalls.get());
    }
}
