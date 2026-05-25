import java.time.Duration;
import java.time.Instant;
import java.util.Random;
import java.util.function.Supplier;

public class CircuitBreakerDemo {

    enum State {
        CLOSED,
        OPEN,
        HALF_OPEN
    }

    static class CircuitBreaker {
        private final int failureThreshold;
        private final Duration openDuration;

        private int failureCount = 0;
        private State state = State.CLOSED;
        private Instant lastFailureTime;

        public CircuitBreaker(int failureThreshold, Duration openDuration) {
            this.failureThreshold = failureThreshold;
            this.openDuration = openDuration;
        }

        public synchronized <T> T execute(Supplier<T> action) {
            if (state == State.OPEN) {
                if (Instant.now().isAfter(lastFailureTime.plus(openDuration))) {
                    state = State.HALF_OPEN;
                    System.out.println("Transition -> HALF_OPEN");
                } else {
                    throw new RuntimeException("Circuit OPEN. Fast fail.");
                }
            }

            try {
                T result = action.get();
                reset();
                return result;
            } catch (Exception e) {
                recordFailure();
                throw e;
            }
        }

        private void recordFailure() {
            failureCount++;
            lastFailureTime = Instant.now();

            if (failureCount >= failureThreshold) {
                state = State.OPEN;
                System.out.println("Transition -> OPEN");
            }
        }

        private void reset() {
            failureCount = 0;
            state = State.CLOSED;
            System.out.println("Transition -> CLOSED");
        }
    }

    static class PaymentGateway {
        private final Random random = new Random();

        public String processPayment() {
            if (random.nextInt(10) < 7) {
                throw new RuntimeException("Gateway timeout");
            }
            return "Payment Success";
        }
    }

    public static void main(String[] args) throws InterruptedException {
        CircuitBreaker breaker = new CircuitBreaker(3, Duration.ofSeconds(5));
        PaymentGateway gateway = new PaymentGateway();

        for (int i = 1; i <= 15; i++) {
            try {
                String result = breaker.execute(gateway::processPayment);
                System.out.println("Request " + i + ": " + result);
            } catch (Exception e) {
                System.out.println("Request " + i + ": " + e.getMessage());
            }

            Thread.sleep(1000);
        }
    }
}