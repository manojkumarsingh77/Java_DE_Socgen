package com.bank.resilience.saga;

import com.bank.resilience.model.PaymentOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * SagaChoreographer — Event-driven saga with NO central coordinator.
 *
 * ── CHOREOGRAPHY APPROACH ────────────────────────────────────────────────────
 * There is no orchestrator. Each service:
 *   1. Listens for specific events on a message bus (Kafka topic).
 *   2. Reacts by doing its own local work.
 *   3. Publishes a new event signalling the result.
 *
 * The saga "emerges" from the chain of event reactions — no service
 * directly calls any other service.
 *
 * Event chain for a successful payment:
 *   PAYMENT_INITIATED
 *       → [FraudService reacts]   → FRAUD_CLEARED
 *           → [CardService reacts] → CARD_AUTHORIZED
 *               → [CBS reacts]     → ACCOUNT_DEBITED
 *                   → [Notif reacts] → CUSTOMER_NOTIFIED
 *
 * ── CONTRAST WITH ORCHESTRATION ──────────────────────────────────────────────
 * Orchestration: OrchestratorService directly calls FraudService,
 *                then CardService, then CBS — tightly coupled chain.
 *
 * Choreography:  FraudService has NO knowledge of CardService.
 *                CardService has NO knowledge of CBS.
 *                They only know which events to consume and which to emit.
 *                Completely decoupled.
 *
 * ── WHEN EACH WINS ───────────────────────────────────────────────────────────
 * Use ORCHESTRATION when:
 *   - Flow is linear and rarely changes
 *   - You need a clear audit trail in one place
 *   - Team size < 10 and debugging speed matters
 *   - Example: Bank Aadhara's 5-step payment auth (this project's main demo)
 *
 * Use CHOREOGRAPHY when:
 *   - Many independent services must react to the same events
 *   - Services need to scale and deploy independently
 *   - You have 50+ microservices and centralization creates a bottleneck
 *   - Example: Bank Aadhara's fraud alert fan-out to 12 downstream systems
 *
 * ── PRODUCTION TOOLS ─────────────────────────────────────────────────────────
 *   Apache Kafka    — durable, ordered event log; guaranteed delivery
 *   AWS EventBridge — serverless event bus; pay-per-event
 *   Spring Cloud Stream — abstraction over Kafka/RabbitMQ
 *   Axon Framework  — supports BOTH orchestration AND choreography sagas
 *
 * NOTE: This is a package-private demonstration class — it is not used by
 * PaymentAuthorizationService directly. Demo 6 in the simulation discusses
 * the conceptual contrast; this class provides the runnable event-chain proof.
 */
public class SagaChoreographer {

    private static final Logger log = LoggerFactory.getLogger(SagaChoreographer.class);

    // ── Event definitions (would be Kafka topic messages in production) ───────

    /**
     * All payment events in the choreography saga.
     * In production each of these maps to a dedicated Kafka topic.
     */
    public enum PaymentEvent {
        PAYMENT_INITIATED,
        FRAUD_CLEARED,   FRAUD_FLAGGED,
        CARD_AUTHORIZED, CARD_DECLINED,  CARD_VOIDED,
        ACCOUNT_DEBITED, DEBIT_FAILED,   DEBIT_REVERSED,
        CUSTOMER_NOTIFIED
    }

    /**
     * An event on the bus — immutable record.
     * In production: serialized to JSON/Avro, published to Kafka with orderId as key.
     */
    public record Event(
        PaymentEvent type,
        String       orderId,
        String       payload,
        Instant      occurredAt
    ) {
        static Event of(PaymentEvent type, String orderId, String payload) {
            return new Event(type, orderId, payload, Instant.now());
        }

        @Override public String toString() {
            return String.format("Event[%s | order=%s | %s]", type, orderId, payload);
        }
    }

    // ── Simulated event bus (in-memory LinkedBlockingQueue) ───────────────────
    // In production: KafkaTemplate.send() / @KafkaListener

    private final Queue<Event> eventBus    = new LinkedBlockingQueue<>();
    private final List<Event>  eventHistory = new ArrayList<>();

    /**
     * Publish an event to the bus.
     * Production equivalent: kafkaTemplate.send(topicName, orderId, eventPayload)
     */
    private void publish(Event event) {
        eventBus.offer(event);
        eventHistory.add(event);
        log.info("CHOREOGRAPHY | [BUS] Published: {}", event);
    }

    // ── Choreography runner ───────────────────────────────────────────────────

    /**
     * Run a complete payment authorization saga via event choreography.
     *
     * Each service is simulated inline here for clarity. In a real system
     * these would be separate Spring Boot applications each running their
     * own @KafkaListener. The saga is the SAME — only the deployment model differs.
     *
     * @param order     the payment order to authorize
     * @param fraudFail if true, fraud service emits FRAUD_FLAGGED instead of FRAUD_CLEARED
     * @return ordered list of all events emitted during the saga
     */
    public List<Event> runChoreography(PaymentOrder order, boolean fraudFail) {
        log.info("CHOREOGRAPHY | Starting event-driven saga for order: {}", order.orderId());
        log.info("CHOREOGRAPHY | No central orchestrator — services react to events.");

        // ── Trigger: payment initiated ────────────────────────────────────────
        publish(Event.of(PaymentEvent.PAYMENT_INITIATED, order.orderId(),
            "amount=" + order.amountFormatted() + " merchant=" + order.merchantName()));

        // ── FraudService REACTS to PAYMENT_INITIATED ──────────────────────────
        Event initiated = eventBus.poll();
        if (initiated != null && initiated.type() == PaymentEvent.PAYMENT_INITIATED) {
            log.info("CHOREOGRAPHY | [FraudService] consumed PAYMENT_INITIATED — running ML check...");
            sleep(80);
            if (fraudFail) {
                publish(Event.of(PaymentEvent.FRAUD_FLAGGED, order.orderId(),
                    "reason=high-velocity-pattern score=92"));
                log.warn("CHOREOGRAPHY | [FraudService] emitted FRAUD_FLAGGED — " +
                    "CardService will NOT react; saga naturally halted.");
                return List.copyOf(eventHistory);
            }
            publish(Event.of(PaymentEvent.FRAUD_CLEARED, order.orderId(), "score=12"));
        }

        // ── CardNetworkService REACTS to FRAUD_CLEARED ────────────────────────
        Event fraudCleared = eventBus.poll();
        if (fraudCleared != null && fraudCleared.type() == PaymentEvent.FRAUD_CLEARED) {
            log.info("CHOREOGRAPHY | [CardNetworkService] consumed FRAUD_CLEARED — calling Visa...");
            sleep(120);
            publish(Event.of(PaymentEvent.CARD_AUTHORIZED, order.orderId(),
                "authCode=VIS-887654 network=VISA"));
        }

        // ── CoreBankingSystem REACTS to CARD_AUTHORIZED ───────────────────────
        Event cardAuth = eventBus.poll();
        if (cardAuth != null && cardAuth.type() == PaymentEvent.CARD_AUTHORIZED) {
            log.info("CHOREOGRAPHY | [CoreBankingSystem] consumed CARD_AUTHORIZED — debiting account...");
            sleep(60);
            publish(Event.of(PaymentEvent.ACCOUNT_DEBITED, order.orderId(),
                "amount=" + order.amountFormatted() + " status=SUCCESS"));
        }

        // ── NotificationService REACTS to ACCOUNT_DEBITED ────────────────────
        Event debited = eventBus.poll();
        if (debited != null && debited.type() == PaymentEvent.ACCOUNT_DEBITED) {
            log.info("CHOREOGRAPHY | [NotificationService] consumed ACCOUNT_DEBITED — sending SMS...");
            sleep(30);
            publish(Event.of(PaymentEvent.CUSTOMER_NOTIFIED, order.orderId(),
                "channel=SMS customer=" + order.customerName()));
        }

        log.info("CHOREOGRAPHY | Saga complete via event choreography.");
        log.info("CHOREOGRAPHY | {} events emitted. Full audit trail preserved.", eventHistory.size());
        eventHistory.forEach(e -> log.info("CHOREOGRAPHY |   {}", e));

        return List.copyOf(eventHistory);
    }

    // ── Utility ───────────────────────────────────────────────────────────────

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms / 10);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
