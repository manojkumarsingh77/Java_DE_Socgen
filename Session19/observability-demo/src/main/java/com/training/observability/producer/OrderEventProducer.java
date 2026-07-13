package com.training.observability.producer;

import brave.Span;
import brave.Tracer;
import brave.Tracing;
import brave.kafka.clients.KafkaTracing;
import com.sun.net.httpserver.HttpServer;
import com.training.observability.config.GoldenSignals;
import com.training.observability.config.ObservabilityConfig;
import com.training.observability.model.OrderEvent;
import com.training.observability.util.CorrelationIdSupport;
import io.micrometer.core.instrument.Timer;
//import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Synthetic order-event traffic generator.
 *
 * Demonstrates:
 *  - Structured (JSON) logging with a correlationId in MDC on every log line
 *  - A Zipkin root span per produced message ("produce-order-event"), automatically
 *    propagated into the Kafka record's headers by brave-instrumentation-kafka-clients
 *  - Prometheus golden-signal metrics: traffic (orders.produced), latency
 *    (orders.produce.duration), errors (orders.production.errors)
 *  - Configurable failure-injection and traffic-burst modes so dashboards/alerts have
 *    something interesting to react to during the live demo.
 *
 * All configuration is via environment variables / -D system properties (with sane
 * defaults matching the docker-compose stack in this project), so this class needs zero
 * code changes to run from IntelliJ.
 */
public class OrderEventProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventProducer.class);

    private static final String SERVICE_NAME = "order-producer";
    private static final String[] PRODUCT_CATALOG = {"SKU-LAPTOP", "SKU-PHONE", "SKU-HEADSET", "SKU-MONITOR", "SKU-KEYBOARD"};
    private static final String[] CUSTOMERS = {"CUST-101", "CUST-102", "CUST-103", "CUST-104", "CUST-105"};

    private static final Random RANDOM = new Random();
    private static final AtomicBoolean RUNNING = new AtomicBoolean(true);

    public static void main(String[] args) throws InterruptedException {
        String bootstrapServers = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        String topic = env("ORDERS_TOPIC", "orders-topic");
        String zipkinEndpoint = env("ZIPKIN_ENDPOINT", "http://localhost:9411/api/v2/spans");
        int metricsPort = Integer.parseInt(env("METRICS_PORT", "8081"));
        long produceIntervalMs = Long.parseLong(env("PRODUCE_INTERVAL_MS", "400"));
        double failureInjectionRate = Double.parseDouble(env("FAILURE_INJECTION_RATE", "0.0"));

        log.info("Starting {} | bootstrapServers={} topic={} zipkin={} metricsPort={}",
                SERVICE_NAME, bootstrapServers, topic, zipkinEndpoint, metricsPort);

        // ---- Observability bootstrap ----
        PrometheusMeterRegistry registry = ObservabilityConfig.createMeterRegistry(SERVICE_NAME);
        HttpServer metricsServer = ObservabilityConfig.startMetricsServer(registry, metricsPort);
        Tracing tracing = ObservabilityConfig.buildTracing(SERVICE_NAME, zipkinEndpoint);
        KafkaTracing kafkaTracing = ObservabilityConfig.buildKafkaTracing(tracing);
        Tracer tracer = tracing.tracer();
        GoldenSignals metrics = GoldenSignals.forProducer(registry);

        // ---- Kafka producer ----
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 20);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, SERVICE_NAME);

        Producer<String, String> rawProducer = new KafkaProducer<>(props);
        // Wrapping with KafkaTracing automatically injects B3 trace headers into every
        // record sent through this producer, and starts a "send" span per record.
        Producer<String, String> producer = kafkaTracing.producer(rawProducer);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown signal received, closing {} cleanly", SERVICE_NAME);
            RUNNING.set(false);
            try {
                producer.close(Duration.ofSeconds(5));
            } catch (Exception e) {
                log.warn("Error closing Kafka producer", e);
            }
            tracing.close();
            metricsServer.stop(1);
        }, "shutdown-hook"));

        log.info("{} entering main produce loop. Press Ctrl+C to stop.", SERVICE_NAME);

        int tick = 0;
        while (RUNNING.get()) {
            tick++;
            // Every ~20 ticks, simulate a short traffic burst to give dashboards/alerts
            // something visually interesting to react to.
            boolean burst = (tick / 20) % 5 == 4;
            int messagesThisTick = burst ? 8 : 1;

            for (int i = 0; i < messagesThisTick; i++) {
                produceOneEvent(producer, topic, tracer, metrics, failureInjectionRate);
            }
            Thread.sleep(burst ? Math.max(50, produceIntervalMs / 4) : produceIntervalMs);
        }
    }

    private static void produceOneEvent(Producer<String, String> producer, String topic, Tracer tracer,
                                          GoldenSignals metrics, double failureInjectionRate) {
        String orderId = "ORD-" + RANDOM.nextInt(1_000_000);
        String correlationId = CorrelationIdSupport.newCorrelationId();

        MDC.put(CorrelationIdSupport.MDC_CORRELATION_ID, correlationId);
        MDC.put(CorrelationIdSupport.MDC_ORDER_ID, orderId);

        Span span = tracer.nextSpan().name("produce-order-event").kind(Span.Kind.PRODUCER)
                .tag("order.id", orderId)
                .tag("messaging.system", "kafka")
                .tag("messaging.destination", topic)
                .start();

        try (Tracer.SpanInScope ignored = tracer.withSpanInScope(span)) {
            OrderEvent event = randomOrderEvent(orderId, correlationId);

            // Simulate an upstream/business failure BEFORE we touch Kafka at all - this is
            // what generates the "errors" golden signal and feeds the alerting demo.
            if (RANDOM.nextDouble() < failureInjectionRate) {
                throw new IllegalStateException("Simulated upstream validation failure for " + orderId);
            }

            String payload = event.toJson();
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, orderId, payload);
            record.headers().add(CorrelationIdSupport.KAFKA_HEADER_CORRELATION_ID,
                    correlationId.getBytes(StandardCharsets.UTF_8));

            log.info("Producing order event amount={} quantity={} productId={}",
                    event.getAmount(), event.getQuantity(), event.getProductId());

            Timer.Sample sample = Timer.start(metrics.registry);
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    log.error("Kafka send failed for orderId={}", orderId, exception);
                    metrics.recordError(exception.getClass().getSimpleName());
                } else {
                    sample.stop(metrics.processingDuration);
                    metrics.producedCounter.increment();
                }
            });
        } catch (Exception ex) {
            span.error(ex);
            metrics.recordError(ex.getClass().getSimpleName());
            log.error("Failed to produce order event orderId={}", orderId, ex);
        } finally {
            span.finish();
            MDC.remove(CorrelationIdSupport.MDC_CORRELATION_ID);
            MDC.remove(CorrelationIdSupport.MDC_ORDER_ID);
        }
    }

    private static OrderEvent randomOrderEvent(String orderId, String correlationId) {
        String productId = PRODUCT_CATALOG[RANDOM.nextInt(PRODUCT_CATALOG.length)];
        String customerId = CUSTOMERS[RANDOM.nextInt(CUSTOMERS.length)];
        int quantity = 1 + RANDOM.nextInt(5);
        double amount = Math.round((10 + RANDOM.nextDouble() * 490) * 100.0) / 100.0;
        return new OrderEvent(orderId, correlationId, customerId, productId, quantity, amount,
                "ORDER_CREATED", System.currentTimeMillis());
    }

    private static String env(String key, String defaultValue) {
        String fromEnv = System.getenv(key);
        if (fromEnv != null && !fromEnv.isBlank()) {
            return fromEnv;
        }
        return System.getProperty(key, defaultValue);
    }
}
