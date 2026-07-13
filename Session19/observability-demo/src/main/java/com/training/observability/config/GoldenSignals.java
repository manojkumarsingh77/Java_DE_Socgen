package com.training.observability.config;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.time.Duration;

/**
 * Deliberate Prometheus metrics modeling for the four golden signals
 * (Google SRE: Latency, Traffic, Errors, Saturation).
 *
 * Modeling decisions worth highlighting in training:
 *  - Counters always end up exposed with a "_total" suffix in Prometheus - we never
 *    put that suffix in the Micrometer name ourselves.
 *  - The processing-duration Timer publishes a percentile HISTOGRAM (not client-side
 *    quantiles/Summary) specifically so PromQL's histogram_quantile() can be used and
 *    so percentiles can be correctly AGGREGATED across multiple instances - client-side
 *    quantiles (Summary) cannot be averaged/aggregated across instances and are a classic
 *    Prometheus modeling mistake.
 *  - High-cardinality identifiers (orderId, correlationId, customerId) are NEVER used as
 *    metric tags/labels - only bounded-cardinality dimensions (service, status, error_type)
 *    are. Putting an unbounded ID in a label is the #1 cause of Prometheus cardinality
 *    blow-ups in real production systems.
 */
public class GoldenSignals {

    public final Counter producedCounter;     // TRAFFIC (producer side), null if forConsumer()
    public final Counter consumedCounter;     // TRAFFIC (consumer side), null if forProducer()
    public final Timer processingDuration;    // LATENCY
    public final MeterRegistry registry;
    private final String errorMetricName;     // ERRORS metric name used by recordError()

    private GoldenSignals(MeterRegistry registry, Counter produced, Counter consumed,
                           Timer processingDuration, String errorMetricName) {
        this.registry = registry;
        this.producedCounter = produced;
        this.consumedCounter = consumed;
        this.processingDuration = processingDuration;
        this.errorMetricName = errorMetricName;
    }

    public static GoldenSignals forProducer(MeterRegistry registry) {
        Counter produced = Counter.builder("orders.produced")
                .description("Total number of order events successfully produced to Kafka")
                .register(registry);

        Timer produceLatency = Timer.builder("orders.produce.duration")
                .description("Time to produce a single order event to Kafka (send-to-ack)")
                .publishPercentileHistogram(true)
                .minimumExpectedValue(Duration.ofMillis(1))
                .maximumExpectedValue(Duration.ofSeconds(5))
                .register(registry);

        return new GoldenSignals(registry, produced, null, produceLatency, "orders.production.errors");
    }

    public static GoldenSignals forConsumer(MeterRegistry registry) {
        Counter consumed = Counter.builder("orders.consumed")
                .description("Total number of order events successfully processed")
                .register(registry);

        Timer processingDuration = Timer.builder("orders.processing.duration")
                .description("End-to-end time to process a single order event in the Spark pipeline")
                .publishPercentileHistogram(true)
                .minimumExpectedValue(Duration.ofMillis(1))
                .maximumExpectedValue(Duration.ofSeconds(10))
                .register(registry);

        return new GoldenSignals(registry, null, consumed, processingDuration, "orders.processing.errors");
    }

    /**
     * Increments the errors counter for this service with a bounded-cardinality
     * error_type tag (e.g. "IllegalArgumentException", "DownstreamTimeout").
     * Counters are looked up/created lazily via registry.counter(...) because the
     * set of error types is small and known ahead of time in practice, even though
     * we don't pre-register every combination.
     */
    public void recordError(String errorType) {
        registry.counter(errorMetricName, "error_type", errorType).increment();
    }
}
