package com.training.observability.config;

import brave.Tracing;
import brave.context.slf4j.MDCScopeDecorator;
import brave.kafka.clients.KafkaTracing;
import brave.propagation.ThreadLocalCurrentTraceContext;
import brave.sampler.Sampler;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.binder.system.UptimeMetrics;
// FIXED: Changed from io.micrometer.prometheusmetrics.* to legacy 1.x package paths
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zipkin2.reporter.brave.AsyncZipkinSpanHandler;
import zipkin2.reporter.urlconnection.URLConnectionSender;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

/**
 * Bootstraps the three pillars of observability for a single JVM service:
 *
 * 1. METRICS  - a {@link PrometheusMeterRegistry} exposed on /metrics for Prometheus to scrape
 * (golden signals + JVM saturation indicators).
 * 2. TRACING  - Brave {@link Tracing}, wired to ship spans to Zipkin, with the
 * {@link MDCScopeDecorator} so every log line written *inside* an open span
 * automatically carries traceId/spanId in its structured JSON output.
 * 3. LOGGING  - handled declaratively by logback.xml (JSON encoder + MDC), nothing to wire here.
 *
 * Each microservice process (producer, consumer) calls this once at startup.
 */
public final class ObservabilityConfig {

    private static final Logger log = LoggerFactory.getLogger(ObservabilityConfig.class);

    private ObservabilityConfig() {
    }

    // ---------------------------------------------------------------------
    // METRICS
    // ---------------------------------------------------------------------

    /**
     * Creates a Prometheus-backed Micrometer registry and binds standard JVM/process
     * metrics used as our "saturation" golden signal (heap usage, GC pause time, thread
     * count, CPU load).
     */
    public static PrometheusMeterRegistry createMeterRegistry(String serviceName) {
        PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

        registry.config().commonTags("service", serviceName);

        new ClassLoaderMetrics().bindTo(registry);
        new JvmMemoryMetrics().bindTo(registry);
        new JvmGcMetrics().bindTo(registry);
        new JvmThreadMetrics().bindTo(registry);
        new ProcessorMetrics().bindTo(registry);
        new UptimeMetrics().bindTo(registry);

        log.info("Prometheus meter registry initialized for service={}", serviceName);
        return registry;
    }

    /**
     * Starts a minimal HTTP server exposing the Prometheus text-format scrape endpoint at
     * GET /metrics. Uses the JDK's built-in com.sun.net.httpserver so the demo has zero
     * extra web-framework dependencies.
     */
    public static HttpServer startMetricsServer(PrometheusMeterRegistry registry, int port) {
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
            server.createContext("/metrics", exchange -> {
                // In Micrometer 1.x, the method is scrape() just like below
                byte[] body = registry.scrape().getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
                exchange.sendResponseHeaders(200, body.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(body);
                }
            });
            server.setExecutor(null);
            server.start();
            log.info("Prometheus /metrics endpoint listening on http://0.0.0.0:{}/metrics", port);
            return server;
        } catch (IOException e) {
            throw new IllegalStateException("Unable to start metrics HTTP server on port " + port, e);
        }
    }

    // ---------------------------------------------------------------------
    // TRACING
    // ---------------------------------------------------------------------

    /**
     * Builds a Brave {@link Tracing} component that:
     * - names this JVM's spans with {@code serviceName} (shows up as the Zipkin "service")
     * - always samples (fine for a demo; in production use a rate-based sampler)
     * - decorates the current trace context onto SLF4J's MDC so traceId/spanId appear
     * automatically in every JSON log line emitted while a span is open
     * - asynchronously reports finished spans to Zipkin over HTTP
     */
    public static Tracing buildTracing(String serviceName, String zipkinEndpoint) {
        URLConnectionSender sender = URLConnectionSender.create(zipkinEndpoint);

        AsyncZipkinSpanHandler spanHandler = AsyncZipkinSpanHandler.create(sender);

        Tracing tracing = Tracing.newBuilder()
                .localServiceName(serviceName)
                .currentTraceContext(
                        ThreadLocalCurrentTraceContext.newBuilder()
                                .addScopeDecorator(MDCScopeDecorator.get())
                                .build())
                .sampler(Sampler.ALWAYS_SAMPLE)
                .addSpanHandler(spanHandler)
                .build();

        log.info("Brave tracing initialized for service={}, shipping spans to {}", serviceName, zipkinEndpoint);
        return tracing;
    }

    public static KafkaTracing buildKafkaTracing(Tracing tracing) {
        return KafkaTracing.newBuilder(tracing)
                .remoteServiceName("kafka")
                .build();
    }
}