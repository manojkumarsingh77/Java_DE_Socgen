package com.training.observability.consumer;

import brave.Span;
import brave.Tracer;
import brave.Tracing;
import brave.propagation.Propagation;
import brave.propagation.TraceContext;
import brave.propagation.TraceContextOrSamplingFlags;
import com.sun.net.httpserver.HttpServer;
import com.training.observability.config.GoldenSignals;
import com.training.observability.config.ObservabilityConfig;
import com.training.observability.model.OrderEvent;
import com.training.observability.util.CorrelationIdSupport;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Timer;
// FIXED: Explicitly use the 1.x Prometheus registry type to match your ObservabilityConfig signatures
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryListener;
import org.apache.spark.sql.streaming.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Spark Structured Streaming pipeline: Kafka("orders-topic") -> validate/enrich -> sinks.
 */
public class OrderStreamProcessor {

    private static final Logger log = LoggerFactory.getLogger(OrderStreamProcessor.class);
    private static final String SERVICE_NAME = "order-stream-processor";
    private static final Random RANDOM = new Random();

    public static void main(String[] args) throws Exception {
        String bootstrapServers = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        String topic = env("ORDERS_TOPIC", "orders-topic");
        String zipkinEndpoint = env("ZIPKIN_ENDPOINT", "http://localhost:9411/api/v2/spans");
        int metricsPort = Integer.parseInt(env("METRICS_PORT", "8082"));
        String checkpointLocation = env("CHECKPOINT_LOCATION", "./checkpoint/order-stream-processor");
        String triggerIntervalSeconds = env("TRIGGER_INTERVAL_SECONDS", "5");
        double failureRate = Double.parseDouble(env("FAILURE_RATE", "0.05"));
        int maxInjectedLatencyMs = Integer.parseInt(env("LATENCY_INJECTION_MAX_MS", "0"));

        log.info("Starting {} | bootstrapServers={} topic={} zipkin={} metricsPort={} failureRate={}",
                SERVICE_NAME, bootstrapServers, topic, zipkinEndpoint, metricsPort, failureRate);

        // ---- Observability bootstrap ----
        // FIXED: Declare 'registry' with the matching implementation type required by startMetricsServer
        PrometheusMeterRegistry registry = ObservabilityConfig.createMeterRegistry(SERVICE_NAME);
        HttpServer metricsServer = ObservabilityConfig.startMetricsServer(registry, metricsPort);
        Tracing tracing = ObservabilityConfig.buildTracing(SERVICE_NAME, zipkinEndpoint);
        Tracer tracer = tracing.tracer();
        GoldenSignals metrics = GoldenSignals.forConsumer(registry);

        AtomicLong stalenessSeconds = new AtomicLong(0);
        Gauge.builder("orders.processing.staleness.seconds", stalenessSeconds, AtomicLong::get)
                .description("How far behind (seconds) processing is relative to event creation time - a practical saturation/lag proxy")
                .register(registry);

        Propagation.Getter<Headers, String> headerGetter = (carrier, key) -> {
            Header h = carrier.lastHeader(key);
            return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
        };
        TraceContext.Extractor<Headers> extractor = tracing.propagation().extractor(headerGetter);

        // ---- Spark session ----
        SparkSession spark = SparkSession.builder()
                .appName(SERVICE_NAME)
                .master(env("SPARK_MASTER", "local[*]"))
                .config("spark.sql.shuffle.partitions", "2")
                .config("spark.ui.enabled", "true")
                .getOrCreate();

        spark.streams().addListener(new StreamingQueryListener() {
            @Override
            public void onQueryStarted(QueryStartedEvent event) {
                log.info("Streaming query started id={} runId={}", event.id(), event.runId());
            }

            @Override
            public void onQueryProgress(QueryProgressEvent event) {
                long batchId = event.progress().batchId();
                long inputRows = event.progress().numInputRows();
                Long triggerMs = event.progress().durationMs().get("triggerExecution");
                log.info("Micro-batch completed batchId={} inputRows={} triggerExecutionMs={}",
                        batchId, inputRows, triggerMs);
            }

            @Override
            public void onQueryTerminated(QueryTerminatedEvent event) {
                log.warn("Streaming query terminated id={} exception={}", event.id(), event.exception());
            }
        });

        Dataset<Row> rawEvents = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServers)
                .option("subscribe", topic)
                .option("startingOffsets", "latest")
                .option("includeHeaders", "true")
                .option("maxOffsetsPerTrigger", 1000)
                .load();

        VoidFunction2<Dataset<Row>, Long> batchHandler = (batchDf, batchId) -> {
            List<Row> rows = batchDf.collectAsList();
            if (rows.isEmpty()) {
                return;
            }
            log.info("Processing micro-batch batchId={} size={}", batchId, rows.size());

            for (Row row : rows) {
                processRow(row, tracer, extractor, metrics, stalenessSeconds, failureRate, maxInjectedLatencyMs);
            }
        };

        StreamingQuery query = rawEvents.writeStream()
                .foreachBatch(batchHandler)
                .trigger(Trigger.ProcessingTime(triggerIntervalSeconds + " seconds"))
                .option("checkpointLocation", checkpointLocation)
                .start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown signal received, stopping {} cleanly", SERVICE_NAME);
            try {
                query.stop();
            } catch (Exception ignored) {
            }
            tracing.close();
            metricsServer.stop(1);
            spark.stop();
        }, "shutdown-hook"));

        log.info("{} is now consuming from topic '{}'. Spark UI: http://localhost:4040", SERVICE_NAME, topic);
        query.awaitTermination();
    }

    private static void processRow(Row row, Tracer tracer, TraceContext.Extractor<Headers> extractor,
                                   GoldenSignals metrics, AtomicLong stalenessSeconds,
                                   double failureRate, int maxInjectedLatencyMs) {
        byte[] valueBytes = (byte[]) row.getAs("value");
        String payload = new String(valueBytes, StandardCharsets.UTF_8);

        Headers headers = extractKafkaHeaders(row);
        OrderEvent event;
        try {
            event = OrderEvent.fromJson(payload);
        } catch (Exception parseEx) {
            metrics.recordError("JsonParseException");
            log.error("Could not parse order event payload, skipping record: {}", payload, parseEx);
            return;
        }

        String correlationId = event.getCorrelationId() != null
                ? event.getCorrelationId()
                : headerValueOrNull(headers, CorrelationIdSupport.KAFKA_HEADER_CORRELATION_ID);

        MDC.put(CorrelationIdSupport.MDC_CORRELATION_ID, correlationId);
        MDC.put(CorrelationIdSupport.MDC_ORDER_ID, event.getOrderId());

        TraceContextOrSamplingFlags extracted = extractor.extract(headers);
        Span span = tracer.nextSpan(extracted).name("process-order-event").kind(Span.Kind.CONSUMER)
                .tag("order.id", event.getOrderId())
                .tag("messaging.system", "kafka")
                .start();

        long startNanos = System.nanoTime();
        try (Tracer.SpanInScope ignored = tracer.withSpanInScope(span)) {
            log.info("Processing order event amount={} quantity={} productId={}",
                    event.getAmount(), event.getQuantity(), event.getProductId());

            if (maxInjectedLatencyMs > 0) {
                Thread.sleep(RANDOM.nextInt(maxInjectedLatencyMs + 1));
            }

            validateBusinessRules(event);

            if (RANDOM.nextDouble() < failureRate) {
                throw new RuntimeException("Simulated downstream inventory-service timeout for " + event.getOrderId());
            }

            long staleness = Math.max(0, (System.currentTimeMillis() - event.getEventTimestamp()) / 1000);
            stalenessSeconds.set(staleness);

            metrics.consumedCounter.increment();
            metrics.processingDuration.record(java.time.Duration.ofNanos(System.nanoTime() - startNanos));

            log.info("Successfully processed order event durationMs={}", (System.nanoTime() - startNanos) / 1_000_000);
        } catch (Exception ex) {
            span.error(ex);
            metrics.recordError(ex.getClass().getSimpleName());
            log.error("Failed to process order event orderId={}", event.getOrderId(), ex);
        } finally {
            span.finish();
            MDC.remove(CorrelationIdSupport.MDC_CORRELATION_ID);
            MDC.remove(CorrelationIdSupport.MDC_ORDER_ID);
        }
    }

    private static void validateBusinessRules(OrderEvent event) {
        if (event.getAmount() <= 0) {
            throw new IllegalArgumentException("Order amount must be positive, got " + event.getAmount());
        }
        if (event.getQuantity() <= 0 || event.getQuantity() > 1000) {
            throw new IllegalArgumentException("Order quantity out of range: " + event.getQuantity());
        }
    }

    @SuppressWarnings("unchecked")
    private static Headers extractKafkaHeaders(Row row) {
        RecordHeaders headers = new RecordHeaders();
        int headersFieldIndex;
        try {
            headersFieldIndex = row.fieldIndex("headers");
        } catch (IllegalArgumentException notPresent) {
            return headers;
        }
        List<Row> headerRows = row.getList(headersFieldIndex);
        if (headerRows == null) {
            return headers;
        }
        for (Row headerRow : headerRows) {
            String key = headerRow.getAs("key");
            byte[] value = (byte[]) headerRow.getAs("value");
            headers.add(key, value);
        }
        return headers;
    }

    private static String headerValueOrNull(Headers headers, String key) {
        Header header = headers.lastHeader(key);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    private static String env(String key, String defaultValue) {
        String fromEnv = System.getenv(key);
        if (fromEnv != null && !fromEnv.isBlank()) {
            return fromEnv;
        }
        return System.getProperty(key, defaultValue);
    }
}