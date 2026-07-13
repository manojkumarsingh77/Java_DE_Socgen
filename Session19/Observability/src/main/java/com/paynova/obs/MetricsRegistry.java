package com.paynova.obs;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;

public class MetricsRegistry {

    public static final Counter PAYMENTS_PROCESSED = Counter.build()
            .name("spark_payments_processed_total")
            .help("Total payments processed")
            .labelNames("status", "merchant_id")
            .register();

    public static final Histogram BATCH_DURATION = Histogram.build()
            .name("spark_batch_duration_seconds")
            .help("Micro-batch processing time")
            .buckets(0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)
            .register();

    public static final Histogram PAYMENT_LATENCY = Histogram.build()
            .name("spark_payment_end_to_end_latency_seconds")
            .help("End-to-end latency per payment")
            .labelNames("merchant_id")
            .buckets(0.5, 1.0, 2.0, 5.0, 10.0, 30.0)
            .register();

    public static final Gauge CONSUMER_LAG = Gauge.build()
            .name("spark_kafka_consumer_lag")
            .help("Latest consumer lag per partition")
            .labelNames("partition")
            .register();

    public static final Gauge BATCH_IN_FLIGHT = Gauge.build()
            .name("spark_batch_in_flight")
            .help("1 if a micro-batch is currently running")
            .register();
}