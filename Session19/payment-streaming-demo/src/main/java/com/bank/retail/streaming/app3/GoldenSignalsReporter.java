package com.bank.retail.streaming.app3;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.*;

/**
 * ============================================================================
 *  GoldenSignalsReporter
 * ============================================================================
 * BUSINESS PROBLEM THIS SOLVES:
 * Google's SRE book defines four "Golden Signals" every production system
 * should monitor: LATENCY, TRAFFIC, ERRORS and SATURATION. This class
 * computes all four for every Spark micro-batch and prints a single,
 * grep-able structured log line - exactly the kind of line a real
 * dashboard/alerting system (Grafana, Datadog, etc.) would scrape.
 *
 * >>> THE METHOD THAT SOLVES THIS PROBLEM IS: report() <<<
 * It is called once per micro-batch from inside the foreachBatch() callback
 * in PaymentStreamProcessorApp.
 * ============================================================================
 */
public class GoldenSignalsReporter {

    private static final Logger LOG = LogManager.getLogger(GoldenSignalsReporter.class);

    // If saturation (records/sec this batch) exceeds this, we treat the
    // pipeline as "under pressure" - a stand-in for a real capacity ceiling
    // (e.g. max throughput your payment gateway connection pool can take).
    private static final double SATURATION_THROUGHPUT_THRESHOLD = 50.0;

    // Roughly how often Spark triggers a micro-batch in this demo (matches
    // the trigger interval set in PaymentStreamProcessorApp) - used only to
    // turn "records in this batch" into an approximate records/sec figure.
    private static final double BATCH_INTERVAL_SECONDS = 5.0;

    /**
     * >>> SOLUTION METHOD <<<
     * Computes Latency / Traffic / Errors / Saturation for one micro-batch
     * of PROCESSED payments and logs them as one structured line.
     *
     * @param batchDf a Dataset<Row> of ProcessedPaymentEvent rows for this
     *                micro-batch (already cached by the caller).
     * @param batchId Spark's monotonically increasing micro-batch id.
     */
    public void report(Dataset<Row> batchDf, long batchId) {

        long traffic = batchDf.count(); // GOLDEN SIGNAL #1: TRAFFIC
        if (traffic == 0) {
            LOG.info("GOLDEN_SIGNALS batchId={} traffic=0 (empty batch - no new orders arrived)", batchId);
            return;
        }

        // One aggregation pass computes errors, latency avg/p95/max in a
        // single Spark job, rather than four separate .count()/.agg() calls -
        // far cheaper, and how you'd actually write this in production.
        Row stats = batchDf.agg(
                sum(when(col("paymentStatus").equalTo("FAILED")
                        .or(col("paymentStatus").equalTo("FRAUD_BLOCKED")), 1).otherwise(0)).as("errorCount"),
                avg(col("processingLatencyMs")).as("avgLatency"),
                expr("percentile_approx(processingLatencyMs, 0.95)").as("p95Latency"),
                max(col("processingLatencyMs")).as("maxLatency"),
                sum(when(col("slaBreach").equalTo(true), 1).otherwise(0)).as("slaBreaches")
        ).first();

        long errorCount = stats.getAs("errorCount") == null ? 0L : ((Number) stats.getAs("errorCount")).longValue();
        double avgLatency = stats.getAs("avgLatency") == null ? 0.0 : ((Number) stats.getAs("avgLatency")).doubleValue();
        long p95Latency = stats.getAs("p95Latency") == null ? 0L : ((Number) stats.getAs("p95Latency")).longValue();
        long maxLatency = stats.getAs("maxLatency") == null ? 0L : ((Number) stats.getAs("maxLatency")).longValue();
        long slaBreaches = stats.getAs("slaBreaches") == null ? 0L : ((Number) stats.getAs("slaBreaches")).longValue();

        double errorRatePct = (errorCount * 100.0) / traffic; // GOLDEN SIGNAL #2: ERRORS
        double throughputPerSec = traffic / BATCH_INTERVAL_SECONDS; // feeds GOLDEN SIGNAL #4
        boolean saturated = throughputPerSec > SATURATION_THROUGHPUT_THRESHOLD; // GOLDEN SIGNAL #4: SATURATION

        // One single structured line: this is intentionally formatted as
        // key=value pairs (not prose) because that is what makes a log line
        // machine-parseable for a real dashboard/alerting tool, and human
        // grep-able for an SRE doing live investigation.
        LOG.info("GOLDEN_SIGNALS batchId={} traffic={} errorRatePct={} avgLatencyMs={} p95LatencyMs={} "
                        + "maxLatencyMs={} slaBreaches={} throughputPerSec={} saturated={}",
                batchId, traffic,
                String.format("%.1f", errorRatePct),
                String.format("%.0f", avgLatency),
                p95Latency, maxLatency, slaBreaches,
                String.format("%.1f", throughputPerSec), saturated);

        if (saturated) {
            LOG.warn("CAPACITY_WARNING batchId={} throughputPerSec={} exceeds threshold={}",
                    batchId, String.format("%.1f", throughputPerSec), SATURATION_THROUGHPUT_THRESHOLD);
        }
    }
}
