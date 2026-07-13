package com.paynova.obs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

public class StructuredLogger {
    private static final Logger LOG = LoggerFactory.getLogger("paynova.obs");

    public static void logBatchStart(long batchId, int recordCount) {
        MDC.put("batch_id", String.valueOf(batchId));
        MDC.put("signal", "traffic");
        LOG.info("event=batch_start batch_id={} record_count={}", batchId, recordCount);
    }

    public static void logBatchEnd(long batchId, long durationMs, long success, long failed) {
        MDC.put("signal", "latency");
        LOG.info("event=batch_end batch_id={} duration_ms={} success={} failed={}",
                batchId, durationMs, success, failed);
        MDC.remove("signal");
        MDC.remove("batch_id");
    }

    public static void logError(long batchId, String correlationId, Throwable t) {
        MDC.put("correlation_id", correlationId == null ? "unknown" : correlationId);
        MDC.put("signal", "errors");
        LOG.error("event=batch_failed batch_id={} error_type={} message={}",
                batchId, t.getClass().getSimpleName(), t.getMessage(), t);
        MDC.clear();
    }

    public static void logSaturation(long batchId, long consumerLag, long heapUsedMb) {
        MDC.put("signal", "saturation");
        LOG.warn("event=saturation_snapshot batch_id={} consumer_lag={} heap_used_mb={}",
                batchId, consumerLag, heapUsedMb);
        MDC.remove("signal");
    }
}