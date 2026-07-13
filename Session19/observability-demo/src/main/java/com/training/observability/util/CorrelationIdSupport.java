package com.training.observability.util;

import java.util.UUID;

/**
 * Centralizes the MDC key names used for structured logging, and correlation-id generation.
 * Keeping these in one place avoids "magic string" drift between producer and consumer logs,
 * which is a common real-world cause of broken log correlation.
 */
public final class CorrelationIdSupport {

    public static final String MDC_CORRELATION_ID = "correlationId";
    public static final String MDC_ORDER_ID = "orderId";
    public static final String KAFKA_HEADER_CORRELATION_ID = "correlationId";

    private CorrelationIdSupport() {
    }

    public static String newCorrelationId() {
        return UUID.randomUUID().toString();
    }
}
