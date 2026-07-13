package com.training.observability.model;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.Serializable;

/**
 * Business event flowing through the pipeline: Producer -> Kafka -> Spark Structured Streaming.
 *
 * correlationId: a business/log correlation identifier that travels with the event in the
 * JSON payload AND as a Kafka record header. It is independent of the Zipkin trace/span id -
 * in real systems correlationId is often the one identifier EVERY system can carry (including
 * legacy systems with no tracing instrumentation at all), whereas traceId/spanId only exist
 * where Brave/Zipkin instrumentation is present. Keeping both is a deliberate, realistic
 * design choice for this demo.
 */
public class OrderEvent implements Serializable {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private String orderId;
    private String correlationId;
    private String customerId;
    private String productId;
    private int quantity;
    private double amount;
    private String eventType;
    private long eventTimestamp;

    public OrderEvent() {
        // required by Jackson
    }

    public OrderEvent(String orderId, String correlationId, String customerId, String productId,
                       int quantity, double amount, String eventType, long eventTimestamp) {
        this.orderId = orderId;
        this.correlationId = correlationId;
        this.customerId = customerId;
        this.productId = productId;
        this.quantity = quantity;
        this.amount = amount;
        this.eventType = eventType;
        this.eventTimestamp = eventTimestamp;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public int getQuantity() {
        return quantity;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public long getEventTimestamp() {
        return eventTimestamp;
    }

    public void setEventTimestamp(long eventTimestamp) {
        this.eventTimestamp = eventTimestamp;
    }

    public String toJson() {
        try {
            return OBJECT_MAPPER.writeValueAsString(this);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to serialize OrderEvent to JSON", e);
        }
    }

    public static OrderEvent fromJson(String json) {
        try {
            return OBJECT_MAPPER.readValue(json, OrderEvent.class);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to parse OrderEvent JSON: " + json, e);
        }
    }

    @Override
    public String toString() {
        return "OrderEvent{" +
                "orderId='" + orderId + '\'' +
                ", correlationId='" + correlationId + '\'' +
                ", customerId='" + customerId + '\'' +
                ", productId='" + productId + '\'' +
                ", quantity=" + quantity +
                ", amount=" + amount +
                ", eventType='" + eventType + '\'' +
                ", eventTimestamp=" + eventTimestamp +
                '}';
    }
}
