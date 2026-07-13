package com.bank.retail.streaming.model;

import java.io.Serializable;

/**
 * ProcessedPaymentEvent
 * ----------------------
 * The OUTPUT record of App3's processing pipeline: every field from
 * PaymentOrderEvent, PLUS the results of fraud-checking and payment-gateway
 * processing. This is the row shape that lands in the Delta Lake table, and
 * is exactly what App4 (Incident Investigator) and App5 (Dashboard) read
 * back out.
 *
 * We deliberately FLATTEN all the original order fields into this class
 * (instead of "extends PaymentOrderEvent" or nesting it as an object) because
 * Spark's bean-based Dataset encoder and Parquet/Delta's columnar storage
 * both work most predictably with flat, non-nested beans for a class like
 * this used purely as a data carrier.
 */
public class ProcessedPaymentEvent implements Serializable {

    // ---- carried over from the original order ----
    private String correlationId;
    private String orderId;
    private String customerId;
    private String customerName;
    private String accountNumber;
    private String ifscCode;
    private String bankName;
    private String channel;
    private String merchantCategory;
    private String merchantName;
    private double amount;
    private String currency;
    private String deviceId;
    private boolean newDevice;
    private String city;
    private long orderTimestamp;

    // ---- produced by App3's processing logic ----

    // SUCCESS | FAILED | FRAUD_BLOCKED
    private String paymentStatus;

    private boolean fraudFlag;
    private String fraudReason;     // human-readable reason, "" if not flagged

    // How long the simulated payment-gateway call took, in milliseconds.
    // This is the field that lets us reproduce a realistic "slow payments"
    // incident and then go hunt for it later.
    private long processingLatencyMs;

    private long processedTimestamp; // epoch millis - when App3 finished processing

    // true if processingLatencyMs exceeded our agreed Service Level
    // Agreement threshold (SLA_THRESHOLD_MS in PaymentGatewaySimulator).
    private boolean slaBreach;

    // Used as a Delta partition column ("yyyy-MM-dd") so that Incident
    // Investigation queries that filter by day stay fast even as the table
    // grows - a standard data-lake partitioning practice.
    private String eventDate;

    public ProcessedPaymentEvent() {
        // no-arg constructor required for Spark's Encoders.bean(...) reflection
    }

    public String getCorrelationId() { return correlationId; }
    public void setCorrelationId(String correlationId) { this.correlationId = correlationId; }

    public String getOrderId() { return orderId; }
    public void setOrderId(String orderId) { this.orderId = orderId; }

    public String getCustomerId() { return customerId; }
    public void setCustomerId(String customerId) { this.customerId = customerId; }

    public String getCustomerName() { return customerName; }
    public void setCustomerName(String customerName) { this.customerName = customerName; }

    public String getAccountNumber() { return accountNumber; }
    public void setAccountNumber(String accountNumber) { this.accountNumber = accountNumber; }

    public String getIfscCode() { return ifscCode; }
    public void setIfscCode(String ifscCode) { this.ifscCode = ifscCode; }

    public String getBankName() { return bankName; }
    public void setBankName(String bankName) { this.bankName = bankName; }

    public String getChannel() { return channel; }
    public void setChannel(String channel) { this.channel = channel; }

    public String getMerchantCategory() { return merchantCategory; }
    public void setMerchantCategory(String merchantCategory) { this.merchantCategory = merchantCategory; }

    public String getMerchantName() { return merchantName; }
    public void setMerchantName(String merchantName) { this.merchantName = merchantName; }

    public double getAmount() { return amount; }
    public void setAmount(double amount) { this.amount = amount; }

    public String getCurrency() { return currency; }
    public void setCurrency(String currency) { this.currency = currency; }

    public String getDeviceId() { return deviceId; }
    public void setDeviceId(String deviceId) { this.deviceId = deviceId; }

    public boolean isNewDevice() { return newDevice; }
    public void setNewDevice(boolean newDevice) { this.newDevice = newDevice; }

    public String getCity() { return city; }
    public void setCity(String city) { this.city = city; }

    public long getOrderTimestamp() { return orderTimestamp; }
    public void setOrderTimestamp(long orderTimestamp) { this.orderTimestamp = orderTimestamp; }

    public String getPaymentStatus() { return paymentStatus; }
    public void setPaymentStatus(String paymentStatus) { this.paymentStatus = paymentStatus; }

    public boolean isFraudFlag() { return fraudFlag; }
    public void setFraudFlag(boolean fraudFlag) { this.fraudFlag = fraudFlag; }

    public String getFraudReason() { return fraudReason; }
    public void setFraudReason(String fraudReason) { this.fraudReason = fraudReason; }

    public long getProcessingLatencyMs() { return processingLatencyMs; }
    public void setProcessingLatencyMs(long processingLatencyMs) { this.processingLatencyMs = processingLatencyMs; }

    public long getProcessedTimestamp() { return processedTimestamp; }
    public void setProcessedTimestamp(long processedTimestamp) { this.processedTimestamp = processedTimestamp; }

    public boolean isSlaBreach() { return slaBreach; }
    public void setSlaBreach(boolean slaBreach) { this.slaBreach = slaBreach; }

    public String getEventDate() { return eventDate; }
    public void setEventDate(String eventDate) { this.eventDate = eventDate; }
}
