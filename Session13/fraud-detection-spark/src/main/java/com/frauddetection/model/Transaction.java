package com.frauddetection.model;

import java.io.Serializable;

/**
 * FRAUD DETECTION PIPELINE - Transaction Domain Model
 *
 * Represents a financial transaction flowing through the pipeline.
 * This is the core event in our Structured Streaming pipeline.
 *
 * Business Context:
 * - Banks process millions of transactions per second
 * - Each transaction must be evaluated for fraud in < 5 seconds (P99)
 * - Late events (network delays, mobile apps) arrive up to 10 minutes late
 */
public class Transaction implements Serializable {

    private String transactionId;
    private String customerId;
    private String cardNumber;
    private double amount;
    private String merchantCategory;
    private String merchantCountry;
    private String merchantCity;
    private long eventTimestamp;      // When transaction ACTUALLY happened (event time)
    private long processingTimestamp; // When we RECEIVED the transaction (processing time)
    private String channel;           // ATM, ONLINE, POS, MOBILE
    private boolean isLateEvent;      // Flag for late-arriving events

    // Default constructor (required for Spark serialization)
    public Transaction() {}

    public Transaction(String transactionId, String customerId, String cardNumber,
                       double amount, String merchantCategory, String merchantCountry,
                       String merchantCity, long eventTimestamp, String channel) {
        this.transactionId = transactionId;
        this.customerId = customerId;
        this.cardNumber = cardNumber;
        this.amount = amount;
        this.merchantCategory = merchantCategory;
        this.merchantCountry = merchantCountry;
        this.merchantCity = merchantCity;
        this.eventTimestamp = eventTimestamp;
        this.processingTimestamp = System.currentTimeMillis();
        this.channel = channel;
        this.isLateEvent = false;
    }

    // ======= Getters and Setters =======

    public String getTransactionId() { return transactionId; }
    public void setTransactionId(String transactionId) { this.transactionId = transactionId; }

    public String getCustomerId() { return customerId; }
    public void setCustomerId(String customerId) { this.customerId = customerId; }

    public String getCardNumber() { return cardNumber; }
    public void setCardNumber(String cardNumber) { this.cardNumber = cardNumber; }

    public double getAmount() { return amount; }
    public void setAmount(double amount) { this.amount = amount; }

    public String getMerchantCategory() { return merchantCategory; }
    public void setMerchantCategory(String merchantCategory) { this.merchantCategory = merchantCategory; }

    public String getMerchantCountry() { return merchantCountry; }
    public void setMerchantCountry(String merchantCountry) { this.merchantCountry = merchantCountry; }

    public String getMerchantCity() { return merchantCity; }
    public void setMerchantCity(String merchantCity) { this.merchantCity = merchantCity; }

    public long getEventTimestamp() { return eventTimestamp; }
    public void setEventTimestamp(long eventTimestamp) { this.eventTimestamp = eventTimestamp; }

    public long getProcessingTimestamp() { return processingTimestamp; }
    public void setProcessingTimestamp(long processingTimestamp) { this.processingTimestamp = processingTimestamp; }

    public String getChannel() { return channel; }
    public void setChannel(String channel) { this.channel = channel; }

    public boolean isLateEvent() { return isLateEvent; }
    public void setLateEvent(boolean lateEvent) { isLateEvent = lateEvent; }

    @Override
    public String toString() {
        return String.format("Transaction{id='%s', customer='%s', amount=%.2f, category='%s', country='%s', channel='%s', late=%s}",
                transactionId, customerId, amount, merchantCategory, merchantCountry, channel, isLateEvent);
    }
}
