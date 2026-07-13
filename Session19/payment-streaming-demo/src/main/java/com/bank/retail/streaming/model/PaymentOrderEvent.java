package com.bank.retail.streaming.model;

import java.io.Serializable;

/**
 * PaymentOrderEvent
 * ------------------
 * This is the CONTRACT of the JSON message that App2 (Producer) publishes to
 * the Kafka topic "retail.payments.orders", and that App3 (Spark Streaming
 * Consumer) parses back out of Kafka.
 *
 * WHY a plain Java bean (getters + setters + no-arg constructor) and not a
 * Java record?
 * Spark's Encoders.bean(...) - which we use in App3 to turn a generic
 * Dataset<Row> into a typed Dataset<PaymentOrderEvent> - relies on classic
 * JavaBean introspection (getX()/setX() pairs). Records don't expose that
 * shape, so a plain mutable bean is the safest choice for Spark interop.
 *
 * It also implements Serializable because Spark may need to move instances
 * of this class between JVM threads/partitions during processing.
 */
public class PaymentOrderEvent implements Serializable {

    // Unique ID that ties ONE customer order to every downstream log line,
    // metric and Delta row it produces. This is the single most important
    // field in the whole demo: it is what lets an SRE trace one transaction
    // end-to-end through a distributed system.
    private String correlationId;

    private String orderId;
    private String customerId;
    private String customerName;

    // Stored already-masked (e.g. "5001230000005678" -> "500123XXXX5678")
    // by the data generator - real banking systems NEVER log/store a full,
    // unmasked account number in a downstream event stream.
    private String accountNumber;

    private String ifscCode;     // synthetic bank-branch routing code
    private String bankName;     // synthetic bank name

    // One of: UPI, NEFT, IMPS, RTGS, CARD
    private String channel;

    // One of: GROCERY, FUEL, ECOMMERCE, UTILITY, TRAVEL, DINING
    private String merchantCategory;
    private String merchantName;

    private double amount;
    private String currency;     // "INR"

    private String deviceId;
    private boolean newDevice;   // true => never seen this device for this customer before

    private String city;

    private long orderTimestamp; // epoch millis - when the customer placed the order

    public PaymentOrderEvent() {
        // Required no-arg constructor: Jackson (JSON <-> object) and Spark's
        // bean encoder both need to be able to instantiate this class
        // reflectively with no arguments, then call setters to populate it.
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

    @Override
    public String toString() {
        return "PaymentOrderEvent{correlationId='" + correlationId + "', orderId='" + orderId
                + "', customerId='" + customerId + "', channel='" + channel
                + "', merchantCategory='" + merchantCategory + "', amount=" + amount + "}";
    }
}
