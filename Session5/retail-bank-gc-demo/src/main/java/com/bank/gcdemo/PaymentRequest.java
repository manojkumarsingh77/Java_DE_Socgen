package com.bank.gcdemo;

public class PaymentRequest {

    private final long txnId;
    private final long customerId;
    private final double amount;
    private final String merchantId;

    public PaymentRequest(long txnId,
                          long customerId,
                          double amount,
                          String merchantId) {
        this.txnId = txnId;
        this.customerId = customerId;
        this.amount = amount;
        this.merchantId = merchantId;
    }

    public long getTxnId() {
        return txnId;
    }

    public long getCustomerId() {
        return customerId;
    }

    public double getAmount() {
        return amount;
    }

    public String getMerchantId() {
        return merchantId;
    }
}