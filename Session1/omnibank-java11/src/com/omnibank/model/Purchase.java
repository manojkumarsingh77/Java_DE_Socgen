package com.omnibank.model;

import java.math.BigDecimal;
import java.util.Objects;

/**
 * Retail Purchase - Java 11 compatible.
 */
public final class Purchase {
    private final String orderId;
    private final String customerId;
    private final String category;
    private final BigDecimal amount;
    private final String storeCity;

    public Purchase(String orderId, String customerId, String category,
                    BigDecimal amount, String storeCity) {
        this.orderId = orderId;
        this.customerId = customerId;
        this.category = category;
        this.amount = amount;
        this.storeCity = storeCity;
    }

    public String orderId()      { return orderId; }
    public String customerId()   { return customerId; }
    public String category()     { return category; }
    public BigDecimal amount()   { return amount; }
    public String storeCity()    { return storeCity; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Purchase)) return false;
        Purchase p = (Purchase) o;
        return Objects.equals(orderId, p.orderId);
    }
    @Override public int hashCode() { return Objects.hash(orderId); }

    @Override
    public String toString() {
        return String.format("Purchase[%s, %s, %s, %s, %s]",
                orderId, customerId, category, amount, storeCity);
    }
}
