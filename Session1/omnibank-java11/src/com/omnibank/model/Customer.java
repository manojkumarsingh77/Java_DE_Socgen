package com.omnibank.model;

import java.math.BigDecimal;
import java.util.Objects;

/**
 * Customer DTO - Java 11 compatible (final class instead of record).
 */
public final class Customer {
    private final String customerId;
    private final String name;
    private final String tier;
    private final BigDecimal lifetimeValue;
    private final String city;

    public Customer(String customerId, String name, String tier,
                    BigDecimal lifetimeValue, String city) {
        if (customerId == null || name == null) {
            throw new IllegalArgumentException("Required fields missing");
        }
        if (lifetimeValue == null || lifetimeValue.signum() < 0) {
            throw new IllegalArgumentException("Invalid lifetime value");
        }
        this.customerId = customerId;
        this.name = name;
        this.tier = tier;
        this.lifetimeValue = lifetimeValue;
        this.city = city;
    }

    public String customerId()       { return customerId; }
    public String name()             { return name; }
    public String tier()             { return tier; }
    public BigDecimal lifetimeValue(){ return lifetimeValue; }
    public String city()             { return city; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Customer)) return false;
        Customer c = (Customer) o;
        return Objects.equals(customerId, c.customerId);
    }

    @Override public int hashCode() { return Objects.hash(customerId); }

    @Override
    public String toString() {
        return String.format("Customer[id=%s, name=%s, tier=%s, lifetimeValue=%s, city=%s]",
                customerId, name, tier, lifetimeValue, city);
    }
}
