package com.omnibank.topics;

import com.omnibank.model.Customer;
import com.omnibank.model.Transaction;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Topic5_ImmutableDTODesign {

    /** Immutable balance class (was a record in Java 16+). */
    public static final class AccountBalance {
        private final String accountId;
        private final BigDecimal balance;
        private final LocalDateTime asOf;

        public AccountBalance(String accountId, BigDecimal balance, LocalDateTime asOf) {
            this.accountId = accountId;
            this.balance = balance;
            this.asOf = asOf;
        }
        public String accountId()      { return accountId; }
        public BigDecimal balance()    { return balance; }
        public LocalDateTime asOf()    { return asOf; }

        public AccountBalance withBalance(BigDecimal newBalance) {
            return new AccountBalance(accountId, newBalance, LocalDateTime.now());
        }
        public AccountBalance credit(BigDecimal amt) { return withBalance(balance.add(amt)); }
        public AccountBalance debit (BigDecimal amt) { return withBalance(balance.subtract(amt)); }
    }

    public static final class LoanApplication {
        private final String applicationId;
        private final String customerId;
        private final BigDecimal amount;
        private final int tenureMonths;
        private final String purpose;
        private final boolean coApplicant;

        private LoanApplication(Builder b) {
            this.applicationId = b.applicationId;
            this.customerId    = b.customerId;
            this.amount        = b.amount;
            this.tenureMonths  = b.tenureMonths;
            this.purpose       = b.purpose;
            this.coApplicant   = b.coApplicant;
        }

        public static Builder builder() { return new Builder(); }

        @Override public String toString() {
            return String.format("Loan[%s, %s, INR %s, %dM, %s, coApp=%s]",
                    applicationId, customerId, amount, tenureMonths, purpose, coApplicant);
        }

        public static class Builder {
            private String applicationId;
            private String customerId;
            private BigDecimal amount;
            private int tenureMonths;
            private String purpose;
            private boolean coApplicant;

            public Builder applicationId(String v) { this.applicationId = v; return this; }
            public Builder customerId(String v)    { this.customerId = v; return this; }
            public Builder amount(BigDecimal v)    { this.amount = v; return this; }
            public Builder tenureMonths(int v)     { this.tenureMonths = v; return this; }
            public Builder purpose(String v)       { this.purpose = v; return this; }
            public Builder coApplicant(boolean v)  { this.coApplicant = v; return this; }
            public LoanApplication build()         { return new LoanApplication(this); }
        }
    }

    public static void run() {
        System.out.println("\n=========== TOPIC 5: IMMUTABLE DTO DESIGN ===========");

        Customer c = new Customer("ACC1", "Asha", "PREMIUM",
                new BigDecimal("450000"), "Bengaluru");
        System.out.println("Customer DTO: " + c);

        List<String> tags = new ArrayList<>(Arrays.asList("MOBILE", "HIGH-RISK"));
        Transaction t = new Transaction("TXN1", "ACC1",
                new BigDecimal("75000"), LocalDateTime.now(),
                Transaction.Type.WITHDRAWAL, tags);

        tags.add("INJECTED");
        System.out.println("Caller's list:  " + tags);
        System.out.println("DTO's tags:     " + t.getTags()
                + "  <-- defensive copy in protected us");

        try {
            t.getTags().add("HACK");
        } catch (UnsupportedOperationException ex) {
            System.out.println("Cannot mutate via getter (UnsupportedOperationException)");
        }

        AccountBalance ab = new AccountBalance("ACC1", new BigDecimal("10000"), LocalDateTime.now());
        AccountBalance afterCredit = ab.credit(new BigDecimal("5000"));
        AccountBalance afterDebit  = afterCredit.debit(new BigDecimal("2000"));
        System.out.println("\nWith-er pattern:");
        System.out.println("  Original: " + ab.balance());
        System.out.println("  Credited: " + afterCredit.balance());
        System.out.println("  Debited:  " + afterDebit.balance());
        System.out.println("  Original unchanged? " + ab.balance().equals(new BigDecimal("10000")));

        LoanApplication la = LoanApplication.builder()
                .applicationId("LN001")
                .customerId("ACC1")
                .amount(new BigDecimal("500000"))
                .tenureMonths(60)
                .purpose("HOME_RENOVATION")
                .coApplicant(true)
                .build();
        System.out.println("\nBuilder pattern: " + la);
    }
}
