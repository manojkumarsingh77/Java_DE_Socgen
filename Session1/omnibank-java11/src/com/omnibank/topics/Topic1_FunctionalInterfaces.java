package com.omnibank.topics;

import com.omnibank.model.Transaction;
import com.omnibank.util.DataFactory;

import java.math.BigDecimal;
import java.util.List;
import java.util.function.*;

public class Topic1_FunctionalInterfaces {

    @FunctionalInterface
    interface FraudRule {
        boolean isSuspicious(Transaction txn);

        default FraudRule and(FraudRule other) {
            return txn -> this.isSuspicious(txn) && other.isSuspicious(txn);
        }
        default FraudRule or(FraudRule other) {
            return txn -> this.isSuspicious(txn) || other.isSuspicious(txn);
        }
    }

    public static void run() {
        System.out.println("\n=========== TOPIC 1: FUNCTIONAL INTERFACES ===========");

        List<Transaction> txns = DataFactory.generateTransactions(10, 42);

        Predicate<Transaction> highValue =
                t -> t.getAmount().compareTo(new BigDecimal("50000")) > 0;
        Predicate<Transaction> isWithdrawal =
                t -> t.getType() == Transaction.Type.WITHDRAWAL;

        long highValueWithdrawals = txns.stream()
                .filter(highValue.and(isWithdrawal))
                .count();
        System.out.println("High-value withdrawals: " + highValueWithdrawals);

        Function<Transaction, String> accountExtractor = Transaction::getAccountId;
        System.out.println("First account: " + accountExtractor.apply(txns.get(0)));

        Consumer<Transaction> auditLog =
                t -> System.out.println("  AUDIT -> " + t.getTxnId() + " | " + t.getType());
        txns.stream().limit(3).forEach(auditLog);

        Supplier<String> idGen = () -> "TXN-" + System.nanoTime();
        System.out.println("Generated ID: " + idGen.get());

        BiFunction<BigDecimal, BigDecimal, BigDecimal> applyFee =
                (amt, rate) -> amt.multiply(rate);
        System.out.println("Fee on 1000 @ 2% = " +
                applyFee.apply(new BigDecimal("1000"), new BigDecimal("0.02")));

        FraudRule largeAmount = t -> t.getAmount().compareTo(new BigDecimal("80000")) > 0;
        FraudRule mobileChannel = t -> t.getTags().contains("channel:MOBILE");
        FraudRule combined = largeAmount.and(mobileChannel);

        long suspicious = txns.stream().filter(combined::isSuspicious).count();
        System.out.println("Suspicious (large + mobile): " + suspicious);

        UnaryOperator<BigDecimal> normalize = a -> a.setScale(2, java.math.RoundingMode.HALF_UP);
        System.out.println("Normalized: " + normalize.apply(new BigDecimal("123.4567")));
    }
}
