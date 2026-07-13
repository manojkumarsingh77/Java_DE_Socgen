package com.omnibank.util;

import com.omnibank.model.Customer;
import com.omnibank.model.Purchase;
import com.omnibank.model.Transaction;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class DataFactory {

    private static final String[] CITIES = {"Bengaluru", "Mumbai", "Delhi", "Pune", "Hyderabad"};
    private static final String[] TIERS  = {"PREMIUM", "GOLD", "SILVER"};
    private static final String[] CATS   = {"GROCERY", "ELECTRONICS", "APPAREL", "BOOKS"};
    private static final Transaction.Type[] TYPES = Transaction.Type.values();

    public static List<Transaction> generateTransactions(int count, long seed) {
        Random r = new Random(seed);
        List<Transaction> list = new ArrayList<>(count);
        LocalDateTime base = LocalDateTime.now().minusDays(30);
        for (int i = 0; i < count; i++) {
            BigDecimal amt = BigDecimal.valueOf(100 + r.nextInt(99_900));
            List<String> tags = Collections.singletonList(
                    "channel:" + (r.nextBoolean() ? "MOBILE" : "WEB"));
            list.add(new Transaction(
                    "TXN" + i,
                    "ACC" + (r.nextInt(1000)),
                    amt,
                    base.plusMinutes(r.nextInt(43_200)),
                    TYPES[r.nextInt(TYPES.length)],
                    tags
            ));
        }
        return list;
    }

    public static List<Customer> generateCustomers(int count, long seed) {
        Random r = new Random(seed);
        List<Customer> list = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            list.add(new Customer(
                    "ACC" + i,
                    "Customer-" + i,
                    TIERS[r.nextInt(TIERS.length)],
                    BigDecimal.valueOf(1_000 + r.nextInt(500_000)),
                    CITIES[r.nextInt(CITIES.length)]
            ));
        }
        return list;
    }

    public static List<Purchase> generatePurchases(int count, long seed) {
        Random r = new Random(seed);
        List<Purchase> list = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            list.add(new Purchase(
                    "ORD" + i,
                    "ACC" + r.nextInt(1000),
                    CATS[r.nextInt(CATS.length)],
                    BigDecimal.valueOf(50 + r.nextInt(50_000)),
                    CITIES[r.nextInt(CITIES.length)]
            ));
        }
        return list;
    }
}
