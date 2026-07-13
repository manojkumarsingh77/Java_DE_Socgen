package com.bank.retail.streaming.util;

import com.bank.retail.streaming.model.PaymentOrderEvent;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * SyntheticDataGenerator
 * ----------------------
 * THIS IS THE METHOD THAT SOLVES: "give a real-world synthetic dataset from
 * the Retail Banking domain". Nothing here is real customer data - every
 * name, bank, account number and IFSC code is generated. The SHAPE of the
 * data (fields, value ranges, ID formats) mirrors what a real Indian retail
 * bank's order/payment event would look like, which is what makes the demo
 * believable without using a single real record.
 *
 * >>> generateOrder() is the core "solving" method - see below. <<<
 */
public class SyntheticDataGenerator {

    private final ThreadLocalRandom random = ThreadLocalRandom.current();

    // A believable spread of common Indian first/last names. These are
    // generic given names (the same pool any data-generation/Faker library
    // would draw from), NOT references to real individuals.
    private static final String[] FIRST_NAMES = {
            "Rohan", "Priya", "Arjun", "Sneha", "Vikram", "Ananya", "Karan",
            "Divya", "Aditya", "Pooja", "Rahul", "Neha", "Suresh", "Kavya",
            "Manish", "Ritu", "Sandeep", "Anjali", "Vivek", "Meera"
    };
    private static final String[] LAST_NAMES = {
            "Sharma", "Nair", "Iyer", "Reddy", "Gupta", "Singh", "Patel",
            "Menon", "Joshi", "Verma", "Pillai", "Choudhary", "Rao", "Kulkarni"
    };

    // Fully fictional bank brands + 4-letter bank codes used to build a
    // synthetic IFSC-style code: <4-letter bank code><0><6-char branch code>.
    // None of these codes collide with any real bank's IFSC prefix.
    private static final String[][] BANKS = {
            {"TRST", "TrustIndia Bank"},
            {"METB", "MetroCity Bank"},
            {"NTNL", "National Capital Bank"},
            {"SECB", "SecureNation Bank"},
            {"UNIB", "UnionBharat Bank"}
    };

    private static final String[] CHANNELS = {"UPI", "NEFT", "IMPS", "RTGS", "CARD"};

    // category -> a few fictional merchant brand names in that category
    private static final String[][] MERCHANTS_BY_CATEGORY = {
            {"GROCERY", "GreenBasket Mart", "FreshDaily Grocers", "DailyNeeds Super Store"},
            {"FUEL", "MetroFuel Stations", "HighwayFill Petrol", "CityPump Energy"},
            {"ECOMMERCE", "UrbanCart Online", "ShopNest Retail", "ClickBuy Marketplace"},
            {"UTILITY", "PowerGrid Utilities", "CityWater Board", "HomeGas Connect"},
            {"TRAVEL", "SkyWay Travels", "RailEase Bookings", "CityCab Rides"},
            {"DINING", "QuickBite Foods", "TasteHub Restaurant", "CafeCorner Eats"}
    };

    private static final String[] CITIES = {
            "Bengaluru", "Mumbai", "Pune", "Hyderabad", "Chennai", "Delhi", "Kolkata"
    };

    /**
     * SOLUTION METHOD - generates one fully-synthetic retail banking payment
     * order, with realistic field shapes (masked account numbers, synthetic
     * IFSC codes, INR amounts, Indian cities/channels/merchant categories).
     *
     * This is called once per Kafka message by App2 (PaymentOrderProducerApp).
     */
    public PaymentOrderEvent generateOrder() {
        PaymentOrderEvent order = new PaymentOrderEvent();

        // A fresh UUID per order. This becomes the correlationId that is
        // threaded through EVERY downstream log line, metric and Delta row -
        // the backbone of the whole "SRE investigation" story.
        order.setCorrelationId(UUID.randomUUID().toString());
        order.setOrderId("ORD-" + random.nextInt(100_000, 999_999));
        order.setCustomerId("CUST-" + random.nextInt(10_000, 99_999));

        String firstName = FIRST_NAMES[random.nextInt(FIRST_NAMES.length)];
        String lastName = LAST_NAMES[random.nextInt(LAST_NAMES.length)];
        order.setCustomerName(firstName + " " + lastName);

        order.setAccountNumber(maskedSyntheticAccountNumber());

        String[] bank = BANKS[random.nextInt(BANKS.length)];
        String branchCode = String.valueOf(random.nextInt(100_000, 999_999));
        order.setIfscCode(bank[0] + "0" + branchCode); // e.g. TRST0418273
        order.setBankName(bank[1]);

        order.setChannel(CHANNELS[random.nextInt(CHANNELS.length)]);

        String[] merchantRow = MERCHANTS_BY_CATEGORY[random.nextInt(MERCHANTS_BY_CATEGORY.length)];
        order.setMerchantCategory(merchantRow[0]);
        order.setMerchantName(merchantRow[1 + random.nextInt(merchantRow.length - 1)]);

        // Realistic INR retail transaction sizes: mostly small/medium, with
        // an occasional large-ticket purchase (e.g. travel booking) - this
        // long tail is what later makes our fraud rule ("large CARD spend
        // on a brand-new device") trigger only occasionally, not constantly.
        double amount = round2(random.nextDouble(50, 5_000));
        if (random.nextDouble() < 0.05) {
            amount = round2(random.nextDouble(40_000, 120_000)); // rare big-ticket spend
        }
        order.setAmount(amount);
        order.setCurrency("INR");

        order.setDeviceId("DEV-" + UUID.randomUUID().toString().substring(0, 8));
        // ~12% of orders simulate a customer paying from a device the bank
        // has never seen for that customer before - a classic fraud signal.
        order.setNewDevice(random.nextDouble() < 0.12);

        order.setCity(CITIES[random.nextInt(CITIES.length)]);
        order.setOrderTimestamp(System.currentTimeMillis());

        return order;
    }

    /** Builds a synthetic 16-digit account number, masked in the middle - exactly
     *  how a real bank would display/log it (never the full number). */
    private String maskedSyntheticAccountNumber() {
        long first6 = random.nextLong(100_000, 999_999);
        long last4 = random.nextLong(1000, 9999);
        return first6 + "XXXXXX" + last4;
    }

    private double round2(double value) {
        return Math.round(value * 100.0) / 100.0;
    }
}
