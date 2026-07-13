package com.training.containerization.data;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Generates a deterministic, in-memory retail sales dataset. No files, no network
 * calls - the whole training environment stays reproducible whether a learner is
 * offline, behind a corporate proxy, or inside a locked-down container.
 */
public class SampleDataGenerator {

    private static final String[] REGIONS = {"North", "South", "East", "West"};
    private static final String[] CATEGORIES = {"Electronics", "Grocery", "Apparel", "Home", "Sports"};
    private static final String[][] PRODUCTS_BY_CATEGORY = {
            {"Laptop", "Headphones", "Smartphone", "Tablet"},
            {"Rice-5kg", "Cooking-Oil-1L", "Wheat-Flour-5kg", "Sugar-2kg"},
            {"T-Shirt", "Jeans", "Jacket", "Sneakers"},
            {"Vacuum-Cleaner", "Mixer-Grinder", "Bedsheet-Set", "Table-Lamp"},
            {"Cricket-Bat", "Football", "Yoga-Mat", "Dumbbell-Set"}
    };

    public static List<SalesRecord> generate(int count) {
        Random random = new Random(42L); // fixed seed -> identical output every run/every machine
        LocalDate startDate = LocalDate.now().minusDays(90);

        List<SalesRecord> records = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            int categoryIdx = random.nextInt(CATEGORIES.length);
            String category = CATEGORIES[categoryIdx];
            String[] products = PRODUCTS_BY_CATEGORY[categoryIdx];
            String product = products[random.nextInt(products.length)];
            String region = REGIONS[random.nextInt(REGIONS.length)];
            String date = startDate.plusDays(random.nextInt(90)).toString();
            int quantity = 1 + random.nextInt(10);
            double unitPrice = 5 + (random.nextDouble() * 495); // 5.0 - 500.0

            records.add(new SalesRecord(date, region, category, product, quantity,
                    Math.round(unitPrice * 100.0) / 100.0));
        }
        return records;
    }
}
