package com.training.gitops.data;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Generates a deterministic, in-memory stock-movement dataset - no files, no
 * network. Keeps the "smoke test" every deployment stage runs fast, offline-safe,
 * and identical across macOS/Windows/containers.
 */
public class StockMovementGenerator {

    private static final String[] WAREHOUSES = {"WH-North", "WH-South", "WH-East", "WH-West"};
    private static final String[] SKUS = {
            "SKU-LAPTOP-14", "SKU-MONITOR-27", "SKU-KEYBOARD-MECH", "SKU-MOUSE-WL",
            "SKU-DOCK-USBC", "SKU-WEBCAM-HD", "SKU-HEADSET-BT", "SKU-CHAIR-ERG"
    };
    private static final String[] MOVEMENT_TYPES = {"INBOUND", "OUTBOUND", "ADJUSTMENT"};

    public static List<StockMovement> generate(int count) {
        Random random = new Random(7L); // fixed seed -> reproducible smoke tests
        LocalDate start = LocalDate.now().minusDays(30);

        List<StockMovement> movements = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String warehouse = WAREHOUSES[random.nextInt(WAREHOUSES.length)];
            String sku = SKUS[random.nextInt(SKUS.length)];
            String type = MOVEMENT_TYPES[random.nextInt(MOVEMENT_TYPES.length)];
            String date = start.plusDays(random.nextInt(30)).toString();
            int quantity = type.equals("OUTBOUND") ? -(1 + random.nextInt(50)) : (1 + random.nextInt(50));
            double unitCost = Math.round((10 + random.nextDouble() * 490) * 100.0) / 100.0;
            movements.add(new StockMovement(date, warehouse, sku, type, quantity, unitCost));
        }
        return movements;
    }
}
