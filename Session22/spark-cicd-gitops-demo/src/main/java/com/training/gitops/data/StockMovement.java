package com.training.gitops.data;

import java.io.Serializable;

/**
 * Plain Java bean for Spark's bean encoder. Represents one warehouse stock
 * movement event. This is the "business payload" that the versioned,
 * scanned, and progressively-deployed artifact in this demo actually processes -
 * every CI/CD concept in this project is applied to a real, running Spark job,
 * not a placeholder.
 */
public class StockMovement implements Serializable {
    private String date;
    private String warehouse;
    private String sku;
    private String movementType; // INBOUND / OUTBOUND / ADJUSTMENT
    private int quantity;
    private double unitCost;

    public StockMovement() {
    }

    public StockMovement(String date, String warehouse, String sku, String movementType,
                          int quantity, double unitCost) {
        this.date = date;
        this.warehouse = warehouse;
        this.sku = sku;
        this.movementType = movementType;
        this.quantity = quantity;
        this.unitCost = unitCost;
    }

    public String getDate() { return date; }
    public void setDate(String date) { this.date = date; }
    public String getWarehouse() { return warehouse; }
    public void setWarehouse(String warehouse) { this.warehouse = warehouse; }
    public String getSku() { return sku; }
    public void setSku(String sku) { this.sku = sku; }
    public String getMovementType() { return movementType; }
    public void setMovementType(String movementType) { this.movementType = movementType; }
    public int getQuantity() { return quantity; }
    public void setQuantity(int quantity) { this.quantity = quantity; }
    public double getUnitCost() { return unitCost; }
    public void setUnitCost(double unitCost) { this.unitCost = unitCost; }
}
