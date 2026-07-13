package com.training.containerization.data;

import java.io.Serializable;

/**
 * Plain Java bean. Spark's bean encoder (spark.createDataFrame(List, Class)) needs
 * public no-arg constructor + public getters/setters + Serializable.
 * <p>
 * DESIGN NOTE (Windows compatibility): we deliberately generate and consume data
 * in-memory instead of reading/writing CSV/Parquet from local disk. Spark's local
 * file writes go through Hadoop's LocalFileSystem + FileOutputCommitter, which on
 * Windows requires winutils.exe to be installed and HADOOP_HOME configured - a very
 * common source of "it works on my Mac but not on my Windows laptop" failures in
 * training rooms. Avoiding disk I/O for the core demo removes that failure mode
 * entirely while still exercising real Spark transformations, shuffles and
 * aggregations.
 */
public class SalesRecord implements Serializable {
    private String date;
    private String region;
    private String category;
    private String product;
    private int quantity;
    private double unitPrice;
    private double revenue;

    public SalesRecord() {
    }

    public SalesRecord(String date, String region, String category, String product,
                        int quantity, double unitPrice) {
        this.date = date;
        this.region = region;
        this.category = category;
        this.product = product;
        this.quantity = quantity;
        this.unitPrice = unitPrice;
        this.revenue = quantity * unitPrice;
    }

    public String getDate() { return date; }
    public void setDate(String date) { this.date = date; }

    public String getRegion() { return region; }
    public void setRegion(String region) { this.region = region; }

    public String getCategory() { return category; }
    public void setCategory(String category) { this.category = category; }

    public String getProduct() { return product; }
    public void setProduct(String product) { this.product = product; }

    public int getQuantity() { return quantity; }
    public void setQuantity(int quantity) { this.quantity = quantity; }

    public double getUnitPrice() { return unitPrice; }
    public void setUnitPrice(double unitPrice) { this.unitPrice = unitPrice; }

    public double getRevenue() { return revenue; }
    public void setRevenue(double revenue) { this.revenue = revenue; }
}
