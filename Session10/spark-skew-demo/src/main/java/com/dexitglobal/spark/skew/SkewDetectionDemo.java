package com.dexitglobal.spark.skew;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;

import java.util.*;

import static org.apache.spark.sql.functions.*;

public class SkewDetectionDemo {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("Skew Demo")
                .master("local[*]")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.skewJoin.enabled", "true")
                .getOrCreate();

        Dataset<Row> orders =
                DataGenerator.generateOrders(spark);

        Dataset<Row> customers =
                DataGenerator.generateCustomers(spark);

        System.out.println("===== SKEW ANALYSIS =====");

        orders.groupBy("customer_id")
                .count()
                .orderBy(desc("count"))
                .show(10,false);

        long start = System.currentTimeMillis();

        Dataset<Row> joined =
                orders.join(customers,
                        "customer_id");

        joined.count();

        long end = System.currentTimeMillis();

        System.out.println(
                "Baseline Join Time = "
                        + (end-start)
                        + " ms");

        Dataset<Row> salted =
                JoinOptimizer.saltedJoin(
                        orders,
                        customers
                );

        start = System.currentTimeMillis();

        salted.count();

        end = System.currentTimeMillis();

        System.out.println(
                "Salted Join Time = "
                        + (end-start)
                        + " ms");

        spark.stop();
    }
}