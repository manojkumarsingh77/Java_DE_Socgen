package com.dexitglobal.spark.skew;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.*;

public class JoinOptimizer {

    public static Dataset<Row> saltedJoin(
            Dataset<Row> orders,
            Dataset<Row> customers) {

        Dataset<Row> saltedOrders =
                orders.withColumn(
                        "salt",
                        floor(rand().multiply(10))
                );

        Dataset<Row> saltedCustomers =
                customers.crossJoin(
                        customers.sparkSession()
                                .range(10)
                                .toDF("salt")
                );

        return saltedOrders.join(
                saltedCustomers,
                new String[]{
                        "customer_id",
                        "salt"
                }
        );
    }
}