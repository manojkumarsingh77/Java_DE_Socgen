package com.dexitglobal.spark.skew;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class DataGenerator {

    public static Dataset<Row> generateOrders(
            SparkSession spark) {

        List<Row> rows = new ArrayList<>();

        for (int i = 0; i < 500000; i++) {

            int customerId =
                    (i < 300000)
                            ? 999999
                            : i;

            rows.add(RowFactory.create(
                    customerId,
                    i * 100.0
            ));
        }

        StructType schema =
                new StructType()
                        .add("customer_id",
                                DataTypes.IntegerType)
                        .add("amount",
                                DataTypes.DoubleType);

        return spark.createDataFrame(rows, schema);
    }

    public static Dataset<Row> generateCustomers(
            SparkSession spark) {

        return spark.range(1000000)
                .toDF("customer_id");
    }
}