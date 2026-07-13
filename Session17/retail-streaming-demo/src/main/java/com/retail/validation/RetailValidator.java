package com.retail.validation;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.*;

public class RetailValidator {

 /**
  * Valid Retail Orders
  */
 public static Dataset<Row> valid(
         Dataset<Row> df) {

  return df.filter(

          col("orderId").isNotNull()
                  .and(length(trim(col("orderId"))).gt(0))

                  .and(col("customerId").isNotNull())
                  .and(length(trim(col("customerId"))).gt(0))

                  .and(col("productId").isNotNull())
                  .and(length(trim(col("productId"))).gt(0))

                  .and(col("amount").gt(0))
  );
 }

 /**
  * Invalid Retail Orders
  * Useful for DLQ Processing
  */
 public static Dataset<Row> invalid(
         Dataset<Row> df) {

  return df.filter(

          col("orderId").isNull()
                  .or(length(trim(col("orderId"))).equalTo(0))

                  .or(col("customerId").isNull())
                  .or(length(trim(col("customerId"))).equalTo(0))

                  .or(col("productId").isNull())
                  .or(length(trim(col("productId"))).equalTo(0))

                  .or(col("amount").leq(0))
  );
 }
}