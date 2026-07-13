package com.retail.schema;
import org.apache.spark.sql.types.*;
public class RetailSchemas {
 public static StructType orderSchema(){
  return new StructType().add("orderId",DataTypes.StringType).add("customerId",DataTypes.StringType)
   .add("productId",DataTypes.StringType).add("amount",DataTypes.DoubleType).add("eventTime",DataTypes.StringType);
 }
}
