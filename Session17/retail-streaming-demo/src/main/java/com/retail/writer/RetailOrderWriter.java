package com.retail.writer;
import org.apache.spark.sql.Row;
public class RetailOrderWriter{
 public static void upsert(Row row){ System.out.println("UPSERT "+row.getAs("orderId")); }
}
