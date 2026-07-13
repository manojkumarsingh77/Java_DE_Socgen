package com.retail.processor;
import com.retail.offset.OffsetManager; import com.retail.writer.RetailOrderWriter;
import org.apache.spark.sql.*;
public class RetailForeachBatchProcessor{
 public static void process(Dataset<Row> batch,long batchId){
  for(Row r: batch.collectAsList()){
   RetailOrderWriter.upsert(r);
   OffsetManager.commit(r.getAs("partition"), r.getAs("offset"));
  }
 }
}
