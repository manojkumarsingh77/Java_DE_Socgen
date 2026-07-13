package com.acme.retail.analytics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class SyntheticSalesDataGenerator {

    /**
     * Generate synthetic sales + product dimension data and write to Parquet.
     *
     * @param spark        SparkSession
     * @param basePath     base folder (e.g. "data/sales_warehouse")
     * @param numDays      number of days of data
     * @param rowsPerDay   number of fact rows per day
     * @param numProducts  cardinality of product dimension
     */
    public void generate(SparkSession spark,
                         String basePath,
                         int numDays,
                         long rowsPerDay,
                         int numProducts) {

        long totalRows = (long) numDays * rowsPerDay;

        Dataset<Row> sales = spark.range(totalRows)
                .toDF("id")
                // Deterministic order_date over numDays
                .withColumn("order_date",
                        expr("date_sub(current_date(), cast(id / " + rowsPerDay + " as int))"))
                .withColumn("event_ts",
                        expr("to_timestamp(concat(order_date, ' ', " +
                                "lpad(cast(int(rand() * 24) as string), 2, '0'), ':', " +
                                "lpad(cast(int(rand() * 60) as string), 2, '0'), ':00'))"))
                .withColumn("order_id", expr("concat('ORD-', id)"))
                .withColumn("customer_id", expr("cast(rand() * 100000 as bigint)"))
                // Introduce skew: 2% of rows hit a hot product id 9999
                .withColumn("product_id",
                        expr("case when rand() < 0.02 then 9999 else cast(rand() * " +
                                numProducts + " as bigint) end"))
                .withColumn("store_id", expr("cast(rand() * 200 as int)"))
                .withColumn("channel",
                        expr("case when rand() < 0.6 then 'ONLINE' " +
                                "when rand() < 0.85 then 'MOBILE' else 'STORE' end"))
                .withColumn("country",
                        expr("case when rand() < 0.4 then 'US' " +
                                "when rand() < 0.7 then 'IN' " +
                                "when rand() < 0.85 then 'UK' else 'DE' end"))
                .withColumn("currency",
                        expr("case when country = 'IN' then 'INR' " +
                                "when country = 'UK' then 'GBP' " +
                                "when country = 'DE' then 'EUR' else 'USD' end"))
                .withColumn("quantity", expr("cast(rand() * 5 + 1 as int)"))
                .withColumn("unit_price", expr("round(rand() * 200 + 5, 2)"))
                .withColumn("discount_pct", expr("round(rand() * 0.3, 2)"))
                .withColumn("tax_amount", expr("round(rand() * 20, 2)"))
                .withColumn("payment_method",
                        expr("case when rand() < 0.5 then 'CARD' " +
                                "when rand() < 0.8 then 'WALLET' else 'COD' end"))
                .drop("id");

        Dataset<Row> products = spark.range(numProducts)
                .toDF("product_id")
                .withColumn("category",
                        expr("case " +
                                "when product_id % 5 = 0 then 'ELECTRONICS' " +
                                "when product_id % 5 = 1 then 'FASHION' " +
                                "when product_id % 5 = 2 then 'HOME' " +
                                "when product_id % 5 = 3 then 'SPORTS' " +
                                "else 'GROCERY' end"))
                .withColumn("subcategory",
                        expr("concat(category, '_', cast(product_id % 10 as string))"))
                .withColumn("brand",
                        expr("concat('BRAND_', cast(product_id % 50 as string))"));

        // Persist as Parquet to mimic data lake batch input
        String factPath = basePath + "/fact_sales";
        String dimPath = basePath + "/dim_product";

        sales.write()
                .mode("overwrite")
                .partitionBy("order_date", "country")
                .parquet(factPath);

        products.write()
                .mode("overwrite")
                .parquet(dimPath);
    }
}