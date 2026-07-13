package com.demo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;

public class AfterSchemaEnforcement {

    public static void main(String[] args) throws Exception {

        SparkSession spark =
                SparkSession.builder()
                        .appName("AfterSchemaEnforcement")
                        .master("local[*]")
                        .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        StructType transactionSchema =
                new StructType()
                        .add(
                                "transaction_id",
                                DataTypes.StringType)
                        .add(
                                "customer_id",
                                DataTypes.StringType)
                        .add(
                                "amount",
                                DataTypes.DoubleType)
                        .add(
                                "transaction_type",
                                DataTypes.StringType);

        Dataset<Row> kafkaDf =
                spark.readStream()
                        .format("kafka")
                        .option(
                                "kafka.bootstrap.servers",
                                "localhost:9092")
                        .option(
                                "subscribe",
                                "banking-transactions")
                        .option(
                                "startingOffsets",
                                "earliest")
                        .load();

        Dataset<Row> parsedDf =
                kafkaDf.selectExpr(
                                "CAST(value AS STRING) as json")
                        .select(
                                from_json(
                                        col("json"),
                                        transactionSchema)
                                        .alias("data"))
                        .select("data.*");

        Dataset<Row> validDf =
                parsedDf.filter(
                        col("transaction_id").isNotNull()
                                .and(
                                        col("amount")
                                                .isNotNull()));

        Dataset<Row> invalidDf =
                parsedDf.filter(
                        col("transaction_id").isNull()
                                .or(
                                        col("amount")
                                                .isNull()));

        validDf.writeStream()
                .format("console")
                .queryName("VALID_RECORDS")
                .outputMode("append")
                .option("truncate", false)
                .option(
                        "checkpointLocation",
                        "/tmp/valid-checkpoint")
                .start();

        invalidDf.writeStream()
                .format("console")
                .queryName("INVALID_RECORDS")
                .outputMode("append")
                .option("truncate", false)
                .option(
                        "checkpointLocation",
                        "/tmp/invalid-checkpoint")
                .start();

        spark.streams().awaitAnyTermination();
    }
}