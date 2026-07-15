package com.retailbank.pipeline;

import com.retailbank.config.AppConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import static org.apache.spark.sql.functions.*;

/**
 * The one piece of business logic in the pipeline. Deliberately identical in
 * dev, test and prod - see {@link com.retailbank.config.AppConfig.PromotionRules}:
 * only infrastructure (storage, secrets, scale) is allowed to vary per
 * environment, never the business rule itself. That separation is the whole
 * point of the Multi-Environment Strategy module.
 */
public final class PromotionScoringEngine {

    private PromotionScoringEngine() {
    }

    public static Dataset<Row> score(Dataset<Row> transactions, AppConfig.PromotionRules rules) {

        WindowSpec byAccount = Window.partitionBy(col("accountId"));

        Dataset<Row> withAccountAggregates = transactions
                .withColumn("accountTotalSpend", sum(col("transactionAmount")).over(byAccount))
                .withColumn("accountTxnCount", count(col("transactionId")).over(byAccount));

        Dataset<Row> withEligibilityFlags = withAccountAggregates
                .withColumn("isHighValueAccount",
                        col("accountTotalSpend").geq(lit(rules.highValueThreshold())))
                .withColumn("isLoyaltyBonusEligible",
                        col("loyaltyTier").isin(rules.loyaltyBonusTiers().toArray()))
                .withColumn("isRiskEligible",
                        col("riskScore").leq(lit(rules.riskScoreMaxEligible())));

        Dataset<Row> scored = withEligibilityFlags
                .withColumn("promotionScore",
                        (when(col("isHighValueAccount"), lit(40)).otherwise(lit(0)))
                                .plus(when(col("isLoyaltyBonusEligible"), lit(30)).otherwise(lit(0)))
                                .plus(when(col("isRiskEligible"), lit(20)).otherwise(lit(0)))
                                .plus(when(col("previousPromotionResponse"), lit(10)).otherwise(lit(0))))
                .withColumn("promotionEligible", col("promotionScore").geq(lit(50)));

        return scored.select(
                col("transactionId"),
                col("customerId"),
                col("accountId"),
                col("region"),
                col("customerSegment"),
                col("loyaltyTier"),
                col("accountTotalSpend"),
                col("accountTxnCount"),
                col("riskScore"),
                col("promotionScore"),
                col("promotionEligible")
        );
    }

    public static Dataset<Row> summarizeByRegionAndSegment(Dataset<Row> scored) {
        return scored.groupBy(col("region"), col("customerSegment"))
                .agg(
                        functions.count(lit(1)).alias("customerCount"),
                        functions.round(functions.avg(col("promotionScore")), 2).alias("avgPromotionScore"),
                        functions.sum(when(col("promotionEligible"), lit(1)).otherwise(lit(0))).alias("eligibleCount")
                )
                .orderBy(col("region"), col("customerSegment"));
    }
}
