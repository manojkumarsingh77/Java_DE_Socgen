package com.retailbank.pipeline;

import com.retailbank.config.AppConfig;
import com.retailbank.config.ConfigLoader;
import com.retailbank.data.SyntheticBankingDataGenerator;
import com.retailbank.secrets.SecretsProvider;
import com.retailbank.secrets.SecretsProviderFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Scanner;

/**
 * Retail Platform Promotion Model.
 */
public final class PromotionPipeline {

    private static final Logger log = LoggerFactory.getLogger(PromotionPipeline.class);

    public static void main(String[] args) {

        // ---- Stage 1: Configuration / Initialization -----------------------
        AppConfig config = ConfigLoader.load();

        System.out.println("\n[APP-INFO] === Retail Platform Promotion Model | environment='" + config.environmentTag() + "' ===");
        System.out.println("[APP-INFO] Storage format=" + config.storage().format() + " input=" + config.storage().inputPath() + " output=" + config.storage().outputPath());

        SecretsProvider secretsProvider = SecretsProviderFactory.create(config.secrets());
        String downstreamApiKey = secretsProvider.getSecret(config.secrets().downstreamApiKeySecretName());
        System.out.println("[APP-INFO] Resolved downstream API credential via provider='" + secretsProvider.providerName() + "' (value masked: " + maskSecret(downstreamApiKey) + ")");

        SparkSession.Builder builder = SparkSession.builder()
                .appName(config.appName() + "-" + config.environmentTag())
                .config("spark.sql.shuffle.partitions", config.spark().shufflePartitions());

        if (config.spark().master() != null) {
            builder.master(config.spark().master());
        }
        if ("delta".equals(config.storage().format())) {
            builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog");
        }

        try (SparkSession spark = builder.getOrCreate()) {

            // Wrap the data processing execution to catch and print hidden failures
            try {
                // ---- Stage 2: Synthetic Data Generation -------------------------
                int rowCount = switch (config.environmentTag()) {
                    case "dev" -> 5_000;
                    case "test" -> 250_000;
                    default -> 5_000_000; // prod
                };
                int numAccounts = Math.max(500, rowCount / 20);

                System.out.println("[APP-INFO] Generating synthetic data...");
                Dataset<Row> transactions = SyntheticBankingDataGenerator.generate(spark, rowCount, numAccounts);
                System.out.println("[APP-INFO] Generated " + rowCount + " synthetic transactions across " + numAccounts + " accounts");

                // ---- Stage 3: Core Transformation / Processing Pipeline ---------
                System.out.println("[APP-INFO] Executing transformation engines...");
                Dataset<Row> scored = PromotionScoringEngine.score(transactions, config.promotion());
                Dataset<Row> summary = PromotionScoringEngine.summarizeByRegionAndSegment(scored);

                scored.cache();
                long eligibleCount = scored.filter("promotionEligible = true").count();
                System.out.println("[APP-INFO] " + eligibleCount + " of " + rowCount + " transactions flagged promotion-eligible");

                System.out.println("\n[APP-INFO] Region/segment summary:");
                summary.show(50, false);

                // ---- Stage 4: Write / Egress Stage -------------------------------
                writeOutput(scored, config);
                System.out.println("[APP-INFO] === Pipeline complete for environment '" + config.environmentTag() + "' ===\n");

            } catch (Exception e) {
                System.err.println("\n!!! CRITICAL PIPELINE EXCEPTION CAUGHT !!!");
                e.printStackTrace(System.err);
                System.err.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n");
            }

            // ---- Stage 5: Hold Process for Spark UI Inspection ---------------
            if ("dev".equals(config.environmentTag())) {
                System.out.println("=================================================================================");
                System.out.println("  SPARK UI IS ACTIVE. View live execution logs, DAGs, and memory usage at:");
                System.out.println("  --> http://localhost:4040 <--");
                System.out.println("=================================================================================");
                System.out.print("Press [ENTER] in this console window to stop the session and exit... ");
                System.out.flush();

                try (Scanner scanner = new Scanner(System.in)) {
                    scanner.nextLine();
                }
                System.out.println("\n[APP-INFO] Termination key received. Commencing Spark clean shutdown sequence...");
            }
        }
    }

    private static void writeOutput(Dataset<Row> scored, AppConfig config) {
        scored.write()
                .mode("overwrite")
                .format(config.storage().format())
                .save(config.storage().outputPath());
        System.out.println("[APP-INFO] Wrote scored dataset to " + config.storage().outputPath() + " as " + config.storage().format());
    }

    private static String maskSecret(String secret) {
        if (secret == null || secret.length() < 4) {
            return "****";
        }
        return "*".repeat(secret.length() - 4) + secret.substring(secret.length() - 4);
    }
}