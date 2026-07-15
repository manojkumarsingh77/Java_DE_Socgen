package com.retailbank.dataplatform;

import com.retailbank.dataplatform.config.AppConfig;
import com.retailbank.dataplatform.config.EnvironmentConfigLoader;
import com.retailbank.dataplatform.config.SparkSessionFactory;
import com.retailbank.dataplatform.data.SyntheticBankingDataGenerator;
import com.retailbank.dataplatform.pipeline.ReconciliationOutputWriter;
import com.retailbank.dataplatform.pipeline.TransactionReconciliationPipeline;
import com.retailbank.dataplatform.secrets.SecretsProvider;
import com.typesafe.config.Config;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Retail Platform Promotion Model — demo entry point.
 *
 * <p>Run with a mandatory {@code -Dretail.env=dev|test|prod} system property.
 * The SAME shaded jar is used unmodified across all three environments; only
 * this system property (and the correspondingly-loaded {@code application-<env>.conf})
 * changes at deploy time. See LAB_GUIDE.md for exact IntelliJ VM Options and
 * spark-submit commands per environment.</p>
 *
 * <p>Stages, matching the Configuration/Initialization → Synthetic Data Generation
 * → Core Transformation → Write/Egress structure required for this demo:</p>
 * <ol>
 *   <li>Configuration/Initialization — {@link EnvironmentConfigLoader}, {@link AppConfig}, {@link SecretsProvider}</li>
 *   <li>Synthetic Data Generation — {@link SyntheticBankingDataGenerator}</li>
 *   <li>Core Transformation — {@link TransactionReconciliationPipeline}</li>
 *   <li>Write/Egress — {@link ReconciliationOutputWriter}</li>
 * </ol>
 */
public final class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        long startNanos = System.nanoTime();

        // ---------- Stage 1: Configuration / Initialization ----------
        Config resolvedConfig = EnvironmentConfigLoader.load();
        AppConfig appConfig = AppConfig.from(resolvedConfig);

        SecretsProvider secretsProvider = SecretsProvider.forProvider(
                appConfig.secrets().provider(), appConfig.secrets().keyVaultUri());

        try (SparkSession spark = SparkSessionFactory.create(appConfig)) {

            LOG.info("Spark session initialized. environment='{}', master='{}', appName='{}'",
                    appConfig.environmentName(), appConfig.spark().master(), appConfig.spark().appName());

            // ---------- Stage 2: Synthetic Data Generation ----------
            SyntheticBankingDataGenerator generator = new SyntheticBankingDataGenerator(
                    spark, appConfig.data().syntheticRecordCount(), 42L);
            SyntheticBankingDataGenerator.GeneratedDataset generated = generator.generate();

            LOG.info("Generated {} card transactions and {} ledger entries",
                    generated.cardTransactions().count(), generated.ledgerEntries().count());

            // ---------- Stage 3: Core Transformation ----------
            TransactionReconciliationPipeline pipeline =
                    new TransactionReconciliationPipeline(spark, appConfig);
            Dataset<Row> reconciliationResult =
                    pipeline.reconcile(generated.cardTransactions(), generated.ledgerEntries());

            reconciliationResult.show(20, false);

            // ---------- Stage 4: Write / Egress ----------
            ReconciliationOutputWriter writer =
                    new ReconciliationOutputWriter(spark, appConfig, secretsProvider);
            writer.write(reconciliationResult);

            long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;
            LOG.info("Pipeline completed successfully for environment '{}' in {} ms",
                    appConfig.environmentName(), elapsedMs);

        } catch (Exception e) {
            LOG.error("Pipeline failed for environment '{}': {}",
                    System.getProperty("retail.env", "<unresolved>"), e.getMessage(), e);
            throw e;
        }
    }

    private Main() {
    }
}
