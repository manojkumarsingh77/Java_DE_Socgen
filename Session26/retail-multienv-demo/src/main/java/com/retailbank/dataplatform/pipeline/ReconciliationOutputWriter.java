package com.retailbank.dataplatform.pipeline;

import com.retailbank.dataplatform.config.AppConfig;
import com.retailbank.dataplatform.secrets.SecretsProvider;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Egress stage: writes the reconciliation output as a Delta table to the
 * environment-configured path (local filesystem in dev, ADLS Gen2 in test/prod).
 *
 * <p>When the output path is an {@code abfss://} URI, the ADLS Gen2 storage
 * account key is resolved from {@link SecretsProvider} and injected into the
 * Hadoop configuration at runtime — it is never present in any config file or
 * environment variable checked into source control.</p>
 */
public final class ReconciliationOutputWriter {

    private static final Logger LOG = LoggerFactory.getLogger(ReconciliationOutputWriter.class);

    private final SparkSession spark;
    private final AppConfig config;
    private final SecretsProvider secretsProvider;

    public ReconciliationOutputWriter(SparkSession spark, AppConfig config, SecretsProvider secretsProvider) {
        this.spark = spark;
        this.config = config;
        this.secretsProvider = secretsProvider;
    }

    public void write(Dataset<Row> reconciliationResult) {
        String outputPath = config.data().outputPath();

        if (outputPath.startsWith("abfss://")) {
            configureAdlsAuthentication(outputPath);
        }

        LOG.info("Writing reconciliation output to '{}' (format={})",
                outputPath, config.data().outputFormat());

        reconciliationResult.write()
                .format(config.data().outputFormat())
                .mode(SaveMode.Overwrite)
                .option("mergeSchema", "true")
                .partitionBy("status")
                .save(outputPath);

        LOG.info("Write complete: {} rows written to '{}'", reconciliationResult.count(), outputPath);
    }

    /**
     * Extracts the storage account name from the abfss:// URI and injects the
     * matching account key (resolved from Key Vault / local-env) into the Hadoop
     * configuration under the exact property key the ABFS driver looks up:
     * {@code fs.azure.account.key.<account>.dfs.core.windows.net}.
     */
    private void configureAdlsAuthentication(String abfssUri) {
        String storageAccountName = extractStorageAccountName(abfssUri);
        String secretName = "adls-" + storageAccountName + "-account-key";
        String accountKey = secretsProvider.getSecret(secretName);

        String hadoopConfKey = "fs.azure.account.key." + storageAccountName + ".dfs.core.windows.net";
        spark.sparkContext().hadoopConfiguration().set(hadoopConfKey, accountKey);

        LOG.info("Configured ADLS Gen2 authentication for storage account '{}'", storageAccountName);
    }

    private String extractStorageAccountName(String abfssUri) {
        // abfss://<container>@<account>.dfs.core.windows.net/<path>
        int atIndex = abfssUri.indexOf('@');
        int dotIndex = abfssUri.indexOf(".dfs.core.windows.net");
        if (atIndex < 0 || dotIndex < 0 || dotIndex < atIndex) {
            throw new IllegalArgumentException("Malformed abfss:// URI, cannot extract storage account: " + abfssUri);
        }
        return abfssUri.substring(atIndex + 1, dotIndex);
    }
}
