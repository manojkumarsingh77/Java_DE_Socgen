package com.bank.retail.streaming.app5;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.apache.spark.sql.functions.*;

/**
 * ============================================================================
 *  APP 5 - LIVE DASHBOARD SUMMARY
 * ============================================================================
 * BUSINESS PROBLEM THIS SOLVES:
 * Reproduces, in a console window, what a real Grafana/Datadog Golden
 * Signals dashboard would show an SRE watching a payments system live:
 * traffic, error rate, latency percentiles and SLA-breach count, refreshed
 * every few seconds, for a recent rolling time window.
 *
 * >>> THE METHOD THAT SOLVES THIS PROBLEM IS: renderDashboard() <<<
 * It is called in a loop from main(), and is what you'd point a real
 * dashboarding tool's "refresh" button at if this were wired to one.
 *
 * Run this WHILE App2 + App3 are running, so the numbers visibly move.
 *
 * !!! REQUIRES the SAME VM OPTIONS as App3 (this is also a Spark job) !!!
 * ============================================================================
 */
public class DashboardSummaryApp {

    private static final String BASE_DIR = System.getProperty("user.dir") + File.separator + "spark-demo-data";
    private static final String DELTA_PATH = BASE_DIR + File.separator + "delta" + File.separator + "processed_payments";

    // How far back (in milliseconds) the "live" window looks - this mimics
    // a dashboard's "last 2 minutes" view rather than an all-time total,
    // so the numbers actually reflect CURRENT system behaviour.
    private static final long WINDOW_MS = 2 * 60 * 1000L;

    private static final long REFRESH_INTERVAL_MS = 10_000L;

    public static void main(String[] args) throws InterruptedException {

        SparkSession spark = SparkSession.builder()
                .appName("RetailBankingDashboardSummary")
                .master("local[*]")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        System.out.println("Live dashboard starting. Refreshing every "
                + (REFRESH_INTERVAL_MS / 1000) + "s. Press the IntelliJ Stop button to exit.\n");

        // A simple infinite refresh loop. We deliberately do NOT use Spark
        // Structured Streaming to read the Delta table here - a plain
        // "batch read every N seconds" loop is simpler to reason about for
        // a dashboard use case, and is exactly how many real lightweight
        // monitoring scripts are built (poll, render, sleep, repeat).
        while (true) {
            if (Files.exists(Paths.get(DELTA_PATH))) {
                renderDashboard(spark);
            } else {
                System.out.println("Waiting for Delta table to be created by App3 ("
                        + DELTA_PATH + " not found yet)...");
            }
            Thread.sleep(REFRESH_INTERVAL_MS);
        }
    }

    /**
     * >>> SOLUTION METHOD <<<
     * Reads the Delta table fresh, filters to the recent rolling window, and
     * prints a compact Golden Signals summary - the console equivalent of
     * one dashboard "refresh".
     */
    private static void renderDashboard(SparkSession spark) {

        Dataset<Row> all = spark.read().format("delta").load(DELTA_PATH);

        long windowStart = System.currentTimeMillis() - WINDOW_MS;
        Dataset<Row> recent = all.filter(col("processedTimestamp").geq(windowStart));
        recent.cache();

        long traffic = recent.count();

        System.out.println("------------------------------------------------------------------");
        System.out.println(" RETAIL BANKING PAYMENTS - LIVE GOLDEN SIGNALS DASHBOARD");
        System.out.println(" Window: last " + (WINDOW_MS / 1000) + "s   |   Refreshed at: " + java.time.LocalTime.now());
        System.out.println("------------------------------------------------------------------");

        if (traffic == 0) {
            System.out.println(" TRAFFIC : 0 payments in this window - waiting for new orders...");
            System.out.println("------------------------------------------------------------------\n");
            recent.unpersist();
            return;
        }

        Row stats = recent.agg(
                count("*").as("traffic"),
                sum(when(col("paymentStatus").equalTo("SUCCESS"), 1).otherwise(0)).as("success"),
                sum(when(col("paymentStatus").equalTo("FAILED"), 1).otherwise(0)).as("failed"),
                sum(when(col("paymentStatus").equalTo("FRAUD_BLOCKED"), 1).otherwise(0)).as("fraudBlocked"),
                round(avg("processingLatencyMs"), 0).as("avgLatencyMs"),
                expr("percentile_approx(processingLatencyMs, 0.95)").as("p95LatencyMs"),
                sum(when(col("slaBreach").equalTo(true), 1).otherwise(0)).as("slaBreaches")
        ).first();

        long success = stats.getAs("success");
        long failed = stats.getAs("failed");
        long fraudBlocked = stats.getAs("fraudBlocked");
        double avgLatency = ((Number) stats.getAs("avgLatencyMs")).doubleValue();
        long p95Latency = ((Number) stats.getAs("p95LatencyMs")).longValue();
        long slaBreaches = stats.getAs("slaBreaches");
        double errorRatePct = ((failed + fraudBlocked) * 100.0) / traffic;

        System.out.printf(" TRAFFIC        : %d payments%n", traffic);
        System.out.printf(" SUCCESS        : %d%n", success);
        System.out.printf(" FAILED         : %d%n", failed);
        System.out.printf(" FRAUD_BLOCKED  : %d%n", fraudBlocked);
        System.out.printf(" ERROR RATE     : %.1f%%%n", errorRatePct);
        System.out.printf(" AVG LATENCY    : %.0f ms%n", avgLatency);
        System.out.printf(" P95 LATENCY    : %d ms%n", p95Latency);
        System.out.printf(" SLA BREACHES   : %d%n", slaBreaches);

        // A simple, readable status verdict - the kind of "is it red or
        // green right now" summary line that sits at the top of a real
        // dashboard panel.
        String verdict = errorRatePct > 15 || slaBreaches > traffic * 0.2
                ? "DEGRADED - investigate with App4 (IncidentInvestigatorApp)"
                : "HEALTHY";
        System.out.println(" STATUS         : " + verdict);
        System.out.println("------------------------------------------------------------------\n");

        recent.unpersist();
    }
}
