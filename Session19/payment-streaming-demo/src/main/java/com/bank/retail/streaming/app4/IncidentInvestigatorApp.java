package com.bank.retail.streaming.app4;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;

import static org.apache.spark.sql.functions.*;

/**
 * ============================================================================
 *  APP 4 - INCIDENT INVESTIGATOR  (SRE root-cause analysis)
 * ============================================================================
 * BUSINESS PROBLEM THIS SOLVES:
 * App3 has been running, processing real-looking traffic, and some of it has
 * SLA breaches / failures / fraud blocks mixed in. This app plays the role
 * of an SRE who has just been paged ("payments are slow / failing") and
 * needs to go from "something's wrong" to "here is specifically what, where,
 * and which transactions" using nothing but the data already captured in
 * the Delta Lake table - exactly how a real incident investigation starts.
 *
 * >>> THE METHOD THAT SOLVES THIS PROBLEM IS: investigateIncident() <<<
 *
 * Run this AFTER App3 has been running for at least 30-60 seconds with
 * App2 producing orders, so there is real data in the Delta table to query.
 *
 * !!! REQUIRES the SAME VM OPTIONS as App3 (this is also a Spark job) !!!
 * ============================================================================
 */
public class IncidentInvestigatorApp {

    private static final String BASE_DIR = System.getProperty("user.dir") + File.separator + "spark-demo-data";
    private static final String DELTA_PATH = BASE_DIR + File.separator + "delta" + File.separator + "processed_payments";

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("RetailBankingIncidentInvestigator")
                .master("local[*]")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        investigateIncident(spark);

        spark.stop();
    }

    /**
     * >>> SOLUTION METHOD <<<
     * Runs a sequence of SQL/DataFrame queries against the Delta table that
     * mirror exactly how an SRE would narrow down a "payments are slow"
     * incident: overall health -> breakdown by dimension -> worst offending
     * individual transactions -> an automated root-cause hypothesis.
     */
    private static void investigateIncident(SparkSession spark) {

        // This is a BATCH read (spark.read, not spark.readStream) of
        // whatever has already landed in the Delta table - investigations
        // happen AFTER the fact, against durable, already-committed data,
        // which is exactly what makes Delta Lake (vs. relying on transient
        // Kafka offsets or in-memory metrics) valuable here.
        Dataset<Row> payments = spark.read().format("delta").load(DELTA_PATH);
        payments.cache();

        long total = payments.count();
        System.out.println("\n================ INCIDENT INVESTIGATION REPORT ================");
        System.out.println("Total processed payments in Delta table: " + total);

        if (total == 0) {
            System.out.println("No data found yet. Let App2 + App3 run for a bit longer, then re-run this app.");
            return;
        }

        // ---- STEP 1: Overall health - status breakdown ----
        System.out.println("\n--- STEP 1: Payment status breakdown ---");
        payments.groupBy("paymentStatus")
                .agg(count("*").as("count"),
                        round(avg("processingLatencyMs"), 0).as("avgLatencyMs"))
                .orderBy(desc("count"))
                .show(false);

        // ---- STEP 2: Where are the SLA breaches concentrated? ----
        // This is the key "narrow down the blast radius" step: group SLA
        // breaches by merchant category AND channel to see if the slowness
        // is spread evenly (suggests a systemic/global issue) or concentrated
        // in one category (suggests one specific downstream dependency).
        System.out.println("\n--- STEP 2: SLA breaches by merchant category ---");
        payments.filter(col("slaBreach").equalTo(true))
                .groupBy("merchantCategory")
                .agg(count("*").as("slaBreachCount"),
                        round(avg("processingLatencyMs"), 0).as("avgLatencyMs"),
                        round(max("processingLatencyMs"), 0).as("maxLatencyMs"))
                .orderBy(desc("slaBreachCount"))
                .show(false);

        System.out.println("--- STEP 2b: SLA breaches by channel ---");
        payments.filter(col("slaBreach").equalTo(true))
                .groupBy("channel")
                .agg(count("*").as("slaBreachCount"))
                .orderBy(desc("slaBreachCount"))
                .show(false);

        // ---- STEP 3: Drill into the worst individual transactions ----
        // An SRE doesn't stop at aggregates - they pull the specific
        // correlationIds of the worst offenders to trace them through logs.
        System.out.println("\n--- STEP 3: Top 10 slowest individual transactions (for log tracing) ---");
        payments.orderBy(desc("processingLatencyMs"))
                .select("correlationId", "orderId", "merchantCategory", "channel",
                        "amount", "processingLatencyMs", "paymentStatus")
                .limit(10)
                .show(false);
        System.out.println("TIP: copy any correlationId above and grep your App3 console output for it - "
                + "every log line for that one transaction (fraud check, gateway call, SLA breach) "
                + "is tagged with correlationId=<that value>.");

        // ---- STEP 4: Fraud blocks worth a look ----
        System.out.println("\n--- STEP 4: Fraud-blocked transactions ---");
        payments.filter(col("paymentStatus").equalTo("FRAUD_BLOCKED"))
                .select("correlationId", "orderId", "customerId", "amount", "fraudReason")
                .show(10, false);

        // ---- STEP 5: Automated root-cause hypothesis ----
        // This automates the FINAL step of triage: comparing average latency
        // ACROSS merchant categories and calling out whichever one is the
        // clear outlier - turning Step 2's table into a plain-English
        // statement, the way a runbook or auto-generated incident summary would.
        System.out.println("\n--- STEP 5: Automated root-cause hypothesis ---");
        Row worstCategory = payments.groupBy("merchantCategory")
                .agg(avg("processingLatencyMs").as("avgLatencyMs"))
                .orderBy(desc("avgLatencyMs"))
                .first();

        String category = worstCategory.getAs("merchantCategory");
        double avgLatency = ((Number) worstCategory.getAs("avgLatencyMs")).doubleValue();

        System.out.printf(
                "HYPOTHESIS: merchant category '%s' has the highest average processing latency "
                        + "(%.0f ms). This points to a slow downstream dependency specific to '%s' "
                        + "payments (e.g. the payment gateway integration for that category), "
                        + "rather than a global Kafka/Spark capacity problem - because other "
                        + "categories are not equally affected.%n",
                category, avgLatency, category);

        System.out.println("==================================================================\n");

        payments.unpersist();
    }
}
