package com.fraud.app;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

import static org.apache.spark.sql.functions.*;

/**
 * Fraud App Release Management - Case Study Service.
 *
 * Architecture (intentionally realistic, not a toy):
 *   1. On startup, Spark runs a ONE-TIME batch aggregation over historical
 *      transactions to build a merchant-category / region risk profile.
 *      This is the classic "offline batch computes a lookup table that
 *      feeds a low-latency serving layer" pattern used in real fraud systems.
 *   2. Spark is then stopped (no need to hold cluster resources for a
 *      lightweight serving process) and a plain embedded HTTP server
 *      (JDK's com.sun.net.httpserver, no extra framework needed) serves
 *      scoring requests using the in-memory lookup + ScoringEngine rules.
 *
 * This always-on HTTP service is what makes Blue/Green and Canary release
 * demonstrable: it can sit behind Nginx, answer /version so you can see
 * which release/slot handled a request, and answer /health for the
 * container HEALTHCHECK and load-balancer checks.
 *
 * Endpoints:
 *   GET  /health   -> liveness probe
 *   GET  /version  -> build/deploy metadata (version, git commit, env, slot)
 *   POST /score    -> {"transaction_id","account_id","amount",
 *                       "merchant_category","region_code","device_trust_score"}
 */
public class FraudScoringService {

    public static void main(String[] args) throws IOException {

        String dataPath = System.getenv().getOrDefault("DATA_PATH", "data");
        int port = Integer.parseInt(System.getenv().getOrDefault("PORT", "8080"));

        VersionInfo versionInfo = new VersionInfo();
        System.out.println("========================================================");
        System.out.println(versionInfo.banner());
        System.out.println("========================================================");

        Map<String, Double> merchantRiskLookup = buildMerchantRiskLookup(dataPath);
        ScoringEngine engine = new ScoringEngine(merchantRiskLookup);

        System.out.println("Merchant risk profile loaded (" + merchantRiskLookup.size() + " category/region keys).");

        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.setExecutor(Executors.newFixedThreadPool(4));

        server.createContext("/health", new HealthHandler());
        server.createContext("/version", new VersionHandler(versionInfo));
        server.createContext("/score", new ScoreHandler(engine, versionInfo));

        server.start();
        System.out.println("Fraud Scoring Service listening on port " + port);
        System.out.println("Try: curl http://localhost:" + port + "/version");
    }

    /**
     * Spark batch step: reads historical transactions and computes, per
     * (merchant_category, region_code), a risk contribution of up to 30
     * points scaled from the historical fraud rate. Runs once at boot.
     */
    private static Map<String, Double> buildMerchantRiskLookup(String dataPath) {
        SparkSession spark = SparkSession.builder()
                .appName("FraudMerchantRiskBootstrap")
                .master(System.getenv().getOrDefault("SPARK_MASTER", "local[*]"))
                .config("spark.sql.shuffle.partitions", "4")
                .config("spark.ui.showConsoleProgress", "false")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        Map<String, Double> lookup = new HashMap<>();
        try {
            Dataset<Row> historical = spark.read()
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .csv(dataPath + "/historical_transactions.csv");

            Dataset<Row> riskByMerchant = historical
                    .groupBy("merchant_category", "region_code")
                    .agg(avg("is_fraud_flag").alias("fraud_rate"))
                    .withColumn("risk_points", round(least(lit(30.0), col("fraud_rate").multiply(30.0)), 2));

            List<Row> rows = riskByMerchant.collectAsList();
            for (Row r : rows) {
                String key = ScoringEngine.merchantKey(r.getAs("merchant_category"), r.getAs("region_code"));
                double points = ((Number) r.getAs("risk_points")).doubleValue();
                lookup.put(key, points);
            }
        } finally {
            spark.stop();
        }
        return lookup;
    }

    // ---------------------------------------------------------------
    // HTTP handlers
    // ---------------------------------------------------------------

    static class HealthHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendJson(exchange, 405, new JSONObject().put("error", "method not allowed"));
                return;
            }
            sendJson(exchange, 200, new JSONObject().put("status", "UP"));
        }
    }

    static class VersionHandler implements HttpHandler {
        private final VersionInfo versionInfo;

        VersionHandler(VersionInfo versionInfo) {
            this.versionInfo = versionInfo;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendJson(exchange, 405, new JSONObject().put("error", "method not allowed"));
                return;
            }
            sendJson(exchange, 200, versionInfo.toJson());
        }
    }

    static class ScoreHandler implements HttpHandler {
        private final ScoringEngine engine;
        private final VersionInfo versionInfo;

        ScoreHandler(ScoringEngine engine, VersionInfo versionInfo) {
            this.engine = engine;
            this.versionInfo = versionInfo;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendJson(exchange, 405, new JSONObject().put("error", "method not allowed, use POST"));
                return;
            }
            try {
                String body = readBody(exchange);
                JSONObject req = new JSONObject(body);

                String transactionId = req.optString("transaction_id", "UNKNOWN");
                double amount = req.getDouble("amount");
                String merchantCategory = req.getString("merchant_category");
                String regionCode = req.getString("region_code");
                double deviceTrustScore = req.getDouble("device_trust_score");

                double score = engine.computeRiskScore(amount, merchantCategory, regionCode, deviceTrustScore);
                String band = engine.riskBand(score);
                boolean suspected = engine.isSuspectedFraud(score);

                JSONObject response = new JSONObject();
                response.put("transaction_id", transactionId);
                response.put("risk_score", score);
                response.put("risk_band", band);
                response.put("suspected_fraud", suspected);
                response.put("served_by", versionInfo.toJson());

                sendJson(exchange, 200, response);
            } catch (Exception e) {
                sendJson(exchange, 400, new JSONObject()
                        .put("error", "invalid request")
                        .put("message", e.getMessage()));
            }
        }
    }

    private static String readBody(HttpExchange exchange) throws IOException {
        try (InputStream is = exchange.getRequestBody()) {
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static void sendJson(HttpExchange exchange, int status, JSONObject json) throws IOException {
        byte[] bytes = json.toString().getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }
}
