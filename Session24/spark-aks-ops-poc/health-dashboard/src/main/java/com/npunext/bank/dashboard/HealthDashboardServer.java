package com.npunext.bank.dashboard;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Minimal, dependency-free health-dashboard service for the streaming job.
 *
 * <p>Its own job in this lab is <b>not</b> the data engineering — it exists
 * so this project has a genuine, textbook, multi-replica Kubernetes
 * {@code Deployment} to run {@code kubectl set image} / {@code kubectl
 * rollout status} / {@code kubectl rollout undo} against. Spark's own
 * streaming driver is a Kubernetes-managed singleton Pod (created directly
 * by spark-submit, not a Deployment) — native rolling-update semantics
 * (maxSurge/maxUnavailable across replicas) do not apply to it. This
 * companion service is where that concept is demonstrated honestly.
 *
 * <p>It polls Spark's real, documented Monitoring REST API
 * ({@code /api/v1/applications} and {@code /api/v1/applications/{app-id}/executors})
 * on the streaming driver's Spark UI port (4040, reached via the
 * {@code pos-streaming-driver-ui} Service) and renders a small status page
 * stamped with its own {@code APP_VERSION}, so a browser refresh during a
 * rolling update visibly flips between "v1" and "v2" as new pods take over.
 */
public final class HealthDashboardServer {

    private static final HttpClient HTTP_CLIENT = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(2))
            .build();

    private static final Pattern EXECUTOR_ID_PATTERN = Pattern.compile("\"id\"\\s*:\\s*\"[^\"]*\"");
    private static final Pattern APP_ID_PATTERN = Pattern.compile("\"id\"\\s*:\\s*\"(app-[^\"]*)\"");

    public static void main(String[] args) throws IOException {
        int port = Integer.parseInt(System.getenv().getOrDefault("PORT", "8080"));
        String driverApiBase = System.getenv().getOrDefault(
                "DRIVER_UI_BASE_URL", "http://pos-streaming-driver-ui:4040");
        String appVersion = System.getenv().getOrDefault("APP_VERSION", "v1");
        String podName = System.getenv().getOrDefault("HOSTNAME", "unknown-pod");

        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/healthz", ex -> respondPlainText(ex, 200, "OK"));
        server.createContext("/readyz", ex -> respondPlainText(ex, 200, "READY"));
        server.createContext("/", ex -> handleDashboard(ex, driverApiBase, appVersion, podName));
        server.setExecutor(null);
        server.start();

        System.out.printf(
                "Health dashboard [%s] listening on port %d, polling driver at %s%n",
                appVersion, port, driverApiBase
        );
    }

    private static void handleDashboard(HttpExchange exchange, String driverApiBase,
                                         String appVersion, String podName) throws IOException {
        String appsJson = safeGet(driverApiBase + "/api/v1/applications");
        String appId = extractFirstAppId(appsJson);

        String executorCount = "unavailable";
        if (appId != null) {
            String executorsJson = safeGet(driverApiBase + "/api/v1/applications/" + appId + "/executors");
            if (executorsJson != null) {
                executorCount = String.valueOf(countMatches(EXECUTOR_ID_PATTERN, executorsJson));
            }
        }

        String html = """
                <html>
                <head><title>Streaming Job Health Dashboard</title></head>
                <body style="font-family: sans-serif; padding: 24px;">
                  <h2>Retail POS Streaming - Health Dashboard</h2>
                  <p><b>Dashboard version:</b> %s</p>
                  <p><b>Serving pod:</b> %s</p>
                  <p><b>Checked at:</b> %s</p>
                  <hr/>
                  <p><b>Spark application id:</b> %s</p>
                  <p><b>Executors reported live (via Spark Monitoring REST API):</b> %s</p>
                  <p style="color:#888">Source: %s/api/v1/applications/{app-id}/executors</p>
                </body>
                </html>
                """.formatted(
                appVersion, podName, Instant.now(),
                appId != null ? appId : "not reachable",
                executorCount, driverApiBase
        );

        respondHtml(exchange, 200, html);
    }

    private static String safeGet(String url) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(2))
                    .GET()
                    .build();
            HttpResponse<String> response = HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
            return response.statusCode() == 200 ? response.body() : null;
        } catch (Exception e) {
            return null;
        }
    }

    /** Deliberately minimal string-matching extraction — not a general JSON parser. */
    private static String extractFirstAppId(String appsJson) {
        if (appsJson == null) {
            return null;
        }
        Matcher matcher = APP_ID_PATTERN.matcher(appsJson);
        return matcher.find() ? matcher.group(1) : null;
    }

    private static int countMatches(Pattern pattern, String text) {
        Matcher matcher = pattern.matcher(text);
        int count = 0;
        while (matcher.find()) {
            count++;
        }
        return count;
    }

    private static void respondPlainText(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "text/plain; charset=utf-8");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    private static void respondHtml(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "text/html; charset=utf-8");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }
}
