package com.training.gitops.security;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * === TOPIC: ACR Security Scanning ===
 * <p>
 * THE PROBLEM this class demonstrates:
 * Pushing an image straight to a registry with no vulnerability gate means a
 * container with a known-exploitable CVE can reach Prod. Azure Container
 * Registry integrates with Microsoft Defender for Cloud (and pipelines
 * commonly add a Trivy/Grype scan step) specifically to stop that image
 * BEFORE it is pushed or promoted.
 * <p>
 * THE SOLUTION this class demonstrates:
 * A scan-result gate with a clear, auditable policy:
 *   - ANY Critical finding             -> BLOCK the push (CI fails)
 *   - More than MAX_HIGH_VULNS (env, default 5) High findings -> BLOCK
 *   - Otherwise                         -> PASS (Medium/Low are logged as warnings)
 * <p>
 * This reads a local JSON fixture (config/vulnerability-findings.json by
 * default) so the whole class works with zero network access and zero extra
 * dependencies (a tiny hand-rolled parser - no Jackson/Gson needed for this
 * deliberately simple fixture format). scripts/inject-critical-vuln.* swaps in
 * a fixture WITH a critical CVE so learners can watch the gate trip live.
 * <p>
 * For a real scanner, see scripts/real-trivy-scan.sh (optional, requires
 * Docker + Trivy installed) which produces equivalent JSON from a real image.
 */
public class SecurityScanner {

    private static final Pattern SEVERITY_PATTERN =
            Pattern.compile("\"severity\"\\s*:\\s*\"(CRITICAL|HIGH|MEDIUM|LOW)\"");
    private static final Pattern ID_PATTERN = Pattern.compile("\"id\"\\s*:\\s*\"([^\"]+)\"");
    private static final Pattern PACKAGE_PATTERN = Pattern.compile("\"package\"\\s*:\\s*\"([^\"]+)\"");

    public ScanResult scan(String reportPath) {
        int maxHighAllowed = envInt("MAX_HIGH_VULNS", 5);

        Path path = Path.of(reportPath);
        if (!Files.exists(path)) {
            return new ScanResult(false, 0, 0, 0, List.of(),
                    "Scan report not found at " + reportPath + " - failing closed (no scan = no push)");
        }

        String json;
        try {
            json = Files.readString(path);
        } catch (Exception e) {
            return new ScanResult(false, 0, 0, 0, List.of(),
                    "Could not read scan report: " + e.getMessage());
        }

        List<String> findings = new ArrayList<>();
        int critical = 0, high = 0, medium = 0;

        String[] entries = json.split("\\{");
        for (String entry : entries) {
            Matcher sevMatcher = SEVERITY_PATTERN.matcher(entry);
            if (!sevMatcher.find()) continue;
            String severity = sevMatcher.group(1);

            String id = extractOr(ID_PATTERN, entry, "UNKNOWN-ID");
            String pkg = extractOr(PACKAGE_PATTERN, entry, "unknown-package");
            findings.add(severity + "  " + id + "  (" + pkg + ")");

            switch (severity) {
                case "CRITICAL" -> critical++;
                case "HIGH" -> high++;
                case "MEDIUM" -> medium++;
                default -> { /* LOW - logged only, never blocks */ }
            }
        }

        if (critical > 0) {
            return new ScanResult(false, critical, high, medium, findings,
                    critical + " CRITICAL vulnerability(ies) found - policy requires ZERO critical findings");
        }
        if (high > maxHighAllowed) {
            return new ScanResult(false, critical, high, medium, findings,
                    high + " HIGH vulnerabilities found, exceeds MAX_HIGH_VULNS=" + maxHighAllowed);
        }
        return new ScanResult(true, critical, high, medium, findings,
                "No blocking vulnerabilities found (policy: 0 critical, <=" + maxHighAllowed + " high)");
    }

    private static String extractOr(Pattern pattern, String text, String fallback) {
        Matcher m = pattern.matcher(text);
        return m.find() ? m.group(1) : fallback;
    }

    private static int envInt(String key, int def) {
        String v = System.getenv(key);
        if (v == null || v.isBlank()) return def;
        try { return Integer.parseInt(v.trim()); } catch (NumberFormatException e) { return def; }
    }
}
