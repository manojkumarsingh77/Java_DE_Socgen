package com.training.gitops.registry;

import com.training.gitops.build.BuildInfo;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * === TOPIC: Pipeline Automation + Dev -> Stage -> Prod Promotion ===
 * <p>
 * THE PROBLEM this class demonstrates:
 * Without a registry as the single source of truth, teams fall back to
 * REBUILDING the artifact for every environment ("rebuild for stage",
 * "rebuild for prod"). That breaks the core GitOps promise - "what you tested
 * in Stage is byte-for-byte what runs in Prod" - because two builds of the
 * "same" code can differ (dependency drift, different build agent, a flaky
 * test that passed once).
 * <p>
 * THE SOLUTION this class demonstrates:
 * "Build once, promote everywhere." This class simulates Azure Container
 * Registry locally as a directory (.registry/) containing:
 *   - one manifest.json per version, recording its BuildInfo + scan result
 *   - an "environment tags" file recording which version each environment
 *     (dev/stage/prod) currently points to - promotion RETAGS an existing,
 *     already-scanned artifact; it never rebuilds it.
 *   - promotion-history.json - an append-only audit log (the GitOps trail).
 * <p>
 * In real infrastructure this maps directly onto:
 *   push(version)                 -> `az acr build` / `docker push` to ACR
 *   promote(version, env)         -> `az acr import` (retag within the same
 *                                     registry) or updating a GitOps manifest
 *                                     repo (Flux/Argo CD) that a target
 *                                     environment watches.
 */
public class ArtifactRegistry {

    private final Path registryDir;

    public ArtifactRegistry(String registryDirPath) {
        this.registryDir = Path.of(registryDirPath);
        try {
            Files.createDirectories(registryDir);
        } catch (IOException e) {
            throw new RuntimeException("Could not initialize local registry at " + registryDir, e);
        }
    }

    public String readOrInitVersion() {
        Path versionFile = registryDir.resolve("version.txt");
        try {
            if (Files.exists(versionFile)) {
                return Files.readString(versionFile).trim();
            }
            Files.writeString(versionFile, "1.0.0");
            return "1.0.0";
        } catch (IOException e) {
            throw new RuntimeException("Could not read/init version.txt", e);
        }
    }

    public void persistVersion(String newVersion) {
        try {
            Files.writeString(registryDir.resolve("version.txt"), newVersion,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            throw new RuntimeException("Could not persist version.txt", e);
        }
    }

    /** Simulates `az acr build` / `docker push` - the artifact must already have PASSED its scan. */
    public void push(BuildInfo info, boolean scanPassed, int criticalCount, int highCount) {
        String manifest = "{\n" +
                "  \"version\": \"" + info.version + "\",\n" +
                "  \"gitCommitSha\": \"" + info.gitCommitSha + "\",\n" +
                "  \"buildTimestampUtc\": \"" + info.buildTimestampUtc + "\",\n" +
                "  \"pushedAt\": \"" + Instant.now() + "\",\n" +
                "  \"scanPassed\": " + scanPassed + ",\n" +
                "  \"criticalVulnerabilities\": " + criticalCount + ",\n" +
                "  \"highVulnerabilities\": " + highCount + "\n" +
                "}\n";
        writeFile(registryDir.resolve("manifest-" + info.version + ".json"), manifest);
        System.out.println("  [registry] pushed manifest for version " + info.version
                + " (== `docker push myacr.azurecr.io/inventory-analytics:" + info.version + "` in real ACR)");
    }

    /** Retags an EXISTING, already-pushed version into an environment - never rebuilds. */
    public void promote(String version, String environment) {
        writeFile(registryDir.resolve("env-" + environment + ".txt"), version);
        appendPromotionHistory(version, environment);
        System.out.println("  [registry] " + environment.toUpperCase() + " now points to version " + version
                + " (== `az acr import` retag, or a GitOps manifest-repo commit in real infra)");
    }

    public String currentVersionInEnvironment(String environment) {
        Path f = registryDir.resolve("env-" + environment + ".txt");
        try {
            return Files.exists(f) ? Files.readString(f).trim() : null;
        } catch (IOException e) {
            return null;
        }
    }

    public boolean manifestExists(String version) {
        return Files.exists(registryDir.resolve("manifest-" + version + ".json"));
    }

    private void appendPromotionHistory(String version, String environment) {
        Path historyFile = registryDir.resolve("promotion-history.json");
        String line = "{\"version\":\"" + version + "\",\"environment\":\"" + environment
                + "\",\"promotedAt\":\"" + Instant.now() + "\"}\n";
        try {
            Files.writeString(historyFile, line, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new RuntimeException("Could not append promotion history", e);
        }
    }

    public List<String> readPromotionHistory() {
        Path historyFile = registryDir.resolve("promotion-history.json");
        List<String> lines = new ArrayList<>();
        try {
            if (Files.exists(historyFile)) {
                lines.addAll(Files.readAllLines(historyFile));
            }
        } catch (IOException ignored) {
        }
        return lines;
    }

    private void writeFile(Path path, String content) {
        try {
            Files.writeString(path, content, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            throw new RuntimeException("Could not write " + path, e);
        }
    }

    public Path getRegistryDir() {
        return registryDir;
    }
}
