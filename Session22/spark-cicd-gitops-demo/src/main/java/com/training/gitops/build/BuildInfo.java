package com.training.gitops.build;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

/**
 * === TOPIC: Image Versioning ===
 * <p>
 * THE PROBLEM this class demonstrates:
 * "It works on my machine" / "which build is actually running in Prod?" are
 * classic symptoms of an artifact with no traceable identity. If a container
 * image cannot be traced back to an exact version, an exact commit, and an
 * exact build time, incident response and rollback both become guesswork.
 * <p>
 * THE SOLUTION this class demonstrates:
 * Every artifact this pipeline produces carries three pieces of identity,
 * captured at build/run time with zero extra Maven plugins (so this never
 * fails to build offline or on a fresh machine):
 *   1. SEMANTIC VERSION      - computed by {@link SemanticVersionCalculator}
 *      and persisted in .registry/version.txt (see ArtifactRegistry).
 *   2. GIT COMMIT SHA        - read live via `git rev-parse --short HEAD`
 *      (falls back gracefully to "no-git" if git isn't installed or this
 *      isn't a git repository - keeps the demo unbreakable in a fresh clone).
 *   3. BUILD TIMESTAMP       - captured the moment CI runs, in UTC/ISO-8601.
 * <p>
 * In a real pipeline these three values become: the container image tag
 * (semver), an OCI label (org.opencontainers.image.revision = git sha), and
 * an OCI label (org.opencontainers.image.created = timestamp) - see
 * docker/Dockerfile's LABEL instructions, which accept these as build-args.
 */
public class BuildInfo {

    public final String version;
    public final String gitCommitSha;
    public final String buildTimestampUtc;

    private BuildInfo(String version, String gitCommitSha, String buildTimestampUtc) {
        this.version = version;
        this.gitCommitSha = gitCommitSha;
        this.buildTimestampUtc = buildTimestampUtc;
    }

    public static BuildInfo capture(String version) {
        return new BuildInfo(version, readGitShaOrFallback(), DateTimeFormatter.ISO_INSTANT.format(Instant.now()));
    }

    private static String readGitShaOrFallback() {
        // Env var wins first - this is exactly how a CI runner (GitHub Actions
        // sets GITHUB_SHA, Azure DevOps sets BUILD_SOURCEVERSION) supplies the
        // commit SHA without needing git installed inside a minimal build agent.
        String fromEnv = firstNonBlank(System.getenv("GIT_COMMIT_SHA"), System.getenv("GITHUB_SHA"),
                System.getenv("BUILD_SOURCEVERSION"));
        if (fromEnv != null) {
            return fromEnv.length() > 7 ? fromEnv.substring(0, 7) : fromEnv;
        }
        // Local dev fallback: ask git directly. Safe no-op if git/.git are absent.
        try {
            Process process = new ProcessBuilder("git", "rev-parse", "--short", "HEAD")
                    .redirectErrorStream(true)
                    .start();
            String output;
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                output = reader.readLine();
            }
            int exit = process.waitFor();
            if (exit == 0 && output != null && !output.isBlank()) {
                return output.trim();
            }
        } catch (Exception ignored) {
            // git not installed / not a repo -> fall through to "no-git"
        }
        return "no-git";
    }

    private static String firstNonBlank(String... values) {
        for (String v : values) {
            if (v != null && !v.isBlank()) return v;
        }
        return null;
    }

    public String asImageTag(String repoName) {
        return repoName + ":" + version;
    }

    @Override
    public String toString() {
        return "BuildInfo{version=" + version + ", gitCommitSha=" + gitCommitSha
                + ", buildTimestampUtc=" + buildTimestampUtc + "}";
    }
}
