package com.training.gitops.build;

/**
 * === TOPIC: Image Versioning (automation half) ===
 * <p>
 * Computes the NEXT semantic version from the current one and a "bump type" -
 * this is exactly what tools like semantic-release / conventional-commits
 * automation do in a real pipeline by parsing commit messages for
 * "fix:" (patch), "feat:" (minor), or "BREAKING CHANGE" (major). Here the bump
 * type is read from the VERSION_BUMP env var so learners can drive it directly
 * without needing a real git history of conventional commits.
 */
public class SemanticVersionCalculator {

    public enum BumpType { MAJOR, MINOR, PATCH }

    public static BumpType bumpTypeFromEnv() {
        String raw = System.getenv("VERSION_BUMP");
        if (raw == null || raw.isBlank()) return BumpType.PATCH;
        try {
            return BumpType.valueOf(raw.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            System.out.println("  [version] Unrecognized VERSION_BUMP='" + raw + "', defaulting to PATCH");
            return BumpType.PATCH;
        }
    }

    public static String next(String currentVersion, BumpType bumpType) {
        String[] parts = currentVersion.trim().split("\\.");
        int major = Integer.parseInt(parts[0]);
        int minor = Integer.parseInt(parts[1]);
        int patch = Integer.parseInt(parts[2]);

        switch (bumpType) {
            case MAJOR -> { major++; minor = 0; patch = 0; }
            case MINOR -> { minor++; patch = 0; }
            case PATCH -> patch++;
        }
        return major + "." + minor + "." + patch;
    }
}
