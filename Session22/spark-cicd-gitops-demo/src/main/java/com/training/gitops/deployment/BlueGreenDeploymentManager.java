package com.training.gitops.deployment;

import com.training.gitops.job.InventoryAnalyticsJob;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * === TOPIC: Blue/Green Release ===
 * <p>
 * THE PROBLEM this class demonstrates:
 * A rolling in-place upgrade to Stage means, for a window of time, some
 * requests hit the old version and some hit the new one, and if the new
 * version is broken, rolling back means ANOTHER slow redeploy while users
 * are actively affected.
 * <p>
 * THE SOLUTION this class demonstrates:
 * Two identical environments, "blue" and "green". Only one is "active"
 * (receiving traffic) at any time. The new version is deployed to the
 * INACTIVE slot, fully health-checked there with zero user impact, and only
 * THEN does traffic switch atomically. Rollback is just as instant - flip the
 * pointer back. Nothing gets redeployed to roll back.
 * <p>
 * State is persisted to .registry/bluegreen-state.json so learners can inspect
 * the "current active slot" exactly like a real load balancer / Kubernetes
 * Service selector would record it.
 */
public class BlueGreenDeploymentManager {

    private final Path stateFile;

    public BlueGreenDeploymentManager(String registryDirPath) {
        this.stateFile = Path.of(registryDirPath, "bluegreen-state.txt");
    }

    public String activeSlot() {
        try {
            if (Files.exists(stateFile)) {
                return Files.readString(stateFile).trim();
            }
        } catch (IOException ignored) {
        }
        return "blue"; // default starting slot
    }

    private String inactiveSlot() {
        return activeSlot().equals("blue") ? "green" : "blue";
    }

    /**
     * Deploys {@code version} into the currently INACTIVE slot, health-checks it
     * there, and only switches traffic if healthy. Returns true if the switch
     * happened.
     */
    public boolean deployAndSwitch(String version, boolean injectFailure) {
        String target = inactiveSlot();
        String live = activeSlot();
        System.out.println("  [blue-green] live slot = " + live + " (version untouched, still serving traffic)");
        System.out.println("  [blue-green] deploying candidate version " + version + " into INACTIVE slot = " + target);

        boolean healthy = new InventoryAnalyticsJob().runSmokeTest("bluegreen-" + target, injectFailure);

        if (!healthy) {
            System.out.println("  [blue-green] health check FAILED in slot " + target
                    + " -> traffic switch ABORTED. Slot " + live + " keeps serving version unaffected.");
            return false;
        }

        System.out.println("  [blue-green] health check PASSED in slot " + target
                + " -> switching live traffic " + live + " -> " + target + " (atomic pointer flip)");
        writeActiveSlot(target);
        return true;
    }

    /** Instant rollback - flips the pointer back with no redeploy at all. */
    public void rollback() {
        String live = activeSlot();
        String previous = inactiveSlot();
        System.out.println("  [blue-green] ROLLBACK requested -> flipping traffic " + live + " -> " + previous
                + " (instant, no redeploy)");
        writeActiveSlot(previous);
    }

    private void writeActiveSlot(String slot) {
        try {
            Files.writeString(stateFile, slot, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            throw new RuntimeException("Could not persist blue/green state", e);
        }
    }
}
