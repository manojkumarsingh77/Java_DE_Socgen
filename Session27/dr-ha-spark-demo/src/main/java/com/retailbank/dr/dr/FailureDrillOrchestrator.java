package com.retailbank.dr.dr;

import com.retailbank.dr.config.AppConfig;
import com.retailbank.dr.replication.ReplicationEngine;
import com.retailbank.dr.replication.ReplicationRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Orchestrates a controlled failure drill against the ReplicationEngine and
 * produces a signed-off DrDrillReport comparing achieved RPO/RTO against the
 * business-mandated SLA targets in AppConfig.
 *
 * This is the "Failure drills" topic made executable: instead of a tabletop
 * exercise, the drill runs against real (simulated) Delta commits with real
 * wall-clock timestamps, so the RPO/RTO numbers are measured, not guessed.
 */
public final class FailureDrillOrchestrator {

    private static final Logger log = LoggerFactory.getLogger(FailureDrillOrchestrator.class);

    private final AppConfig config;
    private final ReplicationEngine engine;

    public FailureDrillOrchestrator(AppConfig config, ReplicationEngine engine) {
        this.config = config;
        this.engine = engine;
    }

    /**
     * Step 1 of the drill: inject the failure. Called by Main once the
     * configured batch threshold is reached.
     */
    public Instant injectPrimaryFailure() {
        Instant t0 = Instant.now();
        engine.simulatePrimaryOutage();
        return t0;
    }

    /**
     * Steps 2-4 of the drill, executed sequentially and timed individually so the
     * report shows *where* the RTO budget is actually spent (detection vs promotion) —
     * this is the actionable part of a real DR runbook, not just a pass/fail number.
     */
    public DrDrillReport executeFailoverAndReport(Instant failureInjectedAt) {
        List<String> recommendations = new ArrayList<>();

        // --- Phase A: Failure detection (health probe / synthetic transaction timeout)
        log.info(">> Failover Phase A: detecting failure (health probes)...");
        sleepQuietly(config.failoverDetectionMillis());
        Instant detectedAt = Instant.now();

        // --- Phase B: Promote secondary + repoint routing (DNS/Traffic Manager/Front Door
        //     weight shift in production; here modeled as a fixed operational delay)
        log.info(">> Failover Phase B: promoting SECONDARY [{}] to active + repointing routing...",
                config.secondaryRegionName());
        sleepQuietly(config.failoverPromotionMillis());
        Instant completedAt = Instant.now();
        engine.recoverPrimary(); // conceptually: "primary" role now belongs to former secondary

        // --- Compute RPO from the replication ledger as of the failure instant
        List<ReplicationRecord> ledger = engine.ledgerSnapshot();
        double achievedRpo = RpoRtoCalculator.computeAchievedRpoSeconds(ledger, failureInjectedAt);
        List<ReplicationRecord> lost = RpoRtoCalculator.unreplicatedBatchesAtFailure(ledger, failureInjectedAt);
        long lostRecords = lost.stream().mapToLong(ReplicationRecord::recordCount).sum();
        List<Integer> lostBatchIds = lost.stream().map(ReplicationRecord::batchId).toList();

        // --- Compute RTO from the drill's own timestamps
        double achievedRto = RpoRtoCalculator.computeAchievedRtoSeconds(failureInjectedAt, completedAt);

        boolean rpoMet = achievedRpo <= config.rpoTargetSeconds();
        boolean rtoMet = achievedRto <= config.rtoTargetSeconds();

        if (!rpoMet) {
            recommendations.add(String.format(
                    "RPO breached (%.1fs > %ds target): reduce baseReplicationLagMillis, " +
                    "or move from async to semi-sync replication (ack after secondary write) " +
                    "for high-value transaction types.", achievedRpo, config.rpoTargetSeconds()));
        }
        if (!lostBatchIds.isEmpty()) {
            recommendations.add(String.format(
                    "%d records across %d batches were never confirmed on secondary at failure time — " +
                    "these require replay-from-source or reconciliation against upstream event log (Event Hub) " +
                    "before secondary is trusted as sole source of truth.", lostRecords, lostBatchIds.size()));
        }
        if (!rtoMet) {
            recommendations.add(String.format(
                    "RTO breached (%.1fs > %ds target): automate health-probe detection " +
                    "(reduce failoverDetectionMillis) and pre-warm secondary compute/AKS node pool " +
                    "so promotion doesn't wait on cold-start capacity.", achievedRto, config.rtoTargetSeconds()));
        }
        if (recommendations.isEmpty()) {
            recommendations.add("All SLA targets met. Re-run drill quarterly and after any topology change.");
        }

        boolean overallPassed = rpoMet && rtoMet;

        DrDrillReport report = new DrDrillReport(
                config.primaryRegionName(),
                config.secondaryRegionName(),
                failureInjectedAt,
                detectedAt,
                completedAt,
                config.rpoTargetSeconds(),
                achievedRpo,
                rpoMet,
                config.rtoTargetSeconds(),
                achievedRto,
                rtoMet,
                lostRecords,
                lostBatchIds,
                lastBackupBatchId(),
                config.backupRootPath().toString(),
                overallPassed,
                recommendations
        );

        log.info(">> Failover complete. RPO={}s (target {}s, met={}) | RTO={}s (target {}s, met={})",
                achievedRpo, config.rpoTargetSeconds(), rpoMet, achievedRto, config.rtoTargetSeconds(), rtoMet);

        return report;
    }

    private long lastBackupBatchId() {
        // Derived from config cadence; in production this reads the backup manifest.
        int last = 0;
        for (ReplicationRecord r : engine.ledgerSnapshot()) {
            if (r.batchId() % config.backupEveryNBatches() == 0) {
                last = Math.max(last, r.batchId());
            }
        }
        return last;
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
