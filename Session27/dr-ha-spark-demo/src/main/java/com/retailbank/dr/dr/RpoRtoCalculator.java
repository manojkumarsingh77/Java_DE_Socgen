package com.retailbank.dr.dr;

import com.retailbank.dr.replication.ReplicationRecord;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Pure functions that turn the replication ledger + drill timestamps into the
 * two numbers every DR conversation ultimately reduces to:
 *
 *   RPO (Recovery Point Objective) — how much data can we afford to lose,
 *     measured as the time window between the last durably-replicated
 *     commit and the moment of failure. Achieved RPO = actual data-loss window.
 *
 *   RTO (Recovery Time Objective) — how long can the business be down,
 *     measured as the time between failure detection and the secondary
 *     serving traffic again as the new primary.
 */
public final class RpoRtoCalculator {

    private RpoRtoCalculator() {}

    /**
     * Achieved RPO = time between the last batch that was CONFIRMED replicated to
     * secondary before the failure, and the failure instant itself. Any primary
     * batches committed after that last confirmed replication, but before failure,
     * represent the data-loss window (and are enumerated separately below).
     */
    public static double computeAchievedRpoSeconds(List<ReplicationRecord> ledger, Instant failureInstant) {
        Instant lastReplicated = ledger.stream()
                .filter(ReplicationRecord::replicated)
                .filter(r -> !r.secondaryCommitTimestamp().isAfter(failureInstant))
                .map(ReplicationRecord::primaryCommitTimestamp)
                .max(Comparator.naturalOrder())
                .orElse(failureInstant); // nothing ever replicated => worst case, RPO = full run

        return Duration.between(lastReplicated, failureInstant).toMillis() / 1000.0;
    }

    /**
     * Batches that committed on PRIMARY before failure but were never confirmed
     * replicated to SECONDARY — these transactions are the actual, enumerable
     * data loss a failover incurs. In a real ledger this list drives downstream
     * reconciliation/replay or customer-impact notification.
     */
    public static List<ReplicationRecord> unreplicatedBatchesAtFailure(
            List<ReplicationRecord> ledger, Instant failureInstant) {
        List<ReplicationRecord> lost = new ArrayList<>();
        for (ReplicationRecord r : ledger) {
            boolean committedBeforeFailure = !r.primaryCommitTimestamp().isAfter(failureInstant);
            boolean confirmedReplicated = r.replicated() && !r.secondaryCommitTimestamp().isAfter(failureInstant);
            if (committedBeforeFailure && !confirmedReplicated) {
                lost.add(r);
            }
        }
        return lost;
    }

    /**
     * Achieved RTO = detection latency + promotion/repoint latency, i.e. the full
     * clock time customers/systems experience as "the ledger is unavailable".
     */
    public static double computeAchievedRtoSeconds(Instant failureInjectedAt, Instant failoverCompletedAt) {
        return Duration.between(failureInjectedAt, failoverCompletedAt).toMillis() / 1000.0;
    }
}
