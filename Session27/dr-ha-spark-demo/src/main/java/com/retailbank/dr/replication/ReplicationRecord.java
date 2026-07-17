package com.retailbank.dr.replication;

import java.time.Instant;

/**
 * One entry in the in-memory replication ledger: tracks, per micro-batch, when it
 * committed on PRIMARY, when (if ever) it was confirmed replicated to SECONDARY,
 * and how many records it contained. This ledger is the source of truth the
 * RPORTOCalculator reads to compute actual data-loss windows during a drill.
 */
public record ReplicationRecord(
        int batchId,
        long recordCount,
        Instant primaryCommitTimestamp,
        Instant secondaryCommitTimestamp,   // null until replication confirmed
        boolean replicated,
        boolean primaryAvailableAtCommit    // false if this batch was attempted during simulated outage
) {
    public ReplicationRecord withReplicated(Instant secondaryTs) {
        return new ReplicationRecord(batchId, recordCount, primaryCommitTimestamp, secondaryTs,
                true, primaryAvailableAtCommit);
    }
}
