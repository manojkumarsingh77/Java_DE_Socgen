package com.retailbank.dr.dr;

import java.time.Instant;
import java.util.List;

/**
 * Final, immutable output of a failure drill: everything an SRE/architect needs
 * to sign off (or not) on whether the platform meets its DR SLA.
 */
public record DrDrillReport(
        String primaryRegion,
        String secondaryRegion,
        Instant failureInjectedAt,
        Instant failoverDetectedAt,
        Instant failoverCompletedAt,

        long rpoTargetSeconds,
        double rpoAchievedSeconds,
        boolean rpoMet,

        long rtoTargetSeconds,
        double rtoAchievedSeconds,
        boolean rtoMet,

        long unreplicatedRecordCount,
        List<Integer> unreplicatedBatchIds,

        long lastBackupBatchId,
        String lastBackupPath,

        boolean overallPassed,
        List<String> recommendations
) {
}
