package com.retailbank.dr.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.retailbank.dr.dr.DrDrillReport;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public final class ReportWriter {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .enable(SerializationFeature.INDENT_OUTPUT)
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private ReportWriter() {}

    public static Path writeJson(DrDrillReport report, Path outputDir, String fileName) {
        try {
            Files.createDirectories(outputDir);
            Path target = outputDir.resolve(fileName);
            MAPPER.writeValue(target.toFile(), report);
            return target;
        } catch (IOException e) {
            throw new RuntimeException("Failed to write DR drill report", e);
        }
    }

    public static void printConsoleSummary(DrDrillReport r) {
        System.out.println();
        System.out.println("================ CROSS-REGION FAILOVER DRILL REPORT ================");
        System.out.printf("Primary region             : %s%n", r.primaryRegion());
        System.out.printf("Secondary region            : %s%n", r.secondaryRegion());
        System.out.printf("Failure injected at         : %s%n", r.failureInjectedAt());
        System.out.printf("Failure detected at         : %s%n", r.failoverDetectedAt());
        System.out.printf("Failover completed at       : %s%n", r.failoverCompletedAt());
        System.out.println("-----------------------------------------------------------------");
        System.out.printf("RPO target / achieved       : %ds / %.1fs   -> %s%n",
                r.rpoTargetSeconds(), r.rpoAchievedSeconds(), r.rpoMet() ? "MET" : "BREACHED");
        System.out.printf("RTO target / achieved       : %ds / %.1fs   -> %s%n",
                r.rtoTargetSeconds(), r.rtoAchievedSeconds(), r.rtoMet() ? "MET" : "BREACHED");
        System.out.printf("Unreplicated records lost   : %d (batches: %s)%n",
                r.unreplicatedRecordCount(), r.unreplicatedBatchIds());
        System.out.printf("Last confirmed backup batch : %d (%s)%n", r.lastBackupBatchId(), r.lastBackupPath());
        System.out.println("-----------------------------------------------------------------");
        System.out.printf("OVERALL DRILL RESULT        : %s%n", r.overallPassed() ? "PASS" : "FAIL");
        System.out.println("Recommendations:");
        r.recommendations().forEach(rec -> System.out.println("  - " + rec));
        System.out.println("===================================================================");
        System.out.println();
    }
}
