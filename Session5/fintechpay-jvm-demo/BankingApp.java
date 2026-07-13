package com.fintechpay;

import com.fintechpay.config.JvmDiagnostics;
import com.fintechpay.model.Transaction;
import com.fintechpay.service.FraudScoringService;
import com.fintechpay.service.TransactionService;

import java.util.List;

/**
 * ╔══════════════════════════════════════════════════════════════════════════╗
 * ║  BankingApp — FintechPay JVM Container Demo                             ║
 * ╠══════════════════════════════════════════════════════════════════════════╣
 * ║                                                                          ║
 * ║  BUSINESS SCENARIO:                                                     ║
 * ║  ─────────────────                                                       ║
 * ║  FintechPay's core banking engine runs on AKS (Azure Kubernetes).       ║
 * ║  Each pod: 2 vCPU limit, 2 GB RAM limit, Java 17.                      ║
 * ║                                                                          ║
 * ║  PROBLEMS WE SOLVE IN THIS DEMO:                                        ║
 * ║  ──────────────────────────────                                          ║
 * ║  1. Pods OOMKilled every night during batch processing.                 ║
 * ║     Root cause: JVM heap set to 1.8 GB in 2 GB container.              ║
 * ║     Fix: -XX:MaxRAMPercentage=75.0  (concept 2)                        ║
 * ║                                                                          ║
 * ║  2. Fraud scoring SLA breaches during morning peak (9-11 AM).          ║
 * ║     Root cause: 48-thread pool on 2-CPU container → CFS throttle.      ║
 * ║     Fix: Thread pool = availableProcessors() + ActiveProcessorCount=2  ║
 * ║          (concepts 1 + 3)                                               ║
 * ║                                                                          ║
 * ║  3. JVM using wrong heap defaults on first deploy.                      ║
 * ║     Root cause: Pre-Java 10 style config without UseContainerSupport.  ║
 * ║     Fix: Verify UseContainerSupport=true + print JVM diagnostics       ║
 * ║          (concept 1)                                                     ║
 * ║                                                                          ║
 * ║  HOW TO RUN IN INTELLIJ:                                                ║
 * ║  ────────────────────────                                                ║
 * ║  1. Open as Maven project (File → Open → select pom.xml)               ║
 * ║  2. Run → Edit Configurations → BankingApp                             ║
 * ║  3. In "VM options" add:                                                ║
 * ║       -XX:+UseContainerSupport                                          ║
 * ║       -XX:MaxRAMPercentage=75.0                                         ║
 * ║       -XX:InitialRAMPercentage=50.0                                     ║
 * ║       -XX:ActiveProcessorCount=2                                        ║
 * ║       -XX:+UseG1GC                                                      ║
 * ║       -XX:MaxGCPauseMillis=200                                          ║
 * ║       -XX:InitiatingHeapOccupancyPercent=35                             ║
 * ║       -verbose:gc                                                        ║
 * ║  4. Run BankingApp                                                      ║
 * ║                                                                          ║
 * ║  TO SEE THE PROBLEM (before fix):                                       ║
 * ║  Remove the VM options above and observe JVM uses host RAM defaults.    ║
 * ║                                                                          ║
 * ╚══════════════════════════════════════════════════════════════════════════╝
 */
public class BankingApp {

    public static void main(String[] args) throws InterruptedException {

        printStartupBanner();

        // ════════════════════════════════════════════════════════════════
        // STEP 1: JVM Container Awareness Report
        //
        // CONCEPT 1 — JVM Flags in Docker:
        //   Prints what the JVM "sees" in terms of RAM and CPU.
        //   With UseContainerSupport=true (default Java 17), the JVM
        //   reads cgroup limits instead of host /proc/meminfo.
        //
        //   EXPECTED OUTPUT (IntelliJ with 2-CPU VM option):
        //     Available processors: 2
        //     JVM total view of RAM: ~2048 MB (if MaxRAMPercentage is set)
        //
        //   WITHOUT the VM options:
        //     Available processors: your machine's actual CPU count (e.g., 16)
        //     JVM total view of RAM: your machine's RAM ÷ 4 (JVM default)
        // ════════════════════════════════════════════════════════════════
        System.out.println("\n  ► STEP 1: JVM + Container Diagnostics");
        JvmDiagnostics.printReport();

        // ════════════════════════════════════════════════════════════════
        // STEP 2: Normal Transaction Processing (Low Heap Pressure)
        //
        // CONCEPT 2 — Heap vs Container Limits:
        //   Small batch (100 transactions) = minimal heap usage.
        //   Short-lived objects. Eden space absorbs all of this.
        //   This represents normal intraday payment processing.
        // ════════════════════════════════════════════════════════════════
        System.out.println("\n  ► STEP 2: Normal Intraday Transaction Processing");

        TransactionService txService = new TransactionService();
        FraudScoringService fraudService = new FraudScoringService();

        TransactionService.printMemorySnapshot("Before batch generation");

        // Generate 100 transactions (small batch — normal load)
        List<Transaction> smallBatch = txService.generateTransactionBatch(100);
        TransactionService.printMemorySnapshot("After 100-tx batch created");

        // ════════════════════════════════════════════════════════════════
        // STEP 3: Fraud Scoring (CPU Pressure Demonstration)
        //
        // CONCEPT 3 — CPU Throttling in AKS:
        //   fraudService uses a thread pool sized to availableProcessors().
        //   With -XX:ActiveProcessorCount=2 → 2 worker threads.
        //   Each worker does ~2-3ms of CPU math.
        //   2 workers × 3ms = 6ms CPU per period → well within 200ms quota.
        //
        //   If we had 48 threads (wrong config):
        //   48 × 3ms = 144ms × simultaneous = burst → throttle.
        // ════════════════════════════════════════════════════════════════
        System.out.println("\n  ► STEP 3: Fraud Scoring with CPU-Aware Thread Pool");

        JvmDiagnostics.measureAllocationPressure(
            "Fraud scoring (100 tx)",
            () -> {
                List<Transaction> scored = fraudService.scoreBatch(smallBatch);
                System.out.println("   [FraudService] Scored " + scored.size() + " transactions.");

                // Print a sample of results
                System.out.println("\n   Sample scored transactions:");
                scored.stream()
                    .limit(8)
                    .forEach(tx -> System.out.println("   " + tx));
            }
        );

        txService.processTransactions(smallBatch);
        TransactionService.printMemorySnapshot("After scoring + processing");

        // ════════════════════════════════════════════════════════════════
        // STEP 4: Batch Statement Generation (High Heap Pressure)
        //
        // CONCEPT 2 — Heap vs Container Limits (the CRITICAL scenario):
        //   Statement generation creates large String objects (Old Gen).
        //   In a 2 GB container with MaxRAMPercentage=90%, Xmx ≈ 1.8 GB.
        //   This leaves only 200 MB for Metaspace + threads + JIT.
        //   During batch: heap fills up, GC can't recover fast enough → OOM.
        //
        //   With MaxRAMPercentage=75%, Xmx ≈ 1.5 GB.
        //   500 MB remains for non-heap → stable even during batch runs.
        //
        //   Watch the memory snapshot jump in this step.
        // ════════════════════════════════════════════════════════════════
        System.out.println("\n  ► STEP 4: Month-End Batch Statement Generation (Heap Pressure)");

        // Generate a larger batch to simulate month-end workload
        List<Transaction> monthlyBatch = txService.generateTransactionBatch(500);
        TransactionService.printMemorySnapshot("Before statement generation");

        JvmDiagnostics.measureAllocationPressure(
            "Statement gen (500 tx)",
            () -> {
                // Score them first (realistic: statements show fraud scores)
                List<Transaction> scored = fraudService.scoreBatch(monthlyBatch);

                // Generate statement for each unique account
                scored.stream()
                    .map(Transaction::getAccountId)
                    .distinct()
                    .forEach(accountId -> {
                        List<Transaction> accountTxs = scored.stream()
                            .filter(tx -> tx.getAccountId().equals(accountId))
                            .toList();

                        String statement = txService.generateMonthlyStatement(accountId, accountTxs);
                        // Simulate sending to PDF service (in real bank: writes to ADLS Gen2)
                        System.out.printf(
                            "   [StatementGen] Account %-15s → %,d chars | %d txns%n",
                            accountId, statement.length(), accountTxs.size()
                        );
                    });
            }
        );

        TransactionService.printMemorySnapshot("After statement generation");

        // ════════════════════════════════════════════════════════════════
        // STEP 5: Simulate peak load (AKS throttle scenario)
        //
        // CONCEPT 3 — CPU Throttling in AKS:
        //   Sudden burst of 200 fraud-check transactions (morning peak).
        //   With correct thread pool (2 threads) → processed in waves.
        //   No CPU quota burst. Latency stable.
        //
        //   Contrast with WRONG config (48 threads):
        //   All 200 submitted simultaneously → 48 threads burn CPU →
        //   CFS quota exhausted → all threads frozen 100ms → spike!
        // ════════════════════════════════════════════════════════════════
        System.out.println("\n  ► STEP 5: Peak Load Burst (AKS CFS Throttle Prevention)");
        System.out.println("   Simulating 9 AM payment rush — 200 concurrent fraud checks...");

        List<Transaction> peakBatch = txService.generateTransactionBatch(200);
        long peakStart = System.currentTimeMillis();

        JvmDiagnostics.measureAllocationPressure(
            "Peak load (200 tx)",
            () -> fraudService.scoreBatch(peakBatch)
        );

        long peakEnd = System.currentTimeMillis();
        System.out.printf("   [AKS] Peak burst completed in %dms with CPU-aware thread pool.%n",
            peakEnd - peakStart);
        System.out.println("   [AKS] No CFS throttle: thread count ≤ container CPU limit.");

        // ════════════════════════════════════════════════════════════════
        // STEP 6: Final JVM state — post-workload health check
        //
        // CONCEPTS 1, 2, 3 combined:
        //   Verify JVM is healthy after the full workload.
        //   In AKS: this is what /actuator/health exposes to the liveness probe.
        //   If heap > 95%: pod marked unhealthy → AKS restarts it.
        //   Proper MaxRAMPercentage prevents this.
        // ════════════════════════════════════════════════════════════════
        System.out.println("\n  ► STEP 6: Post-Workload JVM Health Check (AKS Liveness Probe)");
        TransactionService.printMemorySnapshot("Final heap state");

        // Run GC to get a clean reading (simulates what happens after K8s idle period)
        System.out.println("   [GC] Requesting GC (simulating K8s idle → shrink heap)...");
        System.gc();
        Thread.sleep(200); // give GC a moment
        TransactionService.printMemorySnapshot("After GC suggestion");

        // ════════════════════════════════════════════════════════════════
        // STEP 7: Dockerfile + AKS YAML — the complete production config
        // ════════════════════════════════════════════════════════════════
        System.out.println("\n  ► STEP 7: Production Config Reference");
        printProductionConfig();

        fraudService.shutdown();

        System.out.println("\n  ══════════════════════════════════════════════════════");
        System.out.println("  ✓  FintechPay JVM Demo completed successfully.");
        System.out.println("  ══════════════════════════════════════════════════════\n");
    }

    // ── Startup Banner ────────────────────────────────────────────────────────
    private static void printStartupBanner() {
        System.out.println("""
            
            ╔════════════════════════════════════════════════════════════════════╗
            ║                                                                    ║
            ║    FintechPay — JVM Container Mastery Demo                        ║
            ║    Java 17 · Docker · AKS · Retail Banking                        ║
            ║                                                                    ║
            ║    CONCEPTS DEMONSTRATED:                                          ║
            ║    1. JVM Flags in Docker   → UseContainerSupport, cgroup limits  ║
            ║    2. Heap vs Container     → MaxRAMPercentage, safe headroom     ║
            ║    3. CPU Throttling in AKS → CFS scheduler, G1GC tuning         ║
            ║                                                                    ║
            ╚════════════════════════════════════════════════════════════════════╝
            """);
    }

    // ── Production Config Reference ───────────────────────────────────────────
    private static void printProductionConfig() {
        System.out.println("""
            
            ┌──────────────────────────────────────────────────────────────────┐
            │  Dockerfile                                                       │
            └──────────────────────────────────────────────────────────────────┘
            
              FROM eclipse-temurin:17-jre-alpine
              
              # CONCEPT 1: UseContainerSupport is ON by default in Java 17.
              # We only need to set the percentage-based flags.
              
              ENV JAVA_OPTS="\\
                -XX:+UseContainerSupport \\
                -XX:MaxRAMPercentage=75.0 \\
                -XX:InitialRAMPercentage=50.0 \\
                -XX:+UseG1GC \\
                -XX:MaxGCPauseMillis=200 \\
                -XX:InitiatingHeapOccupancyPercent=35 \\
                -XX:+HeapDumpOnOutOfMemoryError \\
                -XX:HeapDumpPath=/dumps/heap.hprof \\
                -Xlog:gc*:file=/logs/gc.log:time,level,tags:filecount=5,filesize=20m"
              
              COPY target/fintechpay-jvm-demo-jar-with-dependencies.jar /app/app.jar
              ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar /app/app.jar"]
            
            ┌──────────────────────────────────────────────────────────────────┐
            │  AKS Pod Spec (deployment.yaml)                                  │
            └──────────────────────────────────────────────────────────────────┘
            
              containers:
              - name: fintechpay-banking
                image: fintechpay/banking-engine:1.0
                resources:
                  requests:
                    memory: "2Gi"
                    cpu: "2"          # CONCEPT 3: request = limit → Guaranteed QoS
                  limits:
                    memory: "2Gi"     # CONCEPT 2: JVM cgroup reads this
                    cpu: "2"          # CONCEPT 3: CFS quota = 200ms per 100ms period
                env:
                - name: JAVA_OPTS
                  value: >-
                    -XX:+UseContainerSupport
                    -XX:MaxRAMPercentage=75.0
                    -XX:ActiveProcessorCount=2
                    -XX:+UseG1GC
                    -XX:MaxGCPauseMillis=200
                    -XX:InitiatingHeapOccupancyPercent=35
                livenessProbe:
                  httpGet:
                    path: /actuator/health/liveness
                    port: 8080
                  initialDelaySeconds: 30
                  periodSeconds: 10
                readinessProbe:
                  httpGet:
                    path: /actuator/health/readiness
                    port: 8080
            
            ┌──────────────────────────────────────────────────────────────────┐
            │  IntelliJ VM Options (Run Configuration)                         │
            └──────────────────────────────────────────────────────────────────┘
            
              -XX:+UseContainerSupport
              -XX:MaxRAMPercentage=75.0
              -XX:InitialRAMPercentage=50.0
              -XX:ActiveProcessorCount=2
              -XX:+UseG1GC
              -XX:MaxGCPauseMillis=200
              -XX:InitiatingHeapOccupancyPercent=35
              -verbose:gc
            """);
    }
}
