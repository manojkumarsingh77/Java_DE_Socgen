package com.bank.recommendation.engine;

import com.bank.recommendation.model.CustomerProfile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

/**
 * SLOW RECOMMENDATION ENGINE  – The "BEFORE" State
 * ═══════════════════════════════════════════════════════════════════════════
 *
 * BUSINESS REQUIREMENT:
 *   When a customer opens the HDFC/Axis/ICICI mobile app or logs into
 *   NetBanking, the home screen must show personalised product tiles
 *   within 200 ms (SLA).  Currently the pre-processor takes 800 ms P99.
 *
 * This class deliberately embeds ALL the bad patterns that cause 800ms P99:
 *
 *   BOTTLENECK-1 ── SYNCHRONISED SCORING (CPU BOTTLENECK)
 *     All three scoring methods hold a global ReentrantLock, serialising
 *     work that is embarrassingly parallel.  CPU profiler shows 70% of
 *     time spent in BLOCKED/WAITING state.
 *
 *   BOTTLENECK-2 ── DISK I/O ON HOT PATH (IO BOTTLENECK)
 *     Rules are loaded from a temp file on EVERY request instead of
 *     being cached.  Adds 300–500 ms on spinning disks, 80–120 ms SSD.
 *     CPU profiler shows high time in FileInputStream.read()
 *
 *   BOTTLENECK-3 ── OBJECT ALLOCATION STORM (GC PRESSURE)
 *     Creates new ArrayList, HashMap, and String objects for every
 *     customer on every call.  At 5000 req/s this generates ~200 MB/s
 *     of short-lived garbage → frequent Minor GC → stop-the-world pauses
 *     of 20–60 ms that appear as P99 spikes.
 *
 *   BOTTLENECK-4 ── SEQUENTIAL STAGE PIPELINE (NO PARALLELISM)
 *     risk → eligibility → propensity → recommendation run one after
 *     another in a single thread even though they are independent.
 *
 *   BOTTLENECK-5 ── BUSY-WAIT SIMULATION (CPU WASTE)
 *     Thread.sleep() simulates synchronous external calls (rule engine
 *     REST API, ML model HTTP endpoint) that block the calling thread.
 */
public class SlowRecommendationEngine {

    private static final Logger log = LoggerFactory.getLogger(SlowRecommendationEngine.class);

    // ── BOTTLENECK-1: Single global lock serialises ALL scoring ───────────────
    // CPU profiler will show threads piling up on this lock.
    // Thread dump will show dozens of threads in BLOCKED state on `scoringLock`.
    private final ReentrantLock scoringLock = new ReentrantLock();

    // Rules file path – recreated each call (BOTTLENECK-2)
    private Path rulesFile;

    public SlowRecommendationEngine() {
        // Write rules to a temp file to simulate the disk-read anti-pattern
        try {
            rulesFile = Files.createTempFile("bank-rules-", ".csv");
            try (PrintWriter pw = new PrintWriter(rulesFile.toFile())) {
                pw.println("rule_id,product,min_credit,min_income,max_dti,min_tenure");
                pw.println("R01,PERSONAL_LOAN,650,300000,0.50,6");
                pw.println("R02,CREDIT_CARD_PREMIUM,720,600000,0.40,12");
                pw.println("R03,CREDIT_CARD_BASIC,580,200000,0.60,3");
                pw.println("R04,MUTUAL_FUND_SIP,600,400000,0.45,12");
                pw.println("R05,FIXED_DEPOSIT,0,50000,1.00,0");
                pw.println("R06,HOME_LOAN,700,500000,0.45,24");
                pw.println("R07,CAR_LOAN,650,350000,0.50,6");
                pw.println("R08,WEALTH_MANAGEMENT,750,2000000,0.30,24");
            }
            log.info("Rules file created at: {}", rulesFile);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create rules file", e);
        }
    }

    /**
     * MAIN ENTRY POINT – processes one customer profile.
     * Measures wall-clock latency end-to-end.
     */
    public long process(CustomerProfile profile) {
        long start = System.nanoTime();
        profile.setProcessingStartNanos(start);

        try {
            // STAGE 1: Risk scoring – holds global lock (BOTTLENECK-1)
            computeRiskScore(profile);

            // STAGE 2: Load rules from disk every time (BOTTLENECK-2)
            List<Map<String, String>> rules = loadRulesFromDisk();

            // STAGE 3: Eligibility check – still under lock (BOTTLENECK-1)
            computeEligibility(profile, rules);

            // STAGE 4: Propensity scoring (BOTTLENECK-1)
            computePropensityScore(profile);

            // STAGE 5: Build recommendations (OBJECT ALLOCATION STORM – BOTTLENECK-3)
            buildRecommendations(profile);

        } catch (Exception e) {
            log.error("Processing failed for customer {}", profile.getCustomerId(), e);
        }

        long end = System.nanoTime();
        profile.setProcessingEndNanos(end);
        return (end - start) / 1_000_000; // return ms
    }

    // ─────────────────────────────────────────────────────────────────────────
    // BOTTLENECK-1: Global lock + simulated CPU-bound work
    // ─────────────────────────────────────────────────────────────────────────

    private void computeRiskScore(CustomerProfile p) {
        scoringLock.lock();   // ← ALL threads queue here
        try {
            // Simulates ML model scoring: matrix multiply, feature transforms
            // In reality this CPU work takes 5–15 ms; we simulate 20 ms
            busyWaitMs(20);

            double dti           = p.getOutstandingDebt() / Math.max(1, p.getAnnualIncome());
            double creditFactor  = (900.0 - p.getCreditScore()) / 600.0;
            double tenureFactor  = Math.max(0, 1.0 - p.getTenureMonths() / 120.0);
            double riskScore     = (dti * 0.5) + (creditFactor * 0.3) + (tenureFactor * 0.2);
            p.setRiskScore(Math.min(1.0, Math.max(0.0, riskScore)));

        } finally {
            scoringLock.unlock();
        }
    }

    private void computePropensityScore(CustomerProfile p) {
        scoringLock.lock();
        try {
            busyWaitMs(15);

            // Simplified propensity: combination of balance, tenure, segment
            double balanceFactor  = Math.min(1.0, p.getCurrentBalance() / 500_000);
            double tenureFactor   = Math.min(1.0, p.getTenureMonths() / 60.0);
            double segmentBoost   = switch (p.getSegment()) {
                case "WEALTH"  -> 0.3;
                case "PREMIER" -> 0.2;
                default        -> 0.0;
            };
            double propensity = (balanceFactor * 0.4) + (tenureFactor * 0.3) + segmentBoost + 0.1;
            p.setPropensityScore(Math.min(1.0, propensity));

        } finally {
            scoringLock.unlock();
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // BOTTLENECK-2: Disk I/O on every hot-path call
    // ─────────────────────────────────────────────────────────────────────────

    private List<Map<String, String>> loadRulesFromDisk() throws IOException {
        // ⚠️  Reading from disk on EVERY request.
        // CPU profiler: hot stack frame = FileInputStream.read()
        // IO bottleneck: 300-500 MB/s disk throughput exhausted at scale

        List<Map<String, String>> rules = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(rulesFile.toFile()))) {
            String headerLine = br.readLine();
            if (headerLine == null) return rules;
            String[] headers = headerLine.split(",");

            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                // BOTTLENECK-3: new HashMap per rule per request
                Map<String, String> rule = new HashMap<>();
                for (int i = 0; i < headers.length; i++) {
                    rule.put(headers[i].trim(), values[i].trim());
                }
                rules.add(rule);
            }
        }
        return rules;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // BOTTLENECK-3: New collections per call
    // ─────────────────────────────────────────────────────────────────────────

    private void computeEligibility(CustomerProfile p, List<Map<String, String>> rules) {
        scoringLock.lock();
        try {
            busyWaitMs(10);

            double dti = p.getOutstandingDebt() / Math.max(1, p.getAnnualIncome());

            for (Map<String, String> rule : rules) {
                String product   = rule.get("product");
                int    minCredit = Integer.parseInt(rule.get("min_credit"));
                double minIncome = Double.parseDouble(rule.get("min_income"));
                double maxDti    = Double.parseDouble(rule.get("max_dti"));
                int    minTenure = Integer.parseInt(rule.get("min_tenure"));

                boolean eligible = p.getCreditScore()  >= minCredit
                                && p.getAnnualIncome() >= minIncome
                                && dti                 <= maxDti
                                && p.getTenureMonths() >= minTenure;

                if (eligible) {
                    switch (product) {
                        case "PERSONAL_LOAN"    -> p.setEligibleForPersonalLoan(true);
                        case "CREDIT_CARD_PREMIUM",
                             "CREDIT_CARD_BASIC" -> p.setEligibleForCreditCard(true);
                        case "MUTUAL_FUND_SIP"   -> p.setEligibleForMutualFund(true);
                        case "FIXED_DEPOSIT"     -> p.setEligibleForFixedDeposit(true);
                    }
                }
            }
        } finally {
            scoringLock.unlock();
        }
    }

    private void buildRecommendations(CustomerProfile p) {
        // BOTTLENECK-3: New ArrayList + Strings on every call
        List<String> recommendations = new ArrayList<>();

        if (p.isEligibleForPersonalLoan() && p.getRiskScore() < 0.6) {
            recommendations.add("PERSONAL_LOAN: Pre-approved up to ₹" +
                    String.format("%,.0f", p.getAnnualIncome() * 0.3));
        }
        if (p.isEligibleForCreditCard()) {
            String tier = p.getCreditScore() >= 720 ? "PREMIUM" : "BASIC";
            recommendations.add("CREDIT_CARD_" + tier + ": Instant approval");
        }
        if (p.isEligibleForMutualFund() && p.getPropensityScore() > 0.5) {
            recommendations.add("MUTUAL_FUND_SIP: Starting ₹500/month");
        }
        if (p.isEligibleForFixedDeposit()) {
            recommendations.add("FIXED_DEPOSIT: 7.1% p.a. for 1 year");
        }
        if ("WEALTH".equals(p.getSegment()) || "PREMIER".equals(p.getSegment())) {
            recommendations.add("WEALTH_MANAGEMENT: Dedicated RM assigned");
        }

        p.setRecommendedProducts(recommendations);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Utility: busy-wait to simulate CPU-bound computation
    // ─────────────────────────────────────────────────────────────────────────

    private void busyWaitMs(long ms) {
        long end = System.nanoTime() + ms * 1_000_000L;
        while (System.nanoTime() < end) {
            // spin – simulates CPU-intensive scoring work
        }
    }
}
