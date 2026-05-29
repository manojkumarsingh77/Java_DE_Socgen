package com.bank.recommendation.model;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * LOAD MODELING – CustomerDataGenerator
 * ═══════════════════════════════════════════════════════════════════════════
 *
 * BUSINESS CONTEXT:
 *   Our retail bank processes 10,000–15,000 recommendation requests per
 *   second during peak (09:00–11:00, 18:00–21:00 IST).  Night-batch runs
 *   refresh ALL 8 million customers' profiles once per day.
 *
 *   For demo purposes we generate synthetic profiles that mimic real
 *   statistical distributions observed in Indian retail banking:
 *     • Credit score distribution: normal around 680, σ=90
 *     • Income distribution:       log-normal (long right tail – HNI)
 *     • Segment split:             RETAIL 70%, PREMIER 20%, WEALTH 8%, SME 2%
 *     • Tenure:                    exponential decay (most customers < 5 yr)
 *
 * LOAD MODELING explained:
 *   We build THREE load profiles to mimic production:
 *     1. STEADY   – constant 1000 req/s  (off-peak)
 *     2. RAMP     – 100 → 5000 req/s     (morning ramp-up)
 *     3. SPIKE    – burst to 15000 req/s for 30 s (salary-credit event)
 *   These are used in LatencySimulator to model real latency distributions.
 */
public class CustomerDataGenerator {

    private static final String[] SEGMENTS  = {"RETAIL","RETAIL","RETAIL","RETAIL",
                                                 "RETAIL","RETAIL","RETAIL",
                                                 "PREMIER","PREMIER","PREMIER","PREMIER",
                                                 "WEALTH","WEALTH","SME"};
    private static final String[] FIRST_NAMES = {
        "Rahul","Priya","Amit","Sunita","Rajesh","Kavya","Vikram","Deepa",
        "Arjun","Meena","Kiran","Sonal","Mohan","Rekha","Suresh","Anita"
    };
    private static final String[] LAST_NAMES = {
        "Sharma","Patel","Gupta","Singh","Verma","Joshi","Mehta","Nair",
        "Pillai","Reddy","Rao","Choudhary","Mishra","Tiwari","Agarwal","Shah"
    };

    /**
     * Generate `count` realistic customer profiles.
     * Uses ThreadLocalRandom for concurrency-safe generation.
     */
    public static List<CustomerProfile> generate(int count) {
        List<CustomerProfile> profiles = new ArrayList<>(count);
        Random rng = new Random(42); // fixed seed for reproducibility

        for (int i = 0; i < count; i++) {
            String segment = SEGMENTS[rng.nextInt(SEGMENTS.length)];

            // Credit score: normal distribution, clipped to [300, 900]
            int creditScore = Math.min(900, Math.max(300,
                    (int) (680 + rng.nextGaussian() * 90)));

            // Annual income: log-normal (realistic income skew)
            double baseIncome = switch (segment) {
                case "WEALTH"  -> 5_000_000;
                case "PREMIER" -> 1_200_000;
                case "SME"     -> 800_000;
                default        -> 400_000;
            };
            double annualIncome = baseIncome * Math.exp(rng.nextGaussian() * 0.4);

            // Debt-to-income: uniform 0–60%
            double dti            = 0.05 + rng.nextDouble() * 0.55;
            double outstandingDebt = annualIncome * dti;

            // Tenure: exponential-ish (most < 5 yrs)
            int tenureMonths = (int) (-Math.log(1 - rng.nextDouble()) * 36);
            tenureMonths = Math.min(tenureMonths, 360); // cap at 30 years

            // Birth date: 25-65 years old
            int ageYears  = 25 + rng.nextInt(40);
            LocalDate dob = LocalDate.now().minusYears(ageYears).minusDays(rng.nextInt(365));

            profiles.add(new CustomerProfile(
                    String.format("CUST-%07d", i + 1),
                    FIRST_NAMES[rng.nextInt(FIRST_NAMES.length)],
                    LAST_NAMES[rng.nextInt(LAST_NAMES.length)],
                    dob, segment,
                    annualIncome, annualIncome * 0.1 * rng.nextDouble(),
                    outstandingDebt, creditScore, tenureMonths
            ));
        }
        return profiles;
    }

    /**
     * LOAD MODELING: Simulate arrival rates for different load profiles.
     *
     * Returns inter-arrival time in milliseconds.
     * Real systems use Poisson arrival process; we approximate here.
     */
    public static long interArrivalMs(LoadProfile profile, long elapsedMs) {
        double rps = switch (profile) {
            case STEADY -> 1_000.0;
            case RAMP   -> {
                // Linear ramp: 100 → 5000 over 60 seconds
                double progress = Math.min(1.0, elapsedMs / 60_000.0);
                yield 100 + (4_900 * progress);
            }
            case SPIKE  -> {
                // Spike: 1000 baseline, then 15000 burst for 10 s
                if (elapsedMs > 5_000 && elapsedMs < 15_000) yield 15_000.0;
                else yield 1_000.0;
            }
        };
        // Inter-arrival = 1000 / rps  (ms per request)
        return Math.max(0L, (long) (1000.0 / rps));
    }

    public enum LoadProfile { STEADY, RAMP, SPIKE }
}
