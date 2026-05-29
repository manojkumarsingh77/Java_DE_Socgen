package com.bank.recommendation.model;

import java.time.LocalDate;
import java.util.List;

/**
 * BUSINESS CONTEXT: Retail Banking Customer Profile
 *
 * Represents a bank customer whose transaction history, demographics,
 * and financial behaviour are fed into the Recommendation Engine to
 * suggest products: credit cards, loans, savings plans, investments.
 *
 * This class is deliberately laid out to DEMONSTRATE FALSE SHARING:
 *   - BAD version:  hotFields are interleaved with cold fields
 *     → multiple fields fit in the same CPU cache line (64 bytes)
 *     → concurrent writes from different threads INVALIDATE each other's cache
 *   - GOOD version: @Contended + padding separates hot fields
 *
 * FALSE SHARING ROOT CAUSE:
 *   CPU cache line = 64 bytes.
 *   If Thread-A writes `riskScore` and Thread-B writes `creditScore`
 *   and both sit in the same 64-byte block, every write forces the
 *   OTHER thread to re-fetch the entire cache line from L3/RAM.
 *   On a 32-core machine processing 10,000 profiles/sec this can
 *   add 150-200ms to P99 latency alone.
 */
public class CustomerProfile {

    // ── Identity (read-once, cold) ────────────────────────────────────────────
    private final String customerId;
    private final String firstName;
    private final String lastName;
    private final LocalDate dateOfBirth;
    private final String segment;          // RETAIL / PREMIER / WEALTH / SME

    // ── Financial snapshot (read-heavy, warm) ─────────────────────────────────
    private final double annualIncome;
    private final double currentBalance;
    private final double outstandingDebt;
    private final int    creditScore;       // 300-900
    private final int    tenureMonths;

    // ── Behavioural signals (written frequently by scoring threads – HOT) ─────
    // ⚠️  WITHOUT padding these land in the SAME cache line as creditScore above
    //     → False-sharing between ScoreThread and FeatureThread
    private volatile double riskScore;          // 0.0 – 1.0
    private volatile double propensityScore;    // 0.0 – 1.0  (likelihood to buy)
    private volatile double churnScore;         // 0.0 – 1.0

    // ── Product eligibility flags (written by EligibilityThread – HOT) ────────
    private volatile boolean eligibleForPersonalLoan;
    private volatile boolean eligibleForCreditCard;
    private volatile boolean eligibleForMutualFund;
    private volatile boolean eligibleForFixedDeposit;

    // ── Recommendation output (written last by RecommenderThread) ─────────────
    private volatile List<String> recommendedProducts;
    private volatile long         processingStartNanos;
    private volatile long         processingEndNanos;

    // ── Constructor ───────────────────────────────────────────────────────────
    public CustomerProfile(String customerId, String firstName, String lastName,
                           LocalDate dateOfBirth, String segment,
                           double annualIncome, double currentBalance,
                           double outstandingDebt, int creditScore, int tenureMonths) {
        this.customerId     = customerId;
        this.firstName      = firstName;
        this.lastName       = lastName;
        this.dateOfBirth    = dateOfBirth;
        this.segment        = segment;
        this.annualIncome   = annualIncome;
        this.currentBalance = currentBalance;
        this.outstandingDebt = outstandingDebt;
        this.creditScore    = creditScore;
        this.tenureMonths   = tenureMonths;
    }

    // ── Getters ───────────────────────────────────────────────────────────────
    public String   getCustomerId()       { return customerId;       }
    public String   getFirstName()        { return firstName;        }
    public String   getLastName()         { return lastName;         }
    public LocalDate getDateOfBirth()     { return dateOfBirth;      }
    public String   getSegment()          { return segment;          }
    public double   getAnnualIncome()     { return annualIncome;     }
    public double   getCurrentBalance()   { return currentBalance;   }
    public double   getOutstandingDebt()  { return outstandingDebt;  }
    public int      getCreditScore()      { return creditScore;      }
    public int      getTenureMonths()     { return tenureMonths;     }

    public double   getRiskScore()        { return riskScore;        }
    public double   getPropensityScore()  { return propensityScore;  }
    public double   getChurnScore()       { return churnScore;       }

    public boolean  isEligibleForPersonalLoan()  { return eligibleForPersonalLoan;  }
    public boolean  isEligibleForCreditCard()    { return eligibleForCreditCard;    }
    public boolean  isEligibleForMutualFund()    { return eligibleForMutualFund;    }
    public boolean  isEligibleForFixedDeposit()  { return eligibleForFixedDeposit;  }

    public List<String> getRecommendedProducts() { return recommendedProducts; }
    public long  getProcessingStartNanos()        { return processingStartNanos; }
    public long  getProcessingEndNanos()          { return processingEndNanos;   }
    public long  getLatencyNanos() {
        return processingEndNanos > 0 ? processingEndNanos - processingStartNanos : 0;
    }

    // ── Setters (volatile writes – these are the HOT paths) ──────────────────
    public void setRiskScore(double v)       { this.riskScore = v;       }
    public void setPropensityScore(double v) { this.propensityScore = v; }
    public void setChurnScore(double v)      { this.churnScore = v;      }

    public void setEligibleForPersonalLoan(boolean v)  { this.eligibleForPersonalLoan = v;  }
    public void setEligibleForCreditCard(boolean v)    { this.eligibleForCreditCard = v;    }
    public void setEligibleForMutualFund(boolean v)    { this.eligibleForMutualFund = v;    }
    public void setEligibleForFixedDeposit(boolean v)  { this.eligibleForFixedDeposit = v;  }

    public void setRecommendedProducts(List<String> products) { this.recommendedProducts = products; }
    public void setProcessingStartNanos(long t)               { this.processingStartNanos = t;       }
    public void setProcessingEndNanos(long t)                 { this.processingEndNanos = t;         }

    @Override
    public String toString() {
        return String.format("Customer[%s | %s %s | Segment=%s | Credit=%d | Risk=%.2f | Products=%s]",
                customerId, firstName, lastName, segment, creditScore,
                riskScore, recommendedProducts);
    }
}
