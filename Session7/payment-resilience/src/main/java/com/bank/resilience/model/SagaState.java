package com.bank.resilience.model;

import java.time.Instant;
import java.util.List;

/**
 * SagaState — complete audit trail of a distributed saga execution.
 *
 * Every step completed, every compensation triggered is recorded here.
 * In production this is persisted to a saga_log table so that even if
 * the orchestrator crashes mid-saga, it can resume from the last checkpoint.
 *
 * This is the "saga journal" — the single source of truth for
 * "where is this distributed transaction right now?"
 */
public class SagaState {

    public enum SagaStatus {
        RUNNING,       // saga is executing forward steps
        COMMITTED,     // all steps succeeded
        COMPENSATING,  // a step failed, rolling back
        COMPENSATED,   // all compensations executed successfully
        FAILED         // compensation itself failed — needs manual ops intervention
    }

    public static class SagaStep {
        public enum StepStatus { COMPLETED, FAILED, COMPENSATED }

        private final String     stepName;
        private final String     serviceName;
        private final StepStatus status;
        private final String     detail;
        private final Instant    executedAt;

        public SagaStep(String stepName, String serviceName,
                        StepStatus status, String detail, Instant executedAt) {
            this.stepName    = stepName;
            this.serviceName = serviceName;
            this.status      = status;
            this.detail      = detail;
            this.executedAt  = executedAt;
        }

        public String     getStepName()    { return stepName;    }
        public String     getServiceName() { return serviceName; }
        public StepStatus getStatus()      { return status;      }
        public String     getDetail()      { return detail;      }
        public Instant    getExecutedAt()  { return executedAt;  }

        @Override
        public String toString() {
            return String.format("[%s] %s → %s (%s)",
                status, stepName, serviceName, detail);
        }
    }

    private final String         sagaId;
    private final String         orderId;
    private final SagaStatus     status;
    private final List<SagaStep> completedSteps;
    private final List<SagaStep> compensatedSteps;
    private final String         failedAtStep;
    private final String         failureReason;
    private final Instant        startedAt;
    private final Instant        completedAt;

    public SagaState(String sagaId, String orderId, SagaStatus status,
                     List<SagaStep> completedSteps, List<SagaStep> compensatedSteps,
                     String failedAtStep, String failureReason,
                     Instant startedAt, Instant completedAt) {
        this.sagaId           = sagaId;
        this.orderId          = orderId;
        this.status           = status;
        this.completedSteps   = completedSteps;
        this.compensatedSteps = compensatedSteps;
        this.failedAtStep     = failedAtStep;
        this.failureReason    = failureReason;
        this.startedAt        = startedAt;
        this.completedAt      = completedAt;
    }

    public String         getSagaId()           { return sagaId;           }
    public String         getOrderId()          { return orderId;          }
    public SagaStatus     getStatus()           { return status;           }
    public List<SagaStep> getCompletedSteps()   { return completedSteps;   }
    public List<SagaStep> getCompensatedSteps() { return compensatedSteps; }
    public String         getFailedAtStep()     { return failedAtStep;     }
    public String         getFailureReason()    { return failureReason;    }
    public Instant        getStartedAt()        { return startedAt;        }
    public Instant        getCompletedAt()      { return completedAt;      }

    public boolean isSuccess()     { return status == SagaStatus.COMMITTED;   }
    public boolean isCompensated() { return status == SagaStatus.COMPENSATED; }
    public int completedStepCount() { return completedSteps.size(); }

    @Override
    public String toString() {
        return String.format(
            "SagaState[sagaId=%s, orderId=%s, status=%s, steps=%d, compensated=%d]",
            sagaId, orderId, status,
            completedSteps.size(), compensatedSteps.size()
        );
    }
}
