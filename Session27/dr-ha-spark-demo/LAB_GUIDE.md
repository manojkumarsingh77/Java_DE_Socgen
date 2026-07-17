# Lab Guide — Disaster Recovery & High Availability with Java Spark
### Cross-Region Failover Simulation for a Retail Banking Transaction Ledger

**Session 22 · Week 11 · Module: Disaster Recovery & High Availability**
**Topics covered:** RPO/RTO modeling · Cross-region replication · Backup strategy · Failure drills
**Duration:** ~3 hours (matches curriculum session length)

---

## 1. Learning Objectives

By the end of this lab you will be able to:

1. Define **RPO** (Recovery Point Objective) and **RTO** (Recovery Time Objective) in terms
   an engineer can actually *measure*, not just describe.
2. Build an **asynchronous cross-region replication pipeline** in Spark/Delta Lake and explain
   why async replication necessarily implies RPO > 0.
3. Implement a **3-2-1-style backup strategy** as a periodic, independent snapshot separate
   from live replication.
4. Run a **controlled failure drill** against the pipeline, inject a real outage, execute a
   failover runbook, and produce a signed-off DR report with measured RPO/RTO vs. SLA targets.
5. Debug the most common failure modes of this class of pipeline (Delta log conflicts,
   Windows/`winutils` issues, executor OOM, replication lag runaway) using a structured
   triage method.
6. Port the exact same application, unmodified, from IntelliJ local execution to AKS
   cluster-mode `spark-submit`.

---

## 2. Prerequisites

| Requirement | macOS (M1 Max) | Windows 11 |
|---|---|---|
| JDK | Temurin 17 (`brew install --cask temurin17`) | Temurin 17 (adoptium.net installer) |
| IntelliJ IDEA | 2024.x Ultimate/Community | 2024.x Ultimate/Community |
| Maven | 3.9+ (bundled with IntelliJ is fine) | 3.9+ |
| Docker | Docker Desktop (Apple Silicon build) | Docker Desktop (WSL2 backend) |
| kubectl / Azure CLI | `brew install azure-cli kubectl` | `winget install Microsoft.AzureCLI Kubernetes.kubectl` |
| winutils.exe | N/A | Required — see §7.3 |

Verify before starting:
```bash
java -version      # must print 17.x
mvn -version        # must show Java 17 as the runtime
docker --version
```

---

## 3. Architecture Overview

```
                         ┌────────────────────────────────────┐
   Synthetic Banking     │        PRIMARY REGION (us-east)     │
   Transaction Generator │  Delta table: primary_txn_ledger    │
   (in-process, seeded)  │  - Synchronous, durable commit      │
         │               └───────────────┬──────────────────┘
         │  batch N                       │ async replicate
         ▼                                │ (base lag + jitter,
┌─────────────────┐                       │  bounded thread pool)
│ ReplicationEngine│───────────────────────┤
│  - commitToPrimary()                    ▼
│  - replicateAsync()          ┌────────────────────────────────────┐
│  - performBackupIfDue()      │      SECONDARY REGION (us-west)     │
└─────────────────┘            │  Delta table: secondary_txn_ledger  │
         │                     └────────────────────────────────────┘
         │ every N batches
         ▼
┌─────────────────┐
│  Backup Snapshot │  (independent 3rd copy — 3-2-1 strategy)
└─────────────────┘

  At batch 15: FailureDrillOrchestrator.injectPrimaryFailure()
     → engine.simulatePrimaryOutage() = true
     → ingestion loop halts new primary commits
     → Phase A: detect (simulated health-probe latency)
     → Phase B: promote secondary + repoint routing (simulated)
     → RpoRtoCalculator reads the replication ledger and computes:
         - achieved RPO  (data-loss time window)
         - unreplicated batches (enumerable lost transactionIds)
         - achieved RTO  (failure → service restored)
```

**Why this design teaches the real problem:** the primary write path
(`commitToPrimary`) never blocks on `replicateAsync`. That single design
decision — asynchronous, non-blocking cross-region replication — is *why*
RPO cannot be zero. Blocking (synchronous multi-region commit) would give
RPO = 0 but at the cost of every single transaction paying full cross-region
round-trip latency. This tradeoff is the crux of the RPO/RTO conversation and
you will see it quantified in the drill report.

---

## 4. Lab Steps — Phase 1: Steady-State Replication

### Step 4.1 — Import the project
1. Open IntelliJ → **File → Open** → select `dr-ha-spark-demo/pom.xml` → *Open as Project*.
2. Let Maven resolve dependencies (first run pulls Spark 3.5.1 + Delta 3.2.0 + Hadoop-Azure — a few
   hundred MB; ensure you have network access to Maven Central).
3. **File → Project Structure → Project SDK** → set to Java 17 (Temurin).

### Step 4.2 — Create the Run Configuration
1. **Run → Edit Configurations → + → Application**.
2. Main class: `com.retailbank.dr.Main`
3. Paste the OS-appropriate **VM Options** block from `README.md` (§ macOS or § Windows).
4. Working directory: project root.
5. Apply → OK.

### Step 4.3 — Run the demo (first pass, no drill focus yet)
Click Run. Expected console output pattern:
```
PRIMARY commit  | batch=1 records=5000 ts=2026-07-17T09:31:02.10Z
SECONDARY apply | batch=1 lagMs=812 ts=2026-07-17T09:31:02.91Z
PRIMARY commit  | batch=2 records=5000 ts=2026-07-17T09:31:03.05Z
...
### DR DRILL TRIGGER: injecting PRIMARY region failure before batch 15 ###
Primary unavailable — halting new ingestion, proceeding to failover runbook.
>> Failover Phase A: detecting failure (health probes)...
>> Failover Phase B: promoting SECONDARY [us-west-secondary] to active + repointing routing...
```

**Checkpoint:** Confirm you see interleaved `PRIMARY commit` and `SECONDARY apply`
lines with `SECONDARY apply` lagging `PRIMARY commit` by roughly 750–1000ms
(`baseReplicationLagMillis` + jitter). This lag *is* the RPO exposure window.

---

## 5. Lab Steps — Phase 2: The Failure Drill

### Step 5.1 — Understand the trigger
Open `AppConfig.defaultLocalDemo()`. The field `failurePrimaryDownAtBatch = 14`
means: after batch 14 commits, `Main` injects failure before batch 15 starts.
Because replication is async with ~0.75–1s lag, batch 14 (and possibly 13) may
still be *in flight* to the secondary at the exact instant of failure — this is
intentional, so the drill produces a **non-zero, measurable RPO** rather than a
trivial always-passing demo.

### Step 5.2 — Run and capture the report
Run `Main` again. At the end you'll see:
```
================ CROSS-REGION FAILOVER DRILL REPORT ================
Primary region             : us-east-primary
Secondary region            : us-west-secondary
Failure injected at         : 2026-07-17T09:31:13.40Z
Failure detected at         : 2026-07-17T09:31:18.41Z
Failover completed at       : 2026-07-17T09:31:30.42Z
-----------------------------------------------------------------
RPO target / achieved       : 60s / 0.9s   -> MET
RTO target / achieved       : 300s / 17.0s   -> MET
Unreplicated records lost   : 5000 (batches: [14])
Last confirmed backup batch : 10 (/tmp/dr-ha-spark-demo/backups)
-----------------------------------------------------------------
OVERALL DRILL RESULT        : PASS
Recommendations:
  - 5000 records across 1 batches were never confirmed on secondary at
    failure time — these require replay-from-source or reconciliation
    against upstream event log (Event Hub) before secondary is trusted
    as sole source of truth.
===================================================================
```
A JSON copy lands in `reports/dr_drill_report_<epoch>.json` — this is the
artifact you'd attach to a change-management/compliance ticket in a real bank.

### Step 5.3 — Interrogate the numbers (do this as a class discussion)
- **Why is achieved RPO not exactly equal to `baseReplicationLagMillis`?**
  Because RPO is measured from the *last confirmed replicated commit*, not the
  last attempted one — if batch 13 replicated but batch 14 didn't, RPO spans
  from batch 13's primary commit time to the failure instant, which is larger
  than one batch's lag.
- **Why is RTO dominated by `failoverDetectionMillis + failoverPromotionMillis`
  and not by data volume?** Because RTO measures *service restoration time*,
  which in a well-designed failover is decoupled from how much data exists —
  it's bounded by health-check cadence and DNS/traffic-manager propagation, not
  by table size. This is the argument for investing in faster health probes
  over "just add more compute."

### Step 5.4 — Push the SLA to breach it (make the drill fail)
Edit `AppConfig`:
```java
long rpoTargetSeconds = 1L;   // tighten from 60s to 1s — unrealistic on purpose
```
Re-run. Observe `RPO target / achieved -> BREACHED` and the corresponding
recommendation about moving to semi-synchronous replication. This demonstrates
that **RPO/RTO targets are a cost/latency tradeoff decision**, not a pure
engineering constant — tightening them has a concrete architectural price
(shown in the recommendation text).

### Step 5.5 — Experiment matrix (assign to pairs)
| Experiment | Change | Expected effect |
|---|---|---|
| A | `baseReplicationLagMillis` 750 → 3000 | RPO achieved increases roughly proportionally |
| B | `failoverDetectionMillis` 5000 → 500 | RTO achieved drops; discuss false-positive risk of faster probes |
| C | `backupEveryNBatches` 5 → 1 | No RPO/RTO effect (backup ≠ replication) — this is the key insight of §6 |
| D | `failurePrimaryDownAtBatch` 14 → 1 | Almost all batches lost — demonstrates why drills must run at realistic steady-state, not cold start |

---

## 6. Why Backup ≠ Replication (Common Misconception)

Students frequently conflate the two. Use this lab to make the distinction concrete:

- **Replication** (`replicateAsync`) propagates *every write, including bad ones*
  (a bug that corrupts data corrupts both regions within `baseReplicationLagMillis`).
  It protects against **infrastructure failure** (region outage), not **logical
  failure** (bad deploy, ransomware, accidental `DELETE`).
- **Backup** (`performBackupIfDue`) is a periodic, independent, immutable snapshot.
  It protects against **logical failure** via point-in-time restore, at the cost
  of a larger RPO (you can only restore to the last backup, not the last write).

In production, back this with Delta Lake's native versioning:
```sql
RESTORE TABLE txn_ledger TO VERSION AS OF 4821;
-- or
SELECT * FROM txn_ledger VERSION AS OF 4821;
```
plus `VACUUM` retention tuned to your compliance-mandated restore window
(never run `VACUUM` with a retention shorter than your longest audit/replay need).

---

## 7. Troubleshooting & Debugging Guide

Work through issues top-to-bottom; each entry states the symptom, root cause,
and fix, in the order you're likely to hit them.

### 7.1 — `NoSuchMethodError` / `NoClassDefFoundError` on Delta or Hadoop classes
**Cause:** Scala binary version mismatch (`_2.12` artifact pulled alongside a
`_2.13` transitive dependency) or a shaded jar that stripped Spark/Hadoop classes
that were expected at runtime for local execution.
**Fix:** For **local IntelliJ runs**, run the `Main` class directly from the IDE
using the full dependency classpath (Maven "Compile" scope) — do **not** run the
shaded jar locally; the shade plugin excludes `org.apache.spark:*` specifically
because AKS's base Spark image already provides it (see `pom.xml` comments). The
shaded jar is for AKS only.

### 7.2 — `java.lang.reflect.InaccessibleObjectException` on Spark startup
**Cause:** Java 17's strong module encapsulation blocks Spark's reflective access
into `java.nio`/`java.util` internals used by its Kryo/Unsafe-based serializers.
**Fix:** Confirm every `--add-opens` flag from `README.md` is present in your Run
Configuration's VM Options — this is the single most common first-run failure on
Java 17. IntelliJ silently truncates VM option strings pasted with line breaks in
some versions; paste as one continuous space-separated line if flags appear ignored.

### 7.3 — Windows: `Could not locate executable null\bin\winutils.exe`
**Cause:** Spark's `LocalFileSystem` implementation shells out to `winutils.exe`
for POSIX-style permission emulation, even for purely local runs.
**Fix:**
1. Download `winutils.exe` + `hadoop.dll` matching Hadoop 3.3.6 from a trusted
   mirror of the `cdarlint/winutils` GitHub project.
2. Place at `C:\hadoop\bin\winutils.exe`.
3. Set `-Dhadoop.home.dir=C:\hadoop` in VM options **and** `HADOOP_HOME=C:\hadoop`
   as a Windows environment variable (some native calls read the env var directly,
   bypassing the system property).
4. Restart IntelliJ fully (not just the run) after setting the env var — IntelliJ
   caches the process environment at launch.

### 7.4 — macOS M1: replication tasks appear to hang / never complete
**Cause:** Usually not Apple Silicon–specific — check first whether
`replicationExecutor` (a fixed pool of 4) is saturated because `totalBatches`
was increased without increasing pool size, causing later batches to queue
behind earlier ones' simulated network sleep.
**Fix:** Either reduce `totalBatches`/increase pool size in
`ReplicationEngine`, or treat this as a *teaching moment*: this queuing is
exactly how real replication backlogs form under sustained write pressure,
and directly widens the achieved RPO — have students screenshot the growing
gap between `PRIMARY commit` and `SECONDARY apply` timestamps as evidence.

### 7.5 — `ConcurrentModificationException` or `DELTA_CONCURRENT_APPEND` errors
**Cause:** Two writers appending to the same Delta table path concurrently
without compatible isolation — can occur if you parallelize batch generation
without also serializing writes to a single table path.
**Fix:** Delta's optimistic concurrency control will retry compatible
appends automatically; if you see this, check you haven't accidentally
pointed both `primaryTablePath` and `secondaryTablePath` at the same
directory (common typo when editing `AppConfig` during Experiment D).

### 7.6 — Executor/driver OOM on larger `recordsPerBatch`
**Cause:** `batch.persist()` in `Main` caches each micro-batch in memory for
reuse across the primary write, replication write, and count — scaling
`recordsPerBatch` up without scaling `-Xmx` accordingly exhausts heap.
**Fix:** Either raise `-Xmx` (local) / `spark.executor.memory` (AKS), or switch
`persist()` to `persist(StorageLevel.MEMORY_AND_DISK())` so Spark spills instead
of OOM-ing. For true cluster-mode scale testing, this is also where you'd
introduce `spark.sql.shuffle.partitions` tuning per the earlier "Spark Memory &
Resource Management" module.

### 7.7 — AKS: pods stuck in `ImagePullBackOff`
**Cause:** AKS's managed identity/kubelet doesn't have `AcrPull` on your ACR.
**Fix:**
```bash
az aks update -n <cluster-name> -g <resource-group> --attach-acr <acr-name>
```
Verify with `kubectl describe pod <driver-pod>` — the Events section will show
the exact pull error if this doesn't resolve it (also check image tag typos).

### 7.8 — AKS: driver pod `CrashLoopBackOff` immediately with no Spark logs
**Cause:** Usually a `spark.kubernetes.authenticate.driver.serviceAccountName`
missing RBAC permission to create executor pods.
**Fix:**
```bash
kubectl create clusterrolebinding spark-role \
  --clusterrole=edit --serviceaccount=data-platform-dr:spark \
  --namespace=data-platform-dr
```

### 7.9 — Structured debugging method (apply generally)
1. **Reproduce locally first.** Every failure mode above except §7.7/7.8 can and
   should be reproduced in IntelliJ before touching AKS — cluster-mode debugging
   is 10x slower per iteration.
2. **Read the Spark UI DAG/stage view** (`http://localhost:4040` during local
   runs) before reading stack traces — most "mysterious" exceptions trace back
   to an obvious stage retry or skew visible in the UI first.
3. **Check the replication ledger, not just console logs**, when RPO numbers look
   wrong — `engine.ledgerSnapshot()` is the ground truth; console log ordering
   can be misleading under concurrent async tasks.
4. **Isolate Delta vs. Spark vs. app logic** by testing `format("delta")` reads/writes
   against a throwaway path in a Spark shell before assuming your orchestration
   code is the bug.

---

## 8. Lab Steps — Phase 3: Containerize

```bash
cd dr-ha-spark-demo
mvn clean package -DskipTests
docker build -f docker/Dockerfile -t bankdracr.azurecr.io/dr-ha-spark-demo:1.0.0 .
docker run --rm bankdracr.azurecr.io/dr-ha-spark-demo:1.0.0 --version   # sanity check entrypoint
```
**Checkpoint:** image build must complete both Maven stages without network
timeouts; if Stage 1 (`mvn dependency:go-offline`) fails, confirm Docker
Desktop has internet access and isn't rate-limited by a corporate proxy —
configure `~/.m2/settings.xml` with your proxy and `COPY` it into the build
stage if needed.

---

## 9. Lab Steps — Phase 4: Deploy to AKS

```bash
az acr login --name bankdracr
docker push bankdracr.azurecr.io/dr-ha-spark-demo:1.0.0

kubectl create namespace data-platform-dr --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f k8s/spark-application.yaml

kubectl get sparkapplications -n data-platform-dr -w
kubectl logs -n data-platform-dr -l workload=dr-drill -f
```
Alternatively, without the spark-operator installed, use the raw `spark-submit`
path:
```bash
bash k8s/spark-submit-command.sh
```

**Checkpoint:** confirm in the driver logs the same `PRIMARY commit` /
`SECONDARY apply` / drill report sequence you saw locally in §5.2 — this is
the point of the lab: **identical code, identical behavior, different
infrastructure.**

---

## 10. Capstone Extension Exercises (optional, for advanced students)

1. Replace the simulated `Thread.sleep` replication lag with a real
   cross-region write by pointing `secondaryTablePath` at an ADLS Gen2
   `abfss://` path in a second Azure region, and measure *actual* network RPO.
2. Replace the polling-style failure detection (`failoverDetectionMillis`
   sleep) with a real Spark Structured Streaming heartbeat query against
   the primary table, alerting via a dead-man's-switch pattern.
3. Extend `RpoRtoCalculator` to compute RPO/RTO percentiles (P50/P95/P99)
   across 20 repeated drill runs with randomized failure injection points,
   and produce a distribution report — this mirrors how real DR programs
   validate SLA compliance statistically, not from a single run.
4. Wire `unreplicatedBatchIds` from the report into a replay job that
   re-reads those specific transactionIds from an upstream Event Hub
   checkpoint and re-applies them to the promoted secondary, closing the
   data-loss gap post-recovery.

---

## 11. Assessment Rubric (for instructor use)

| Criterion | Weight |
|---|---|
| Correctly explains why async replication implies RPO > 0 | 20% |
| Successfully reproduces a passing drill and a breached-SLA drill | 20% |
| Identifies backup vs. replication distinction unprompted | 15% |
| Diagnoses at least one injected fault using §7's method (instructor injects a fault, e.g. points both table paths to the same directory) | 25% |
| Deploys to AKS and confirms identical drill output in cluster logs | 20% |
