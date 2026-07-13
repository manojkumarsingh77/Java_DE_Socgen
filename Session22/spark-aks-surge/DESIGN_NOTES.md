# Black Friday Surge Handling — Design Notes
### Operating Apache Spark on AKS Safely (Instructor Reference)

This document is the **instructor's map**: for every topic in the syllabus,
it names the exact class(es)/method(s) that drive the demonstration, the
teaching narrative used, and the concrete Kubernetes/AKS/Spark concept each
line of output is standing in for.

Use it side-by-side with the console output while presenting — every
`[1) PROBLEM STATEMENT]` / `[2) REQUIREMENT]` / `[3) PROPOSED SOLUTION]` /
`[4) LIVE DEMO]` block printed at runtime corresponds 1:1 to a section below.

---

## How the demo is structured

```
Main.main()
 ├─ DriverExecutorDemo.run()     Topic 1 — Driver/Executor Pods
 ├─ ResourceTuningDemo.run()     Topic 2 — Resource Tuning
 ├─ PdbDemo.run()                Topic 3 — Pod Disruption Budgets
 └─ RollingUpdateDemo.run()      Topic 4 — Rolling Updates
```

Run `Main` with no arguments to play all four in order (recommended for a
live session), or pass `"1"`, `"2"`, `"3"`, `"4"` as a program argument to
jump straight to one topic (handy for re-running a single demo on request).

All four demos share one umbrella retail narrative: the **`checkout-events-aggregator`**
Spark Structured Streaming job (and its batch cousin, `order-fraud-scoring`)
running on AKS during Black Friday peak.

---

## Topic 1 — Driver / Executor Pods

**Class:** `com.retail.sparkaks.sparkpods.SparkJob`
**Demo runner:** `com.retail.sparkaks.sparkpods.DriverExecutorDemo`

| Concept | Driving method | What it shows |
|---|---|---|
| Driver pod created first | `SparkJob.createDriverPod()` | The driver must exist and be `Running` before anything else — modelled as an `IllegalStateException` guard in `createExecutorPods()` if you try to skip it. |
| Executors created BY the driver, not a controller | `SparkJob.createExecutorPods(int count)` | Mirrors the real RBAC requirement: the Spark driver's ServiceAccount needs `pods/create` permission because IT calls the K8s API, unlike a Deployment's ReplicaSet controller. |
| Dynamic allocation add/remove | `SparkJob.requestExecutors(int target, List<AksNode>)` | Used again in later topics to "heal" the executor count after an eviction. |
| Losing an executor = cheap | Scenario A in `DriverExecutorDemo` | Evicts one executor pod directly, then calls `requestExecutors()` to show the driver notices and self-heals — job stays healthy. |
| Losing the driver = expensive (SPOF) | `SparkJob.onDriverLost()` + Scenario B | Evicts the driver pod; every executor is orphaned. This is the *exact* blast-radius asymmetry the class needs to internalize before Topics 3–4 make sense. |

**Underlying model classes used throughout ALL topics:**
`com.retail.sparkaks.common.SparkPod` (pod with role DRIVER/EXECUTOR, cpu/heap/overhead requests, OOM + pending state) and
`com.retail.sparkaks.common.AksNode` (a worker node with allocatable capacity, cordon state, and a pod list).

---

## Topic 2 — Resource Tuning

**Class:** `com.retail.sparkaks.resourcetuning.ResourceTuningAdvisor`
**Demo runner:** `com.retail.sparkaks.resourcetuning.ResourceTuningDemo`

| Concept | Driving method | What it shows |
|---|---|---|
| `spark.executor.memoryOverhead` default formula | `ResourceTuningAdvisor.memoryOverheadMb(int executorHeapMb)` | `max(384Mi, 10% of heap)` — exactly Spark's own default. |
| What the **scheduler/OOMKiller** actually enforces | `ResourceTuningAdvisor.totalPodMemoryMb(int executorHeapMb)` | `heap + overhead` = the pod's memory request **and** limit (Guaranteed QoS) — the number that matters is NOT `-Xmx` alone. |
| Executor packing per node | `ResourceTuningAdvisor.executorsPerNode(...)` | Cores/memory bin-packing math with a reserved slice for kubelet/daemonsets. |
| Anti-pattern detection | `ResourceTuningAdvisor.sizingWarning(...)` | Flags "too many 1-core JVMs" and "executor too big for the node" automatically — turns tribal knowledge into a repeatable check. |
| The actual OOMKill | `SparkPod.simulateMemoryLoad(int workingSetMb)` | If a simulated working set (a skewed shuffle partition) exceeds `totalPodMemoryMb()`, the pod is marked `OOMKilled` and evicted — same trigger as the real kubelet OOM killer. |

**Live A/B comparison in `ResourceTuningDemo`:** same total cluster resources,
sliced as 16×1-core/2 GB executors ("Config A", inherited) vs. 4×4-core/8 GB
executors ("Config B", tuned). Both are fed the same simulated data-skew
event; Config A OOMs, Config B survives — a direct, visual payoff for
"right-size your executors."

---

## Topic 3 — Pod Disruption Budgets

**Classes:** `com.retail.sparkaks.pdb.PodDisruptionBudget`, `com.retail.sparkaks.pdb.EvictionController`
**Demo runner:** `com.retail.sparkaks.pdb.PdbDemo`

| Concept | Driving method | What it shows |
|---|---|---|
| `minAvailable` (absolute) | `PodDisruptionBudget.minAvailableAbsolute(name, role, n)` | Used to protect the single driver pod: `minAvailable=1` on a role with only 1 pod == "never evict me." |
| `maxUnavailable` (percentage) | `PodDisruptionBudget.maxUnavailablePercent(name, role, pct)` | Used to cap simultaneous executor loss at 25%, preserving Black-Friday-level parallelism. |
| The real eviction-admission check | `PodDisruptionBudget.allowsEviction(SparkPod candidate, List<SparkPod> sameRole)` | Recomputes "healthy count if this eviction proceeds" and compares it against the budget — this is *precisely* what the Kubernetes Eviction API subresource does. |
| Drain that respects every PDB | `EvictionController.drainNode(AksNode node, List<SparkPod> allPods)` | Tries every pod on a node one at a time; blocked pods are reported (mirroring the real `HTTP 429 Too Many Requests` response) without aborting the rest of the drain. |

**Demo payoff:** a full node-by-node drain (as if patching every node's
image) is attempted against a live 9-pod job. The driver pod is never
evicted; total executor loss never exceeds the 25% budget — visually
proving the earlier Topic 1 failure mode is now structurally impossible.

---

## Topic 4 — Rolling Updates

**Class:** `com.retail.sparkaks.rollingupdate.RollingUpgradeSimulator`
**Demo runner:** `com.retail.sparkaks.rollingupdate.RollingUpdateDemo`

| Concept | Driving method | What it shows |
|---|---|---|
| Surge upgrade strategy (`maxSurge`) | `RollingUpgradeSimulator.upgradePool(List<AksNode> oldNodes, Supplier<AksNode> newNodeFactory, int maxSurge, SparkJob job, EvictionController api)` | Provisions up to `maxSurge` brand-new (already-patched) nodes **before** touching any old node, so total capacity never dips below today's footprint. |
| Cordon before drain | same method, step 2 | `AksNode.setCordoned(true)` — old node stops accepting new pods immediately, existing pods untouched until drain. |
| Drain reuses Topic 3's exact mechanism | same method, step 3 | Calls `EvictionController.drainNode()` — the PDBs registered in Topic 3 apply unchanged here; nothing new to configure. |
| Zero-gap handover | same method, step 4 | Evicted pods are rescheduled onto already-Ready nodes (new or surviving old) *before* the source node is deleted. |
| Safe partial progress | same method, step 5 + `RollingUpdateDemo`'s trailing diagnostic | A node with PDB-blocked pods is left cordoned rather than forced; any pod that still can't find capacity is reported as "Pending — will retry" rather than silently dropped. |

**Demo payoff:** the class watches one upgrade pass move most of an
8-executor job onto new-image nodes while the driver pod is completely
untouched, and sees firsthand *why* `maxSurge` sizing and PDB correctness
both have to be right before you click "upgrade" during peak season.

---

## Design principles behind this codebase (why it's built this way)

1. **Zero external dependencies.** No real Spark, no real Kubernetes client,
   no Docker required. This guarantees the demo runs identically on an
   Apple Silicon MacBook Pro and a learner's Windows laptop the moment
   IntelliJ finishes importing the (dependency-free) Maven model — nothing
   to download, no cluster to provision, no flaky network step during class.
2. **One shared domain model** (`common` package: `AksNode`, `SparkPod`,
   `Banner`) is reused by all four topics, so behavior learned in Topic 1
   (driver vs. executor blast radius) visibly composes into Topic 3 (PDBs)
   and Topic 4 (rolling upgrades) — exactly like it does in production.
3. **Problem → Requirement → Solution → Demo, enforced in code**, via
   `Banner.problem()/requirement()/solution()/demo()/keyTakeaway()` — the
   narrative structure is not just in this markdown file, it is compiled
   into the program, so it can never drift from what actually runs.
4. **Every simulated mechanism mirrors a real, named Kubernetes/AKS/Spark
   API or default** (the Eviction API's admission check, the
   `memoryOverhead` default formula, `cordoned`/`Pending` semantics, surge
   upgrade ordering) so the mental model transfers directly to `kubectl`
   and the Azure CLI once learners are back on a real cluster.

---

## Running it

```
IntelliJ:  Open the project folder → let Maven import (no internet needed,
           zero dependencies) → right-click Main.java → Run 'Main.main()'.

CLI:       mvn compile exec:java   (or)   mvn package && java -jar target/spark-aks-surge-handling.jar

Single topic only:   java -jar target/spark-aks-surge-handling.jar 3
                     (1=Driver/Executor, 2=Resource Tuning, 3=PDBs, 4=Rolling Updates)
```

Requires JDK 17+ only. Verified to compile and run cleanly with `javac --release 17`.
