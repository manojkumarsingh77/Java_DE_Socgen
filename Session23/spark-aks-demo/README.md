# Retail Banking Fraud Aggregation — Spark on AKS Demo

## Stage 1: Core Technical Concept

Spark's **Kubernetes scheduler backend** (`spark-kubernetes` module) replaces YARN's
ResourceManager / Standalone's Master with the **Kubernetes API server itself**. The driver
process, once started, opens a persistent watch connection to the API server and directly
`POST`s `Pod` manifests to create executors — it *is* the Kubernetes controller for this job.
There is no long-lived cluster manager daemon; when the driver pod dies, `OwnerReference`
garbage collection on the API server cleans up every executor pod automatically.

**Dynamic Allocation without an external shuffle service:** in YARN, scaling executors down
mid-job safely requires a `NodeManager`-hosted External Shuffle Service so shuffle files
outlive the executor. Kubernetes has no equivalent DaemonSet-per-node primitive by default, so
Spark uses **`spark.dynamicAllocation.shuffleTracking.enabled`** — the driver tracks which
executors hold shuffle blocks still needed by downstream stages and defers their removal until
those blocks are no longer referenced, instead of relying on an external service.

**Pod templates** (`spark.kubernetes.driver.podTemplateFile` / `executor.podTemplateFile`) let
you inject arbitrary K8s scheduling primitives — `nodeSelector`, `tolerations`, `affinity`,
resource `requests`/`limits` — that Spark's own conf flags don't expose, without forking Spark.

## Stage 2: Business Scenario
Retail Banking **Real-Time Fraud Score Aggregation**: 50,000 synthetic card transactions across
500 accounts, aggregated into per-account velocity features (transaction count, total/max
amount, distinct countries/channels) that feed a downstream fraud-scoring model. ~5% of accounts
are seeded with an injected "burst" pattern (rapid high-value transactions) to simulate
card-testing fraud.

## Stage 3 & 4: Code Walkthrough — `FraudAggregationJob.java`

| Code | What it does | Kubernetes/Spark mechanism |
|---|---|---|
| `record BankTransaction(...)` | Java 17 record — immutable, auto-generates `equals`/`hashCode`/`toString`, serializes cleanly across executor JVM boundaries via Spark's Java bean encoder. | N/A (language feature) |
| `buildSparkSession()` → `.config("spark.dynamicAllocation.enabled", "true")` | Enables elastic executor scaling. | Driver's `ExecutorPodsAllocator` creates/deletes `Pod` objects via the K8s API in response to `TaskSchedulerImpl` backlog signals. |
| `.config("spark.dynamicAllocation.shuffleTracking.enabled", "true")` | Allows safe scale-down without an external shuffle service pod. | `BlockManagerMasterEndpoint` tracks shuffle block ownership; executor removal is deferred until blocks are unreferenced. |
| `.config("spark.kubernetes.allocation.batch.size", "3")` | Throttles how many executor `Pod` create requests are sent to the API server per allocation cycle. | Prevents API server rate-limiting / AKS node-pool autoscaler thrash from a burst of simultaneous pod creations. |
| `.config("spark.sql.adaptive.enabled", "true")` | Enables Adaptive Query Execution. | Catalyst re-optimizes the physical plan mid-execution using runtime shuffle statistics (e.g., coalescing small post-shuffle partitions). |
| `generateSyntheticTransactions(...)` | Builds `List<Row>` in the driver JVM, then `spark.createDataFrame(rows, schema)`. | `createDataFrame` triggers a `LocalRelation` logical plan node — no distributed read required; Catalyst treats it as an in-memory literal collection. |
| `transactions.cache()` | Marks the DataFrame for `MEMORY_AND_DISK` persistence. | Physical plan inserts an `InMemoryTableScan`; avoids re-materializing the synthetic dataset for both the `.show()` call and the downstream aggregation. |
| `computeAccountVelocityFeatures(...)` → `.groupBy(col("accountId")).agg(...)` | Computes count/sum/max/countDistinct per account. | Catalyst plans this as a **partial aggregation** on each executor (`HashAggregateExec`) followed by a shuffle (`Exchange hashpartitioning`) and a **final aggregation** — minimizes shuffled bytes versus a naive groupBy. |
| `least(lit(1.0), ...)` composite score expression | Builds the risk score using only Catalyst-native `Column` expressions (no UDF). | Avoids Python/JVM UDF serialization overhead — the entire expression compiles into Spark's whole-stage code generation (Tungsten), producing native bytecode instead of a boxed function call per row. |
| `.write().partitionBy("riskTier").parquet(outputPath)` | Writes columnar output split into `riskTier=HIGH/MEDIUM/LOW` directories. | Enables downstream **partition pruning** — a query filtering `riskTier = 'HIGH'` skips reading MEDIUM/LOW files entirely at the file-listing stage. |
| `spark.stop()` in `finally` | Ensures driver deregisters from the API server and executor pods receive their termination signal even on exception. | On K8s, `spark.stop()` triggers `OwnerReference`-based cascade deletion of all executor pods tied to the driver pod — no orphaned pods left running on AKS. |

## Running Locally in IntelliJ
1. Import as Maven project (auto-activates the `mac-arm64` or `windows-x64` profile based on OS detection).
2. Run configuration → Main class: `com.bank.spark.aks.FraudAggregationJob`.
3. Paste the OS-specific VM Options block from the deliverable above.
4. Run — output written to `/tmp/spark-aks-demo/fraud-output` (macOS) or `%TEMP%\spark-aks-demo\fraud-output` (Windows).

## Deploying to AKS
```bash
az acr build --registry <acr-name> --image spark-aks-fraud-demo:1.0.0 .
bash k8s/spark-submit-aks.sh
kubectl get pods -n spark-jobs -w   # observe driver pod create executor pods live
```
