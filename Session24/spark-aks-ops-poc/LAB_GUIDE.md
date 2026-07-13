# Guided Lab: Spark on AKS Operations
### Driver/Executor Pods · Resource Tuning · Pod Disruption Budgets · Rolling Updates

**Azure Subscription:** `npunext-1680261103285`
**Builds on:** the Foundation Lab (`spark-aks-fraud-poc`) — same AKS cluster
and ACR are reused here; only a new namespace (`spark-ops-poc`) is created.

This lab exists specifically to make four operational concepts *observable*,
not just configured:

| Topic | How this lab makes it observable |
|---|---|
| Driver/executor pods | A **long-running** Structured Streaming job (never exits on its own) so pod lifecycle, restarts, and scheduling are visible over time — a batch job that finishes in 10 seconds never exercises this. |
| Resource tuning | Explicit `requests`/`limits` set via **pod template files**, tied back to `spark.executor.memoryOverhead` math, verified with `kubectl top` and QoS class inspection. |
| Pod Disruption Budgets | Real `PodDisruptionBudget` objects on both driver and executors, exercised against a real `kubectl drain`. |
| Rolling updates | A genuine multi-replica Kubernetes `Deployment` (the health-dashboard companion service) — Spark's own driver is a singleton Pod, not a Deployment, so native rolling-update semantics don't apply to it directly; this is explained honestly in Step 8 rather than glossed over. |

---

## Table of Contents
1. [Architecture](#1-architecture)
2. [Project Structure](#2-project-structure)
3. [Step 0 — Reuse or Provision AKS + ACR](#step-0--reuse-or-provision-aks--acr)
4. [Step 1 — Namespace & RBAC](#step-1--namespace--rbac)
5. [Step 2 — Run the Streaming Job Locally in IntelliJ](#step-2--run-the-streaming-job-locally-in-intellij)
6. [Step 3 — Build & Push the Streaming Job Image](#step-3--build--push-the-streaming-job-image)
7. [Step 4 — Build & Push the Health-Dashboard Image (v1)](#step-4--build--push-the-health-dashboard-image-v1)
8. [Step 5 — spark-submit with Pod Templates](#step-5--spark-submit-with-pod-templates)
9. [Step 6 — Deploy the Health Dashboard](#step-6--deploy-the-health-dashboard)
10. [Step 7 — Pod Disruption Budgets in Action](#step-7--pod-disruption-budgets-in-action)
11. [Step 8 — Rolling Update in Action](#step-8--rolling-update-in-action)
12. [Step 9 — Resource Tuning Verification](#step-9--resource-tuning-verification)
13. [Step 10 — Azure Portal Verification](#step-10--azure-portal-verification)
14. [Discussion Guide](#11-discussion-guide)
15. [Cleanup](#12-cleanup)
16. [Troubleshooting](#13-troubleshooting)

---

## 1. Architecture

```
IntelliJ (local run first, rate-source stream, console sink)
        │  mvn package
        ▼
┌───────────────────────┐        ┌──────────────────────────────┐
│ pos-streaming-        │        │ streaming-health-dashboard    │
│ aggregator.jar        │        │ .jar (zero Spark dependency)  │
└──────────┬────────────┘        └──────────────┬───────────────┘
           │ docker build (+ Spark base image)   │ docker build
           ▼                                     ▼
   ACR: fraud-preprocessor-style push       ACR: streaming-health-dashboard:v1/v2
           │                                     │
           ▼                                     ▼
┌──────────────────────────────────────────────────────────────────┐
│                     AKS — namespace: spark-ops-poc                │
│                                                                    │
│  spark-submit (podTemplateFile x2) ──▶ ┌────────────┐             │
│                                          │ Driver Pod │◀── PDB     │
│                                          │ (singleton)│  maxUnavail:0
│                                          └─────┬──────┘             │
│                                                │ creates            │
│                                   ┌────────────┼────────────┐       │
│                                   ▼            ▼            ▼       │
│                            ┌──────────┐ ┌──────────┐ ┌──────────┐   │
│                            │Executor 1│ │Executor 2│ │Executor 3│   │
│                            └──────────┘ └──────────┘ └──────────┘   │
│                                   ▲ all covered by PDB minAvailable:2│
│                                                                    │
│  pos-streaming-driver-ui (Service, port 4040) ◀── polled by ──┐    │
│                                                                 │    │
│  streaming-health-dashboard Deployment (3 replicas) ───────────┘    │
│    RollingUpdate: maxSurge=1, maxUnavailable=0                      │
└──────────────────────────────────────────────────────────────────┘
           │
           ▼
   Azure Portal (Workloads, node pool, Insights)
```

---

## 2. Project Structure

```
spark-aks-ops-poc/
├── LAB_GUIDE.md
├── streaming-job/
│   ├── pom.xml
│   ├── docker/Dockerfile
│   ├── k8s/
│   │   ├── namespace-rbac.yaml
│   │   ├── driver-pod-template.yaml
│   │   ├── executor-pod-template.yaml
│   │   ├── pdb.yaml
│   │   └── driver-service.yaml
│   └── src/main/java/com/npunext/bank/streaming/job/
│       └── POSStreamingAggregatorJob.java
└── health-dashboard/
    ├── pom.xml
    ├── docker/Dockerfile
    ├── k8s/
    │   ├── deployment.yaml
    │   └── service.yaml
    └── src/main/java/com/npunext/bank/dashboard/
        └── HealthDashboardServer.java
```

---

## Step 0 — Reuse or Provision AKS + ACR

If you completed the Foundation Lab, reuse the same cluster and registry:

```bash
az account set --subscription "npunext-1680261103285"
az aks get-credentials --resource-group rg-spark-aks-poc --name aks-spark-poc
az acr login --name <UNIQUE_ACR_NAME>
kubectl get nodes
```

If not, run Steps 1–3 of `spark-aks-fraud-poc/LAB_GUIDE.md` first — this lab
assumes the AKS cluster and ACR already exist.

---

## Step 1 — Namespace & RBAC

```bash
kubectl apply -f streaming-job/k8s/namespace-rbac.yaml
kubectl get serviceaccount -n spark-ops-poc
```

---

## Step 2 — Run the Streaming Job Locally in IntelliJ

Open `streaming-job/pom.xml` as a project. Create a Run Configuration:
- **Main class:** `com.npunext.bank.streaming.job.POSStreamingAggregatorJob`
- **Program arguments:** `200 "30 seconds" "1 minute"` (rows/sec, window, watermark)
- ☑️ "Include dependencies with 'Provided' scope"
- **VM options:** identical to the Foundation Lab's Step 5 (`-Dspark.master.override=local[*]`
  plus the same `--add-opens` flags; add `-Dhadoop.home.dir=C:\hadoop` on Windows).

Run it. Unlike the batch job, **this one never stops** — every 10 seconds
you'll see a new micro-batch's windowed aggregation printed to console.
Let it run for at least 2–3 minutes so you see multiple overlapping windows
close out, then stop it manually (red square in IntelliJ). This local run
is your functional correctness check before touching AKS.

---

## Step 3 — Build & Push the Streaming Job Image

Reuses the Spark base image already pushed to ACR in the Foundation Lab
(`<UNIQUE_ACR_NAME>.azurecr.io/spark:3.5.1`).

```bash
# macOS M1 Max
cd streaming-job
docker buildx build \
  --platform linux/amd64 \
  -f docker/Dockerfile \
  --build-arg SPARK_BASE_IMAGE=<UNIQUE_ACR_NAME>.azurecr.io/spark:3.5.1 \
  -t <UNIQUE_ACR_NAME>.azurecr.io/pos-streaming-aggregator:v1 \
  --push .
```

```powershell
# Windows 11
cd streaming-job
docker build -f docker/Dockerfile `
  --build-arg SPARK_BASE_IMAGE=<UNIQUE_ACR_NAME>.azurecr.io/spark:3.5.1 `
  -t <UNIQUE_ACR_NAME>.azurecr.io/pos-streaming-aggregator:v1 .
docker push <UNIQUE_ACR_NAME>.azurecr.io/pos-streaming-aggregator:v1
```

---

## Step 4 — Build & Push the Health-Dashboard Image (v1)

```bash
# macOS M1 Max
cd ../health-dashboard
docker buildx build \
  --platform linux/amd64 \
  -f docker/Dockerfile \
  -t <UNIQUE_ACR_NAME>.azurecr.io/streaming-health-dashboard:v1 \
  --push .
```

```powershell
# Windows 11
cd ..\health-dashboard
docker build -f docker/Dockerfile -t <UNIQUE_ACR_NAME>.azurecr.io/streaming-health-dashboard:v1 .
docker push <UNIQUE_ACR_NAME>.azurecr.io/streaming-health-dashboard:v1
```

Edit `health-dashboard/k8s/deployment.yaml` — replace `<UNIQUE_ACR_NAME>`
in the `image:` field with your actual registry name.

---

## Step 5 — spark-submit with Pod Templates

From the extracted `spark-3.5.1-bin-hadoop3` distribution used in the
Foundation Lab:

```bash
kubectl cluster-info    # copy the API server URL

./bin/spark-submit \
  --master k8s://https://<AKS_API_SERVER> \
  --deploy-mode cluster \
  --name pos-streaming-aggregator \
  --class com.npunext.bank.streaming.job.POSStreamingAggregatorJob \
  --conf spark.kubernetes.namespace=spark-ops-poc \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-driver-sa \
  --conf spark.kubernetes.container.image=<UNIQUE_ACR_NAME>.azurecr.io/pos-streaming-aggregator:v1 \
  --conf spark.kubernetes.driver.pod.name=pos-streaming-driver \
  --conf spark.kubernetes.driver.podTemplateFile=streaming-job/k8s/driver-pod-template.yaml \
  --conf spark.kubernetes.executor.podTemplateFile=streaming-job/k8s/executor-pod-template.yaml \
  --conf spark.executor.instances=3 \
  --conf spark.executor.cores=1 \
  --conf spark.executor.memory=1200m \
  --conf spark.driver.memory=1200m \
  local:///opt/spark/jars/pos-streaming-aggregator.jar 200 "30 seconds" "1 minute"
```

Then create the Service that lets the health dashboard reach the driver's
Spark UI:

```bash
kubectl apply -f streaming-job/k8s/driver-service.yaml
kubectl get pods -n spark-ops-poc -w
```

Confirm 1 driver + 3 executor pods reach `Running` and stay running
(this job never completes on its own).

---

## Step 6 — Deploy the Health Dashboard

```bash
kubectl apply -f health-dashboard/k8s/deployment.yaml
kubectl apply -f health-dashboard/k8s/service.yaml
kubectl get pods -n spark-ops-poc -l app=streaming-health-dashboard
kubectl port-forward svc/streaming-health-dashboard-svc 8080:8080 -n spark-ops-poc
```

Open `http://localhost:8080` — you should see the dashboard reporting
`Dashboard version: v1`, the Spark application id, and an executor count
of 3, sourced live from Spark's Monitoring REST API on the driver.

---

## Step 7 — Pod Disruption Budgets in Action

```bash
kubectl apply -f streaming-job/k8s/pdb.yaml
kubectl get pdb -n spark-ops-poc
```

Expected output shows `ALLOWED DISRUPTIONS` = 1 for the executors PDB
(3 running, minAvailable 2) and 0 for the driver PDB.

**Demonstrate protection during a node drain:**

```bash
kubectl get nodes
kubectl get pods -n spark-ops-poc -o wide   # note which node hosts the driver

kubectl drain <node-hosting-driver> \
  --ignore-daemonsets \
  --delete-emptydir-data
```

- If the driver pod is on that node: the drain **blocks** on it (PDB
  `maxUnavailable: 0`) until you `kubectl uncordon <node>` or explicitly
  force it — demonstrating that PDBs give you a human decision point
  before a stateful singleton is moved.
- If only executor pods are on that node: the drain proceeds but respects
  `minAvailable: 2`, evicting/rescheduling executors one at a time rather
  than all at once — Spark's scheduler then replaces evicted executors
  automatically (shown in `kubectl get pods -w`).

Uncordon afterward to restore the node:

```bash
kubectl uncordon <node>
```

---

## Step 8 — Rolling Update in Action

Make a small visible change and build `v2`:

```bash
cd health-dashboard
# edit deployment.yaml's APP_VERSION env value locally for reference only —
# the actual version is passed at deploy time via `kubectl set image` +
# a separate `kubectl set env`, or simply bake it into a new image tag.
```

For this lab, tag a new image so the running container reports `v2`:

```bash
docker build -f docker/Dockerfile -t <UNIQUE_ACR_NAME>.azurecr.io/streaming-health-dashboard:v2 .
docker push <UNIQUE_ACR_NAME>.azurecr.io/streaming-health-dashboard:v2
```

Trigger the rolling update:

```bash
kubectl set image deployment/streaming-health-dashboard \
  dashboard=<UNIQUE_ACR_NAME>.azurecr.io/streaming-health-dashboard:v2 \
  -n spark-ops-poc

kubectl set env deployment/streaming-health-dashboard \
  APP_VERSION=v2 -n spark-ops-poc

kubectl rollout status deployment/streaming-health-dashboard -n spark-ops-poc
```

While it rolls out, in a second terminal keep hitting the service to see
zero-downtime in action:

```bash
kubectl port-forward svc/streaming-health-dashboard-svc 8080:8080 -n spark-ops-poc &
while true; do curl -s http://localhost:8080 | grep "Dashboard version"; sleep 1; done
```

You should see requests continue succeeding throughout, flipping from `v1`
to `v2` pod-by-pod because `maxUnavailable: 0` + `maxSurge: 1` guarantees
3 healthy replicas are always serving.

**Roll back:**

```bash
kubectl rollout undo deployment/streaming-health-dashboard -n spark-ops-poc
kubectl rollout history deployment/streaming-health-dashboard -n spark-ops-poc
```

> **Why this isn't done on the Spark driver itself:** the streaming
> driver is a single Pod created directly by `spark-submit`, not a
> Deployment-managed replica set — there is nothing for `kubectl rollout`
> to operate on. To "roll" the streaming job itself in production, you'd
> submit a new spark-submit invocation with an updated image tag,
> let the new driver pick up from its checkpoint location, then tear down
> the old one — a manually-orchestrated blue/green swap, not a native
> Kubernetes rolling update. That distinction is itself worth raising in
> the discussion guide below.

---

## Step 9 — Resource Tuning Verification

```bash
kubectl top pods -n spark-ops-poc
kubectl describe pod pos-streaming-driver -n spark-ops-poc | grep -A5 "Limits\|Requests\|QoS"
```

- Confirm the driver/executor pods show `QoS Class: Burstable` (requests <
  limits, as configured in the pod templates) rather than `BestEffort`
  (no resources set at all — the default if you skip pod templates
  entirely) or `Guaranteed` (requests == limits).
- Watch memory climb slowly over several minutes due to the streaming
  aggregation's window state — this is the direct, observable link between
  `withWatermark(...)` in the code and the `memory.limits` you set in the
  executor pod template. Shrinking the watermark duration reduces retained
  state and therefore executor memory pressure; this is a live tuning knob
  you can demonstrate by resubmitting with a shorter watermark (e.g. `"20
  seconds"`) and comparing `kubectl top pods` memory over time.

---

## Step 10 — Azure Portal Verification

1. Resource group `rg-spark-aks-poc` → `aks-spark-poc` → **Workloads** →
   filter namespace `spark-ops-poc` → confirm driver, 3 executors, and 3
   dashboard replicas.
2. **Insights** blade → Containers view → watch live CPU/memory per pod
   during Steps 7–9.
3. During the Step 7 node drain, watch the **Node pools** blade / cluster
   autoscaler events to see rescheduled pods land on remaining nodes.

---

## 11. Discussion Guide

Use these as talking points/questions in a training session after the
hands-on demo.

### Driver/executor pods
- **Q:** What actually happens on the Kubernetes API server when
  `spark-submit --deploy-mode cluster` runs? *(A: the submission client
  creates a single driver Pod directly via the K8s API; the driver process,
  once running, uses its own K8s client — authorized by the RBAC
  RoleBinding you applied in Step 1 — to create/watch/delete executor
  Pods for the life of the application.)*
- **Q:** Why does the executor pod count in `kubectl get pods` sometimes
  not match `spark.executor.instances`? *(Dynamic allocation, if enabled,
  or executors that failed and are being replaced.)*
- **Q:** What's lost if an executor pod is killed mid-shuffle versus if
  the driver pod is killed? *(Executor loss: Spark recomputes lost
  partitions/shuffle blocks from lineage. Driver loss: the entire
  application — and for streaming, its in-memory query state since last
  checkpoint — is gone; only checkpointing to durable storage
  (not configured in this lab's console-sink demo) would allow resume.)*

### Resource tuning
- **Q:** Why must the pod's memory `limit` exceed
  `spark.executor.memory + spark.executor.memoryOverhead`, not just equal
  `spark.executor.memory`? *(JVM heap isn't the only memory consumer —
  off-heap buffers, native libraries, and OS overhead all count against the
  pod's cgroup limit; setting the pod limit equal to just the heap size
  guarantees an eventual OOMKill.)*
- **Q:** What's the practical difference between a CPU `limit` being hit
  versus a memory `limit` being hit? *(CPU is throttled — the process
  slows down but keeps running; memory limit breach is a hard OOMKill —
  the container is terminated.)*
- **Q:** Given this lab's watermark-bounded streaming state, what would
  happen to memory usage if the watermark were removed entirely? *(State
  would grow unbounded as long as the query runs, since Spark would never
  know it's safe to evict old aggregation buckets — this is the single
  most common root cause of a streaming job that runs fine for hours then
  OOMs.)*

### Pod Disruption Budgets
- **Q:** A PDB blocked a `kubectl drain`. Does that mean the node
  can never be drained? *(No — it means voluntary eviction via the
  Eviction API is refused; an operator can still `--force` it, cordon and
  wait, or scale up capacity elsewhere first. The PDB's purpose is to make
  that a deliberate decision, not to make nodes undrainable.)*
- **Q:** Would this PDB have prevented an outage if the node had simply
  crashed instead of being drained? *(No — PDBs only govern *voluntary*
  disruptions initiated through the Eviction API; hardware failure,
  kubelet crash, or an OOMKill are involuntary and bypass the PDB
  entirely. This is the most commonly misunderstood point about PDBs.)*
- **Q:** Why does the driver's PDB use `maxUnavailable: 0` while the
  executors' uses `minAvailable: 2` out of 3? *(The driver is a
  irreplaceable singleton holding query state — any disruption is
  effectively a full outage, so it's fully protected. Executors are
  interchangeable and Spark already recovers from losing them, so the
  PDB just caps *how many at once*, preserving throughput during a drain
  rather than preventing disruption altogether.)*

### Rolling updates
- **Q:** Why can't `kubectl rollout` be used on the Spark driver pod
  directly? *(It isn't managed by a Deployment/ReplicaSet/StatefulSet —
  spark-submit creates a bare Pod. Rolling-update semantics — surge,
  unavailable budget, revision history — are properties of those
  higher-level controllers, not of Pods themselves.)*
- **Q:** In this lab, what production pattern would actually replace the
  driver pod without downtime? *(Submit the new version as a second,
  independent spark-submit invocation with its own driver pod name — a
  manual blue/green swap — cut traffic over at the ingestion source, then
  tear down the old application. There is no zero-orchestration
  equivalent of `kubectl set image` for a Spark driver itself.)*
- **Q:** During the Step 8 rollout, why did `maxUnavailable: 0` combined
  with `maxSurge: 1` guarantee zero dropped requests? *(A 4th pod is
  created and must pass its readiness probe *before* any of the original
  3 are terminated — the Service's endpoint list only ever shrinks after
  a replacement is already accepting traffic.)*

---

## 12. Cleanup

```bash
kubectl delete namespace spark-ops-poc

# If you're done with the whole series and not just this lab:
az aks delete --resource-group rg-spark-aks-poc --name aks-spark-poc --yes --no-wait
az acr delete --resource-group rg-spark-aks-poc --name <UNIQUE_ACR_NAME> --yes
az group delete --name rg-spark-aks-poc --yes --no-wait
```

---

## 13. Troubleshooting

| Symptom | Root Cause | Fix |
|---|---|---|
| `spark-submit` accepted but no pods appear | Wrong `--conf spark.kubernetes.namespace` or RBAC not applied | Re-check Step 1; confirm `spark-driver-sa` exists in `spark-ops-poc` |
| Dashboard shows "not reachable" for app id | `pos-streaming-driver-ui` Service selector doesn't match the driver pod's labels | Confirm `driver-pod-template.yaml`'s `demoapp: pos-streaming-driver` label actually landed on the running pod: `kubectl get pod pos-streaming-driver -n spark-ops-poc --show-labels` |
| `kubectl drain` hangs indefinitely | Driver PDB `maxUnavailable: 0` correctly blocking eviction | Expected behavior — `kubectl uncordon` to abort, or explicitly accept the outage with `--disable-eviction` |
| Rolling update stuck at "1 out of 3 updated" | New pod failing readiness probe (image pull error, app crash) | `kubectl describe pod <new-pod>` and `kubectl logs <new-pod>` in the namespace |
| Executor pods `OOMKilled` | Pod memory limit lower than `spark.executor.memory + memoryOverhead` | Increase `executor-pod-template.yaml` memory limit or lower `spark.executor.memory` |
| `exec format error` on Apple Silicon | Image built natively `arm64` | Rebuild both images with `--platform linux/amd64` |
