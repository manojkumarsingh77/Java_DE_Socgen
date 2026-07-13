# Guided Lab: Java 17 + Apache Spark 3.5 on Azure Kubernetes Service (AKS)
### Retail Banking Fraud Pre-Processor — End-to-End POC

**Azure Subscription:** `npunext-1680261103285`
**IDE:** IntelliJ IDEA · **Runtime:** Java 17 (Temurin) · **Engine:** Apache Spark 3.5.1 · **Target:** AKS

This lab is deliberately scoped as a **foundation module**: one Spark batch job,
generating its own synthetic dataset, run first locally in IntelliJ, then
containerized and submitted to a real AKS cluster — so you can see the exact
mechanics of how Spark-on-Kubernetes works before layering in streaming,
Delta Lake, or Event Hubs in later modules.

Every resource name below (resource group, ACR name, cluster name) is an
**explicit placeholder**. Replace `<...>` values with your own — do not
assume they already exist.

---

## Table of Contents
1. [Architecture Overview](#1-architecture-overview)
2. [Prerequisites](#2-prerequisites-macos-m1-max--windows-11)
3. [Project Structure](#3-project-structure)
4. [Step 1 — Azure Login & Resource Group](#step-1--azure-login--resource-group)
5. [Step 2 — Create Azure Container Registry (ACR)](#step-2--create-azure-container-registry-acr)
6. [Step 3 — Create the AKS Cluster](#step-3--create-the-aks-cluster)
7. [Step 4 — Namespace & RBAC for the Spark Driver](#step-4--namespace--rbac-for-the-spark-driver)
8. [Step 5 — Open & Run the Job Locally in IntelliJ](#step-5--open--run-the-job-locally-in-intellij)
9. [Step 6 — Build the Spark-on-Kubernetes Base Image](#step-6--build-the-spark-on-kubernetes-base-image)
10. [Step 7 — Build & Push the Application Image](#step-7--build--push-the-application-image)
11. [Step 8 — spark-submit to AKS](#step-8--spark-submit-to-aks)
12. [Step 9 — Observe Execution (kubectl + Azure Portal)](#step-9--observe-execution-kubectl--azure-portal)
13. [Step 10 — Retrieve Results](#step-10--retrieve-results)
14. [Step 11 — Cleanup](#step-11--cleanup-avoid-ongoing-cost)
15. [Troubleshooting](#12-troubleshooting)

---

## 1. Architecture Overview

```
 ┌─────────────────────┐        docker build         ┌──────────────────────┐
 │   IntelliJ IDEA      │ ───────────────────────────▶│  Local Docker Engine  │
 │  (Java 17 source,    │                              │  (Apple Silicon ARM64│
 │   local run first)   │                              │   or Windows x64)    │
 └──────────┬───────────┘                              └──────────┬───────────┘
            │ mvn package (fat jar)                                │ docker push
            ▼                                                      ▼
 ┌──────────────────────┐                              ┌──────────────────────┐
 │ fraud-preprocessor.jar│                              │  Azure Container     │
 └──────────────────────┘                              │  Registry (ACR)      │
                                                          └──────────┬───────────┘
                                                                     │ image pull
                                                                     ▼
 spark-submit --master k8s://...          ┌─────────────────────────────────────┐
 (run from your workstation) ────────────▶│           AKS Cluster                │
                                            │  namespace: spark-poc               │
                                            │  ┌────────────┐   ┌──────────────┐  │
                                            │  │ Driver Pod │──▶│ Executor Pod │  │
                                            │  └────────────┘   │ Executor Pod │  │
                                            │                    └──────────────┘  │
                                            └─────────────────────────────────────┘
                                                             │
                                                             ▼
                                                  Azure Portal (Workloads, Logs,
                                                  Insights) — visual verification
```

**What the job does:** generates ~50,000 synthetic retail-banking card
transactions in-memory, applies rule-based fraud risk scoring (foreign +
large-amount + unusual-hour = HIGH risk), aggregates flagged exposure per
account, prints the Catalyst physical plan, and writes flagged records as
partitioned Parquet.

---

## 2. Prerequisites (macOS M1 Max & Windows 11)

Install on **both** workstations:

| Tool | macOS (Apple Silicon) | Windows 11 (x64) |
|---|---|---|
| Java 17 (Temurin) | `brew install --cask temurin17` | `winget install EclipseAdoptium.Temurin.17.JDK` |
| Maven 3.9+ | `brew install maven` | `winget install Apache.Maven` |
| Azure CLI | `brew install azure-cli` | `winget install Microsoft.AzureCLI` |
| kubectl | `az aks install-cli` (installs for either OS) | same |
| Docker | Docker Desktop for Apple Silicon | Docker Desktop (WSL2 backend enabled) |
| IntelliJ IDEA | Latest, Community or Ultimate | Latest |
| Hadoop `winutils.exe` | not required | Required — see Step 5 note |

Also download the **prebuilt Spark 3.5.1 (Hadoop 3) binary distribution**
from the official Apache Spark archive and extract it locally on
**both** machines — you will use its `bin/spark-submit` and
`bin/docker-image-tool.sh` scripts directly (these are not Maven
dependencies, they are CLI tooling):

```bash
tar -xzf spark-3.5.1-bin-hadoop3.tgz
cd spark-3.5.1-bin-hadoop3
```

> ⚠️ **Apple Silicon note:** AKS node pools default to `amd64` (x86_64)
> VM sizes. Every Docker image you build on the M1 Max must be built for
> `linux/amd64`, not the host's native `arm64`, or pods will fail with
> `exec format error`. This is called out explicitly in Steps 6 and 7.

---

## 3. Project Structure

```
spark-aks-fraud-poc/
├── pom.xml
├── LAB_GUIDE.md
├── docker/
│   └── Dockerfile
├── k8s/
│   └── spark-rbac.yaml
└── src/main/java/com/npunext/bank/fraud/
    ├── model/Transaction.java
    ├── generator/SyntheticDataGenerator.java
    └── job/FraudPreProcessorJob.java
```

---

## Step 1 — Azure Login & Resource Group

```bash
az login
az account set --subscription "npunext-1680261103285"
az account show --output table   # confirm the active subscription

az group create \
  --name rg-spark-aks-poc \
  --location centralindia
```

> Replace `centralindia` with your preferred region if different — this is
> a placeholder, not a requirement.

---

## Step 2 — Create Azure Container Registry (ACR)

ACR names must be **globally unique**, lowercase alphanumeric only.

```bash
az acr create \
  --resource-group rg-spark-aks-poc \
  --name <UNIQUE_ACR_NAME> \
  --sku Basic

az acr login --name <UNIQUE_ACR_NAME>

# Note the login server for later steps:
az acr show --name <UNIQUE_ACR_NAME> --query loginServer --output tsv
```

Record the output — it will look like `<UNIQUE_ACR_NAME>.azurecr.io`.
This exact string is used in Steps 6–8.

---

## Step 3 — Create the AKS Cluster

```bash
az aks create \
  --resource-group rg-spark-aks-poc \
  --name aks-spark-poc \
  --node-count 2 \
  --node-vm-size Standard_D4s_v5 \
  --generate-ssh-keys \
  --attach-acr <UNIQUE_ACR_NAME> \
  --enable-managed-identity
```

`--attach-acr` grants the AKS cluster's kubelet identity `AcrPull` on your
registry, so pods can pull images without a separate `imagePullSecret`.

Fetch cluster credentials into your local kubeconfig:

```bash
az aks get-credentials \
  --resource-group rg-spark-aks-poc \
  --name aks-spark-poc

kubectl get nodes
```

You should see 2 nodes in `Ready` state — this is your first concrete
confirmation that AKS is provisioned and reachable.

---

## Step 4 — Namespace & RBAC for the Spark Driver

Spark's driver pod must be authorized to create, watch, and delete executor
pods inside the cluster — this is not automatic.

```bash
kubectl create namespace spark-poc
kubectl apply -f k8s/spark-rbac.yaml
kubectl get serviceaccount -n spark-poc
```

You should see `spark-driver-sa` listed.

---

## Step 5 — Open & Run the Job Locally in IntelliJ

1. **File → Open** → select `spark-aks-fraud-poc/pom.xml` → *Open as Project*.
2. Let Maven resolve dependencies (`spark-core`, `spark-sql`, both scoped
   `provided` since the AKS runtime image supplies them — see the note below).
3. **Run → Edit Configurations → Add New → Application**
   - **Main class:** `com.npunext.bank.fraud.job.FraudPreProcessorJob`
   - **Program arguments:** `5000 /tmp/fraud-preprocessor-output`
   - ☑️ **"Include dependencies with 'Provided' scope"** — this checkbox is
     mandatory; without it Spark classes are absent from the local run
     classpath even though they compile fine.

### VM Options — macOS (Apple Silicon M1 Max)
```
-Xms2g -Xmx4g
-Dspark.master.override=local[*]
--add-opens=java.base/java.lang=ALL-UNNAMED
--add-opens=java.base/java.lang.invoke=ALL-UNNAMED
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED
--add-opens=java.base/java.io=ALL-UNNAMED
--add-opens=java.base/java.net=ALL-UNNAMED
--add-opens=java.base/java.nio=ALL-UNNAMED
--add-opens=java.base/java.util=ALL-UNNAMED
--add-opens=java.base/java.util.concurrent=ALL-UNNAMED
--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED
--add-opens=java.base/sun.nio.cs=ALL-UNNAMED
--add-opens=java.base/sun.security.action=ALL-UNNAMED
--add-opens=java.base/sun.util.calendar=ALL-UNNAMED
```

### VM Options — Windows 11 (x64)
Same flags **plus** a Hadoop home directory, because `winutils.exe` is
required by Spark's `FileOutputCommitter` on Windows even for purely local
Parquet writes:

```
-Xms2g -Xmx4g
-Dspark.master.override=local[*]
-Dhadoop.home.dir=C:\hadoop
--add-opens=java.base/java.lang=ALL-UNNAMED
--add-opens=java.base/java.lang.invoke=ALL-UNNAMED
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED
--add-opens=java.base/java.io=ALL-UNNAMED
--add-opens=java.base/java.net=ALL-UNNAMED
--add-opens=java.base/java.nio=ALL-UNNAMED
--add-opens=java.base/java.util=ALL-UNNAMED
--add-opens=java.base/java.util.concurrent=ALL-UNNAMED
--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED
--add-opens=java.base/sun.nio.cs=ALL-UNNAMED
--add-opens=java.base/sun.security.action=ALL-UNNAMED
--add-opens=java.base/sun.util.calendar=ALL-UNNAMED
```

Place `winutils.exe` (matching Hadoop 3.x) inside `C:\hadoop\bin\` before
running.

4. Click **Run**. Expected console output ends with:
```
=== Summary: 5000 transactions processed, NN flagged HIGH risk (X.XXX%) ===
Flagged transactions written to: /tmp/fraud-preprocessor-output
```

This confirms the job logic is correct **before** you spend time on
containers or cloud infrastructure.

---

## Step 6 — Build the Spark-on-Kubernetes Base Image

From inside the extracted `spark-3.5.1-bin-hadoop3` directory (not your
IntelliJ project):

```bash
# macOS M1 Max — force linux/amd64 to match AKS node architecture
export DOCKER_DEFAULT_PLATFORM=linux/amd64

./bin/docker-image-tool.sh \
  -r <UNIQUE_ACR_NAME>.azurecr.io \
  -t 3.5.1 \
  build
```

```bash
# Windows 11 (x64) — native architecture already matches AKS nodes
./bin/docker-image-tool.sh -r <UNIQUE_ACR_NAME>.azurecr.io -t 3.5.1 build
```

> Open `kubernetes/dockerfiles/spark/Dockerfile` in the extracted
> distribution and confirm the `java_image_tag` build argument — Spark
> 3.5.x's shipped default already targets a Java 17 base, so no override is
> normally needed. Verify this on your own copy rather than assuming it,
> since it can vary by exact patch release.

Push the base image to ACR:

```bash
./bin/docker-image-tool.sh -r <UNIQUE_ACR_NAME>.azurecr.io -t 3.5.1 push
```

---

## Step 7 — Build & Push the Application Image

From your **IntelliJ project root** (`spark-aks-fraud-poc/`):

```bash
# macOS M1 Max
docker buildx build \
  --platform linux/amd64 \
  -f docker/Dockerfile \
  --build-arg SPARK_BASE_IMAGE=<UNIQUE_ACR_NAME>.azurecr.io/spark:3.5.1 \
  -t <UNIQUE_ACR_NAME>.azurecr.io/fraud-preprocessor:v1 \
  --push \
  .
```

```powershell
# Windows 11 (x64)
docker build `
  -f docker/Dockerfile `
  --build-arg SPARK_BASE_IMAGE=<UNIQUE_ACR_NAME>.azurecr.io/spark:3.5.1 `
  -t <UNIQUE_ACR_NAME>.azurecr.io/fraud-preprocessor:v1 `
  .

docker push <UNIQUE_ACR_NAME>.azurecr.io/fraud-preprocessor:v1
```

Verify the image landed in ACR:

```bash
az acr repository list --name <UNIQUE_ACR_NAME> --output table
az acr repository show-tags --name <UNIQUE_ACR_NAME> --repository fraud-preprocessor --output table
```

---

## Step 8 — spark-submit to AKS

Get the AKS API server URL:

```bash
kubectl cluster-info
# Look for: "Kubernetes control plane is running at https://<AKS_API_SERVER>"
```

From the extracted `spark-3.5.1-bin-hadoop3` directory:

```bash
./bin/spark-submit \
  --master k8s://https://<AKS_API_SERVER> \
  --deploy-mode cluster \
  --name fraud-preprocessor \
  --class com.npunext.bank.fraud.job.FraudPreProcessorJob \
  --conf spark.kubernetes.namespace=spark-poc \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-driver-sa \
  --conf spark.kubernetes.container.image=<UNIQUE_ACR_NAME>.azurecr.io/fraud-preprocessor:v1 \
  --conf spark.kubernetes.driver.pod.name=fraud-preprocessor-driver \
  --conf spark.executor.instances=2 \
  --conf spark.driver.memory=1g \
  --conf spark.executor.memory=2g \
  --conf spark.executor.cores=1 \
  local:///opt/spark/jars/fraud-preprocessor.jar 50000 /tmp/fraud-preprocessor-output
```

Key points on this command:
- `k8s://https://<AKS_API_SERVER>` — the `k8s://` scheme tells Spark's
  cluster manager to use the Kubernetes API to launch pods rather than a
  standalone or YARN scheduler.
- `--deploy-mode cluster` — the **driver itself** runs inside a pod on
  AKS (not on your laptop); only the submission client runs locally.
- `local:///opt/spark/jars/fraud-preprocessor.jar` — the `local://` scheme
  tells Spark the jar is already present **inside the container image**
  at that path (placed there by the `Dockerfile` in Step 7), so no jar
  upload/staging step is needed.
- Kubernetes authentication is inherited from your current `kubectl`
  context (`~/.kube/config`, populated by `az aks get-credentials` in
  Step 3) — Spark's Kubernetes client library reads the same default
  kubeconfig kubectl uses.

---

## Step 9 — Observe Execution (kubectl + Azure Portal)

**Via kubectl:**
```bash
kubectl get pods -n spark-poc -w
# fraud-preprocessor-driver        1/1  Running
# fraud-preprocessor-...-exec-1    1/1  Running
# fraud-preprocessor-...-exec-2    1/1  Running

kubectl logs -f fraud-preprocessor-driver -n spark-poc
```

You should see the same `=== Physical Execution Plan ===`, top-20 accounts
table, and summary line you saw locally in Step 5 — now executed
distributed across executor pods instead of `local[*]` threads.

**Via Azure Portal:**
1. Portal → your resource group `rg-spark-aks-poc` → `aks-spark-poc`.
2. **Workloads** blade → filter namespace `spark-poc` → confirm the driver
   pod and executor pods are listed with status `Running`/`Succeeded`.
3. **Insights** blade → view live CPU/memory per pod during the run.
4. Resource group → `<UNIQUE_ACR_NAME>` → **Repositories** → confirm the
   `spark` and `fraud-preprocessor` image tags you pushed.

---

## Step 10 — Retrieve Results

`--deploy-mode cluster` terminates and removes the driver pod shortly
after completion, and the output path (`/tmp/...`) is written to the
**container's own ephemeral filesystem** — it is not retrievable after the
pod is deleted. For this foundation demo, treat the `kubectl logs` output
(Step 9) as the result artifact.

> For a production pattern, mount an Azure Files-backed `PersistentVolumeClaim`
> or write `scored.write().parquet(...)` directly to an `abfss://` path
> backed by ADLS Gen2 — this is covered in the Data Lake Architecture module
> of the curriculum, not this foundation lab.

---

## Step 11 — Cleanup (avoid ongoing cost)

```bash
kubectl delete namespace spark-poc
az aks delete --resource-group rg-spark-aks-poc --name aks-spark-poc --yes --no-wait
az acr delete --resource-group rg-spark-aks-poc --name <UNIQUE_ACR_NAME> --yes
az group delete --name rg-spark-aks-poc --yes --no-wait
```

---

## 12. Troubleshooting

| Symptom | Root Cause | Fix |
|---|---|---|
| `ImagePullBackOff` | ACR not attached, or wrong image tag | Re-run `az aks update --attach-acr <name>`; confirm tag with `az acr repository show-tags` |
| `exec /opt/spark/bin/... exec format error` | Image built for `arm64` on M1 Max, node is `amd64` | Rebuild with `--platform linux/amd64` / `DOCKER_DEFAULT_PLATFORM=linux/amd64` |
| Driver pod `Forbidden: cannot create pods` | Missing/incorrect RBAC | Re-apply `k8s/spark-rbac.yaml`; confirm `serviceAccountName` matches `--conf spark.kubernetes.authenticate.driver.serviceAccountName` |
| Local run: `NoClassDefFoundError: org/apache/spark/...` | "Provided" scope dependencies not on run classpath | Enable "Include dependencies with 'Provided' scope" in the IntelliJ run configuration |
| Local run on Windows: `UnsatisfiedLinkError` on `NativeIO` | Missing `winutils.exe` | Set `-Dhadoop.home.dir=C:\hadoop` and place a matching `winutils.exe` in `C:\hadoop\bin` |
| `spark-submit` hangs after driver pod `Completed` | Executors not yet garbage-collected | Harmless — check `kubectl get pods -n spark-poc` for final `Completed`/`Succeeded` states |

---

### What's Next

This foundation lab intentionally used a **single batch job with in-code
synthetic data**. The next modules in the curriculum build directly on this
same AKS cluster and ACR:
- Structured Streaming with Event Hubs ingestion (stateful, watermarking).
- Delta Lake write/merge patterns replacing the plain Parquet egress here.
- Prometheus + Grafana wired to this same namespace for SLA dashboards.
