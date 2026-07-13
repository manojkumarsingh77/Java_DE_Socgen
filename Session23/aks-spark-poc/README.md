# AKS Architecture Deep Dive — Guided Lab
## Retail Banking POC: Core Ledger Reconciliation on Java Spark + AKS

**Topics covered (one by one, in this exact order):**
1. Node Pools
2. Autoscaler Internals
3. Network Policies
4. Secrets Management
5. Multi-Env Isolation

**Business scenario:** Core Ledger Reconciliation — raw core-banking ledger
events land in Azure Blob Storage (RAW zone), get reconciled by Spark, and
are upserted into ADLS Gen2 Delta tables (CURATED zone), all orchestrated on
Apache Spark running on Azure Kubernetes Service.

This lab runs **100% offline on your laptop first** (no Azure account
needed), then shows you the **exact same jar** deployed to real AKS with
real Azure Storage once you're ready.

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Prerequisites](#2-prerequisites)
3. [Project Structure](#3-project-structure)
4. [Part A — Run Locally in IntelliJ (macOS M1 Max)](#4-part-a--run-locally-in-intellij-macos-m1-max)
5. [Part A — Run Locally in IntelliJ (Windows 11)](#5-part-a--run-locally-in-intellij-windows-11)
6. [Part B — Understanding the Code, Topic by Topic](#6-part-b--understanding-the-code-topic-by-topic)
7. [Part C — Connecting to Real Azure Storage](#7-part-c--connecting-to-real-azure-storage)
8. [Part D — Provisioning AKS (Node Pools + Autoscaler + Network Policy)](#8-part-d--provisioning-aks)
9. [Part E — Building & Pushing the Container Image](#9-part-e--building--pushing-the-container-image)
10. [Part F — Deploying to AKS (spark-submit + SparkApplication CRD)](#10-part-f--deploying-to-aks)
11. [Part G — Verifying Each Topic on the Live Cluster](#11-part-g--verifying-each-topic-on-the-live-cluster)
12. [Troubleshooting](#12-troubleshooting)
13. [Appendix: Full File Tree](#13-appendix-full-file-tree)

---

## 1. Architecture Overview

```
                         ┌─────────────────────────────────────────────┐
                         │            AKS CLUSTER (aks-banking-poc)      │
                         │                                                │
  Azure Blob Storage     │  ┌──────────┐ ┌──────────┐ ┌───────────────┐  │
  (RAW zone, JSON)  ◄────┼──┤ systempool│ │driverpool│ │ dev/staging/  │  │
        │                │  │(taint:    │ │(nodeSel: │ │ prod userpool │  │
        │ read            │  │Critical  │ │spark-    │ │ (autoscaling  │  │
        ▼                │  │AddonsOnly)│ │driver)   │ │  1..32 nodes) │  │
  ┌─────────────┐         │  └──────────┘ └────┬─────┘ └───────┬───────┘  │
  │ Spark Driver│◄────────┼───────────────────┘               │          │
  │ (Java 17)   │         │       NetworkPolicy: default-deny  │          │
  └──────┬──────┘         │       + explicit allow (7078/7079) │          │
         │ schedules       │                                    ▼          │
         ▼                │                          ┌──────────────────┐│
  ┌─────────────┐         │                          │ Spark Executors  ││
  │  Executors   │◄────────┼─────────────────────────►│ (region-skewed  ││
  │  (N pods)    │         │                          │  data → forces  ││
  └──────┬──────┘         │                          │  autoscale-up)  ││
         │ write           │                          └──────────────────┘│
         ▼                │   Secrets: Key Vault CSI driver mounts       │
  ADLS Gen2 (Delta,        │   /mnt/secrets-store/* into driver+executor  │
  CURATED zone)            │   pods via Workload Identity Federation      │
                           └─────────────────────────────────────────────┘
```

**Data flow:** Synthetic ledger generator → RAW zone (Blob/JSON) → Ledger
Reconciliation transform (window functions, dedup, exception flagging) →
CURATED zone (Delta Lake `MERGE` upsert, partitioned by `region`).

---

## 2. Prerequisites

### Both operating systems

| Tool | Version | Notes |
|---|---|---|
| Java (JDK) | **17** (Temurin/OpenJDK) | Spark 3.5.x officially supports Java 17 |
| IntelliJ IDEA | 2023.3+ (Community or Ultimate) | With the Maven plugin (bundled by default) |
| Apache Maven | 3.9.x | Bundled with IntelliJ, or install standalone |
| Git | any recent | To clone/organize the project |
| Docker Desktop | latest | Only needed for Part E (container build) |
| Azure CLI (`az`) | 2.60+ | Only needed for Part C/D/E/F |
| kubectl | 1.28+ | Only needed for Part F/G |

### macOS (Apple Silicon M1 Max) specific
```bash
brew install --cask temurin17
brew install maven git docker azure-cli kubectl
java -version   # confirm: openjdk version "17.x.x" ... 
```
Confirm you are running the native ARM64 JDK, NOT under Rosetta 2:
```bash
file $(which java)
# expect: Mach-O 64-bit executable arm64
```

### Windows 11 specific
1. Install Temurin 17 MSI: https://adoptium.net/temurin/releases/?version=17
2. Install Maven via https://maven.apache.org/download.cgi (unzip + add
   `bin` to `PATH`), or let IntelliJ use its bundled Maven.
3. **Winutils setup (mandatory for any Hadoop/Spark local file I/O on
   Windows):**
   ```powershell
   mkdir C:\hadoop\bin
   ```
   Download `winutils.exe` and `hadoop.dll` matching Hadoop **3.3.6** from
   the community-maintained mirror:
   `https://github.com/kontext-tech/winutils` → place both files into
   `C:\hadoop\bin`.
   Add `C:\hadoop\bin` to your **System PATH** environment variable, then
   **restart your machine** (not just IntelliJ) so the PATH change is
   picked up by all processes.
4. Install Docker Desktop with WSL2 backend, Azure CLI (MSI installer),
   kubectl (`az aks install-cli`).

---

## 3. Project Structure

```
aks-spark-poc/
├── pom.xml
├── README.md                          <- this file
├── docker/
│   └── Dockerfile
├── k8s/
│   ├── 00-namespaces.yaml
│   ├── 01-network-policies.yaml
│   ├── 02-secrets-csi.yaml
│   ├── 03-resource-quotas.yaml
│   ├── 04-nodepools-azcli.sh
│   ├── 05-cluster-autoscaler-config.yaml
│   ├── 06-sparkapplication-dev.yaml
│   ├── 06-sparkapplication-staging.yaml
│   ├── 06-sparkapplication-prod.yaml
│   └── executor-pod-template.yaml
├── run-configs/
│   ├── intellij-vmoptions-macos-m1.txt
│   ├── intellij-vmoptions-windows.txt
│   └── .env.template
├── scripts/
│   ├── run-local.sh                   <- macOS/Linux one-shot build+run
│   └── run-local.ps1                  <- Windows one-shot build+run
└── src/main/java/com/bankcorp/dataeng/
    ├── App.java                       <- main orchestrator (start here)
    ├── config/
    │   ├── AppConfig.java             <- all runtime parameters
    │   └── EnvironmentProfile.java    <- dev/staging/prod profile record
    ├── data/
    │   ├── TransactionRecord.java     <- ledger event schema
    │   └── SyntheticBankingDataGenerator.java
    ├── pipeline/
    │   ├── NodePoolTopologyAdvisor.java     <- Topic 1
    │   ├── AutoscalerLoadSimulator.java     <- Topic 2
    │   ├── NetworkPolicyValidator.java      <- Topic 3
    │   ├── MultiEnvIsolationGuard.java      <- Topic 5
    │   └── LedgerReconciliationProcessor.java  <- business logic
    ├── security/
    │   └── SecretsResolver.java       <- Topic 4
    └── egress/
        ├── BlobRawIngestor.java       <- RAW zone writer/reader
        └── DeltaLakeWriter.java       <- CURATED Delta zone writer
```

---

## 4. Part A — Run Locally in IntelliJ (macOS M1 Max)

### Step 1 — Open the project
`File → Open` → select the `aks-spark-poc` folder (the one containing
`pom.xml`). Wait for IntelliJ to finish Maven dependency indexing (bottom
status bar). **This first sync will download ~350MB of jars** (Spark,
Delta Lake, Hadoop-Azure, Azure SDK) — takes 2-5 minutes depending on your
connection.

### Step 2 — Verify Project SDK
`File → Project Structure → Project` → set **SDK: 17** and
**Language level: 17**. If Temurin 17 isn't listed, click `Add SDK → JDK`
and browse to `/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home`.

### Step 3 — Create the Run Configuration
1. Open `src/main/java/com/bankcorp/dataeng/App.java`.
2. Click the green ▶ gutter icon next to `public static void main` →
   **Modify Run Configuration...** (do NOT just click Run yet).
3. In **VM options**, paste the entire contents of
   `run-configs/intellij-vmoptions-macos-m1.txt`.
4. Leave **Program arguments** empty (not used).
5. Leave **Environment variables** empty for the default offline run — the
   app defaults `APP_ENV=dev` and `AZURE_MODE=false` via VM options
   already. (You'll populate this field in Part C.)
6. Click **Apply → OK**.

### Step 4 — Run it
Click the green ▶ button. Expected console output (abbreviated):
```
INFO  App - ############################################################
INFO  App - # AKS ARCHITECTURE DEEP DIVE POC - environment = dev
INFO  App - # AZURE_MODE = false
INFO  App - ############################################################
INFO  MultiEnvIsolationGuard - ========== MULTI-ENV ISOLATION GUARD ==========
INFO  MultiEnvIsolationGuard - Isolation checks PASSED for environment 'dev'.
INFO  SecretsResolver - Secret 'dev-adls-storage-key' not found via CSI mount, env var, or Key Vault SDK. Falling back to local dev default...
INFO  NetworkPolicyValidator - ========== NETWORK POLICY EXPECTED TRAFFIC MATRIX ==========
INFO  NodePoolTopologyAdvisor - ========== NODE POOL PLACEMENT PLAN (dev) ==========
INFO  AutoscalerLoadSimulator - ========== AUTOSCALER LOAD SIMULATION START (5 waves) ==========
INFO  AutoscalerLoadSimulator - [WAVE 1/5] rows=250000 partitions=8 ...
INFO  AutoscalerLoadSimulator - [WAVE 5/5] rows=1250000 partitions=40 ...
INFO  LedgerReconciliationProcessor - Exception summary by region (top skew indicator):
+---------------+--------------------------------+---------+-------------+
|region         |reconciliationStatus              |txn_count|total_amount |
+---------------+--------------------------------+---------+-------------+
|APAC-MUMBAI    |OK                                |...      |...          |
...
INFO  App - # POC RUN COMPLETE - all 5 topics executed successfully.
```

Check the output on disk:
```bash
ls -R ./data/dev/raw
ls -R ./data/dev/curated
```
You'll see JSON part-files in `raw/` and a Delta table (`_delta_log/` +
Parquet part-files) in `curated/`.

### Step 5 (alternative) — one-shot terminal script
```bash
chmod +x scripts/run-local.sh
./scripts/run-local.sh dev
```

---

## 5. Part A — Run Locally in IntelliJ (Windows 11)

### Step 1 — Open the project
`File → Open` → select the `aks-spark-poc` folder. Wait for Maven sync.

### Step 2 — Verify Project SDK
`File → Project Structure → Project` → **SDK: 17**, **Language level: 17**.

### Step 3 — Create the Run Configuration
1. Open `App.java` → gutter ▶ → **Modify Run Configuration...**
2. Paste the entire contents of
   `run-configs\intellij-vmoptions-windows.txt` into **VM options**. This
   includes `-Dhadoop.home.dir=C:\hadoop` — make sure you completed the
   winutils setup in [Prerequisites](#2-prerequisites) first.
3. Click **Apply → OK**, then Run.

### Step 3 (alternative) — PowerShell one-shot script
```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\scripts\run-local.ps1 -EnvName dev
```

Expected output is identical to the macOS run in Section 4, Step 4.
Verify with:
```powershell
Get-ChildItem -Recurse .\data\dev\raw
Get-ChildItem -Recurse .\data\dev\curated
```

---

## 6. Part B — Understanding the Code, Topic by Topic

### Topic 1: Node Pools — `pipeline/NodePoolTopologyAdvisor.java`
AKS separates compute into 4 **node pools**, each a distinct Azure VMSS
(Virtual Machine Scale Set) with its own VM SKU, taint, and autoscaling
range:

| Pool | VM SKU | Taint | Purpose |
|---|---|---|---|
| `systempool` | Standard_D4s_v5 | `CriticalAddonsOnly=true:NoSchedule` | CoreDNS, metrics-server, CSI driver — never runs user workload |
| `driverpool` | Standard_D8s_v5 | none (nodeSelector `role=spark-driver`) | Spark driver pods ONLY, isolated from executor OOM-kills |
| `dev-userpool` / `staging-userpool` | Standard_D8s_v5 | `workload=<env>-spark:NoSchedule` | Executor pods for non-prod |
| `prodhighmem` | Standard_E16s_v5 (memory-optimized) | `workload=prod-spark:NoSchedule` | Executor pods for prod, sized to absorb region-skewed shuffle partitions |

`App.java` sets `spark.kubernetes.node.selector.agentpool` from
`EnvironmentProfile.executorNodeSelector()`, and
`k8s/executor-pod-template.yaml` carries the matching `nodeSelector` +
`tolerations`. **Method to look at:**
`NodePoolTopologyAdvisor.reportPlannedTopology()` — validates that your
requested executor shape (`spark.executor.cores` / `spark.executor.memory`)
actually fits the target VM SKU's allocatable capacity, and warns if a
prod job under-requests cores relative to the `E16s_v5` design.

### Topic 2: Autoscaler Internals — `pipeline/AutoscalerLoadSimulator.java`
Two autoscalers cooperate:
- **Spark Dynamic Allocation** (JVM-level): `ExecutorAllocationManager`
  polls the pending-task backlog every `spark.dynamicAllocation.schedulerBacklogTimeout`
  (1s here) and requests more executors when
  `numRunningTasks < numPendingTasks`.
- **AKS Cluster Autoscaler** (infra-level): watches for `Pending` pods with
  `FailedScheduling` events (insufficient node capacity) every
  `scan-interval` (10s), and calls the Azure VMSS API to add nodes.

`AutoscalerLoadSimulator.runProgressiveLoadWaves()` runs 5 waves of
growing data volume and partition count
(`numSlices = wave * 8`), deliberately outpacing `maxExecutors` so you can
see, in the log, exactly the point where a real AKS deployment would
transition from Spark-level scaling to infra-level node scale-up. See
`k8s/05-cluster-autoscaler-config.yaml` for every tunable parameter
(`scale-down-unneeded-time`, `scale-down-utilization-threshold`,
`expander`) with a mechanical explanation of each.

### Topic 3: Network Policies — `pipeline/NetworkPolicyValidator.java`
`k8s/01-network-policies.yaml` enforces **default-deny** ingress+egress per
namespace, then punches explicit holes for:
- Driver↔executor traffic (BlockManager ports 7078/7079)
- Egress to Azure Storage + Key Vault (443, via Azure service-tag `ipBlock` CIDRs)
- DNS (port 53 to `kube-dns`)
- An explicit, auditable deny of cross-namespace (cross-env) traffic

`NetworkPolicyValidator.validateExpectedTrafficMatrix()` performs live TCP
probes to `dfs.core.windows.net`, `blob.core.windows.net`,
`vault.azure.net`, and a public-internet control (`example.com`), then
prints whether each result **matches the intended AKS policy** — a
pre-flight smoke test pattern.

### Topic 4: Secrets Management — `security/SecretsResolver.java`
Three-tier resolution chain (mirrors real production behavior):
1. **CSI file mount** `/mnt/secrets-store/<name>` — populated by the Azure
   Key Vault CSI Provider (`k8s/02-secrets-csi.yaml`), never touches etcd.
2. **K8s Secret env var** — `envFrom.secretRef` synced from Key Vault via
   `secretObjects` in the `SecretProviderClass`.
3. **Direct Key Vault SDK call** — `SecretClient.getSecret()` using
   `DefaultAzureCredential` (Workload Identity Federation).
4. **Local dev fallback** — so the POC never crashes offline.

`App.main()` calls `secretsResolver.resolve(envProfile.keyVaultSecretPrefix() + "adls-storage-key", ...)`
— trace this call to see the fallback chain execute in the logs.

### Topic 5: Multi-Env Isolation — `pipeline/MultiEnvIsolationGuard.java` + `config/EnvironmentProfile.java`
Four independent isolation layers, all validated **before any data moves**:
1. **Namespace** — `banking-dev` / `banking-staging` / `banking-prod`
2. **Compute** — node pool taint/toleration pairs
3. **Data** — distinct ADLS containers (`raw-dev` vs `raw-prod`, etc.)
4. **Network** — NetworkPolicy denies all cross-namespace traffic

`MultiEnvIsolationGuard.enforce()` throws `IllegalStateException` and
**aborts the entire job** if `POD_NAMESPACE` (injected via K8s Downward
API) doesn't match the namespace implied by `APP_ENV` — this is a fail-fast
guardrail against a misconfigured deployment leaking prod credentials into
dev, or vice versa.

---

## 7. Part C — Connecting to Real Azure Storage

> Skip this section if you're happy running fully offline. Everything in
> Part A already works with zero Azure account.

### Step 1 — Create the storage account and containers
```bash
az group create --name rg-banking-spark-poc --location centralindia

az storage account create \
  --name <yourstorageacct> \
  --resource-group rg-banking-spark-poc \
  --location centralindia \
  --sku Standard_LRS \
  --kind StorageV2 \
  --hierarchical-namespace true    # required for ADLS Gen2 (Delta Lake)

for c in raw-dev curated-dev raw-staging curated-staging raw-prod curated-prod; do
  az storage container create --account-name <yourstorageacct> --name "$c"
done
```

### Step 2 — Get your storage account key
```bash
az storage account keys list \
  --account-name <yourstorageacct> \
  --resource-group rg-banking-spark-poc \
  --query "[0].value" -o tsv
```

### Step 3 — Fill in `.env`
```bash
cp run-configs/.env.template run-configs/.env
```
Edit `run-configs/.env`:
```ini
AZURE_MODE=true
AZURE_STORAGE_ACCOUNT_NAME=<yourstorageacct>
AZURE_STORAGE_ACCOUNT_KEY=<the key from Step 2>
AZURE_ADLS_FILESYSTEM_RAW=raw-dev
AZURE_ADLS_FILESYSTEM_CURATED=curated-dev
```

### Step 4 — Run against real Azure
```bash
./scripts/run-local.sh dev          # macOS/Linux
.\scripts\run-local.ps1 -EnvName dev # Windows
```
The app will now write to
`abfss://raw-dev@<yourstorageacct>.dfs.core.windows.net/...` — verify in
the Azure Portal under Storage Account → Containers → `raw-dev`.

### Step 5 (optional) — Azure Key Vault for Secrets Management topic
```bash
az keyvault create --name <yourkeyvault> --resource-group rg-banking-spark-poc --location centralindia
az keyvault secret set --vault-name <yourkeyvault> --name dev-adls-storage-key --value "<the key from Step 2>"
```
Add to `.env`:
```ini
AZURE_KEYVAULT_URI=https://<yourkeyvault>.vault.azure.net/
AZURE_TENANT_ID=<your tenant id>
```
Run `az login` before executing the app locally so
`DefaultAzureCredential` can pick up your interactive Azure CLI session —
this exercises `SecretsResolver`'s Tier 3 (direct Key Vault SDK) path.

---

## 8. Part D — Provisioning AKS

> Requires an active Azure subscription with Owner/Contributor rights.

### Step 1 — Edit the provisioning script
Open `k8s/04-nodepools-azcli.sh` and set:
```bash
RG="rg-banking-spark-poc"
CLUSTER_NAME="aks-banking-poc"
LOCATION="centralindia"     # change to your preferred region
```

### Step 2 — Run it
```bash
chmod +x k8s/04-nodepools-azcli.sh
./k8s/04-nodepools-azcli.sh
```
This provisions the cluster with Calico network policy, Workload Identity,
OIDC issuer, and Key Vault CSI addon enabled, then adds all 4 node pools
(`driverpool`, `devuserpool`, `stagingpool`, `prodhighmem`) with the
autoscaling ranges described in Section 6, Topic 1. **Takes ~15-20
minutes.**

### Step 3 — Get cluster credentials
```bash
az aks get-credentials --resource-group rg-banking-spark-poc --name aks-banking-poc
kubectl get nodes -o wide   # confirm 4 node pools are Ready
```

### Step 4 — Apply namespaces, quotas, and network policies
```bash
kubectl apply -f k8s/00-namespaces.yaml
kubectl apply -f k8s/03-resource-quotas.yaml
kubectl apply -f k8s/01-network-policies.yaml
```
> **Important:** `01-network-policies.yaml` is written for `banking-dev`.
> Duplicate the file (or use `kubectl apply -f - --namespace=banking-staging`
> with `kustomize`/`envsubst`) to apply the same policies to
> `banking-staging` and `banking-prod`.

### Step 5 — Configure the cluster autoscaler profile
```bash
az aks update \
  --resource-group rg-banking-spark-poc \
  --name aks-banking-poc \
  --cluster-autoscaler-profile \
      scan-interval=10s \
      scale-down-delay-after-add=10m \
      scale-down-unneeded-time=10m \
      scale-down-utilization-threshold=0.5 \
      expander=least-waste
```

### Step 6 — Set up Workload Identity Federation + Key Vault CSI
```bash
export AKS_OIDC_ISSUER=$(az aks show --resource-group rg-banking-spark-poc \
  --name aks-banking-poc --query "oidcIssuerProfile.issuerUrl" -o tsv)

az identity create --name spark-workload-identity --resource-group rg-banking-spark-poc

export UAMI_CLIENT_ID=$(az identity show --name spark-workload-identity \
  --resource-group rg-banking-spark-poc --query "clientId" -o tsv)

az role assignment create \
  --assignee "$UAMI_CLIENT_ID" \
  --role "Storage Blob Data Contributor" \
  --scope "/subscriptions/<sub-id>/resourceGroups/rg-banking-spark-poc/providers/Microsoft.Storage/storageAccounts/<yourstorageacct>"

az identity federated-credential create \
  --name spark-fed-credential \
  --identity-name spark-workload-identity \
  --resource-group rg-banking-spark-poc \
  --issuer "$AKS_OIDC_ISSUER" \
  --subject "system:serviceaccount:banking-dev:spark-workload-identity-sa"
```
Update `k8s/02-secrets-csi.yaml` and `k8s/executor-pod-template.yaml`
placeholders (`<AZURE_WORKLOAD_IDENTITY_CLIENT_ID>`, `<YOUR_KEYVAULT_NAME>`,
`<YOUR_AZURE_TENANT_ID>`) with the values above, then:
```bash
kubectl apply -f k8s/02-secrets-csi.yaml
```

---

## 9. Part E — Building & Pushing the Container Image

### Step 1 — Create an Azure Container Registry
```bash
az acr create --resource-group rg-banking-spark-poc --name <youracr> --sku Basic
az aks update --resource-group rg-banking-spark-poc --name aks-banking-poc --attach-acr <youracr>
```

### Step 2 — Build (multi-arch aware, since you may build on M1 Max but
### deploy to amd64 AKS nodes)
```bash
az acr login --name <youracr>
docker buildx create --use --name aks-poc-builder || true

docker buildx build \
  --platform linux/amd64 \
  -t <youracr>.azurecr.io/aks-spark-poc:1.0.0 \
  -f docker/Dockerfile \
  --push \
  .
```
> On Windows 11, `docker buildx build --platform linux/amd64 ...` works
> identically via Docker Desktop's WSL2 backend — no changes needed.

### Step 3 — Update the SparkApplication manifests
In each of `k8s/06-sparkapplication-{dev,staging,prod}.yaml`, replace
`<YOUR_ACR_NAME>` and `<YOUR_STORAGE_ACCOUNT_NAME>` with your real values.

---

## 10. Part F — Deploying to AKS

### Option 1 — Spark Operator (recommended, matches the CRDs in this repo)
```bash
helm repo add spark-operator https://kubeflow.github.io/spark-operator
helm repo update
helm install spark-operator spark-operator/spark-operator \
  --namespace spark-operator --create-namespace \
  --set webhook.enable=true

kubectl apply -f k8s/06-sparkapplication-dev.yaml
kubectl get sparkapplications -n banking-dev -w
kubectl logs -n banking-dev -l spark-role=driver -f
```

### Option 2 — Raw `spark-submit` in cluster mode (no operator)
```bash
spark-submit \
  --master k8s://https://<AKS_API_SERVER_FQDN>:443 \
  --deploy-mode cluster \
  --name ledger-reconciliation-dev \
  --class com.bankcorp.dataeng.App \
  --conf spark.kubernetes.namespace=banking-dev \
  --conf spark.kubernetes.container.image=<youracr>.azurecr.io/aks-spark-poc:1.0.0 \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-workload-identity-sa \
  --conf spark.kubernetes.driver.label.role=spark-driver \
  --conf spark.kubernetes.driver.node.selector.role=spark-driver \
  --conf spark.kubernetes.executor.label.role=spark-executor \
  --conf spark.kubernetes.node.selector.agentpool=dev-userpool \
  --conf spark.kubernetes.executor.podTemplateFile=/opt/spark/conf/executor-pod-template.yaml \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.dynamicAllocation.shuffleTracking.enabled=true \
  --conf spark.dynamicAllocation.minExecutors=1 \
  --conf spark.dynamicAllocation.maxExecutors=4 \
  --conf spark.executor.cores=4 \
  --conf spark.executor.memory=8g \
  --conf spark.driver.memory=4g \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.kubernetes.driverEnv.APP_ENV=dev \
  --conf spark.kubernetes.driverEnv.AZURE_MODE=true \
  --conf spark.kubernetes.driverEnv.AZURE_STORAGE_ACCOUNT_NAME=<yourstorageacct> \
  local:///opt/spark/jars/aks-spark-poc-shaded.jar
```

---

## 11. Part G — Verifying Each Topic on the Live Cluster

```bash
# Topic 1: Node Pools — confirm driver/executor pods land on correct pools
kubectl get pods -n banking-dev -o wide

# Topic 2: Autoscaler Internals — watch nodes scale up under load
kubectl get nodes -w
kubectl get events -n banking-dev --field-selector reason=FailedScheduling

# Topic 3: Network Policies — confirm egress allow-list works, cross-env denied
kubectl exec -it <driver-pod> -n banking-dev -- nc -zv dfs.core.windows.net 443
kubectl exec -it <driver-pod> -n banking-dev -- nc -zv example.com 443   # should hang/timeout

# Topic 4: Secrets Management — confirm CSI mount populated
kubectl exec -it <driver-pod> -n banking-dev -- ls -la /mnt/secrets-store

# Topic 5: Multi-Env Isolation — confirm namespace-scoped quota enforcement
kubectl describe resourcequota banking-dev-quota -n banking-dev
```

---

## 12. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `InaccessibleObjectException` on startup | Missing `--add-opens` flags | Re-paste the full VM options file for your OS |
| `Could not locate executable null\bin\winutils.exe` | Windows winutils not configured | Complete Prerequisites Step 3; confirm `-Dhadoop.home.dir=C:\hadoop` is in VM options |
| `NoSuchMethodError` on Delta/Spark classes | Scala version mismatch | Confirm `scala.binary.version=2.12` in `pom.xml` matches all `_2.12` artifact suffixes |
| App throws `FATAL ISOLATION BREACH` | `POD_NAMESPACE` env var doesn't match `APP_ENV` | Locally this defaults correctly; on AKS check the SparkApplication CRD's `metadata.namespace` matches `APP_ENV` |
| `mvn dependency:go-offline` fails in Docker build | No network egress from build agent | Ensure your CI/build host allows `repo.maven.apache.org` |
| Executor pods stuck `Pending` forever on AKS | Node pool taint/toleration mismatch | Confirm `k8s/executor-pod-template.yaml` toleration `value` matches the taint set in `04-nodepools-azcli.sh` for that pool |
| `AZURE_MODE=true` but writes fail with 403 | Storage key wrong or container missing | Re-run Part C Steps 1-2; confirm container names match `AZURE_ADLS_FILESYSTEM_*` |

---

## 13. Appendix: Full File Tree

See [Section 3](#3-project-structure) above for the complete annotated tree.
