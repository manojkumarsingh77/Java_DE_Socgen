# Retail Platform Promotion Model — Multi-Environment Strategy
## Complete Guided Lab: Dev / Test / Prod Separation, Config Externalization, Secrets Management, Promotion Workflows

**Topic:** Session 21 — Multi-Environment Strategy
**Stack:** Java 17 · Apache Spark 3.5.1 · Delta Lake 3.2.0 · Azure Key Vault · Azure Kubernetes Service (AKS) · Azure Container Registry (ACR)
**Business scenario:** Retail Banking — Card Transaction ↔ Core Ledger Reconciliation

---

## Table of Contents

1. [Part 1 — Architectural Topic & Business Context](#part-1)
2. [Part 2 — Prerequisites (macOS M1 Max and Windows 11)](#part-2)
3. [Part 3 — Project Structure](#part-3)
4. [Part 4 — Running Locally in IntelliJ IDEA](#part-4)
5. [Part 5 — Line-by-Line Code Walkthrough](#part-5)
6. [Part 6 — Building the Container Image and Pushing to ACR](#part-6)
7. [Part 7 — AKS Cluster Prerequisites (Key Vault, Workload Identity, Spark Operator)](#part-7)
8. [Part 8 — Deploying to the TEST Namespace](#part-8)
9. [Part 9 — Promotion Workflow: TEST → PROD](#part-9)
10. [Part 10 — Verifying the Output (Delta Table Inspection)](#part-10)
11. [Part 11 — Troubleshooting](#part-11)
12. [Part 12 — Scaling This Demo Further](#part-12)

---

<a name="part-1"></a>
## Part 1 — Architectural Topic & Business Context

### 1.1 The technical problem

A single Spark application must run **unmodified** — same compiled jar, same
container image — across three environments that differ in:

| Concern                | Dev                          | Test                                   | Prod                                    |
|-------------------------|-------------------------------|------------------------------------------|-------------------------------------------|
| Spark master             | `local[*]`                    | `k8s://<test-aks-api-server>`             | `k8s://<prod-aks-api-server>`              |
| Storage                  | Local filesystem `/tmp/...`   | ADLS Gen2 (`retailplatformtestsa`)        | ADLS Gen2 (`retailplatformprodsa`)         |
| Secrets                  | OS environment variables      | Azure Key Vault (`retail-platform-test-kv`) | Azure Key Vault (`retail-platform-prod-kv`) |
| Scale                    | 25,000 synthetic rows          | 250,000 synthetic rows                     | 2,000,000 synthetic rows                    |
| Shuffle partitions        | 4                              | 16                                        | 64                                          |

The architectural discipline being demonstrated is **strict separation between
code and configuration** (12-factor "config in the environment"), plus a
**secrets boundary** that the application code never crosses directly — it
always goes through the `SecretsProvider` abstraction.

If configuration or secrets leak into code (hardcoded storage account keys,
hardcoded ADLS paths, `if (env.equals("prod"))` branches inside pipeline
logic), the artifact promoted from test is no longer the artifact you tested
— which defeats the entire purpose of having a test environment.

### 1.2 Business context — Retail Banking

**Use case:** Card Transaction ↔ Core Ledger Reconciliation. Every card swipe,
ATM withdrawal, mobile payment, or ACH transfer captured at the channel edge
must have a matching posting in the core banking ledger with an identical
amount. Discrepancies (missing ledger entries, amount mismatches) are exactly
the kind of finding that trips regulatory reporting (RBI/PCI reconciliation
requirements) and fraud investigation workflows.

**Scale target:** Prod processes ~2M transactions per run; test validates
correctness at 250K; dev iterates at 25K for fast feedback.

---

<a name="part-2"></a>
## Part 2 — Prerequisites

### 2.1 Common (both OS)

| Tool | Version | Notes |
|---|---|---|
| Java | 17 (Temurin/OpenJDK) | `java -version` must print `17.x` |
| Maven | 3.9+ | `mvn -version` |
| IntelliJ IDEA | Latest (2024.x+) | Community or Ultimate |
| Docker Desktop | Latest | Needed for Part 6 onward |
| Azure CLI | 2.60+ | `az version` |
| kubectl | matching your AKS version | `kubectl version --client` |
| Helm | 3.x | for installing the Spark Operator |

### 2.2 macOS (Apple Silicon M1 Max, ARM64)

```bash
brew install openjdk@17 maven azure-cli kubectl helm docker
sudo ln -sfn /opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk \
    /Library/Java/JavaVirtualMachines/openjdk-17.jdk
java -version   # expect: openjdk version "17.x" ... aarch64
```

**Native library note (RocksDB / Netty):** this project does not use Spark
Structured Streaming's RocksDB state store (the demo is batch, not
streaming), so no RocksDB native library concerns apply. `rocksdbjni` and
Spark's own Netty (`io.netty:netty-all`) dependencies published to Maven
Central since Spark 3.4+ already ship `osx-aarch64` classifier jars
automatically resolved by Maven on Apple Silicon — no manual profile or
classifier override is required in `pom.xml`. Netty's native epoll transport
is Linux-only and is never selected on macOS; Spark falls back to the
portable NIO transport automatically, so this is not something you configure.

### 2.3 Windows 11 (x64)

```powershell
winget install EclipseAdoptium.Temurin.17.JDK
winget install Apache.Maven
winget install Microsoft.AzureCLI
winget install Kubernetes.kubectl
winget install Helm.Helm
winget install Docker.DockerDesktop
```

**Hadoop native binaries (`winutils.exe`) — REQUIRED on Windows:**
Spark's `org.apache.hadoop.util.Shell` calls native Windows binaries even for
purely local-filesystem runs (permission checks on `/tmp` equivalents). Without
this you will see `UnsatisfiedLinkError: org.apache.hadoop.io.nativeio.NativeIO$Windows.access0`.

1. Download the Hadoop 3.3.6 Windows binaries (matching `hadoop-azure:3.3.6`
   in `pom.xml`) from a trusted community mirror, e.g.
   `https://github.com/cdarlint/winutils` (folder `hadoop-3.3.6/bin`).
2. Create `C:\hadoop\bin\` and copy `winutils.exe` and `hadoop.dll` into it.
3. Set a permanent environment variable:
   ```powershell
   [Environment]::SetEnvironmentVariable("HADOOP_HOME", "C:\hadoop", "User")
   ```
4. Add `%HADOOP_HOME%\bin` to your `PATH`.
5. Restart IntelliJ IDEA and any open terminal so the variable is picked up.

---

<a name="part-3"></a>
## Part 3 — Project Structure

```
retail-multienv-demo/
├── pom.xml
├── LAB_GUIDE.md                          ← this file
├── src/main/java/com/retailbank/dataplatform/
│   ├── Main.java                          ← entry point, 4-stage orchestration
│   ├── config/
│   │   ├── EnvironmentConfigLoader.java   ← resolves -Dretail.env, layered HOCON load
│   │   ├── AppConfig.java                 ← typed config records
│   │   └── SparkSessionFactory.java       ← builds SparkSession + Delta wiring
│   ├── secrets/
│   │   ├── SecretsProvider.java           ← interface + factory
│   │   ├── LocalEnvSecretsProvider.java   ← dev: OS environment variables
│   │   └── AzureKeyVaultSecretsProvider.java  ← test/prod: Azure Key Vault
│   ├── data/
│   │   └── SyntheticBankingDataGenerator.java ← generates card txns + ledger entries
│   ├── pipeline/
│   │   ├── TransactionReconciliationPipeline.java ← core join/classify transform
│   │   └── ReconciliationOutputWriter.java        ← Delta write, ADLS auth injection
│   └── model/
│       └── DomainModels.java              ← CardTransaction, LedgerEntry, ReconciliationResult
├── src/main/resources/
│   ├── reference.conf                     ← packaged safe defaults
│   ├── application-dev.conf
│   ├── application-test.conf
│   ├── application-prod.conf
│   └── log4j2.xml
├── docker/
│   └── Dockerfile                         ← multi-stage: Maven build → Spark runtime
├── k8s/
│   ├── dev/README.md                      ← why dev has no manifests
│   ├── test/
│   │   ├── namespace.yaml
│   │   ├── configmap.yaml                 ← externalized application-test.conf
│   │   ├── secretproviderclass.yaml       ← Workload Identity SA + Key Vault CSI
│   │   └── spark-application.yaml         ← SparkApplication CRD
│   └── prod/                              ← structurally identical to test/
│       ├── namespace.yaml
│       ├── configmap.yaml
│       ├── secretproviderclass.yaml
│       └── spark-application.yaml
└── scripts/
    ├── run-local-dev.sh / .bat
    ├── build-and-push-acr.sh
    └── deploy-aks.sh
```

---

<a name="part-4"></a>
## Part 4 — Running Locally in IntelliJ IDEA

### 4.1 Import the project

`File → Open` → select the `retail-multienv-demo` folder containing `pom.xml`.
Let IntelliJ auto-import the Maven project. Confirm **Project SDK = 17** under
`File → Project Structure → Project`.

### 4.2 Create the Run Configuration

`Run → Edit Configurations → + → Application`

| Field | Value |
|---|---|
| Name | `Retail Demo - DEV` |
| Module | `retail-multienv-demo` |
| Main class | `com.retailbank.dataplatform.Main` |

### 4.3 VM Options — macOS (Apple Silicon M1 Max)

Paste exactly into the **VM options** field (enable it via "Modify options →
Add VM options" if not visible):

```
--add-opens=java.base/java.lang=ALL-UNNAMED
--add-opens=java.base/java.lang.invoke=ALL-UNNAMED
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED
--add-opens=java.base/java.io=ALL-UNNAMED
--add-opens=java.base/java.net=ALL-UNNAMED
--add-opens=java.base/java.nio=ALL-UNNAMED
--add-opens=java.base/java.util=ALL-UNNAMED
--add-opens=java.base/java.util.concurrent=ALL-UNNAMED
--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED
--add-opens=java.base/java.util.concurrent.locks=ALL-UNNAMED
--add-opens=java.base/java.nio.charset=ALL-UNNAMED
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED
--add-opens=java.base/sun.nio.cs=ALL-UNNAMED
--add-opens=java.base/sun.security.action=ALL-UNNAMED
--add-opens=java.base/sun.util.calendar=ALL-UNNAMED
--add-opens=java.base/javax.security.auth=ALL-UNNAMED
-Djdk.reflect.useDirectMethodHandle=false
-Dio.netty.tryReflectionSetAccessible=true
-Dretail.env=dev
-Xms1g -Xmx4g
```

Every `--add-opens` flag above addresses a specific Java 17 strong-encapsulation
error Spark 3.5.x triggers at runtime (`InaccessibleObjectException`) when it
reflectively accesses JDK internals for serialization, memory management, and
Netty buffer handling. `-Djdk.reflect.useDirectMethodHandle=false` avoids a
known Spark/Java 17 `MethodHandle`-based reflection crash in Catalyst code
generation. `-Dio.netty.tryReflectionSetAccessible=true` is required because
Spark's shaded Netty allocator otherwise fails to access `DirectByteBuffer`
internals under Java 17's module system.

### 4.4 VM Options — Windows 11

Identical to 4.3, **plus** the Hadoop home directory flag (append this line):

```
-Dhadoop.home.dir=C:\hadoop
```

### 4.5 Environment variables (both OS)

Under **Environment variables** in the Run Configuration, add:

```
RETAIL_SECRET_ADLS_RETAILPLATFORMDEVSA_ACCOUNT_KEY=not-needed-for-local-filesystem-output
```

This is not actually read by the default `application-dev.conf` (which writes
to local disk, not ADLS) — it is included so you can see the
`LocalEnvSecretsProvider` naming convention (`RETAIL_SECRET_<SECRET_NAME_UPPER_SNAKE>`)
in action if you later point dev at an `abfss://` path.

### 4.6 Run it

Click ▶. Expected console output (abridged):

```
INFO  EnvironmentConfigLoader - Resolved runtime environment = 'dev'
INFO  EnvironmentConfigLoader - Loading config bundled on classpath: application-dev.conf
INFO  EnvironmentConfigLoader - ==== Effective configuration for environment 'dev' ====
INFO  EnvironmentConfigLoader -   retail-platform.environment.name = dev
INFO  EnvironmentConfigLoader -   retail-platform.spark.app-name = retail-multienv-demo-dev
INFO  EnvironmentConfigLoader -   retail-platform.data.output-path = /tmp/retail-platform/dev/output/transaction_reconciliation
INFO  EnvironmentConfigLoader -   retail-platform.secrets.provider = local-env
INFO  Main - Spark session initialized. environment='dev', master='local[*]', appName='retail-multienv-demo-dev'
INFO  Main - Generated 25000 card transactions and 23907 ledger entries
INFO  TransactionReconciliationPipeline - ==== Reconciliation summary (environment='dev') ====
INFO  TransactionReconciliationPipeline -   MATCHED = 23157
INFO  TransactionReconciliationPipeline -   AMOUNT_MISMATCH = 750
INFO  TransactionReconciliationPipeline -   MISSING_LEDGER_ENTRY = 1093
INFO  ReconciliationOutputWriter - Write complete: 25000 rows written to '/tmp/retail-platform/dev/output/transaction_reconciliation'
INFO  Main - Pipeline completed successfully for environment 'dev' in 4213 ms
```

Verify the Delta table was written:

```bash
ls -R /tmp/retail-platform/dev/output/transaction_reconciliation
# macOS/Linux — expect status=MATCHED/, status=AMOUNT_MISMATCH/, status=MISSING_LEDGER_ENTRY/, _delta_log/
```

On Windows, check `C:\tmp\retail-platform\dev\output\transaction_reconciliation`
(Spark maps `/tmp/...` to the current drive root on Windows local filesystem).

---

<a name="part-5"></a>
## Part 5 — Line-by-Line Code Walkthrough

### 5.1 `EnvironmentConfigLoader.resolveEnvironmentNameOrFail()`

```java
String env = System.getProperty(ENV_SYSTEM_PROPERTY);
if (env == null || env.isBlank()) {
    throw new IllegalStateException(...);
}
```
Reads the **mandatory** `-Dretail.env` system property. There is intentionally
no `String.valueOf(...).orElse("dev")` fallback: a missing environment flag
fails the JVM at startup rather than silently defaulting to dev and
potentially writing 2M rows of synthetic prod data into a dev bucket by
accident (the single most common real-world multi-env incident).

### 5.2 `EnvironmentConfigLoader.load()` — layered config resolution

```java
Config resolved = ConfigFactory.systemProperties()
        .withFallback(environmentConfig)
        .withFallback(ConfigFactory.parseResourcesAnySyntax("reference.conf"))
        .resolve();
```
This is **Typesafe Config's** `withFallback` chain, read right-to-left in
precedence: `reference.conf` (packaged defaults) is consulted only if a key is
absent from `environmentConfig` (the `application-<env>.conf` file), which in
turn is consulted only if a key is absent from JVM `-D` system properties.
`.resolve()` triggers substitution resolution (HOCON `${...}` references) —
required before any `.getString(...)` call.

`ConfigFactory.parseResourcesAnySyntax(envFileName)` is the API call
solving the core architectural problem: it loads a **named, environment-specific
classpath resource** rather than the conventionally auto-loaded
`application.conf` — which lets three files coexist in the same jar without
Typesafe Config's default loader merging them unpredictably.

### 5.3 `AppConfig.from(Config resolved)`

```java
Config root = resolved.getConfig("retail-platform");
...
return new AppConfig(root.getString("environment.name"), spark, data, secrets, reconciliation);
```
Converts the loosely-typed `Config` tree into a compile-time-checked Java
`record`. Every other class in the codebase depends on `AppConfig`, never on
`com.typesafe.config.Config` — if a HOCON key is renamed, the compiler catches
every call site instead of failing at runtime deep inside a Spark job.

### 5.4 `SecretsProvider.forProvider(...)` — the environment boundary

```java
return switch (providerName) {
    case "local-env" -> new LocalEnvSecretsProvider();
    case "azure-key-vault" -> new AzureKeyVaultSecretsProvider(keyVaultUri);
    default -> throw new IllegalArgumentException(...);
};
```
A Java 17 `switch` expression selecting the secrets backend purely from the
`secrets.provider` config key. `application-dev.conf` hardcodes `local-env`;
`application-test.conf`/`application-prod.conf` hardcode `azure-key-vault` —
this means a developer physically cannot point dev at Key Vault by accident
(there's no config flag combination that would route dev traffic there),
and equally cannot accidentally leave prod running against a local
environment variable.

### 5.5 `AzureKeyVaultSecretsProvider` — `DefaultAzureCredential`

```java
DefaultAzureCredential credential = new DefaultAzureCredentialBuilder().build();
this.secretClient = new SecretClientBuilder().vaultUrl(keyVaultUri).credential(credential).buildClient();
```
`DefaultAzureCredential` implements the standard Azure credential chain. On
AKS, with the pod's `ServiceAccount` annotated for **Workload Identity**
(`azure.workload.identity/client-id`), it resolves a federated OIDC token
automatically — no client secret, certificate, or connection string is ever
present in the container image, ConfigMap, or Kubernetes Secret. Locally in
IntelliJ this same code path is never exercised, because dev's `secrets.provider
= "local-env"` selects `LocalEnvSecretsProvider` instead — Key Vault
connectivity is never required to run the demo on a laptop.

### 5.6 `SyntheticBankingDataGenerator.generate()` — controlled data quality injection

```java
double errorRoll = random.nextDouble();
if (errorRoll < 0.02) {
    continue; // no ledger row emitted at all
} else if (errorRoll < 0.05) {
    BigDecimal mismatchAmount = amount.add(...); // ledger row with wrong amount
    ...
} else {
    ledgerRows.add(...); // clean matching row
}
```
Uses a seeded `java.util.Random(42L)` for **deterministic** synthetic data —
the same seed produces the same discrepancy distribution across every run,
which matters for demo reproducibility and for writing assertions in tests.
2% of transactions get no ledger row (`MISSING_LEDGER_ENTRY` downstream), 3%
get a ledger row with a perturbed amount (`AMOUNT_MISMATCH` downstream), 95%
match cleanly.

### 5.7 `TransactionReconciliationPipeline.reconcile()` — the core transform

```java
Dataset<Row> joined = cardTransactions.alias("ch")
        .join(ledgerEntries.alias("lg"),
              cardTransactions.col("transactionId").equalTo(ledgerEntries.col("transactionId")),
              "left_outer");
```
A **left outer join** keyed on `transactionId` — every channel transaction is
preserved even when no ledger posting exists, which is exactly the row we
need to flag as `MISSING_LEDGER_ENTRY`. Under Catalyst, because the ledger
side has a NULL-producing join and no broadcast hint, Spark's Adaptive Query
Execution (enabled in `SparkSessionFactory`) evaluates actual post-shuffle
partition statistics at runtime and will convert this to a broadcast hash
join automatically if the ledger side turns out smaller than
`spark.sql.autoBroadcastJoinThreshold` after filtering — you can confirm this
by inspecting `.explain(true)` in the Spark UI's SQL tab.

```java
Column status = functions.when(functions.col("lg.postedAmount").isNull(), functions.lit("MISSING_LEDGER_ENTRY"))
        .when(functions.abs(functions.col("ch.amount").minus(functions.col("lg.postedAmount"))).geq(threshold), functions.lit("AMOUNT_MISMATCH"))
        .otherwise(functions.lit("MATCHED"));
```
A chained `functions.when(...).when(...).otherwise(...)` — compiled by
Catalyst into a single `CASE WHEN` expression in the physical plan, evaluated
column-wise in a single pass with no row-by-row UDF overhead (UDFs would
opt the query out of whole-stage code generation; this stays fully
codegen-eligible).

```java
result = result.repartition(targetPartitions);
```
`targetPartitions` comes from `config.spark().shufflePartitions()` — **4 in
dev, 16 in test, 64 in prod**. This is the single line in the entire codebase
whose behavior differs numerically per environment, and it is driven purely
by externalized config, not a code branch.

### 5.8 `ReconciliationOutputWriter.configureAdlsAuthentication()`

```java
String hadoopConfKey = "fs.azure.account.key." + storageAccountName + ".dfs.core.windows.net";
spark.sparkContext().hadoopConfiguration().set(hadoopConfKey, accountKey);
```
Sets the **exact** Hadoop configuration property the ABFS driver
(`org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem`, pulled in via
`hadoop-azure`) looks up at connection time:
`fs.azure.account.key.<account>.dfs.core.windows.net`. This is set
programmatically at runtime — after the key is resolved from Key Vault —
rather than statically in `core-site.xml`, so the key is never written to
disk anywhere in the container image or ConfigMap.

### 5.9 `Main.main()` — the four-stage orchestration

```java
try (SparkSession spark = SparkSessionFactory.create(appConfig)) {
    ... generate ... reconcile ... write ...
}
```
`SparkSession` implements `AutoCloseable`; the try-with-resources block
guarantees `spark.stop()` runs even on exception, releasing executors back to
the Kubernetes scheduler in cluster mode instead of leaking allocated pods
until a timeout.

---

<a name="part-6"></a>
## Part 6 — Building the Container Image and Pushing to ACR

### 6.1 Create the ACR (one-time, per subscription)

```bash
az group create --name rg-retail-platform --location centralindia
az acr create --resource-group rg-retail-platform \
  --name retailplatformacr --sku Standard
```

### 6.2 Build and push

```bash
chmod +x scripts/build-and-push-acr.sh
./scripts/build-and-push-acr.sh retailplatformacr.azurecr.io 1.0.0
```

This runs, in order: `mvn clean package` (produces
`target/retail-multienv-demo.jar`, the shaded fat jar) → `docker build`
(multi-stage: Maven+JDK17 builder stage, then copies only the jar and conf
files into the slim `apache/spark:3.5.1-java17` runtime stage) → `az acr
login` → `docker push`.

### 6.3 Verify the image landed

```bash
az acr repository show-tags --name retailplatformacr --repository retail-multienv-demo
# expect: ["1.0.0"]
```

---

<a name="part-7"></a>
## Part 7 — AKS Cluster Prerequisites

### 7.1 Enable OIDC issuer + Workload Identity on the AKS cluster

```bash
az aks update -g rg-retail-platform -n aks-retail-platform \
  --enable-oidc-issuer --enable-workload-identity
```

### 7.2 Create per-environment Managed Identities and federate them

```bash
for ENV in test prod; do
  az identity create -g rg-retail-platform -n mi-retail-platform-${ENV}

  AKS_OIDC_ISSUER=$(az aks show -g rg-retail-platform -n aks-retail-platform \
    --query "oidcIssuerProfile.issuerUrl" -o tsv)

  az identity federated-credential create \
    --name fc-retail-platform-${ENV} \
    --identity-name mi-retail-platform-${ENV} \
    --resource-group rg-retail-platform \
    --issuer "${AKS_OIDC_ISSUER}" \
    --subject "system:serviceaccount:retail-platform-${ENV}:retail-platform-spark-sa" \
    --audience api://AzureADTokenExchange

  IDENTITY_CLIENT_ID=$(az identity show -g rg-retail-platform \
    -n mi-retail-platform-${ENV} --query clientId -o tsv)
  echo "${ENV} managed identity clientId: ${IDENTITY_CLIENT_ID}"
done
```

Copy each printed `clientId` into the matching
`REPLACE_WITH_TEST_MANAGED_IDENTITY_CLIENT_ID` / `REPLACE_WITH_PROD_MANAGED_IDENTITY_CLIENT_ID`
placeholders in `k8s/test/secretproviderclass.yaml` and
`k8s/prod/secretproviderclass.yaml`.

### 7.3 Grant each identity access to its Key Vault

```bash
az keyvault create -g rg-retail-platform -n retail-platform-test-kv --enable-rbac-authorization
az keyvault create -g rg-retail-platform -n retail-platform-prod-kv --enable-rbac-authorization

for ENV in test prod; do
  IDENTITY_PRINCIPAL_ID=$(az identity show -g rg-retail-platform \
    -n mi-retail-platform-${ENV} --query principalId -o tsv)
  az role assignment create \
    --role "Key Vault Secrets User" \
    --assignee-object-id "${IDENTITY_PRINCIPAL_ID}" \
    --assignee-principal-type ServicePrincipal \
    --scope $(az keyvault show -n retail-platform-${ENV}-kv --query id -o tsv)
done
```

### 7.4 Store the ADLS account key as a secret

```bash
for ENV in test prod; do
  ACCOUNT_KEY=$(az storage account keys list \
    -g rg-retail-platform -n retailplatform${ENV}sa --query "[0].value" -o tsv)
  az keyvault secret set \
    --vault-name retail-platform-${ENV}-kv \
    --name adls-retailplatform${ENV}sa-account-key \
    --value "${ACCOUNT_KEY}"
done
```

### 7.5 Install the Spark-on-Kubernetes Operator and the Key Vault CSI driver

```bash
helm repo add spark-operator https://kubeflow.github.io/spark-operator
helm repo add csi-secrets-store-provider-azure \
  https://azure.github.io/secrets-store-csi-driver-provider-azure/charts
helm repo update

helm install spark-operator spark-operator/spark-operator \
  --namespace spark-operator --create-namespace --set webhook.enable=true

helm install csi-secrets-store-provider-azure \
  csi-secrets-store-provider-azure/csi-secrets-store-provider-azure \
  --namespace kube-system
```

### 7.6 Replace remaining placeholders

In `k8s/test/spark-application.yaml`, `k8s/prod/spark-application.yaml`, and
both `configmap.yaml` files, replace:

- `REPLACE_WITH_ACR_LOGIN_SERVER` → your ACR name (e.g. `retailplatformacr`)
- `REPLACE_WITH_AKS_API_SERVER` → output of:
  ```bash
  az aks show -g rg-retail-platform -n aks-retail-platform \
    --query "fqdn" -o tsv
  ```
- `REPLACE_WITH_AZURE_TENANT_ID` → output of `az account show --query tenantId -o tsv`

---

<a name="part-8"></a>
## Part 8 — Deploying to the TEST Namespace

```bash
chmod +x scripts/deploy-aks.sh
./scripts/deploy-aks.sh test
```

Watch it run:

```bash
kubectl get sparkapplication -n retail-platform-test -w
kubectl logs -n retail-platform-test -l spark-role=driver -f
```

Expected `SparkApplication` status progression: `SUBMITTED` → `RUNNING` →
`COMPLETED`. The driver log tail should show the same "Reconciliation
summary" block from Part 4.6, but at test scale (250,000 rows) and with
`retail-platform.data.output-path` pointing at
`abfss://retail-data@retailplatformtestsa.dfs.core.windows.net/...`.

---

<a name="part-9"></a>
## Part 9 — Promotion Workflow: TEST → PROD

This is the operational core of "Multi-Environment Strategy." The promotion
unit is **the immutable image tag** — `retailplatformacr.azurecr.io/retail-multienv-demo:1.0.0`
— never a rebuild.

### 9.1 Promotion checklist (gate before running Part 9.2)

- [ ] `SparkApplication` in `retail-platform-test` reached `COMPLETED` state
- [ ] Reconciliation summary counts reviewed (no unexpected spike in `MISSING_LEDGER_ENTRY`)
- [ ] Delta table schema in test output matches the expected `ReconciliationResult` shape
- [ ] Image tag `1.0.0` has not been overwritten since the test run (ACR tags are immutable by policy — enable this with `az acr config content-trust`)
- [ ] Change reviewed/approved per your organization's release process

### 9.2 Promote — deploy the SAME image tag to prod

```bash
./scripts/deploy-aks.sh prod
```

Note this script requires typing `promote-to-prod` to continue — a
deliberate manual gate. It applies `k8s/prod/*.yaml`, which references:

```yaml
image: "retailplatformacr.azurecr.io/retail-multienv-demo:1.0.0"
```

— **the identical tag** validated in test. Only the namespace, resource
sizing (`driver.memory: 8g` vs `4g`), `dynamicAllocation.maxExecutors` (40 vs
8), and the mounted `application-prod.conf` differ.

### 9.3 What must NEVER happen in a real promotion

- Building a fresh image for prod ("just to be safe") — this reintroduces
  the exact risk multi-stage promotion exists to eliminate: an artifact in
  prod that was never the artifact tested.
- Editing `k8s/prod/configmap.yaml` values ad hoc with `kubectl edit` —
  route changes through source control and re-apply via `deploy-aks.sh`.

---

<a name="part-10"></a>
## Part 10 — Verifying the Output (Delta Table Inspection)

From a Spark shell or a small script pointed at the same storage account
(read-only credentials are sufficient):

```java
Dataset<Row> result = spark.read().format("delta")
    .load("abfss://retail-data@retailplatformprodsa.dfs.core.windows.net/reconciliation/output");
result.groupBy("status").count().show();
result.filter("status = 'AMOUNT_MISMATCH'").show(20, false);
```

Or via the Delta table's transaction log directly (`_delta_log/*.json`) to
confirm `partitionBy("status")` produced the expected
`status=MATCHED/`, `status=AMOUNT_MISMATCH/`, `status=MISSING_LEDGER_ENTRY/`
directory layout.

---

<a name="part-11"></a>
## Part 11 — Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `IllegalStateException: System property -Dretail.env is required` | Forgot `-Dretail.env=dev` in VM options | Re-check Part 4.3/4.4 |
| `InaccessibleObjectException` on startup | Missing `--add-opens` flags | Copy the exact block from Part 4.3 |
| `UnsatisfiedLinkError: NativeIO$Windows.access0` | `HADOOP_HOME`/winutils missing on Windows | Part 2.3 |
| `SecretNotFoundException ... local-env` | Dev config references an env var you haven't exported | Only needed if you point dev at `abfss://`; otherwise check for typos in the secret name |
| `SecretNotFoundException ... azure-key-vault` | Managed identity lacks `Key Vault Secrets User` role, or wrong `clientID` in `secretproviderclass.yaml` | Re-run Part 7.2/7.3, confirm `az role assignment list --assignee <principalId>` |
| `SparkApplication` stuck in `SUBMISSION_FAILED` | `mainApplicationFile` path wrong or image pull failure | `kubectl describe sparkapplication -n retail-platform-test <name>`; confirm ACR attach: `az aks update -g rg-retail-platform -n aks-retail-platform --attach-acr retailplatformacr` |
| Executors can't reach ADLS (`403`) | Storage account key in Key Vault is stale/rotated | Re-run Part 7.4 |
| `ClassNotFoundException: io.delta.sql.DeltaSparkSessionExtension` | Shade plugin dropped `META-INF/services` entries | Confirm `ServicesResourceTransformer` is present in `pom.xml` shade config (it is, by default in this project) |

---

<a name="part-12"></a>
## Part 12 — Scaling This Demo Further

- **Distributed generation at very large scale:** replace the driver-side
  `List<Row>` construction in `SyntheticBankingDataGenerator` with
  `spark.range(recordCount).mapPartitions(...)` so generation itself
  parallelizes across executors instead of materializing on the driver.
- **Streaming variant:** swap `ReconciliationOutputWriter`'s batch
  `.write().save(...)` for `.writeStream().trigger(...).start(...)`,
  reusing the identical `AppConfig`/`SecretsProvider` plumbing — the
  multi-environment strategy in this project does not change shape when the
  pipeline becomes streaming.
- **Add a 4th environment (UAT/staging):** add `application-uat.conf`, add
  `"uat"` to `KNOWN_ENVIRONMENTS` in `EnvironmentConfigLoader`, copy
  `k8s/test/` to `k8s/uat/`. No pipeline code changes required — this is the
  test that confirms the separation is real.
