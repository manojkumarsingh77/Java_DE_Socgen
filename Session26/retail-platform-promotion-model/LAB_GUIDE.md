# Lab Guide — Retail Platform Promotion Model
### Module: Multi-Environment Strategy (Dev/Test/Prod separation, Config externalization, Secrets management, Promotion workflows)

This lab builds, runs, and promotes one Spark-on-Java application — the
**Retail Platform Promotion Model** — through three environments without
ever recompiling it. You will run it on your laptop in five minutes, then
walk it all the way to a simulated AKS production namespace.

---

## 0. What you are building

**Business problem.** A retail bank wants to score every card transaction
for promotion eligibility (loyalty offers, cashback campaigns) using three
signals: account spend level, loyalty tier, and fraud/risk score. The same
scoring logic must run identically in a developer's IDE, in a QA/test AKS
namespace against a small ADLS Gen2 dataset, and in production against the
full-scale dataset — with zero code changes between them.

**Architectural problem being solved.** How do you guarantee that:
1. A developer can iterate in IntelliJ with no Azure account at all.
2. The exact same jar, unmodified, is what gets promoted to test and prod.
3. Storage locations, cluster sizing, and credentials change per
   environment — but business logic and the artifact itself never do.
4. Secrets are never present in Git, in the Docker image, or in a config
   file — in test/prod they are pulled live from Azure Key Vault via
   workload identity.

**The four levers used to solve it** (this is the syllabus content, made concrete):

| Lever | Mechanism in this repo |
|---|---|
| Dev/Test/Prod separation | `-Denv={dev,test,prod}` selects everything else |
| Config externalization | Typesafe Config: `application.conf` (base) + `application-<env>.conf` (overlay) |
| Secrets management | `SecretsProvider` interface — `LocalEnvSecretsProvider` (dev) vs `AzureKeyVaultSecretsProvider` (test/prod) |
| Promotion workflows | Kustomize overlays (`k8s/overlays/{dev,test,prod}`) + GitHub Actions build-once/promote-many pipeline |

---

## 1. Prerequisites

### Common (both OSes)
- IntelliJ IDEA (Community or Ultimate), latest version
- Java 17 — Eclipse Temurin distribution (**not** Java 21+, Spark 3.5.x is validated on 17)
- Git
- Docker Desktop (for the container/AKS stages, Section 6 onward)
- Azure CLI (`az`) version 2.60+
- `kubectl` version 1.29+
- `kustomize` version 5.x (or use the version bundled inside `kubectl` via `kubectl apply -k`)
- An Azure subscription with permissions to create: an AKS cluster, an ACR
  registry, an ADLS Gen2 storage account, and a Key Vault — **only required
  from Section 6 onward.** Sections 1–5 run entirely offline.

### macOS (Apple Silicon M1 Max)
```bash
brew install openjdk@17 maven kubectl kustomize azure-cli
brew install --cask docker intellij-idea-ce
java -version   # confirm: openjdk version "17.x.x"
```

### Windows 11 (x64)
```powershell
winget install EclipseAdoptium.Temurin.17.JDK
winget install Apache.Maven
winget install Kubernetes.kubectl
winget install Microsoft.AzureCLI
winget install Docker.DockerDesktop
winget install JetBrains.IntelliJIDEA.Community
java -version
```

**Windows-only extra step — winutils.exe:**
Spark's local filesystem shim requires Hadoop's Windows native binaries even
though this demo never touches HDFS.
1. Download `winutils.exe` and `hadoop.dll` for Hadoop 3.3.x from
   `https://github.com/cdarlint/winutils`.
2. Place both files in `C:\hadoop\bin\`.
3. This path is already wired into `run-configs/dev-windows.vmoptions.txt`
   via `-Dhadoop.home.dir=C:\hadoop`.

---

## 2. Import the project

1. Unzip the project archive to a working folder, e.g.
   `~/dev/retail-platform-promotion-model` (macOS) or
   `C:\dev\retail-platform-promotion-model` (Windows).
2. In IntelliJ: **File → Open** → select the folder → open as a **Maven project**.
3. Wait for IntelliJ to index and let Maven resolve dependencies (first run
   needs internet access to Maven Central to download Spark, Delta, Azure
   SDK jars — a few hundred MB).
4. Set the Project SDK: **File → Project Structure → Project → SDK → 17**.

---

## 3. Run it locally in Dev (zero Azure required)

### 3.1 Create the Run/Debug Configuration
1. **Run → Edit Configurations → + → Application**
2. **Name:** `PromotionPipeline (dev)`
3. **Main class:** `com.retailbank.pipeline.PromotionPipeline`
4. **Module:** `retail-platform-promotion-model`
5. **VM options:** paste the entire contents of:
   - macOS M1 Max → `run-configs/dev-macos-m1.vmoptions.txt`
   - Windows 11 → `run-configs/dev-windows.vmoptions.txt`
   (IntelliJ's VM options field accepts multi-line input directly.)
6. **Program arguments:** leave empty — environment selection is entirely
   through the `-Denv=dev` VM option, not a program argument. This matters:
   it means the identical `java -jar app.jar` invocation works in every
   environment, only the `-Denv` flag differs.
7. **Critical step — include `provided`-scope dependencies:** click
   **"Modify options"** (top-right of the config panel) and enable
   **"Add dependencies with 'Provided' scope to classpath."**
   `spark-core`/`spark-sql`/`spark-streaming` are declared `<scope>provided</scope>`
   in `pom.xml` on purpose — the `apache/spark:3.5.1-java17` base image already
   supplies them at runtime on AKS, so they're deliberately excluded from the
   shaded jar to avoid bundling Spark twice. IntelliJ's default Application
   run configuration mirrors Maven's `provided` semantics and leaves those
   jars off the classpath too — skipping this checkbox is the single most
   common cause of:
   ```
   Exception in thread "main" java.lang.NoClassDefFoundError: org/apache/spark/sql/SparkSession
   Caused by: java.lang.ClassNotFoundException: org.apache.spark.sql.SparkSession
   ```
8. Click **Apply → OK**.

### 3.2 Run it
Click the green ▶ next to `PromotionPipeline (dev)`.

**Expected console output (abridged):**
```
=== Retail Platform Promotion Model | environment='dev' ===
Storage format=parquet input=/var/folders/.../retailbank/dev/input output=/var/folders/.../retailbank/dev/output
Env var RETAILBANK_PROMOTION_DOWNSTREAM_API_KEY not set - using non-sensitive demo fallback value for local dev run only
Resolved downstream API credential via provider='local-env' (value masked: ****alue)
Generated 5000 synthetic transactions across 500 accounts
3512 of 5000 transactions flagged promotion-eligible
Region/segment summary:
+-------+----------------+-------------+-----------------+--------------+
|region |customerSegment |customerCount|avgPromotionScore|eligibleCount |
+-------+----------------+-------------+-----------------+--------------+
|CENTRAL|AFFLUENT        |...          |...              |...           |
...
Wrote scored dataset to /var/folders/.../retailbank/dev/output as parquet
=== Pipeline complete for environment 'dev' ===
```
(Exact counts vary slightly by JVM/locale but are deterministic given the
fixed random seed in `SyntheticBankingDataGenerator`.)

### 3.3 Try the secrets override
Stop the run. Set a real environment variable and re-run to see the
`LocalEnvSecretsProvider` pick it up instead of the fallback:
- macOS: `export RETAILBANK_PROMOTION_DOWNSTREAM_API_KEY=sk-demo-12345678`
- Windows (PowerShell): `$env:RETAILBANK_PROMOTION_DOWNSTREAM_API_KEY="sk-demo-12345678"`

Re-run from the *same terminal/IDE session* and confirm the log line changes
to `(value masked: ****5678)` with no `"not set"` warning.

---

## 4. Code walkthrough

### `ConfigLoader.load()`
```java
Config overlay = ConfigFactory.parseResourcesAnySyntax("application-" + env);
Config base = ConfigFactory.parseResourcesAnySyntax("application");
Config merged = ConfigFactory.systemProperties()
        .withFallback(overlay)
        .withFallback(base)
        .resolve();
```
`withFallback` is Typesafe Config's layering primitive: the object it's
called *on* wins; the argument only fills in keys the caller doesn't have.
So precedence here is **JVM system properties > environment overlay file >
base file** — meaning an operator can always override any single key with
`-Dsome.path=value` without touching any file, which is exactly how
`spark.master` is deliberately left unset in `application-test.conf` /
`application-prod.conf` for `spark-submit --master k8s://...` to supply it.

### `SecretsProviderFactory.create()`
```java
return switch (settings.provider()) {
    case "local" -> new LocalEnvSecretsProvider();
    case "keyvault" -> new AzureKeyVaultSecretsProvider(settings.keyVaultUrl());
    default -> throw new IllegalArgumentException(...);
};
```
This is the **only** branch on environment anywhere in the codebase, and it
branches on config (`secrets.provider`), not on `env` directly — the
pipeline code that calls `secretsProvider.getSecret(...)` never knows or
cares which one is active. That's what makes it safe to run the identical
jar everywhere.

### `AzureKeyVaultSecretsProvider`
```java
this.secretClient = new SecretClientBuilder()
        .vaultUrl(vaultUrl)
        .credential(new DefaultAzureCredentialBuilder().build())
        .buildClient();
```
`DefaultAzureCredentialBuilder` tries credential sources in order (env vars,
managed identity, Azure CLI, etc.). On AKS with Azure AD Workload Identity
configured (Section 7), it transparently picks up the federated token
projected into the pod at `/var/run/secrets/azure/tokens/azure-identity-token`
— **no client secret ever appears in code, config, or a Kubernetes Secret.**

### `PromotionPipeline.main()`
```java
if (config.spark().master() != null) {
    builder.master(config.spark().master());
}
```
`application-dev.conf` sets `spark.master = "local[*]"`; the test/prod
overlays omit the key entirely, so `config.spark().master()` is `null` and
this line is skipped — `spark-submit --master k8s://...` (or the
`SparkApplication` CRD) is the only thing that ever sets master in a
cluster deployment. This one `if` is what prevents an accidental
`local[*]` hardcode from ever reaching a cluster run.

```java
int rowCount = switch (config.environmentTag()) {
    case "dev" -> 5_000;
    case "test" -> 250_000;
    default -> 5_000_000; // prod
};
```
Data *volume* is allowed to vary by environment (that's a scale concern,
not a business-logic concern) — contrast this with
`PromotionScoringEngine.score()`, which takes only `AppConfig.PromotionRules`
and contains zero references to `environmentTag` anywhere: the scoring
*rules* are identical in all three environments by construction.

### `PromotionScoringEngine.score()`
```java
WindowSpec byAccount = Window.partitionBy(col("accountId"));
Dataset<Row> withAccountAggregates = transactions
        .withColumn("accountTotalSpend", sum(col("transactionAmount")).over(byAccount))
        .withColumn("accountTxnCount", count(col("transactionId")).over(byAccount));
```
`Window.partitionBy` + `.over()` gives a **running aggregate per account
without collapsing rows** — Catalyst plans this as a single shuffle on
`accountId` followed by a windowed sort-aggregate, versus the two shuffles
you'd get from a naive `groupBy().join()` back onto the original rows. This
is the standard pattern for "enrich each transaction with its account's
totals" at scale.

```java
.withColumn("promotionScore",
        (when(col("isHighValueAccount"), lit(40)).otherwise(lit(0)))
                .plus(when(col("isLoyaltyBonusEligible"), lit(30)).otherwise(lit(0)))
                .plus(when(col("isRiskEligible"), lit(20)).otherwise(lit(0)))
                .plus(when(col("previousPromotionResponse"), lit(10)).otherwise(lit(0))))
```
A weighted point system built entirely from Spark's `functions.when`/`.otherwise`
column expressions — Catalyst compiles this whole chain into a single
`CASE WHEN` projection in the physical plan (verify by running
`scored.explain(true)` and looking for `Project` with nested `CASE WHEN`
in the *Optimized Logical Plan* — no UDF, so no serialization boundary or
lost predicate pushdown, unlike an equivalent `.map(row -> ...)` UDF would incur).

---

## 5. Package the deployable jar

```bash
cd retail-platform-promotion-model
mvn -B -DskipTests clean package
ls target/retail-platform-promotion-model-1.0.0.jar
```
The `maven-shade-plugin` produces one fat jar containing Delta Lake, the
Kafka connector, `hadoop-azure`, Typesafe Config, and the Azure SDKs — this
exact jar is what gets baked into the Docker image in Section 6 and is
**never rebuilt per environment** from this point forward.

> Note: `spark-core`/`spark-sql`/`spark-streaming` are `<scope>provided</scope>`
> in `pom.xml` because the `apache/spark:3.5.1-java17` base image already
> supplies them on the classpath — shading them in would bloat the image
> and risk a version mismatch against the cluster's Spark runtime.

---

## 6. Build and push the container image

```bash
az acr login --name <your-acr-name>

docker build \
  -f docker/Dockerfile \
  -t <your-acr-name>.azurecr.io/retail-platform-promotion-model:demo-v1 \
  .

docker push <your-acr-name>.azurecr.io/retail-platform-promotion-model:demo-v1
```
The Dockerfile is a two-stage build: stage 1 compiles with
`maven:3.9.6-eclipse-temurin-17`, stage 2 copies only the resulting jar onto
`apache/spark:3.5.1-java17` — the final pushed image never contains the
Maven cache or build toolchain.

---

## 7. One-time AKS environment setup

Run once per environment (dev/test/prod each get their own AKS namespace in
this lab; in a larger org they might be separate clusters — the Kustomize
overlays work identically either way).

```bash
# 7.1 Namespaces
kubectl create namespace retail-dev
kubectl create namespace retail-test
kubectl create namespace retail-prod

# 7.2 Install the Spark Operator (once per cluster, not per namespace)
helm repo add spark-operator https://kubeflow.github.io/spark-operator
helm repo update
helm install spark-operator spark-operator/spark-operator \
  --namespace spark-operator --create-namespace \
  --set webhook.enable=true

# 7.3 Enable Azure AD Workload Identity on the AKS cluster (one-time)
az aks update -g rg-retailbank -n aks-retailbank --enable-oidc-issuer --enable-workload-identity

# 7.4 Per environment: create a Key Vault and a federated Entra identity
for ENV in dev test prod; do
  az keyvault create -g rg-retailbank -n kv-retailbank-$ENV
  az keyvault secret set --vault-name kv-retailbank-$ENV \
    --name promotion-downstream-api-key --value "demo-secret-value-$ENV"

  az identity create -g rg-retailbank -n id-retailbank-$ENV
  CLIENT_ID=$(az identity show -g rg-retailbank -n id-retailbank-$ENV --query clientId -o tsv)

  az keyvault set-policy -n kv-retailbank-$ENV --secret-permissions get list --spn $CLIENT_ID

  AKS_OIDC_ISSUER=$(az aks show -g rg-retailbank -n aks-retailbank --query "oidcIssuerProfile.issuerUrl" -o tsv)
  az identity federated-credential create \
    --name fed-retail-$ENV \
    --identity-name id-retailbank-$ENV \
    --resource-group rg-retailbank \
    --issuer "$AKS_OIDC_ISSUER" \
    --subject "system:serviceaccount:retail-$ENV:$ENV-spark-driver" \
    --audience api://AzureADTokenExchange

  echo "$ENV client-id: $CLIENT_ID   <-- paste into k8s/overlays/$ENV/patch-serviceaccount.yaml"
done
```
Paste each printed `client-id` into the matching
`k8s/overlays/<env>/patch-serviceaccount.yaml` in place of
`CHANGE_ME-<env>-entra-app-client-id`, replacing every placeholder in the
repo before deploying.

**Also required, once per environment:** an ADLS Gen2 container matching the
`abfss://` path in `application-test.conf` / `application-prod.conf`, and
`Storage Blob Data Contributor` role assignment on that container for the
same managed identity created above.

---

## 8. Deploy to Test

```bash
cd k8s/overlays/test
kustomize edit set image \
  acrretailbank.azurecr.io/retail-platform-promotion-model=<your-acr-name>.azurecr.io/retail-platform-promotion-model:demo-v1

cd ../../..
kubectl apply -k k8s/overlays/test

kubectl get sparkapplications -n retail-test
kubectl logs -n retail-test -l spark-role=driver -f
```
Confirm in the driver logs:
```
=== Retail Platform Promotion Model | environment='test' ===
Fetching secret 'promotion-downstream-api-key' from Key Vault https://kv-retailbank-test.vault.azure.net/
Resolved downstream API credential via provider='azure-keyvault(...)' (value masked: ****-test)
Generated 250000 synthetic transactions across 12500 accounts
...
Wrote scored dataset to abfss://curated@retailbanktestadls.dfs.core.windows.net/promotion-scores/ as delta
```
This confirms: the secret came from Key Vault (not env vars), the data
volume matches the `test` case in `PromotionPipeline`, and the output
format flipped from `parquet` to `delta` — all from the overlay, zero code
changes.

---

## 9. Promote to Production

Two equivalent paths — pick whichever fits how your org actually promotes:

**Path A — manual (for this lab):**
```bash
cd k8s/overlays/prod
kustomize edit set image \
  acrretailbank.azurecr.io/retail-platform-promotion-model=<your-acr-name>.azurecr.io/retail-platform-promotion-model:demo-v1
cd ../../..
kubectl apply -k k8s/overlays/prod
kubectl get sparkapplications -n retail-prod
```
Note the image tag is **identical** to what was verified in Test — nothing
was rebuilt.

**Path B — automated (`.github/workflows/promote.yml`):**
Push to `main` → `build` job compiles once and pushes one immutable
`<git-sha>` tag → `deploy-dev` runs automatically → `deploy-test` runs
automatically (or gated, if you add required reviewers to the `test`
GitHub Environment) → `deploy-prod` **pauses for manual approval** because
the `prod` GitHub Environment is configured with required reviewers in
**Settings → Environments → prod → Required reviewers**. Approving that
gate deploys the exact `<git-sha>`-tagged image already running in test.

---

## 10. Verifying the promotion actually worked

Compare the three environments side by side:

| Check | Dev | Test | Prod |
|---|---|---|---|
| `-Denv` | dev | test | prod |
| Secrets source (see driver log) | `local-env` | `azure-keyvault(...test...)` | `azure-keyvault(...prod...)` |
| Storage format | parquet | delta | delta |
| Row count generated | 5,000 | 250,000 | 5,000,000 |
| Shuffle partitions | 4 (local) / 8 (dev overlay) | 64 | 400 |
| Business rule thresholds (`promotion.high.value.threshold`, etc.) | identical | identical | identical |

The last row is the point of the whole exercise: everything infrastructural
changed, the thing that actually decides who gets a promotion did not.

---

## 11. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `NoClassDefFoundError: org/apache/spark/sql/SparkSession` when running from IntelliJ | `spark-core`/`spark-sql` are `provided` scope and IntelliJ excludes them from the run classpath by default | Edit Configurations → Modify options → enable "Add dependencies with 'Provided' scope to classpath" (see Section 3.1, step 7) |
| `UnsatisfiedLinkError` / `Could not locate executable null\bin\winutils.exe` | Missing Windows Hadoop native libs | Complete Section 1's winutils.exe step |
| `IllegalStateException: secrets.key.vault.url is required` | Ran with `-Denv=test` locally without workload identity | Expected — Key Vault path only works inside AKS with the federated identity from Section 7, or with `az login` + `AZURE_CLIENT_ID` env var set locally for a quick smoke test |
| `ClassNotFoundException: io.delta.sql.DeltaSparkSessionExtension` | Ran the shaded jar with `spark-submit` against a cluster whose Spark version doesn't match `provided` deps | Confirm the AKS Spark base image is exactly `3.5.1` — Delta 3.2.0 is compiled against Spark 3.5.x |
| `SparkApplication` stuck in `SUBMITTED` | Spark Operator webhook not ready, or service account missing `azure.workload.identity/use: "true"` label | `kubectl get pods -n spark-operator`; confirm label from `k8s/base/serviceaccount.yaml` survived the overlay |
| Kustomize patch has no effect | `target.name` in overlay `kustomization.yaml` doesn't match `metadata.name` in base resource before `namePrefix` is applied | Kustomize patch `target` selectors always match the **pre-prefix** base name (`retail-promotion`), not `dev-retail-promotion` |

---

## 12. Cleanup

```bash
kubectl delete -k k8s/overlays/dev
kubectl delete -k k8s/overlays/test
kubectl delete -k k8s/overlays/prod
helm uninstall spark-operator -n spark-operator
az group delete -g rg-retailbank --yes --no-wait
```
