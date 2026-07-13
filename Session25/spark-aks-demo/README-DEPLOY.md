# Deploying `spark-aks-demo` to AKS via ACR — Full Walkthrough

This runs the exact same jar two ways: first locally in IntelliJ for a fast
feedback loop, then unmodified inside a container on the AKS cluster you
provisioned with Terraform. Nothing in the Java code changes between the
two — only how `spark-submit` is invoked.

**What the job does:** generates 50,000 synthetic retail-banking card
transactions in memory, buckets them into 5-minute windows per account
using Spark SQL's `window()` function, flags any account/window that hits
≥5 transactions or ≥$10,000 — a classic "velocity attack" fraud pattern —
and writes both the raw transactions (partitioned by state) and the
flagged windows as files.

---

## 0. Prerequisites

| Tool | macOS (Apple Silicon) | Windows 11 |
|---|---|---|
| Java 17 | `brew install --cask temurin17` | Temurin 17 MSI from adoptium.net |
| Maven | `brew install maven` | `choco install maven` |
| Docker Desktop | `brew install --cask docker` | `winget install Docker.DockerDesktop` |
| Azure CLI | `brew install azure-cli` | `winget install Microsoft.AzureCLI` |
| kubectl | `az aks install-cli` (installs both) | same |

Confirm versions before continuing:
```bash
java -version      # must report 17.x
mvn -version       # should show "Java version: 17..."
docker version     # Client and Server both present (Docker Desktop running)
az version
kubectl version --client
```

You should already have, from the Terraform apply:
- Resource group `rg-aks-spark-fundamentals-eus2`
- AKS cluster `aks-spark-fundamentals`
- ACR registry (your chosen unique name, e.g. `acrsparkdemo`)

---

## 1. Project layout

```
spark-aks-demo/
├── pom.xml
├── Dockerfile
├── README-DEPLOY.md          (this file)
├── k8s/
│   └── spark-rbac.yaml
└── src/main/java/com/training/spark/aks/
    └── RetailFraudVelocityJob.java
```

---

## 2. Run it locally first (IntelliJ)

Always prove the logic works locally before you spend time on containers
and cluster networking — it isolates "is my Spark code correct" from "is my
Kubernetes deployment correct."

1. Open the `spark-aks-demo` folder in IntelliJ IDEA as a Maven project
   (`File → Open`, select the folder containing `pom.xml`).
2. Let Maven finish indexing (bottom-right progress bar).
3. `Run → Edit Configurations → + → Application`:
   - **Main class:** `com.training.spark.aks.RetailFraudVelocityJob`
   - **Program arguments:** `/tmp/spark-aks-demo-output 50000`
   - **VM options** (Modify options → Add VM options):
     ```
     -Xms1g -Xmx3g
     -Dspark.master=local[*]
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
     --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED
     ```
   These `--add-opens` flags are required on Java 17 because Spark's
   internals (Kryo/Tungsten unsafe memory access, Hadoop's shim layer)
   use reflection into JDK internal packages that are strongly
   encapsulated by default from Java 16 onward. Without them you'll hit
   `InaccessibleObjectException` at startup — this list is identical on
   Windows and Apple Silicon; nothing here is OS-specific.
4. Change **"Use classpath of module"** dropdown scope from `Test` to
   the module's main classpath if prompted.
5. Click Run (▶). You should see log lines ending with:
   ```
   === TOP FLAGGED ACCOUNT WINDOWS ...
   +-----------+--------------------+---------+------------+-----------+...
   ...
   Job complete.
   ```
7. Confirm output landed locally:
   ```bash
   ls -R /tmp/spark-aks-demo-output
   ```

If this step fails, fix it here — don't move on to Docker/AKS until local
`local[*]` execution is clean.

---

## 3. Build the application jar

```bash
cd spark-aks-demo
mvn -B clean package -DskipTests
ls -la target/spark-aks-demo-1.0.0.jar
```

This produces a **thin jar** (tens of KB) — it contains only your compiled
classes, not Spark itself, because `spark-core`/`spark-sql` are scoped
`provided` in `pom.xml`. Spark's actual classes come from `/opt/spark/jars`
already baked into the base Docker image in the next step. This is the
standard pattern for Spark-on-Kubernetes: your jar and the cluster's Spark
distribution must be the same version, but you don't ship Spark itself
inside your app jar.

---

## 4. Build the Docker image

The `Dockerfile` is a two-stage build:
- **Stage 1** (`maven:3.9.9-eclipse-temurin-17`): compiles the jar inside a
  clean, reproducible container — you don't need Maven's exact version or
  Java 17 pinned correctly on your host for this stage to work identically
  on macOS or Windows.
- **Stage 2** (`apache/spark:3.5.3-java17-python3`): the official Apache
  Spark image, with only `target/spark-aks-demo-1.0.0.jar` copied in. This
  keeps the final image close to Spark's own published image size instead
  of re-downloading/re-shading Spark's ~300MB of jars yourself.

```bash
cd spark-aks-demo
docker build -t spark-aks-demo:1.0.0 .
```

On Apple Silicon (M1 Max), Docker Desktop builds an `arm64` image by
default. AKS nodes are `amd64` (`Standard_D2s_v3`). Build explicitly for
the target platform so the image actually runs on the cluster:

```bash
docker buildx build --platform linux/amd64 -t spark-aks-demo:1.0.0 . --load
```

On Windows 11 (x64), the default build already targets `amd64` — no flag
needed, but adding `--platform linux/amd64` is harmless and makes the
command identical across both OSes, which is worth doing so your team runs
one copy-pasted command regardless of laptop.

Quick sanity check before pushing anywhere:
```bash
docker run --rm spark-aks-demo:1.0.0 /opt/spark/bin/spark-submit \
  --class com.training.spark.aks.RetailFraudVelocityJob \
  --master local[2] \
  local:///opt/spark-apps/spark-aks-demo.jar /tmp/output 5000
```
This runs the job *inside the container* with a local (non-Kubernetes)
Spark master, proving the image itself is correctly built before you
involve AKS networking/RBAC at all. `local:///...` (three slashes) tells
Spark the jar is already present in the container filesystem — no
download/copy step needed at submit time.

---

## 5. Push the image to ACR

Get your ACR login server (from Terraform output or the portal):
```bash
cd terraform-aks-spark
terraform output -raw acr_login_server
# e.g. acrsparkdemo.azurecr.io
```

Authenticate Docker against ACR (uses your `az login` session — no
separate password to manage):
```bash
az acr login --name acrsparkdemo
```

Tag and push:
```bash
ACR_LOGIN_SERVER=acrsparkdemo.azurecr.io

docker tag spark-aks-demo:1.0.0 $ACR_LOGIN_SERVER/spark-aks-demo:1.0.0
docker push $ACR_LOGIN_SERVER/spark-aks-demo:1.0.0
```

Verify it landed:
```bash
az acr repository show-tags --name acrsparkdemo --repository spark-aks-demo
```

You do **not** need an `imagePullSecret` for AKS to pull this image — the
Terraform config already granted the AKS kubelet identity the `AcrPull`
role on this registry (`azurerm_role_assignment.aks_acr_pull`). Kubernetes
nodes authenticate to ACR transparently via that managed identity.

---

## 6. Connect kubectl to the cluster

```bash
az aks get-credentials \
  --resource-group rg-aks-spark-fundamentals-eus2 \
  --name aks-spark-fundamentals \
  --overwrite-existing

kubectl get nodes
```
You should see 2 nodes in `Ready` state (`Standard_D2s_v3`, matching the
Terraform node pool).

> If your AKS cluster has Azure AD/Entra RBAC enabled and interactive
> browser login becomes a problem for scripted `spark-submit` runs later,
> re-fetch with `--admin` to use the cluster's local admin credentials
> instead (cert-based, no browser prompt):
> `az aks get-credentials ... --admin --overwrite-existing`

---

## 7. Apply the Spark RBAC manifest

The Spark driver, once scheduled onto a pod in the cluster, needs its own
Kubernetes API permissions to create executor pods and a driver↔executor
Service — separate from your personal `kubectl` identity. Apply once:

```bash
kubectl apply -f k8s/spark-rbac.yaml
```

Confirm:
```bash
kubectl get sa,role,rolebinding -n spark-jobs
```
Expect to see `spark-sa`, `spark-driver-role`, `spark-driver-rolebinding`.

---

## 8. Get the Kubernetes API server URL

`spark-submit`'s Kubernetes backend needs the cluster's API endpoint as
its `--master`, in the form `k8s://https://<host>:<port>`:

```bash
kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}'
# e.g. https://aks-spark-fund-rg-aks-spark-fun-51ca1e-xxxxxxxx.hcp.eastus2.azmk8s.io:443
```
Save this as an env var for the next step:
```bash
K8S_API_SERVER=$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')
```

---

## 9. Submit the job to AKS

Because `spark-submit`'s version must match the image's Spark version
exactly, the cleanest cross-platform way to invoke it — without installing
a separate local Spark distribution on macOS/Windows — is to run
`spark-submit` **from the image you just built**, mounting your kubeconfig
into the container:

```bash
docker run --rm \
  -v "$HOME/.kube/config:/opt/spark/.kube/config:ro" \
  -e KUBECONFIG=/opt/spark/.kube/config \
  $ACR_LOGIN_SERVER/spark-aks-demo:1.0.0 \
  /opt/spark/bin/spark-submit \
    --master "k8s://$K8S_API_SERVER" \
    --deploy-mode cluster \
    --name retail-fraud-velocity-job \
    --class com.training.spark.aks.RetailFraudVelocityJob \
    --conf spark.kubernetes.namespace=spark-jobs \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-sa \
    --conf spark.kubernetes.container.image=$ACR_LOGIN_SERVER/spark-aks-demo:1.0.0 \
    --conf spark.kubernetes.container.image.pullPolicy=Always \
    --conf spark.executor.instances=2 \
    --conf spark.driver.memory=1g \
    --conf spark.executor.memory=1g \
    --conf spark.executor.cores=1 \
    --conf spark.kubernetes.driver.pod.name=retail-fraud-driver \
    local:///opt/spark-apps/spark-aks-demo.jar /opt/spark-apps/output 50000
```

**What each flag is doing:**
- `--master k8s://...` — tells Spark to use the Kubernetes cluster
  manager backend rather than YARN/Standalone/local.
- `--deploy-mode cluster` — the driver itself runs *inside* a pod on the
  cluster (not on your laptop). This is what you want for anything beyond
  local debugging: the job survives your laptop closing its lid.
- `spark.kubernetes.namespace` — matches the namespace from `spark-rbac.yaml`.
- `spark.kubernetes.authenticate.driver.serviceAccountName` — the driver
  pod runs as `spark-sa`, which has permission (via the Role/RoleBinding
  you applied) to create executor pods.
- `spark.kubernetes.container.image` — same image for driver AND
  executors by default; Spark launches executor pods from this image
  automatically, no separate executor image needed.
- `spark.executor.instances=2` — matches your 2-node cluster; one
  executor per node keeps this demo simple. Bump this only after
  increasing node count or enabling the cluster autoscaler.
- `local:///opt/spark-apps/spark-aks-demo.jar` — `local://` (not
  `file://` or a bare path) tells the Kubernetes backend the jar is
  already inside the container image at that path — Spark does not try
  to upload/stage it from your laptop, which is what makes this fast and
  avoids needing a distributed file system just to submit a job.

`spark-submit` in cluster mode returns almost immediately once the driver
pod is accepted — it does not block waiting for the job to finish. Move to
the next step to watch it run.

---

## 10. Watch it run

```bash
kubectl get pods -n spark-jobs -w
```
You'll see, in order:
1. `retail-fraud-driver` → `Pending` → `ContainerCreating` → `Running`
2. Two executor pods appear (`retail-fraud-velocity-job-<id>-exec-1`, `-exec-2`) once the driver requests them
3. Executors finish and disappear (Spark tears them down after the job completes)
4. Driver pod status moves to `Completed`

Stream the driver's logs live (this is where your `System.out.println`
tables and SLF4J log lines show up):
```bash
kubectl logs -n spark-jobs retail-fraud-driver -f
```

If you want to watch progress in the actual Spark UI (DAG visualization,
stage/task timing, shuffle read/write):
```bash
kubectl port-forward -n spark-jobs retail-fraud-driver 4040:4040
```
Then open `http://localhost:4040` in a browser while the driver pod is
still `Running` (the UI process exits with the driver).

---

## 11. Verify the output

The job wrote output *inside the driver pod's ephemeral container
filesystem* at `/opt/spark-apps/output` — fine for this demo, but it
disappears when the pod is garbage-collected after completion. To pull it
out before that happens:

```bash
kubectl cp spark-jobs/retail-fraud-driver:/opt/spark-apps/output ./aks-job-output
find ./aks-job-output -maxdepth 3
```

For anything beyond a demo, point `outputBasePath` (the job's first
argument) at `abfss://<container>@<storageaccount>.dfs.core.windows.net/...`
backed by an ADLS Gen2 storage account instead of local pod storage — the
job code doesn't need to change, since it already takes the output path as
a parameter and uses Spark's standard `DataFrameWriter`, which supports
`abfss://` paths natively once the Hadoop Azure connector and storage
credentials are on the classpath.

---

## 12. Re-running after a code change

```bash
mvn -B clean package -DskipTests
docker buildx build --platform linux/amd64 -t spark-aks-demo:1.0.1 . --load
docker tag spark-aks-demo:1.0.1 $ACR_LOGIN_SERVER/spark-aks-demo:1.0.1
docker push $ACR_LOGIN_SERVER/spark-aks-demo:1.0.1
```
Bump the version tag (`1.0.1`, `1.0.2`, ...) rather than reusing `1.0.0` —
with `pullPolicy=Always` reusing a tag still works, but distinct tags make
it unambiguous in `kubectl describe pod` exactly which build is running,
which matters once you have more than one person deploying to the same
cluster. Re-run the `spark-submit` from step 9 with the new
`spark.kubernetes.container.image` value.

---

## 13. Cleanup

```bash
# Remove just this job's driver pod/service if still lingering:
kubectl delete pod -n spark-jobs retail-fraud-driver --ignore-not-found

# Remove the namespace (also removes the ServiceAccount/Role/RoleBinding):
kubectl delete namespace spark-jobs

# Remove the pushed image from ACR:
az acr repository delete --name acrsparkdemo --repository spark-aks-demo --yes
```
To tear down the whole cluster, go back to the Terraform project and run
`terraform destroy`.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `InaccessibleObjectException` running locally | Missing `--add-opens` VM options | Re-check step 2, VM options exactly as listed |
| `ImagePullBackOff` on driver/executor pods | Image tag doesn't exist in ACR, or wrong login server | `az acr repository show-tags` to confirm; check `spark.kubernetes.container.image` matches exactly |
| `Forbidden: pods "..." is forbidden` in driver logs | RBAC not applied, or wrong service account | Re-apply `k8s/spark-rbac.yaml`; confirm `--conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-sa` |
| Driver pod `Pending` forever | Cluster has no schedulable capacity (both `Standard_D2s_v3` nodes already full) | `kubectl describe pod -n spark-jobs retail-fraud-driver` for the exact scheduling error; reduce `spark.driver.memory`/`spark.executor.memory` or scale the node pool |
| `exec /opt/entrypoint.sh: exec format error` | Image built for `arm64` on Apple Silicon, but nodes are `amd64` | Rebuild with `docker buildx build --platform linux/amd64 ...` as shown in step 4 |
| `spark-submit` hangs with no pod ever created | `K8S_API_SERVER` wrong, or kubeconfig not mounted correctly into the submit container | Re-run step 8; confirm `kubectl cluster-info` works from your host first |
