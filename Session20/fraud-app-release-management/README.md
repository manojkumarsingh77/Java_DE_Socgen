# Fraud App Release Management — Case Study

**Topic coverage:** Image versioning · ACR-style security scanning · Pipeline automation · Blue/Green · Canary release · Dev→Stage→Prod promotion

A trainer-ready, fully working Java 17 + Apache Spark 3.5.1 **fraud-scoring microservice**,
released through a complete local CI/CD & GitOps rig.

---

## 1. The Case Study (business framing for the class)

A payments company runs a fraud-scoring service in production. A bad release here has real
cost: too-loose scoring lets fraud through, too-strict scoring blocks legitimate customers.
The platform team therefore requires that **every** release:

1. Is built once, as an **immutable, versioned artifact** (never rebuilt per environment).
2. Is **security-scanned** before it's allowed to leave the "dev" registry namespace.
3. Moves through **Dev → Stage → Prod** by *promotion* (retagging), not by rebuilding.
4. Reaches production via **Blue/Green** infrastructure, ramped in via a **Canary**
   (10% → 25% → 50% → 100%) so a bad model/rules change affects a small slice of
   transactions before it affects all of them — with **instant rollback**.
5. All of the above is **pipeline-automated**, not run by hand in production.

### The application

`FraudScoringService` is a small, realistic architecture:
- **On startup**, Spark runs a one-time batch aggregation over
  `data/historical_transactions.csv`, computing a per (merchant-category, region) risk
  profile — the classic "offline batch feeds a low-latency serving layer" pattern.
- Spark then stops, and a lightweight embedded HTTP server (JDK's own
  `com.sun.net.httpserver`, zero extra framework) serves scoring requests using that
  in-memory profile plus explainable rules (`ScoringEngine`).

**Endpoints:**
| Method | Path | Purpose |
|---|---|---|
| GET | `/health` | liveness probe (container `HEALTHCHECK`, load balancer checks) |
| GET | `/version` | returns `appVersion`, `gitCommit`, `environment`, `deploySlot` — **this is what makes Blue/Green and Canary demonstrable** |
| POST | `/score` | scores a transaction JSON payload |

---

## 2. Project Structure

```
fraud-app-release-management/
├── pom.xml                              # Java 17, Spark 3.5.1, JUnit5, shade plugin
├── Dockerfile                           # multi-stage build + OCI version labels + JVM tuning
├── docker-compose.dev.yml               # Dev: builds from source
├── docker-compose.stage.yml             # Stage: deploys promoted artifact
├── docker-compose.prod.yml              # Prod: Blue/Green pair + Nginx
├── nginx/
│   ├── nginx.conf
│   └── conf.d/
│       ├── server.conf                  # static proxy rules
│       └── upstream.conf                # REWRITTEN by the canary script
├── .github/workflows/ci-cd.yml          # runnable GitHub Actions pipeline (reference registry)
├── azure-pipelines.yml                  # reference pipeline using real ACR tasks
├── data/historical_transactions.csv     # synthetic data for the Spark boot-time aggregation
├── src/main/java/com/fraud/app/
│   ├── FraudScoringService.java         # Spark bootstrap + HTTP server
│   ├── ScoringEngine.java               # pure, unit-tested scoring rules
│   └── VersionInfo.java                 # build/deploy metadata
├── src/test/java/com/fraud/app/ScoringEngineTest.java
├── scripts/
│   ├── 00-full-demo.sh                  # runs the ENTIRE pipeline end-to-end, with pauses
│   ├── 01-build-and-version.sh          # image versioning
│   ├── 02-scan-image.sh                 # Trivy security-scan gate (ACR/Defender stand-in)
│   ├── 03-start-local-registry.sh       # local ACR stand-in
│   ├── 04-push-dev.sh
│   ├── 05-promote-stage.sh              # retag dev -> stage
│   ├── 06-promote-prod.sh               # retag stage -> prod-green
│   ├── 07-deploy-blue-green.sh          # stand up Blue/Green, 0% canary, smoke test green
│   ├── 08-canary-rollout.sh             # shift traffic 0/10/25/50/100%
│   ├── 09-rollback.sh                   # instant revert to 100% blue
│   ├── 10-finalize-promotion.sh         # green becomes the new blue
│   └── run-local.sh                     # run outside Docker for a quick check
└── .idea/runConfigurations/             # pre-built IntelliJ run config
```

---

## 3. Prerequisites

- **IntelliJ IDEA** + **JDK 17**
- **Docker Desktop** (or Docker Engine + Compose v2) — for everything past section 5
- `git` (optional — used only to embed a commit SHA into the version tag; scripts fall back
  to `nogit` if you're not in a git repo)
- Internet access (Maven Central for dependencies, Docker Hub for base images)

> **One-time Docker daemon setting** for the local registry: if `docker push
> localhost:5000/...` fails with a TLS/certificate error, add `localhost:5000` to Docker's
> `insecure-registries` list (Docker Desktop → Settings → Docker Engine, add
> `"insecure-registries": ["localhost:5000"]`, then Apply & Restart). Most modern Docker
> installs already trust plain-HTTP registries on `localhost` by default, but this is the
> fix if yours doesn't.

---

## 4. Run It in IntelliJ (Java 17) — Step by Step

1. **Unzip** and **File → Open…** the `fraud-app-release-management` folder (containing `pom.xml`).
2. Let Maven import finish (downloads Spark 3.5.1, JUnit 5, org.json).
3. **Set Project SDK to Java 17**: `File → Project Structure → Project → SDK/Language level → 17`.
4. Open **Run → Edit Configurations** — you should see a pre-built **`FraudScoringService`**
   configuration (shipped in `.idea/runConfigurations/`) with the correct `--add-opens` VM
   options and env vars already set. If it isn't visible, create one manually:
   - **Main class:** `com.fraud.app.FraudScoringService`
   - **VM options:**
     ```
     --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED
     ```
   - **Environment variables:** `APP_VERSION=1.0.0-intellij;GIT_COMMIT=local;APP_ENV=local;DEPLOY_SLOT=local;PORT=8080`
5. Click **Run ▶**. Expected console output:
   ```
   Fraud Scoring Service | version=1.0.0-intellij gitCommit=local env=local slot=local ...
   Merchant risk profile loaded (N category/region keys).
   Fraud Scoring Service listening on port 8080
   ```
6. **Test it** — open a terminal and run:
   ```bash
   curl http://localhost:8080/health
   curl http://localhost:8080/version
   curl -X POST http://localhost:8080/score \
     -H "Content-Type: application/json" \
     -d '{"transaction_id":"TX9001","account_id":"ACC900","amount":18000,"merchant_category":"CRYPTO_EXCHANGE","region_code":"RGN-06","device_trust_score":0.2}'
   ```
   Expected: a JSON response with `"risk_band":"CRITICAL"` and `"suspected_fraud":true`.
7. Run the unit tests: right-click `src/test/java/com/fraud/app/ScoringEngineTest.java` →
   **Run 'ScoringEngineTest'**. All 6 tests should pass — this is exactly what the CI
   pipeline's first gate runs via `mvn test`.

**Windows note:** running Spark locally on Windows sometimes needs `winutils.exe` /
`HADOOP_HOME`. Easiest fix: run inside WSL2, or install winutils. This does **not** affect
the Docker path below, which always runs on Linux.

---

## 5. Run It Locally via Terminal (optional, no IntelliJ)

```bash
cd fraud-app-release-management
chmod +x scripts/*.sh
./scripts/run-local.sh
```

---

## 6. The Full Pipeline — Step by Step

Everything below can be run stage-by-stage (recommended for teaching) or all at once with
`scripts/00-full-demo.sh`.

### 6.1 Start the local registry (ACR stand-in)

```bash
chmod +x scripts/*.sh
./scripts/03-start-local-registry.sh
```

### 6.2 Build a versioned image

```bash
./scripts/01-build-and-version.sh 1.0.0
```
Builds `fraud-app:1.0.0-<git-sha>`, with `APP_VERSION`/`GIT_COMMIT`/`BUILD_DATE` baked in as
both OCI image labels (`docker inspect`) and runtime env vars (`GET /version`). Tag is
recorded in `.last-build.env` so later scripts can pick it up automatically.

Inspect the version metadata on the built image:
```bash
docker inspect fraud-app:$(grep IMAGE_TAG .last-build.env | cut -d= -f2) \
  --format '{{json .Config.Labels}}'
```

### 6.3 Security-scan gate (Trivy — ACR/Defender stand-in)

```bash
./scripts/02-scan-image.sh
```
Fails (non-zero exit) if any unfixed HIGH/CRITICAL CVE is found — exactly the gate a real
`az acr task` + Microsoft Defender for Containers scan enforces before letting an image
leave the registry's "dev" namespace. **This should PASS** for the demo image (Temurin JRE
base images are actively patched); try building against an old/unpatched base image to see
it fail and block the pipeline.

### 6.4 Push to the registry as `dev`

```bash
./scripts/04-push-dev.sh
```

### 6.5 Promote Dev → Stage (retag, no rebuild)

```bash
./scripts/05-promote-stage.sh
docker compose -f docker-compose.stage.yml up -d
curl http://localhost:8082/version
```

### 6.6 Promote Stage → Prod-Green (release candidate)

```bash
./scripts/06-promote-prod.sh stage
# first release only - bootstrap prod-blue so there's a stable baseline:
docker tag localhost:5000/fraud-app:stage localhost:5000/fraud-app:prod-blue
docker push localhost:5000/fraud-app:prod-blue
```

### 6.7 Deploy Blue/Green (starts at 0% canary)

```bash
./scripts/07-deploy-blue-green.sh
```
This brings up `fraud-app-blue`, `fraud-app-green`, and `nginx`, resets traffic to 100%
blue, and smoke-tests green **directly** (bypassing the load balancer) before it ever
receives real traffic.

### 6.8 Canary rollout — the payoff step

```bash
./scripts/08-canary-rollout.sh 10     # 10% of traffic now hits the new version
./scripts/08-canary-rollout.sh 50     # 50/50
./scripts/08-canary-rollout.sh 100    # full cutover
```
After each step, verify the traffic split by hammering the load balancer and counting which
slot answered:
```bash
for i in $(seq 1 20); do curl -s http://localhost:8080/version | grep -o '"deploySlot":"[a-z]*"'; done | sort | uniq -c
```

**Instant rollback** at any point:
```bash
./scripts/09-rollback.sh
```

**Finalize** once you're happy at 100% (green becomes the new blue for next time):
```bash
./scripts/10-finalize-promotion.sh
```

### 6.9 Or run the whole thing in one guided pass

```bash
./scripts/00-full-demo.sh 1.1.0
```
Pauses before each stage so an instructor can narrate — ideal for a live classroom walkthrough.

---

## 7. Pipeline Automation Files (for reference / adapt to your org)

- **`.github/workflows/ci-cd.yml`** — a runnable GitHub Actions workflow with the same
  stages as the shell scripts: build & test → build & version image → Trivy scan gate →
  push dev → promote stage (`environment: staging`, configurable required reviewers) →
  promote prod-green (`environment: production`, configurable required reviewers) → canary.
  Fork this repo to GitHub, push, and watch it run (registry push steps are commented out by
  default since they target `localhost:5000`, which a GitHub-hosted runner can't reach —
  swap in a real ACR/Docker Hub and uncomment to make it fully live).
- **`azure-pipelines.yml`** — the same stages expressed with native **ACR tasks**
  (`az acr import` for retag-based promotion, `Docker@2` for build, environment approval
  gates) since the case study calls out ACR specifically. This needs a real Azure DevOps
  project + ACR instance + service connections to execute; it's included as the
  Azure-native reference implementation.

### Mapping the local demo to real Azure Container Registry

| Local demo command | Real ACR equivalent |
|---|---|
| `docker run -d -p 5000:5000 registry:2` | `az acr create --name myregistry --sku Standard` |
| `docker push localhost:5000/fraud-app:dev` | `az acr login --name myregistry` then `docker push myregistry.azurecr.io/fraud-app:dev` |
| `scripts/02-scan-image.sh` (Trivy) | Microsoft Defender for Cloud's ACR scan-on-push, or an `az acr task` running Trivy/Grype |
| `docker tag ... stage && docker push` | `az acr import --name myregistry --source fraud-app:dev-tag --image fraud-app:stage` (server-side retag, no re-pull needed) |
| `docker exec fraud-nginx nginx -s reload` | Traffic-split via Azure Front Door / Application Gateway weighted backends, or Argo Rollouts / Flagger on AKS |

---

## 8. Topic → File Mapping (for the training deck)

| Topic | Where to show it |
|---|---|
| **Image versioning** | `Dockerfile` `ARG`/`LABEL org.opencontainers.image.*`; `scripts/01-build-and-version.sh`; `GET /version` endpoint; `docker inspect ... --format '{{json .Config.Labels}}'` |
| **ACR security scanning** | `scripts/02-scan-image.sh` (Trivy gate, exit-code enforced); `.github/workflows/ci-cd.yml` `security-scan` job; `azure-pipelines.yml` `SecurityScan` stage; README §7 mapping table |
| **Pipeline automation** | `.github/workflows/ci-cd.yml` (runnable), `azure-pipelines.yml` (ACR-native reference), `scripts/00-full-demo.sh` (the same flow, scriptable/teachable locally) |
| **Blue/Green** | `docker-compose.prod.yml` (`fraud-app-blue` / `fraud-app-green` services); `nginx/conf.d/upstream.conf`; `scripts/07-deploy-blue-green.sh`; `scripts/09-rollback.sh` |
| **Canary release** | `scripts/08-canary-rollout.sh` (weighted Nginx upstream: 0/10/25/50/100%); `served_by.deploySlot` field in every `/score` response for live verification |
| **Dev→Stage→Prod promotion** | `docker-compose.dev.yml` → `docker-compose.stage.yml` → `docker-compose.prod.yml`; `scripts/05-promote-stage.sh` → `scripts/06-promote-prod.sh` (retag-only promotion, never rebuild) |

---

## 9. Troubleshooting

| Symptom | Fix |
|---|---|
| `InaccessibleObjectException` running in IntelliJ | The `--add-opens` VM options are missing from the run configuration — see step 4.4. |
| `docker push localhost:5000/...` fails with TLS error | Add `localhost:5000` to Docker's `insecure-registries` (see Prerequisites). |
| `nginx: [emerg] host not found in upstream` | The blue/green containers aren't up yet — run `docker compose -f docker-compose.prod.yml up -d` before the canary script. |
| Canary script says "No such container: fraud-nginx" | Run `scripts/07-deploy-blue-green.sh` first to start the prod stack. |
| Trivy scan hangs or fails to pull its DB | Needs outbound internet access on first run (downloads the vulnerability DB); subsequent runs use the `trivy-cache` volume. |
| Maven can't download Spark/JUnit | Check proxy/firewall allows `repo.maven.apache.org`. |

---

## 10. Cleanup

```bash
docker compose -f docker-compose.dev.yml down
docker compose -f docker-compose.stage.yml down
docker compose -f docker-compose.prod.yml down
docker rm -f local-registry 2>/dev/null || true
docker rmi -f $(docker images 'fraud-app*' -q) $(docker images 'localhost:5000/fraud-app*' -q) 2>/dev/null || true
rm -f .last-build.env
```

---

## 11. Suggested Classroom Flow (120 minutes)

1. **(10 min)** Business framing: why fraud-app releases need extra care.
2. **(15 min)** Run in IntelliJ, exercise `/score`, run the unit tests — show the app itself.
3. **(15 min)** Walk `Dockerfile` — versioning ARGs/LABELs, build & run §6.1–6.2.
4. **(15 min)** Run the Trivy gate (§6.3) — discuss shift-left security, show a failing scan
   by pointing at an old base image.
5. **(15 min)** Dev → Stage promotion (§6.4–6.5) — emphasize retag-not-rebuild.
6. **(25 min)** Blue/Green + Canary (§6.6–6.8) — the centerpiece. Deploy, ramp 10→50→100,
   hammer `/version` live on a shared screen to *watch* traffic shift, then trigger a
   rollback live.
7. **(15 min)** Walk `.github/workflows/ci-cd.yml` and `azure-pipelines.yml` — map every
   manual step just performed to its pipeline-automated equivalent, discuss environment
   approval gates.
8. **(10 min)** Q&A / discussion: mapping this to Argo Rollouts, Flagger, or Azure Front
   Door weighted routing for a real Kubernetes/AKS production setup.
