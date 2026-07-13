# CI/CD & GitOps for Spark — Instructor Demo Guide

**Audience:** Senior data engineers
**Format:** Problem → Solution, code-first, live-executed
**Stack:** Java 17, Apache Spark 3.5.1 (local mode), plain files as a local "GitOps store" (no cloud account required)
**Runs on:** IntelliJ IDEA (macOS Apple Silicon and Windows), and standalone via terminal/Docker

---

## 0. The idea behind this demo

Real CI/CD needs a git host, a container registry, and a cluster. A training room usually has none of those wired up identically for every learner. So this demo **simulates a complete CI/CD & GitOps pipeline as a runnable Java application** — every stage (build, version, scan, push, promote, blue/green, canary) is a real class that does real work (it actually builds/executes a Spark job as its health check), backed by a local folder (`.registry/`) standing in for your container registry + deployment state.

Alongside it, **real** GitHub Actions and Azure DevOps pipeline files are provided, annotated line-by-line with which Java class/method they correspond to — so you teach the concept live with code that always works, then show the room "and here is exactly this, as it would really look in Azure/GitHub."

| # | Topic | Primary solution artifact |
|---|-------|---------------------------|
| 1 | Image versioning | [`BuildInfo.java`](src/main/java/com/training/gitops/build/BuildInfo.java), [`SemanticVersionCalculator.java`](src/main/java/com/training/gitops/build/SemanticVersionCalculator.java) |
| 2 | ACR security scanning | [`SecurityScanner.java`](src/main/java/com/training/gitops/security/SecurityScanner.java) |
| 3 | Pipeline automation | [`CIPipeline.java`](src/main/java/com/training/gitops/pipeline/CIPipeline.java) + [`CDPipeline.java`](src/main/java/com/training/gitops/pipeline/CDPipeline.java) + [`.github/workflows/ci.yml`](.github/workflows/ci.yml) / [`cd.yml`](.github/workflows/cd.yml) |
| 4 | Blue/Green release | [`BlueGreenDeploymentManager.java`](src/main/java/com/training/gitops/deployment/BlueGreenDeploymentManager.java) |
| 5 | Canary release | [`CanaryReleaseManager.java`](src/main/java/com/training/gitops/deployment/CanaryReleaseManager.java) |
| 6 | Dev → Stage → Prod promotion | [`EnvironmentPromoter` logic inside `CDPipeline.java`](src/main/java/com/training/gitops/pipeline/CDPipeline.java) + [`ArtifactRegistry.java`](src/main/java/com/training/gitops/registry/ArtifactRegistry.java) |

The "artifact" being built/versioned/scanned/deployed throughout is a real Spark job:
[`InventoryAnalyticsJob.java`](src/main/java/com/training/gitops/job/InventoryAnalyticsJob.java) — every "deployment" in this demo actually **executes** this job as a health check. Nothing is deployed just by printing text.

---

## 1. CI vs CD — how this demo keeps them clearly separate

This app has **two distinct, independently runnable modes**, exactly matching your request to demo CI and CD as clearly separate concepts:

| Mode | Class | What it proves |
|---|---|---|
| `ci` | [`CIPipeline.java`](src/main/java/com/training/gitops/pipeline/CIPipeline.java) | Build → Version → Scan → Push → auto-deploy **Dev**. Produces ONE immutable, scanned artifact. |
| `cd` | [`CDPipeline.java`](src/main/java/com/training/gitops/pipeline/CDPipeline.java) | Takes the artifact CI already produced and **promotes** it (never rebuilds it) through Stage (Blue/Green) and Prod (Canary), gated by approvals. |
| `pipeline` | `App.java` calls `CIPipeline` then `CDPipeline` | The full flow in one command, for a fast end-to-end pass. |

Run them **separately** in the session (Part A = `ci`, Part B = `cd`) so learners see the CI/CD boundary explicitly, then run `pipeline` once at the end to show them chained together.

---

## 2. Suggested session flow (≈100–130 minutes)

1. **Setup & orientation** (10 min) — open in IntelliJ, run `8_Print_Version`, look at `.registry/`.
2. **Part A — CI, step by step** (30 min) — topics: Image Versioning, ACR Security Scanning, Pipeline Automation (build half).
3. **Part B — CD, step by step** (40 min) — topics: Dev→Stage→Prod Promotion, Blue/Green, Canary, Pipeline Automation (deploy half).
4. **Failure scenarios live** (20 min) — scan gate trips, blue/green refuses a bad deploy, canary auto-rolls-back.
5. **Map to real infra** (15 min) — walk `.github/workflows/ci.yml` / `cd.yml` / `azure-pipelines.yml` side-by-side with the Java classes.

---

## 3. STEP BY STEP — Part A: the CI pipeline

**Say to the room:** *"CI answers one question: is this specific commit, built into a specific versioned artifact, PROVEN safe to exist in a registry at all? CI never touches Stage or Prod."*

### Step A0 — one-time setup
```bash
mvn -q clean package -DskipTests
```
Or in IntelliJ: let Maven import, set Project SDK to 17, done.

### Step A1 — run CI for the first time
- IntelliJ: run configuration `1_CI_Only`
- Terminal (macOS/Linux): `./scripts/run-local.sh ci`
- Terminal (Windows): `scripts\run-local.bat ci`

**Walk the console output stage by stage, pointing at the matching method:**

| Console line prefix | Method driving it |
|---|---|
| `[CI 1/4] BUILD` | `InventoryAnalyticsJob.runSmokeTest()` |
| `[CI 2/4] VERSION` | `SemanticVersionCalculator.next()` + `BuildInfo.capture()` |
| `[CI 3/4] SCAN` | `SecurityScanner.scan()` |
| `[CI 4/4] PUSH` | `ArtifactRegistry.push()` then `ArtifactRegistry.promote(version, "dev")` |

**Expected outcome:** version `1.0.1` (or `1.0.0` → `1.0.1` on a fresh registry — see Step A0.5 below), scan passes (default fixture is clean), artifact pushed, Dev auto-deployed and smoke-tested healthy.

> **Step A0.5 (optional, first run only):** if you want to start from a guaranteed-clean slate at the top of a session, run mode `reset` first (IntelliJ config `9_Reset_Demo_State`, or `./scripts/reset-demo-state.sh`).

### Step A2 — run CI again, show version auto-increments
Run mode `ci` a second time. **Point out:** version goes `1.0.1 → 1.0.2` with zero code changes — this is exactly what a real pipeline does on every merge to `main`. Try:
```bash
VERSION_BUMP=minor ./scripts/run-local.sh ci   # -> 1.1.0
VERSION_BUMP=major ./scripts/run-local.sh ci   # -> 2.0.0
```
**Discussion:** *"In a real pipeline, what decides major vs minor vs patch automatically?"* → conventional commit prefixes (`fix:`, `feat:`, `BREAKING CHANGE:`) parsed by tools like semantic-release; here we simulate that decision directly via `VERSION_BUMP`.

### Step A3 — trip the ACR security scanning gate (the money shot for this topic)
```bash
./scripts/inject-critical-vuln.sh      # macOS/Linux
scripts\inject-critical-vuln.bat       # Windows
./scripts/run-local.sh ci              # or scripts\run-local.bat ci
```
**Expected outcome:** CI stops at `[CI 3/4] SCAN` — `1 CRITICAL vulnerability(ies) found`, `PUSH` never runs, **no new version reaches the registry or Dev**. This is `SecurityScanner.scan()`'s policy (zero criticals, configurable HIGH ceiling via `MAX_HIGH_VULNS`) in action.

Restore and re-run to prove the gate is dynamic, not a one-time check:
```bash
./scripts/restore-clean-vuln.sh
./scripts/run-local.sh ci   # passes again
```

**Discussion prompts:**
- *"Why fail on ANY critical but tolerate up to N highs?"* → zero-tolerance for known-exploitable severe CVEs; a small, monitored backlog of high findings is a normal, triaged reality in most orgs.
- *"What's the real ACR equivalent?"* → Microsoft Defender for Cloud's registry scanning (automatic on push) or an explicit Trivy/Grype step — see the annotated `security-scan` job in `.github/workflows/ci.yml`.

---

## 4. STEP BY STEP — Part B: the CD pipeline

**Say to the room:** *"CD answers a different question: should THIS artifact — already built once, already scanned once — be trusted with more traffic? CD never rebuilds anything; it only decides who gets to see what already exists."*

### Step B1 — attempt CD with no approvals
- IntelliJ: run configuration `2_CD_Only_No_Approval`
- Terminal: `./scripts/run-local.sh cd`

**Expected outcome:** `[CD 1/2] STAGE - gate: APPROVE_STAGE=false` → **BLOCKED**. Nothing deploys. This proves the approval gate is a hard stop, not a suggestion.

### Step B2 — approve Stage only
- IntelliJ: run configuration `3_CD_Approve_Stage_Only`
- Terminal (macOS/Linux): `APPROVE_STAGE=true ./scripts/run-local.sh cd`
- Terminal (Windows): `set APPROVE_STAGE=true && scripts\run-local.bat cd`

**Walk the console output:**

| Console line | Method driving it |
|---|---|
| `[stage] approved -> deploying via BLUE/GREEN strategy` | `BlueGreenDeploymentManager.deployAndSwitch()` |
| `deploying candidate version ... into INACTIVE slot` | proves zero impact to the currently-live slot during deploy |
| `switching live traffic blue -> green` | the atomic cutover |

**Expected outcome:** Stage is now live on the new version; **Prod is untouched** because `APPROVE_PROD` was never set — point this out explicitly, it's the whole point of a two-stage gate.

### Step B3 — approve both Stage and Prod
- IntelliJ: run configuration `4_CD_Approve_Stage_And_Prod`
- Terminal (macOS/Linux): `APPROVE_STAGE=true APPROVE_PROD=true ./scripts/run-local.sh cd`
- Terminal (Windows): `set APPROVE_STAGE=true&& set APPROVE_PROD=true&& scripts\run-local.bat cd`

**Walk the console output:**

| Console line | Method driving it |
|---|---|
| `[prod] approved -> deploying via CANARY strategy` | `CanaryReleaseManager.rollout()` |
| `wave 10% -> candidate reqs= 5 ...` | one line PER wave (10/25/50/100%) |
| `all waves completed within error threshold -> candidate PROMOTED to 100%` | canary success |

**Expected outcome:** all four waves pass (default error rate ~1%, threshold 15%), Prod is promoted, final state block shows `dev`/`stage`/`prod` all on the same version.

### Step B4 — full pipeline in one shot
- IntelliJ: run configuration `5_Full_Pipeline_CI_then_CD`
- Terminal: `APPROVE_STAGE=true APPROVE_PROD=true ./scripts/run-local.sh pipeline`

This is the "show me the whole thing end to end" command for a live demo finale.

---

## 5. Failure scenarios — do these live, they are the highest-value moments

### 5.1 Blue/Green refuses a broken deployment
- IntelliJ: run configuration `6_BlueGreen_Deep_Dive`
- Terminal: `./scripts/run-local.sh bluegreen-demo`

Walks through: healthy deploy+switch → **a deploy with an injected failure, which the health check catches BEFORE any traffic switch** → the active slot is provably unchanged → a manual rollback for comparison.

**Discussion:** *"Why is this safer than a rolling update?"* → the broken version never received live traffic at all; a rolling update would have exposed some fraction of users before anyone noticed.

### 5.2 Canary auto-rollback under real-traffic-like failure
- IntelliJ: run configuration `7_Canary_Deep_Dive`
- Terminal: `./scripts/run-local.sh canary-demo`

Shows a healthy rollout completing all waves, immediately followed by a rollout with `injectFailure=true` (35% simulated candidate error rate) — watch it pass the 10% wave (small sample noise) and then trip the threshold and **auto-revert to 0%** at a later wave, exactly like `CanaryReleaseManager.rollout()`'s code.

You can also trigger this inside the full `cd` pipeline:
```bash
APPROVE_STAGE=true APPROVE_PROD=true CANARY_INJECT_FAILURE=true ./scripts/run-local.sh cd
```
**Expected outcome:** Stage succeeds (Blue/Green isn't given the failure flag), Prod's canary trips and rolls back — final state shows `prod` still on the OLD version while `stage` is on the new one. This is a great one to leave on screen and ask: *"is this a good place for the system to be? What should happen next?"*

### 5.3 Tune the canary threshold live
```bash
CANARY_ERROR_THRESHOLD_PERCENT=50 APPROVE_STAGE=true APPROVE_PROD=true CANARY_INJECT_FAILURE=true ./scripts/run-local.sh cd
```
Raising the threshold above the injected failure rate lets the same "broken" candidate through — a good prompt for *"who should own this number, and how would you catch it being set dangerously high in a PR review?"*

---

## 6. Inspect the GitOps audit trail

After running through Parts A and B, open `.registry/` in IntelliJ's project tree (or `cat`/`type` the files):

| File | What it represents in real infra |
|---|---|
| `version.txt` | The latest semver the pipeline has produced |
| `manifest-<version>.json` | An ACR image manifest / SBOM record — scan result frozen at push time |
| `env-dev.txt`, `env-stage.txt`, `env-prod.txt` | Which version each environment currently points to (a GitOps manifest repo's desired state) |
| `bluegreen-state.txt` | Which slot (blue/green) is currently live (a Kubernetes Service selector / router upstream) |
| `promotion-history.json` | Append-only audit log of every promotion (who/what/when) |

**Discussion:** *"If this were a real GitOps setup (Argo CD / Flux), where would this state actually live?"* → in a separate, version-controlled "manifests" repository that the cluster continuously reconciles against — the cluster is never manually `kubectl apply`'d to.

---

## 7. Environment variable reference

| Variable | Used by | Default | Effect |
|---|---|---|---|
| `VERSION_BUMP` | `SemanticVersionCalculator` | `patch` | `patch` \| `minor` \| `major` |
| `SCAN_REPORT_PATH` | `SecurityScanner` | `config/vulnerability-findings.json` | Path to the scan fixture read by the gate |
| `MAX_HIGH_VULNS` | `SecurityScanner` | `5` | HIGH findings allowed before CI fails |
| `APPROVE_STAGE` | `CDPipeline` | `false` | Simulates the Stage environment's required-reviewer approval |
| `APPROVE_PROD` | `CDPipeline` | `false` | Simulates the Prod environment's required-reviewer approval |
| `BLUEGREEN_INJECT_FAILURE` | `CDPipeline` → `BlueGreenDeploymentManager` | `false` | Forces the Stage health check to fail, for demoing safe-refusal |
| `CANARY_INJECT_FAILURE` | `CDPipeline` → `CanaryReleaseManager` | `false` | Raises candidate error rate to ~35%, for demoing auto-rollback |
| `CANARY_ERROR_THRESHOLD_PERCENT` | `CanaryReleaseManager` | `15.0` | Error rate that trips auto-rollback |
| `REGISTRY_DIR` | `PipelineConfig` | `.registry` | Where the local "ACR + deployment state" lives |

---

## 8. Mapping to real infrastructure (for the last 15 minutes)

Open these side-by-side with the Java classes above:

| File | Real-world role |
|---|---|
| [`docker/Dockerfile`](docker/Dockerfile) | Multi-stage, hardened image; `--build-arg VERSION`/`GIT_SHA` become OCI labels — the container-level half of Image Versioning |
| [`.github/workflows/ci.yml`](.github/workflows/ci.yml) | Real GitHub Actions CI: build/smoke-test → version → Trivy scan → `az acr build` push → tag `:dev` |
| [`.github/workflows/cd.yml`](.github/workflows/cd.yml) | Real GitHub Actions CD: GitHub **Environments** (`stage`, `prod`) with required reviewers = the real-world `APPROVE_STAGE`/`APPROVE_PROD` gates |
| [`azure-pipelines.yml`](azure-pipelines.yml) | Same pipeline expressed in Azure DevOps, using `az acr build`/`az acr import` natively and Azure DevOps Environments for approvals |

**Emphasize explicitly:** the Java simulator and these YAML files implement the **exact same policy** (build once, scan-gate, approval-gate, strategy-per-environment) — the YAML is not "the real version" and the Java is not "just a toy"; they are two implementations of one design.

---

## 9. Troubleshooting reference (hand this to learners before they start)

| Symptom | Cause | Fix |
|---|---|---|
| `IllegalAccessError` in IntelliJ | Spark needs `--add-opens` into JDK internals; `java -jar` gets these from the manifest automatically, IntelliJ's direct main-class run does not | Use the provided run configurations, or copy flags from `scripts/run-local.sh` |
| `winutils.exe` / `HADOOP_HOME` errors on Windows | Classic Spark-on-Windows local-file-write issue | Not applicable here — all data is in-memory (`StockMovementGenerator`), no Spark file I/O anywhere in this demo |
| CI always reports the same version | `.registry/version.txt` wasn't cleared between sessions | Run mode `reset` (`9_Reset_Demo_State` / `scripts/reset-demo-state.sh`) |
| `cd` says "No artifact found in Dev" | `cd` was run before `ci` ever succeeded once | Run `ci` first (or use `pipeline` to run both together) |
| IntelliJ run configuration shows a red "module not found" | Module name IntelliJ generated on import doesn't match `spark-cicd-gitops-demo` | Open the run configuration → reselect the module from the dropdown |
| Docker build slow on first run | Downloading base images + Maven dependencies | Expected once; subsequent builds reuse layer cache |
| Apple Silicon (M1/M2/M3) | Not an issue | `eclipse-temurin:17-jre-jammy` and the Maven builder image both publish native `arm64` variants |

---

## 10. File-by-file reference (quick lookup during Q&A)

```
src/main/java/com/training/gitops/
├── App.java                                  # single entry point, mode dispatch (ci/cd/pipeline/...)
├── config/PipelineConfig.java                # every env-var-driven pipeline knob
├── build/
│   ├── BuildInfo.java                        # >>> Image Versioning <<< (version + git sha + timestamp)
│   └── SemanticVersionCalculator.java        # >>> Image Versioning <<< (auto version bump)
├── security/
│   ├── SecurityScanner.java                  # >>> ACR Security Scanning <<<
│   └── ScanResult.java
├── registry/ArtifactRegistry.java            # >>> Pipeline Automation + Promotion <<< (local ACR simulator)
├── pipeline/
│   ├── CIPipeline.java                       # >>> THE CI HALF <<<
│   ├── CDPipeline.java                       # >>> THE CD HALF / Dev->Stage->Prod Promotion <<<
│   └── CIResult.java
├── deployment/
│   ├── BlueGreenDeploymentManager.java       # >>> Blue/Green Release <<<
│   └── CanaryReleaseManager.java             # >>> Canary Release <<<
├── job/InventoryAnalyticsJob.java            # the real Spark workload used as every health check
└── data/                                     # in-memory sample data (Windows-safe, no file I/O)

config/
├── vulnerability-findings.json               # active/default scan report (starts clean)
├── vulnerability-findings-critical.json      # fixture: trips the CRITICAL gate
└── vulnerability-findings-too-many-high.json # fixture: trips the MAX_HIGH_VULNS gate

docker/Dockerfile                             # multi-stage, hardened, version-labeled image
.github/workflows/ci.yml, cd.yml              # real GitHub Actions mirror of CIPipeline/CDPipeline
azure-pipelines.yml                           # real Azure DevOps mirror, explicit ACR tasks

scripts/
├── run-local.sh / .bat                       # build + run any mode, no Docker needed
├── inject-critical-vuln.sh / .bat            # swap in a failing scan fixture
├── restore-clean-vuln.sh / .bat              # swap back to a passing scan fixture
└── reset-demo-state.sh / .bat                # clear .registry/ for a fresh run
```
