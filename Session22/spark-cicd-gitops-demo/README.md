# CI/CD & GitOps for Spark — Quick Start

A self-contained training demo for **"CI/CD & GitOps for Spark"**, covering:
Image versioning · ACR security scanning · Pipeline automation · Blue/Green release · Canary release · Dev→Stage→Prod promotion.

> **For the full trainer walkthrough (step-by-step CI, then step-by-step CD, with exact
> classes/commands and failure-scenario demos), see [`DEMO-GUIDE.md`](DEMO-GUIDE.md).**
> This README only covers setup.

## Prerequisites

| Tool | Version | macOS (Apple Silicon) | Windows |
|---|---|---|---|
| JDK | 17 (Temurin recommended) | `brew install --cask temurin17` | https://adoptium.net |
| Maven | 3.9+ | `brew install maven` | https://maven.apache.org/download.cgi |
| IntelliJ IDEA | 2023.x+ | | |
| Docker Desktop | latest (optional — only for the bonus real-image build) | | |

No cloud account, git repository, or dataset download is required — the whole
pipeline runs locally against an in-memory Spark job and a local folder
(`.registry/`) standing in for a container registry.

## Option A — Open in IntelliJ (recommended)

1. `File → Open` and select this project's root folder (the one containing `pom.xml`).
2. Let IntelliJ import the Maven project and download dependencies (first time only).
3. Set **Project SDK** to Java 17: `File → Project Structure → Project → SDK`.
4. Open the **Run/Debug Configurations** dropdown — nine are pre-provided, in
   suggested demo order:
   - `1_CI_Only`
   - `2_CD_Only_No_Approval`
   - `3_CD_Approve_Stage_Only`
   - `4_CD_Approve_Stage_And_Prod`
   - `5_Full_Pipeline_CI_then_CD`
   - `6_BlueGreen_Deep_Dive`
   - `7_Canary_Deep_Dive`
   - `8_Print_Version`
   - `9_Reset_Demo_State`
5. Select one and click ▶ Run.

> If a run configuration shows a red "module not found" the first time, open
> it and reselect the module from the dropdown — a one-click fix caused by
> IntelliJ's auto-generated module name.

## Option B — Run from a terminal, no IDE

macOS / Linux:
```bash
./scripts/run-local.sh ci
APPROVE_STAGE=true APPROVE_PROD=true ./scripts/run-local.sh cd
```
Windows:
```bat
scripts\run-local.bat ci
set APPROVE_STAGE=true&& set APPROVE_PROD=true&& scripts\run-local.bat cd
```
Valid modes: `ci`, `cd`, `pipeline`, `version`, `job`, `bluegreen-demo`, `canary-demo`, `reset`.

## Option C — Docker (optional bonus, for the Image Versioning topic)

```bash
docker build \
  --build-arg VERSION=1.2.3 \
  --build-arg GIT_SHA=$(git rev-parse --short HEAD 2>/dev/null || echo no-git) \
  -f docker/Dockerfile -t inventory-analytics:1.2.3 .

docker inspect inventory-analytics:1.2.3 --format '{{json .Config.Labels}}'
docker run --rm inventory-analytics:1.2.3 version
```

## Project layout

```
pom.xml                     Maven build (Java 17, Spark 3.5.1, shade plugin)
src/main/java/...            Application code (see DEMO-GUIDE.md for the topic map)
config/                      Vulnerability-scan fixtures used by SecurityScanner
docker/                      Multi-stage, hardened, version-labeled Dockerfile
.github/workflows/           Real GitHub Actions CI + CD pipelines, annotated
azure-pipelines.yml          Real Azure DevOps pipeline (explicit ACR tasks), annotated
scripts/                     Cross-platform helper scripts (.sh + .bat)
.registry/                   Local "ACR + deployment state" the demo writes to at runtime
.idea/runConfigurations/     Pre-built IntelliJ run configs (VM options + env vars included)
DEMO-GUIDE.md                Full trainer walkthrough — start here for delivering the session
```

## Why this demo works identically on macOS (Apple Silicon) and Windows

- **No external dataset / network calls** — sample data is generated in-memory and seeded
  deterministically (`StockMovementGenerator`).
- **No Spark file I/O** — sidesteps the classic Windows `winutils.exe`/`HADOOP_HOME`
  failure entirely.
- **No cloud credentials required** — the pipeline, registry, and deployment strategies
  are all simulated locally; the GitHub Actions/Azure DevOps files are provided for
  reference and are not executed as part of the local demo.
- **Spark UI disabled and driver bound to `127.0.0.1`** — avoids hostname-resolution
  issues and port clashes on repeated runs.
- **Every JVM `--add-opens` flag Spark needs is pre-wired** — via the jar's manifest
  (`java -jar`) and via the provided IntelliJ run configurations / scripts.
