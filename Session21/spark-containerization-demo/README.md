# Spark Containerization Demo — Quick Start

A self-contained training demo for **"Containerization for Data Platforms"**, covering:
Docker multi-stage builds · JVM tuning in containers · Resource constraints · Image hardening.

> **For the full trainer walkthrough (problem → solution, per topic, with exact classes/commands),
> see [`DEMO-GUIDE.md`](DEMO-GUIDE.md).** This README only covers setup.

## Prerequisites

| Tool | Version | macOS (Apple Silicon) | Windows |
|---|---|---|---|
| JDK | 17 (Temurin recommended) | `brew install --cask temurin17` | https://adoptium.net |
| Maven | 3.9+ | `brew install maven` | https://maven.apache.org/download.cgi |
| IntelliJ IDEA | 2023.x+ | | |
| Docker Desktop | latest | native Apple Silicon build | WSL2 backend recommended |

No dataset download is required — the demo generates its own in-memory sample data.

## Option A — Open in IntelliJ (recommended for the app/Spark topics)

1. `File → Open` and select this project's root folder (the one containing `pom.xml`).
2. Let IntelliJ import the Maven project and download dependencies (first time only).
3. Set **Project SDK** to Java 17: `File → Project Structure → Project → SDK`.
4. Open the **Run/Debug Configurations** dropdown (top toolbar) — five configurations are
   pre-provided:
   - `1_All_Topics_Demo`
   - `2_JVM_Tuning_Diagnostics`
   - `3_Spark_Analytics_Job`
   - `4_Resource_Constraint_Memory_Stress`
   - `5_Resource_Constraint_CPU_Stress`
5. Select one and click ▶ Run.

> If a run configuration shows a red "module not found" the first time, open it and
> reselect the module from the dropdown — a one-click fix caused by IntelliJ's
> auto-generated module name. See the Troubleshooting table in `DEMO-GUIDE.md`.

## Option B — Run from a terminal, no IDE

macOS / Linux:
```bash
./scripts/run-local.sh all
```
Windows:
```bat
scripts\run-local.bat all
```
Valid modes: `diagnostics`, `job`, `stress-memory`, `stress-cpu`, `all`.

## Option C — Docker (required for the Docker-specific topics)

```bash
# Topic: Docker Multi-Stage Builds — builds all 3 images and compares sizes
./scripts/build-and-compare-images.sh        # macOS/Linux
scripts\build-and-compare-images.bat         # Windows

# Topics: JVM Tuning + Resource Constraints — same image, different budgets
./scripts/run-with-constraints.sh            # macOS/Linux
scripts\run-with-constraints.bat             # Windows

# Everything together
docker compose -f docker/docker-compose.yml up
```

## Project layout

```
pom.xml                     Maven build (Java 17, Spark 3.5.1, shade plugin)
src/main/java/...            Application code (see DEMO-GUIDE.md for the topic map)
docker/                      Dockerfile.naive / .multistage / .hardened + compose file
scripts/                     Cross-platform helper scripts (.sh + .bat)
.idea/runConfigurations/     Pre-built IntelliJ run configs (VM options included)
DEMO-GUIDE.md                Full trainer walkthrough — start here for delivering the session
```

## Why this demo works identically on macOS (Apple Silicon) and Windows

- **No external dataset / network calls** — sample data is generated in-memory and seeded
  deterministically (`SampleDataGenerator`).
- **No Spark file I/O** — sidesteps the classic Windows `winutils.exe`/`HADOOP_HOME`
  failure entirely; results are computed and printed, not written to disk.
- **Spark UI disabled and driver bound to `127.0.0.1`** — avoids hostname-resolution
  issues and port clashes on repeated runs.
- **All base images (`eclipse-temurin:17-jre-jammy`, `maven:3.9.6-eclipse-temurin-17`)
  publish native `arm64` builds** — no Rosetta emulation on Apple Silicon.
- **Every JVM `--add-opens` flag Spark needs is pre-wired** — either via the jar's
  manifest (`java -jar`) or via the provided IntelliJ run configurations / scripts
  (`java -cp` / direct main-class execution).
