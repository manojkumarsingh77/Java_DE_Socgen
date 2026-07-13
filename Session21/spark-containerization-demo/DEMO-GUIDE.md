# Containerization for Data Platforms — Instructor Demo Guide

**Audience:** Senior data engineers
**Format:** Problem → Solution, code-first, live-executed
**Stack:** Java 17, Apache Spark 3.5.1 (local mode), Docker
**Runs on:** IntelliJ IDEA (macOS Apple Silicon and Windows), and standalone via Docker

---

## 0. How this guide is organized

For **each** topic you asked to cover, this guide gives you:

1. **The problem** — what goes wrong in real data platforms without this practice, framed as something to say to the room before touching code.
2. **The exact class / method that is the solution** — so you can jump straight to it on screen.
3. **The command to run live.**
4. **What learners should see, and what to ask them.**

| # | Topic | Primary solution artifact |
|---|-------|---------------------------|
| 1 | Docker multi-stage builds | [`docker/Dockerfile.multistage`](docker/Dockerfile.multistage), [`docker/Dockerfile.naive`](docker/Dockerfile.naive) (the "before") |
| 2 | JVM tuning in containers | [`ContainerDiagnostics.java`](src/main/java/com/training/containerization/diagnostics/ContainerDiagnostics.java) |
| 3 | Resource constraints | [`ResourceConstraintSimulator.java`](src/main/java/com/training/containerization/diagnostics/ResourceConstraintSimulator.java) + [`docker/docker-compose.yml`](docker/docker-compose.yml) |
| 4 | Image hardening | [`docker/Dockerfile.hardened`](docker/Dockerfile.hardened) |

The "data platform" being containerized throughout is a real (small) Spark ETL job:
[`SalesAnalyticsJob.java`](src/main/java/com/training/containerization/job/SalesAnalyticsJob.java) — this keeps the demo honest: every Docker/JVM concept is being applied to an actual Spark workload, not a toy "Hello World".

---

## 1. Suggested session flow (≈90–120 minutes)

1. **Run the Spark job locally in IntelliJ, no Docker** (10 min) — establish the baseline behaviour and show the app works.
2. **Topic: Docker Multi-Stage Builds** (25 min)
3. **Topic: JVM Tuning in Containers** (25 min)
4. **Topic: Resource Constraints** (25 min)
5. **Topic: Image Hardening** (20 min)
6. **Wrap-up: run everything together via docker-compose** (10 min)

---

## 2. Baseline — run without Docker at all

**Say to the room:** *"Before we containerize anything, let's prove the application itself is correct. Containerization should never be the first thing you debug."*

### Class/method driving this step
- Entry point: [`App.java`](src/main/java/com/training/containerization/App.java) → `main(String[] args)`
- Workload: [`SalesAnalyticsJob.java`](src/main/java/com/training/containerization/job/SalesAnalyticsJob.java) → `createSparkSession()` and `run()`
- Data: [`SampleDataGenerator.java`](src/main/java/com/training/containerization/data/SampleDataGenerator.java) — in-memory, deterministic, no files on disk (deliberately avoids the classic Spark-on-Windows `winutils.exe` failure — call this out explicitly, it's a real pain point in mixed Mac/Windows classrooms).

### Run it
- **IntelliJ:** open the project, run configuration `3_Spark_Analytics_Job` (already provided under *Run Configurations*).
- **Terminal (macOS/Linux):** `./scripts/run-local.sh job`
- **Terminal (Windows):** `scripts\run-local.bat job`

### What to point out
- The `--add-opens` VM options baked into the run configuration. Ask: *"Why does a modern JVM need these to run Spark?"* — Spark's Tungsten engine uses `sun.misc.Unsafe`-style reflection into JDK internals that Java 9+'s module system encapsulates by default. This sets up the JVM Tuning section nicely (flags matter, and they must travel with the app).

---

## 3. Topic: Docker Multi-Stage Builds

### 3.1 The problem
**Say to the room:** *"Let's containerize this the way most people do it the first time."*

Show [`docker/Dockerfile.naive`](docker/Dockerfile.naive) on screen. It is intentionally the "wrong way": one stage, `FROM maven:...`, copies the whole repo in, builds in place. Build it:

```bash
docker build -f docker/Dockerfile.naive -t spark-training/demo:naive .
docker images spark-training/demo:naive
```

**Expected outcome:** the image is large (roughly 900MB–1.2GB depending on platform) because it contains the JDK, Maven, the entire `~/.m2` cache, and all source/`.git` history — none of which is needed to *run* the jar.

### 3.2 The solution — the class/file that drives it
[`docker/Dockerfile.multistage`](docker/Dockerfile.multistage)

Walk through the two `FROM` stages:
- **`builder` stage** — `FROM maven:3.9.6-eclipse-temurin-17 AS builder` — has everything needed to compile.
- **`runtime` stage** — `FROM eclipse-temurin:17-jre-jammy AS runtime` — starts completely clean, and the single line that matters is:
  ```dockerfile
  COPY --from=builder /build/target/spark-containerization-demo-1.0.0.jar app.jar
  ```
  Everything else from the builder stage — Maven itself, the dependency cache, source code — is discarded automatically because it never got copied into this final stage.

Also point out the **layer caching trick**: `COPY pom.xml .` + `RUN mvn dependency:go-offline` happens *before* `COPY src ./src`, so editing Java code doesn't invalidate the (slow) dependency-download layer on rebuild.

### 3.3 Run it live
```bash
./scripts/build-and-compare-images.sh      # macOS/Linux
scripts\build-and-compare-images.bat       # Windows
```
This builds all three images (naive / multistage / hardened) and prints a size table plus a `whoami` comparison (sets up the hardening topic).

**Expected outcome:** `multistage` and `hardened` are roughly 3–4x smaller than `naive`, and contain no Maven/JDK/source at all — only the JRE and the one jar.

### 3.4 Discussion prompts
- "What's the security implication of a smaller image beyond just download speed?" (smaller attack surface, fewer CVEs to patch, faster cold-start in autoscaling clusters)
- "What would happen if we accidentally committed a secret/API key into a build-only `RUN` layer in a single-stage image?" → leads naturally into hardening.

---

## 4. Topic: JVM Tuning in Containers

### 4.1 The problem
**Say to the room:** *"A JVM that doesn't know it's in a container will size itself for the WHOLE HOST — then get killed by the container runtime the moment it tries to use that memory."*

This is historically the #1 cause of mysterious `OOMKilled` (exit code 137) events for JVM workloads on Kubernetes.

### 4.2 The solution — the class that drives it
[`ContainerDiagnostics.java`](src/main/java/com/training/containerization/diagnostics/ContainerDiagnostics.java) → `printReport()`, which calls three key private methods:

| Method | What it proves |
|--------|-----------------|
| `printRuntimeView()` | What the JVM *believes* it has (`Runtime.availableProcessors()`, `Runtime.maxMemory()`) |
| `printActiveJvmFlags()` | The actual `-XX:...` flags in effect for this process (via `RuntimeMXBean.getInputArguments()`) — proves `JAVA_OPTS` was really applied |
| `printCgroupView()` | The **ground truth** — reads `/sys/fs/cgroup/memory.max` (cgroup v2) or `/sys/fs/cgroup/memory/memory.limit_in_bytes` (cgroup v1) directly, i.e. what Docker/Kubernetes is *actually* enforcing |

The class also explains (and prints) why the cgroup section will say "not found" when run on a Mac/Windows laptop directly from IntelliJ — Docker Desktop hides the Linux VM that owns those cgroup files. This is an important, often-confusing detail for learners — call it out explicitly.

### 4.3 Run it live — three ways, same class, different JVM configuration

**Way 1 — bare JVM, no tuning at all (IntelliJ run config `2_JVM_Tuning_Diagnostics`, or):**
```bash
java -jar target/spark-containerization-demo-1.0.0.jar diagnostics
```

**Way 2 — inside Docker, unconstrained:**
```bash
docker run --rm spark-training/demo:hardened diagnostics
```

**Way 3 — inside Docker, constrained AND explicitly tuned:**
```bash
docker run --rm --memory=512m --cpus=1 \
  -e JAVA_OPTS="-XX:MaxRAMPercentage=75.0 -XX:InitialRAMPercentage=50.0 -XX:ActiveProcessorCount=1" \
  spark-training/demo:hardened diagnostics
```

### 4.4 What to point out in the output
- Compare `maxMemory()` between Way 2 and Way 3 — it should shrink to roughly 75% of 512MB in Way 3, proving `-XX:MaxRAMPercentage` is being honoured.
- Compare `availableProcessors()` — Java 17 already respects the cgroup CPU quota by default (`-XX:+UseContainerSupport` is **on by default** since JDK 10+), so even Way 2 should already reflect the *host's* CPU count correctly if unconstrained, and the container's quota once `--cpus` is set. This is worth stating explicitly: *"the flag isn't fixing something broken by default in Java 17 — we're setting it explicitly for auditability and to control the percentage, not just turn the feature on."*
- `printActiveJvmFlags()` output should visibly list every flag passed via `JAVA_OPTS` — this is the direct, undeniable proof that environment-driven tuning works without rebuilding the image.

### 4.5 Discussion prompts
- "Why 75%, not 100%, for `MaxRAMPercentage`?" → JVM needs headroom for metaspace, thread stacks, direct buffers, and native Spark/Tungsten off-heap memory; 100% heap in a hard-limited container reliably OOMKills.
- "Why `-XX:+ExitOnOutOfMemoryError`?" → in an orchestrated environment, a JVM that limps along in a corrupted state after OOM is worse than one that exits fast so Kubernetes restarts it cleanly.

---

## 5. Topic: Resource Constraints

### 5.1 The problem
**Say to the room:** *"Your laptop has 'unlimited' resources. Production doesn't. If you've never tested under a ceiling, you don't actually know how your job behaves at the ceiling."*

### 5.2 The solution — the class that drives it
[`ResourceConstraintSimulator.java`](src/main/java/com/training/containerization/diagnostics/ResourceConstraintSimulator.java) — two public methods:

| Method | Demonstrates |
|--------|---------------|
| `simulateMemoryPressure(JobConfig config)` | Progressively allocates memory, printing each step, until either a configurable safety threshold or a real `OutOfMemoryError` — shows the difference between a **catchable JVM OOM** (heap limit hit first — good) vs relying on the container to kill the process (uncatchable, no stack trace — bad). |
| `simulateCpuPressure(JobConfig config)` | Spins CPU-bound work across N threads for a fixed duration — run next to `docker stats` to visually show CFS quota throttling when `--cpus` is below the thread count. |

Both are driven entirely by [`JobConfig.java`](src/main/java/com/training/containerization/config/JobConfig.java), which externalizes every knob (`MEM_STRESS_STEP_MB`, `MEM_STRESS_STOP_PERCENT`, `CPU_STRESS_THREADS`, `CPU_STRESS_DURATION_SEC`) as environment variables — the same class docker-compose.yml uses to reconfigure behaviour without rebuilding the image.

### 5.3 Run it live
```bash
./scripts/run-with-constraints.sh     # macOS/Linux — runs the same image at 3 budgets, then a memory-pressure test
scripts\run-with-constraints.bat      # Windows
```

Or, to see the real Spark ETL job behave correctly under a small, deliberately-tuned container:
```bash
docker compose -f docker/docker-compose.yml up demo-spark-job-constrained
```

For the CPU demo, open a second terminal with `docker stats` running, then:
```bash
docker compose -f docker/docker-compose.yml up demo-cpu-stress
```

### 5.4 What to point out
- In the memory-pressure test under a tight `--memory` limit with a matching `-Xmx`/`MaxRAMPercentage`, you should see a clean, logged `OutOfMemoryError` caught by the application — **not** the container silently killing the process (exit 137, no log line, the classic on-call nightmare).
- In the CPU test, `docker stats` should show CPU% pinned near the `--cpus` ceiling, and wall-clock completion time should increase as you lower `--cpus` below `CPU_STRESS_THREADS`.

### 5.5 Discussion prompts
- "In `docker-compose.yml`, `demo-spark-job-constrained` sets `SHUFFLE_PARTITIONS=2` for a 1-CPU container. Why would you turn shuffle partitions **down** on a smaller container, not up?" → fewer, larger tasks reduce per-task scheduling overhead when there's little parallelism available anyway.
- "How does this map to a Kubernetes pod spec?" → `mem_limit`/`cpus` here are the direct analogue of `resources.limits.memory` / `resources.limits.cpu`.

---

## 6. Topic: Image Hardening

### 6.1 The problem
**Say to the room:** *"The multi-stage image we built is smaller — but it still runs as root, uses no health signal, and has no explicit security posture. Small isn't the same as safe."*

Run this against `naive` (and even `multistage`) to make the point:
```bash
docker run --rm --entrypoint whoami spark-training/demo:naive
# -> root
```

### 6.2 The solution — the file that drives it
[`docker/Dockerfile.hardened`](docker/Dockerfile.hardened) — walk through each hardening measure and what it defends against:

| Hardening measure | Line(s) in Dockerfile.hardened | Defends against |
|---|---|---|
| Dedicated non-root user, no login shell | `groupadd --system spark && useradd --system --gid spark --shell /usr/sbin/nologin spark`, then `USER spark:spark` | A process compromise (e.g. deserialization bug in a dependency) getting root inside the container |
| Ownership set at copy time | `COPY --from=builder --chown=spark:spark ...` | Avoids a separate `RUN chown` layer (extra layer + extra image size) |
| Minimal, pinned base image | `FROM eclipse-temurin:17-jre-jammy` (JRE, not JDK; explicit tag, not `latest`) | Reduces package surface; avoids silent behaviour changes from a floating tag |
| Explicit, auditable JVM defaults | `ENV JAVA_OPTS="-XX:MaxRAMPercentage=75.0 ..."` | Ties back directly to the JVM Tuning topic — hardening and tuning are not separate concerns |
| `HEALTHCHECK` | `HEALTHCHECK ... CMD ["java","-version"]` | Lets an orchestrator detect and restart an unresponsive container instead of routing traffic to a dead one |
| OCI metadata `LABEL`s | `LABEL org.opencontainers.image.*` | Supply-chain traceability — what built this, from where |

### 6.3 Runtime hardening flags (can't be baked into a Dockerfile — must be applied at `docker run`/orchestrator level)
Shown in [`docker/docker-compose.yml`](docker/docker-compose.yml) on the constrained services:
```yaml
read_only: true            # container filesystem is read-only except explicit tmpfs/volumes
tmpfs: ["/tmp"]             # give it a writable scratch space that never persists
security_opt: ["no-new-privileges:true"]   # blocks privilege escalation via setuid binaries
cap_drop: ["ALL"]           # strip every Linux capability the process doesn't need
```
**Emphasize this split explicitly** — a Dockerfile controls what the image *is*; `docker run` / the orchestrator's pod spec controls what the container *is allowed to do*. Both layers matter.

### 6.4 Run it live
```bash
docker run --rm --entrypoint whoami spark-training/demo:hardened
# -> spark

docker run --rm --read-only --tmpfs /tmp --cap-drop=ALL --security-opt no-new-privileges:true \
  spark-training/demo:hardened diagnostics
```

If learners have [Trivy](https://aquasecurity.github.io/trivy/) installed, optionally:
```bash
trivy image spark-training/demo:naive
trivy image spark-training/demo:hardened
```
and compare vulnerability counts — this is a strong visual closer but is **optional** (don't block the session on installing a new tool).

### 6.5 Discussion prompts
- "Why JRE instead of JDK in the runtime stage?" → the JDK ships compilers/dev tools an attacker could use post-compromise; the JRE doesn't.
- "What's still missing for a real production image?" → e.g. distroless/scratch base (no shell at all), signed images (cosign), SBOM generation, automated CVE scanning in CI.

---

## 7. Wrap-up — run everything together

```bash
docker compose -f docker/docker-compose.yml up demo-baseline demo-constrained demo-memory-stress
```

Have learners diff the `diagnostics` output of `demo-baseline` vs `demo-constrained` side by side — every number that differs is something one of today's four topics explains.

---

## 8. Troubleshooting reference (hand this to learners before they start)

| Symptom | Cause | Fix |
|---|---|---|
| `IllegalAccessError` / `InaccessibleObjectException` when running in IntelliJ | Spark's Tungsten engine needs `--add-opens` into JDK internals; running the fat jar via `java -jar` picks these up automatically from the manifest, but IntelliJ's "Run" on the `App` class does not read the jar manifest | Use the provided run configurations (VM options pre-filled), or copy the flags from `scripts/run-local.sh` |
| Spark hangs / binds to a strange hostname on first run | Spark tries to resolve the machine's hostname for its (disabled) UI | Already handled: `spark.driver.bindAddress`/`spark.driver.host` are pinned to `127.0.0.1` in `SalesAnalyticsJob.createSparkSession()` |
| `winutils.exe` / `HADOOP_HOME` errors on Windows | Spark's local file writer needs Hadoop native bindings on Windows | Already avoided by design: this demo never reads/writes files via Spark — all data is generated and consumed in-memory (see `SampleDataGenerator`) |
| Docker build is slow on first run | Downloading base images + Maven dependencies | Expected once; subsequent builds reuse Docker layer cache (see the `pom.xml`-first `COPY` trick in `Dockerfile.multistage`) |
| Apple Silicon (M1/M2/M3) Docker build seems to emulate x86 | Not needed here | `eclipse-temurin:17-jre-jammy` and the `maven:3.9.6-eclipse-temurin-17` builder image both publish native `arm64` variants — no Rosetta emulation required |
| IntelliJ run configuration shows a red "module not found" | Module name IntelliJ generated on import doesn't match `spark-containerization-demo` | Open the run configuration → reselect the correct module from the dropdown (one-time fix) |

---

## 9. File-by-file reference (quick lookup during Q&A)

```
src/main/java/com/training/containerization/
├── App.java                              # single entry point, mode dispatch
├── config/JobConfig.java                 # every env-var-driven tuning knob
├── data/SalesRecord.java                 # bean used for Spark's bean encoder
├── data/SampleDataGenerator.java         # in-memory dataset (no file I/O -> Windows safe)
├── job/SalesAnalyticsJob.java            # the real Spark ETL workload
└── diagnostics/
    ├── ContainerDiagnostics.java         # >>> JVM Tuning in Containers <<<
    └── ResourceConstraintSimulator.java  # >>> Resource Constraints <<<

docker/
├── Dockerfile.naive                      # Act 1: the problem (image bloat, root user)
├── Dockerfile.multistage                 # >>> Docker Multi-Stage Builds <<<
├── Dockerfile.hardened                   # >>> Image Hardening <<< (builds on multistage)
├── entrypoint.sh                         # resolves JAVA_OPTS + mode cleanly
└── docker-compose.yml                    # ties tuning + constraints + hardening together

scripts/
├── run-local.sh / .bat                   # run without Docker at all
├── build-and-compare-images.sh / .bat    # Topic 1 live demo
└── run-with-constraints.sh / .bat        # Topics 2 & 3 live demo
```
