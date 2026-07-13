# Retail Enrichment Containerization — Case Study

**Topic coverage:** Docker multi-stage builds · JVM tuning in containers · Resource constraints · Image hardening

A trainer-ready, fully working Java 17 + Apache Spark 3.5.1 batch job, containerized end-to-end.

---

## 1. The Case Study (business framing for the class)

A retail chain has 3 raw feeds landing daily in a data lake:

| File | Description |
|---|---|
| `data/transactions.csv` | Point-of-sale transactions (store, product, qty, price, date) |
| `data/products.csv` | Product catalog reference data (category, brand, cost price) |
| `data/stores.csv` | Store master data (region, state) |

**Job (`RetailEnrichmentJob`)** joins these, computes `gross_revenue`, `margin_amount`,
`margin_pct`, and a `value_segment` (HIGH/MEDIUM/LOW), then writes:
- `output/enriched_transactions/` (Parquet, partitioned by `region`)
- `output/region_category_summary/` (CSV — revenue & margin rollup)

This job must ship to production as a **container** that is:
- Built via a **multi-stage Dockerfile** (build tools never reach production)
- **JVM-tuned** to respect container memory/CPU limits (not host machine specs)
- Run under explicit **CPU/memory/PID constraints**
- **Hardened** (non-root, read-only filesystem, dropped capabilities, patched base image)

---

## 2. Project Structure

```
retail-enrichment-containerization/
├── pom.xml                          # Maven build, Java 17, Spark 3.5.1, shade plugin
├── Dockerfile                       # Multi-stage build + JVM tuning + hardening
├── docker-compose.yml               # Resource constraints + hardening via compose
├── .dockerignore
├── data/                            # Sample input CSVs
│   ├── transactions.csv
│   ├── products.csv
│   └── stores.csv
├── src/main/java/com/retail/enrichment/
│   └── RetailEnrichmentJob.java     # The Spark job
├── src/main/resources/
│   └── log4j2.properties
├── scripts/
│   ├── run-local.sh                 # mvn package + run on host JVM
│   ├── run-docker.sh                # docker build + docker run with constraints
│   └── verify-hardening.sh          # proves hardening actually took effect
└── output/                          # (generated) job output lands here
```

---

## 3. Prerequisites

- **IntelliJ IDEA** (Community or Ultimate)
- **JDK 17** (Temurin/OpenJDK) — installable from within IntelliJ
- **Docker Desktop** (or Docker Engine + Compose v2) for the containerization steps
- Internet access (Maven needs to download Spark from Maven Central; Docker needs to pull base images)

> Maven is **not** required on your machine — IntelliJ bundles its own Maven and will use it automatically.

---

## 4. Run It in IntelliJ (Java 17) — Step by Step

1. **Unzip** the project and choose **File → Open…** in IntelliJ, select the
   `retail-enrichment-containerization` folder (the one containing `pom.xml`).
2. IntelliJ detects it as a Maven project → click **"Load Maven Project"** if prompted.
   Wait for the `Indexing` / dependency download to finish (downloads Spark 3.5.1 jars).
3. **Set the Project SDK to Java 17**:
   `File → Project Structure → Project → SDK` → select/add **17 (Temurin/OpenJDK)**.
   Also set **Language level: 17**.
4. Open `src/main/java/com/retail/enrichment/RetailEnrichmentJob.java`.
5. **Create a Run Configuration** (Run → Edit Configurations → + → Application):
   - **Main class:** `com.retail.enrichment.RetailEnrichmentJob`
   - **Program arguments:** `data output`
   - **VM options** (⚠️ required — Spark 3.x needs these to run on Java 17's module system):
     ```
     --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED
     ```
     (If IntelliJ's VM options field is hidden, click "Modify options" → "Add VM options".)
   - **Working directory:** the project root (default is fine).
6. Click **Run ▶**. Expected console output:
   - A "JVM Resource Snapshot" block (shows CPUs/heap the JVM sees)
   - "Enriched Transaction Sample" table (20 rows)
   - "Region / Category Revenue Summary" table
   - `Job completed successfully. Output written to: output`
7. Check the `output/` folder in the project — you'll find `enriched_transactions/`
   (Parquet, partitioned by region) and `region_category_summary/` (CSV).

**Windows note:** Spark's local file I/O sometimes wants `winutils.exe`/`HADOOP_HOME` on
Windows. If you hit a `NativeIO` / `UnsatisfiedLinkError`, easiest fix for this demo is to
run it inside **WSL2**, or add a Hadoop `winutils` binary and set `HADOOP_HOME` — this does
**not** affect the Docker path below, which runs on Linux regardless of host OS.

---

## 5. Run It Locally via Terminal (optional, no IntelliJ)

```bash
cd retail-enrichment-containerization
chmod +x scripts/*.sh
./scripts/run-local.sh
```

This runs `mvn clean package` then executes the fat jar with the same `--add-opens` flags.

---

## 6. Containerize It — Step by Step

### 6.1 Build the hardened, multi-stage image

```bash
docker build -t retail-enrichment:1.0.0 .
```

What happens (maps to the Dockerfile comments):
- **Stage 1 (`builder`):** `maven:3.9.6-eclipse-temurin-17` compiles and shades the fat jar.
- **Stage 2 (`runtime`):** `eclipse-temurin:17-jre-jammy` (JRE only, no compiler/Maven)
  copies **only** `retail-enrichment.jar` + `data/` from Stage 1 — the ~600MB of build
  tooling and Maven cache never reaches the final image.

Confirm the image is lean:
```bash
docker images retail-enrichment
docker history retail-enrichment:1.0.0
```

### 6.2 Run with explicit resource constraints + hardening flags

```bash
chmod +x scripts/*.sh
./scripts/run-docker.sh
```

This runs the equivalent of:
```bash
docker run --rm \
  --memory=1g --memory-reservation=512m --cpus=1.5 --pids-limit=100 \
  --read-only --security-opt no-new-privileges:true --cap-drop=ALL \
  --tmpfs /tmp:size=256m,mode=1777 \
  -v "$(pwd)/output:/app/output" \
  retail-enrichment:1.0.0
```

**What to point out in class:** the "JVM Resource Snapshot" printed at job start will show
`Available processors: 2` (rounded up from `--cpus=1.5`) and a max heap around 75% of 1GB —
**even though the host machine may have 8+ cores and 16+ GB RAM**. This is
`-XX:+UseContainerSupport` + `-XX:MaxRAMPercentage=75.0` reading the container's cgroup
limits instead of the host's.

Check `./output/` on your host afterward — same enriched data as the IntelliJ run.

### 6.3 Or run via Docker Compose

```bash
docker compose up --build
docker compose down
```

`docker-compose.yml` declares the same constraints (`deploy.resources.limits`,
`read_only`, `cap_drop: [ALL]`, `security_opt: [no-new-privileges:true]`, `pids_limit`)
declaratively — useful to show students the "infrastructure as code" version of the same
`docker run` flags.

### 6.4 Verify the hardening actually took effect

```bash
./scripts/verify-hardening.sh
```

This will:
1. Print `docker history` (proves the final image has no Maven/JDK-compiler layers).
2. Print the baked-in `USER` (should be `sparkuser`, never root/`0`).
3. Attempt a write to `/etc/` under `--read-only` and confirm it **fails** — proving the
   root filesystem is genuinely immutable, not just documented as such.

---

## 7. Topic → File Mapping (for the training deck)

| Topic | Where to show it |
|---|---|
| **Multi-stage builds** | `Dockerfile` Stage 1 vs Stage 2; `docker history` shows final image has no Maven layer |
| **JVM tuning in containers** | `Dockerfile` → `JVM_OPTS` (`UseContainerSupport`, `MaxRAMPercentage`, `UseG1GC`, `ExitOnOutOfMemoryError`) + `printContainerResourceSnapshot()` in the Java code, which proves the JVM is reading container limits |
| **Resource constraints** | `scripts/run-docker.sh` (`--memory`, `--cpus`, `--pids-limit`) and `docker-compose.yml` (`deploy.resources.limits`) |
| **Image hardening** | `Dockerfile` → non-root `sparkuser`/`sparkgrp` (fixed UID/GID 1000), `apt-get upgrade` + cache cleanup, pinned base image tags (never `:latest`); `docker-compose.yml` → `read_only`, `cap_drop: [ALL]`, `no-new-privileges`, `tmpfs` for writable scratch |

---

## 8. Troubleshooting

| Symptom | Fix |
|---|---|
| `InaccessibleObjectException` / illegal reflective access errors | The `--add-opens` VM options are missing — re-check step 4.5 (IntelliJ) or confirm `JAVA_TOOL_OPTIONS` is set (Docker path already sets this). |
| `OutOfMemoryError` inside Docker | Raise `--memory` in `scripts/run-docker.sh` (or `deploy.resources.limits.memory` in compose) — the container hard-limits the JVM heap via `MaxRAMPercentage`. |
| `Permission denied` writing inside the container | Confirm you're using the provided `-v $(pwd)/output:/app/output` volume mount; the container's root filesystem is intentionally read-only everywhere else. |
| `docker compose` ignores `deploy.resources.limits` | Update Docker Desktop / Compose to v2.20+, or fall back to `scripts/run-docker.sh` which sets limits directly on `docker run` (works on any version). |
| Maven can't download Spark | Check corporate proxy/firewall allows `repo.maven.apache.org`; run `mvn -X` for verbose diagnostics. |

---

## 9. Cleanup

```bash
docker rm -f retail-enrichment-job 2>/dev/null || true
docker rmi retail-enrichment:1.0.0
rm -rf output/* target/
```

---

## 10. Suggested Classroom Flow (90 minutes)

1. **(10 min)** Walk through the case study & data model.
2. **(15 min)** Run in IntelliJ — show the enrichment logic and output.
3. **(20 min)** Walk the Dockerfile line-by-line — multi-stage rationale, then build the image live.
4. **(15 min)** Run with `scripts/run-docker.sh` — pause on the "JVM Resource Snapshot" output,
   change `--cpus`/`--memory` and re-run to show the JVM adapting.
5. **(15 min)** Run `verify-hardening.sh` — discuss why each hardening flag matters
   (blast-radius reduction, defense in depth).
6. **(15 min)** Discussion/Q&A: how this maps to Kubernetes `resources.limits`,
   `securityContext.runAsNonRoot`, `readOnlyRootFilesystem`, pod security standards.
