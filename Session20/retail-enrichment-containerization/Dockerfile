# =============================================================================
# Retail Enrichment Containerization - Dockerfile
#
# Demonstrates:
#   1. Docker MULTI-STAGE BUILD   -> build tools never ship in final image
#   2. JVM TUNING IN CONTAINERS   -> container-aware heap sizing, GC, Java 17
#                                     module-opens required by Spark
#   3. IMAGE HARDENING            -> non-root user, pinned base images,
#                                     minimal packages, no unnecessary tools
#   (Resource CONSTRAINTS are enforced at "docker run" / compose level,
#    see docker-compose.yml and scripts/run-docker.sh)
# =============================================================================

# ---------- STAGE 1: BUILD ----------
# Pinned tag (never ':latest') for reproducible builds.
FROM maven:3.9.6-eclipse-temurin-17 AS builder

WORKDIR /build

# Copy pom.xml first so Maven dependency layer is cached independently
# of source code changes (faster rebuilds).
COPY pom.xml .
RUN mvn -B -q dependency:go-offline

COPY src ./src

# Package the fat jar. Build tools + Maven cache stay in this stage only.
RUN mvn -B -q clean package -DskipTests

# ---------- STAGE 2: RUNTIME (hardened, minimal) ----------
# JRE-only image (no compiler/build tools) keeps the attack surface small.
FROM eclipse-temurin:17-jre-jammy AS runtime

LABEL maintainer="data-platform-team" \
      application="retail-enrichment" \
      description="Hardened runtime image for the Retail Enrichment Spark job"

# --- Hardening: patch OS packages, then remove package cache ---
RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# --- Hardening: dedicated non-root user with fixed, predictable UID/GID ---
RUN groupadd --system --gid 1000 sparkgrp \
    && useradd --system --uid 1000 --gid sparkgrp \
               --home-dir /app --shell /usr/sbin/nologin sparkuser

WORKDIR /app

# Copy ONLY the built artifact from the builder stage - not the JDK,
# not Maven, not the source tree, not the local ~/.m2 cache.
COPY --from=builder --chown=sparkuser:sparkgrp /build/target/retail-enrichment.jar /app/retail-enrichment.jar
COPY --chown=sparkuser:sparkgrp data /app/data

# Writable locations the job needs even under a read-only root filesystem
# (see docker-compose.yml -> read_only: true + tmpfs mounts).
RUN mkdir -p /app/output /tmp/spark-tmp \
    && chown -R sparkuser:sparkgrp /app/output /tmp/spark-tmp

# --- JVM TUNING FOR CONTAINERS ---
# UseContainerSupport (default on since JDK 11 but set explicitly for clarity):
#   JVM reads cgroup limits (docker --memory/--cpus) instead of host totals.
# MaxRAMPercentage / InitialRAMPercentage:
#   Heap sized as a % of the CONTAINER memory limit, not a fixed -Xmx,
#   so the same image behaves correctly under a 512m OR a 4g limit.
# UseG1GC: predictable low-pause GC suited for a bounded-memory container.
# ExitOnOutOfMemoryError: fail fast & let the orchestrator restart cleanly
#   instead of a Spark job hanging in a degraded state.
ENV JVM_OPTS="-XX:+UseContainerSupport \
-XX:MaxRAMPercentage=75.0 \
-XX:InitialRAMPercentage=50.0 \
-XX:+UseG1GC \
-XX:+ExitOnOutOfMemoryError \
-Djava.io.tmpdir=/tmp/spark-tmp"

# Spark 3.x reflects into JDK internals; Java 17's module system blocks this
# by default, so these --add-opens flags are required to run Spark on JDK 17.
ENV JAVA_TOOL_OPTIONS="--add-opens=java.base/java.lang=ALL-UNNAMED \
--add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
--add-opens=java.base/java.io=ALL-UNNAMED \
--add-opens=java.base/java.net=ALL-UNNAMED \
--add-opens=java.base/java.nio=ALL-UNNAMED \
--add-opens=java.base/java.util=ALL-UNNAMED \
--add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
--add-opens=java.base/sun.nio.cs=ALL-UNNAMED \
--add-opens=java.base/sun.security.action=ALL-UNNAMED \
--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"

ENV SPARK_MASTER="local[*]"
ENV SPARK_SHUFFLE_PARTITIONS="4"

# --- Hardening: never run as root ---
USER sparkuser

# Simple liveness signal for orchestrators (checks the jar is present/readable).
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD ["java", "-version"]

# Shell form via explicit array so JVM_OPTS expands, while still keeping
# a single, auditable process as PID 1.
ENTRYPOINT ["sh", "-c", "exec java $JVM_OPTS -jar /app/retail-enrichment.jar /app/data /app/output"]
