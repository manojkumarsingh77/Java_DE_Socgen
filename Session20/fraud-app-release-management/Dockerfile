# =============================================================================
# Fraud App Release Management - Dockerfile
#
# Star topics demonstrated here: IMAGE VERSIONING (build ARGs -> OCI LABELs
# -> runtime ENV -> /version endpoint) and a lean, hardened multi-stage build
# that produces the immutable artifact promoted through Dev -> Stage -> Prod.
# =============================================================================

# ---------- STAGE 1: BUILD ----------
FROM maven:3.9.6-eclipse-temurin-17 AS builder
WORKDIR /build

COPY pom.xml .
RUN mvn -B -q dependency:go-offline

COPY src ./src
# Unit tests already ran in the CI "build-and-test" job before we even get
# here (see .github/workflows/ci-cd.yml); skip re-running them in the image
# build so the Docker layer stays focused on packaging.
RUN mvn -B -q clean package -DskipTests

# ---------- STAGE 2: RUNTIME ----------
FROM eclipse-temurin:17-jre-jammy AS runtime

# --- IMAGE VERSIONING: build-time metadata, injected by the CI/CD pipeline ---
# e.g. docker build --build-arg APP_VERSION=1.4.0 --build-arg GIT_COMMIT=$(git rev-parse --short HEAD) ...
ARG APP_VERSION=0.0.0-dev
ARG GIT_COMMIT=unknown
ARG BUILD_DATE=unknown

# Standard OCI labels -> queryable via `docker inspect`, and what registry
# UIs (ACR, Docker Hub, Harbor) render as the image's version metadata.
LABEL org.opencontainers.image.title="fraud-scoring-service" \
      org.opencontainers.image.version="${APP_VERSION}" \
      org.opencontainers.image.revision="${GIT_COMMIT}" \
      org.opencontainers.image.created="${BUILD_DATE}" \
      org.opencontainers.image.vendor="fraud-platform-team" \
      org.opencontainers.image.description="Spark-backed fraud scoring microservice"

# --- Hardening: patch base OS, minimal footprint ---
RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd --system --gid 1000 fraudgrp \
    && useradd --system --uid 1000 --gid fraudgrp \
               --home-dir /app --shell /usr/sbin/nologin frauduser

WORKDIR /app
COPY --from=builder --chown=frauduser:fraudgrp /build/target/fraud-app.jar /app/fraud-app.jar
COPY --chown=frauduser:fraudgrp data /app/data

RUN mkdir -p /tmp/spark-tmp && chown -R frauduser:fraudgrp /tmp/spark-tmp

# Runtime copies of the same version metadata -> readable by the app and
# returned on GET /version, so a curl against the load balancer proves
# exactly which build/slot answered (essential for Blue/Green & Canary checks).
ENV APP_VERSION=${APP_VERSION}
ENV GIT_COMMIT=${GIT_COMMIT}
ENV BUILD_DATE=${BUILD_DATE}
ENV APP_ENV=dev
ENV DEPLOY_SLOT=n-a
ENV PORT=8080
ENV DATA_PATH=/app/data

ENV JVM_OPTS="-XX:+UseContainerSupport -XX:MaxRAMPercentage=75.0 -XX:InitialRAMPercentage=50.0 -XX:+UseG1GC -XX:+ExitOnOutOfMemoryError -Djava.io.tmpdir=/tmp/spark-tmp"

# Spark 3.x needs these on Java 17 (JPMS blocks reflective access by default).
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

EXPOSE 8080

USER frauduser

# No curl/wget baked into the image (keeps the attack surface small); use
# bash's /dev/tcp facility for a dependency-free HTTP health probe instead.
HEALTHCHECK --interval=15s --timeout=3s --start-period=25s --retries=3 \
    CMD bash -c 'exec 3<>/dev/tcp/127.0.0.1/8080; printf "GET /health HTTP/1.0\r\n\r\n" >&3; head -1 <&3 | grep -q "200"'

ENTRYPOINT ["sh", "-c", "exec java $JVM_OPTS -jar /app/fraud-app.jar"]
