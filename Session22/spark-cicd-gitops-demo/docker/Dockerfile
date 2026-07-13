# =============================================================================
# Multi-stage, hardened image for the CI/CD & GitOps demo.
# (Same multi-stage + non-root pattern as the "Containerization for Data
#  Platforms" module - here extended with build-time VERSION/GIT_SHA args that
#  become OCI labels, i.e. the container-level half of "Image Versioning".)
#
# Build (mirrors what .github/workflows/ci.yml's "docker-build-push" job runs):
#   docker build \
#     --build-arg VERSION=1.2.3 \
#     --build-arg GIT_SHA=$(git rev-parse --short HEAD) \
#     -f docker/Dockerfile -t inventory-analytics:1.2.3 .
#
# Inspect the labels that prove this exact image's identity:
#   docker inspect inventory-analytics:1.2.3 --format '{{json .Config.Labels}}'
# =============================================================================

# ---------- Stage 1: builder ----------
FROM maven:3.9.6-eclipse-temurin-17 AS builder
WORKDIR /build
COPY pom.xml .
RUN mvn -B dependency:go-offline
COPY src ./src
RUN mvn -B clean package -DskipTests

# ---------- Stage 2: hardened runtime ----------
FROM eclipse-temurin:17-jre-jammy AS runtime

ARG VERSION=0.0.0-dev
ARG GIT_SHA=unknown
ARG BUILD_TIMESTAMP=unknown

LABEL org.opencontainers.image.title="spark-cicd-gitops-demo" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${GIT_SHA}" \
      org.opencontainers.image.created="${BUILD_TIMESTAMP}" \
      org.opencontainers.image.description="CI/CD & GitOps for Spark - training demo image"

RUN groupadd --system spark \
 && useradd  --system --gid spark --home-dir /app --shell /usr/sbin/nologin spark \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder --chown=spark:spark /build/target/spark-cicd-gitops-demo-1.0.0.jar app.jar
COPY --chown=spark:spark docker/entrypoint.sh entrypoint.sh
RUN chmod 0550 entrypoint.sh

# These become environment-visible to BuildInfo.java at container runtime,
# so `docker run ... version` reports the SAME identity baked into the labels.
ENV GIT_COMMIT_SHA="${GIT_SHA}"
ENV JAVA_OPTS="-XX:MaxRAMPercentage=75.0 -XX:+ExitOnOutOfMemoryError"

USER spark:spark

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD ["java", "-version"]

ENTRYPOINT ["./entrypoint.sh"]
CMD ["pipeline"]
