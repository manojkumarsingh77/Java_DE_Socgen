# =============================================================================
# Multi-stage build. Stage 1 compiles the shaded jar with Maven; stage 2
# lays it on top of the official Apache Spark 3.5.1 image (which already
# ships Java 17 + the Spark-on-Kubernetes entrypoint) so the final image
# only adds our application layer - keeps the promoted artifact identical
# across dev/test/prod, only the target namespace/tag changes.
# =============================================================================

# ---- Stage 1: build --------------------------------------------------------
FROM maven:3.9.6-eclipse-temurin-17 AS builder
WORKDIR /build
COPY pom.xml .
# Cache dependency resolution as its own layer
RUN mvn -B dependency:go-offline
COPY src ./src
RUN mvn -B -DskipTests package

# ---- Stage 2: runtime -------------------------------------------------------
FROM apache/spark:3.5.1-java17
USER root

ARG APP_VERSION=1.0.0
ENV APP_JAR=/opt/spark/jars/retail-platform-promotion-model-${APP_VERSION}.jar

COPY --from=builder /build/target/retail-platform-promotion-model-${APP_VERSION}.jar ${APP_JAR}

# environment.tag is informational only - the real switch is -Denv, injected
# per environment via the SparkApplication CRD driver/executor env vars.
LABEL org.opencontainers.image.title="retail-platform-promotion-model" \
      org.opencontainers.image.description="Multi-Environment Strategy demo - Retail Platform Promotion Model"

USER 185
ENTRYPOINT ["/opt/entrypoint.sh"]
