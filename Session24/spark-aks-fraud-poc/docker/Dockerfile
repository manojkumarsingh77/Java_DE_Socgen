# =============================================================================
# Stage 1 — Build the fat jar with Maven + Temurin 17
# =============================================================================
FROM maven:3.9.6-eclipse-temurin-17 AS builder
WORKDIR /build

# Copy pom first so dependency resolution is cached as its own layer
COPY pom.xml .
RUN mvn -B dependency:go-offline

COPY src ./src
RUN mvn -B clean package -DskipTests

# =============================================================================
# Stage 2 — Layer the jar onto the official Spark-on-Kubernetes base image.
#
# SPARK_BASE_IMAGE must be built beforehand from your local Spark distribution
# using the bundled docker-image-tool.sh (see LAB_GUIDE.md, Step 7).
# Example: --build-arg SPARK_BASE_IMAGE=<acrname>.azurecr.io/spark-base:3.5.1-java17
# =============================================================================
ARG SPARK_BASE_IMAGE
FROM ${SPARK_BASE_IMAGE}

USER root

COPY --from=builder /build/target/fraud-preprocessor.jar /opt/spark/jars/fraud-preprocessor.jar

# The official Spark docker-image-tool base image creates a non-root
# "spark" user with UID 185 — match it here so the pod does not run as root
# inside the AKS cluster (Pod Security / AKS policy compliance).
USER 185
