# =============================================================================
# Stage 1 — Build the fat jar with Maven + Temurin 17
# =============================================================================
FROM maven:3.9.6-eclipse-temurin-17 AS builder
WORKDIR /build

COPY pom.xml .
RUN mvn -B dependency:go-offline

COPY src ./src
RUN mvn -B clean package -DskipTests

# =============================================================================
# Stage 2 — Layer onto the same Spark-on-Kubernetes base image built in the
# foundation lab (Step 6 of that LAB_GUIDE.md). Reuse it here:
#   --build-arg SPARK_BASE_IMAGE=<acrname>.azurecr.io/spark:3.5.1
# =============================================================================
ARG SPARK_BASE_IMAGE
FROM ${SPARK_BASE_IMAGE}

USER root
COPY --from=builder /build/target/pos-streaming-aggregator.jar /opt/spark/jars/pos-streaming-aggregator.jar
USER 185
