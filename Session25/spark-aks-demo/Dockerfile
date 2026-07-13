# syntax=docker/dockerfile:1

########################################
# Stage 1 — build the application jar
########################################
FROM maven:3.9.9-eclipse-temurin-17 AS builder
WORKDIR /build

# Copy pom first so dependency resolution is cached as its own layer —
# only re-runs when pom.xml actually changes, not on every source edit.
COPY pom.xml .
RUN mvn -B -q dependency:go-offline

COPY src ./src
RUN mvn -B -q clean package -DskipTests

########################################
# Stage 2 — runtime image
########################################
FROM apache/spark:3.5.3-java17-python3 AS runtime

USER root

# Explicitly use 185:0 (Spark's default UID/GID) to ensure absolute
# read/write compatibility on Kubernetes PVs and container layers.
RUN mkdir -p /opt/spark-apps/output && \
    chown -R 185:0 /opt/spark-apps && \
    chmod -R 775 /opt/spark-apps

# Copy the compiled executable from the build stage
COPY --from=builder /build/target/spark-aks-demo-1.0.0.jar /opt/spark-apps/spark-aks-demo.jar

# Revert execution context to Spark's unprivileged user ID
USER 185
WORKDIR /opt/spark-apps