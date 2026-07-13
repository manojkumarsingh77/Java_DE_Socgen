# =============================================================================
# Multi-stage Dockerfile - AKS Architecture Deep Dive POC
# Stage 1: Maven build (cached layer for dependency resolution)
# Stage 2: Spark 3.5.1 + Java 17 runtime (minimal footprint, linux/amd64 +
#          linux/arm64 compatible - build with `docker buildx build --platform`
#          if pushing from an M1 Max laptop to an amd64 AKS node pool)
# =============================================================================

# ---------- STAGE 1: Build ----------
FROM maven:3.9.6-eclipse-temurin-17 AS builder

WORKDIR /build

# Layer-cache dependency resolution: copy ONLY pom.xml first so Docker skips
# re-downloading all dependencies when only source code changes.
COPY pom.xml .
RUN mvn -q dependency:go-offline -B

COPY src ./src
RUN mvn -q clean package -DskipTests -B

# ---------- STAGE 2: Runtime ----------
FROM apache/spark:3.5.1-java17

# apache/spark base image already contains:
#   - Eclipse Temurin JRE 17
#   - Spark 3.5.1 distribution at /opt/spark
#   - Non-root `spark` user (UID 185) - required by AKS Pod Security Standards
#     "restricted" profile (runAsNonRoot: true)

USER root

# Delta Lake + Azure connector jars must be on Spark's runtime classpath
# INSIDE the container (spark-submit --packages is avoided in production to
# eliminate a runtime dependency on Maven Central reachability from inside
# the AKS cluster's restricted egress NetworkPolicy).
COPY --from=builder /build/target/aks-spark-poc-shaded.jar /opt/spark/jars/aks-spark-poc-shaded.jar

# Executor pod template referenced by spark.kubernetes.executor.podTemplateFile
COPY k8s/executor-pod-template.yaml /opt/spark/conf/executor-pod-template.yaml

RUN chown -R spark:spark /opt/spark/jars/aks-spark-poc-shaded.jar /opt/spark/conf/executor-pod-template.yaml

USER spark
WORKDIR /opt/spark/work-dir

ENTRYPOINT ["/opt/entrypoint.sh"]
