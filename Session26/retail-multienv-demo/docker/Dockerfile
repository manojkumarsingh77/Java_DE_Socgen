# =====================================================================================
# Multi-stage Dockerfile
#   Stage 1 "builder": Maven + JDK 17 — compiles and shades the fat jar.
#   Stage 2 "runtime": Official Apache Spark 3.5.1 Java 17 base image — copies in only
#                       the shaded jar and environment config files. Keeps the final
#                       image free of Maven, source code, and build caches.
# Build from the REPO ROOT (not from docker/) so the build context includes pom.xml
# and src/:
#   docker build -f docker/Dockerfile -t retail-multienv-demo:1.0.0 .
# =====================================================================================

# ---------- Stage 1: build ----------
FROM maven:3.9.6-eclipse-temurin-17 AS builder

WORKDIR /build

# Copy pom.xml first to leverage Docker layer caching for dependency resolution
COPY pom.xml .
RUN mvn -B dependency:go-offline

COPY src ./src
RUN mvn -B clean package -DskipTests

# ---------- Stage 2: runtime ----------
# apache/spark:3.5.1-java17 ships Spark 3.5.1 built for Java 17 with the
# Kubernetes entrypoint.sh already configured for spark-submit cluster mode.
FROM apache/spark:3.5.1-java17

USER root

ENV SPARK_HOME=/opt/spark
WORKDIR /opt/spark/work-dir

# Application jar (shaded fat jar containing all Spark/Delta/Azure dependencies)
COPY --from=builder /build/target/retail-multienv-demo.jar /opt/spark/jars-app/retail-multienv-demo.jar

# Environment config files are ALSO baked in as a fallback classpath resource,
# but the K8s Deployment mounts an environment-specific ConfigMap at
# /opt/spark/conf/external which takes precedence via -Dretail.config.dir
# (see EnvironmentConfigLoader). This means the SAME image is promoted
# dev -> test -> prod without rebuilding.
COPY --from=builder /build/src/main/resources/application-dev.conf  /opt/spark/conf/bundled/application-dev.conf
COPY --from=builder /build/src/main/resources/application-test.conf /opt/spark/conf/bundled/application-test.conf
COPY --from=builder /build/src/main/resources/application-prod.conf /opt/spark/conf/bundled/application-prod.conf

RUN chmod -R +r /opt/spark/jars-app /opt/spark/conf/bundled && \
    chown -R spark:spark /opt/spark/jars-app /opt/spark/conf/bundled

USER spark

# No CMD/ENTRYPOINT override — the base apache/spark image's entrypoint.sh
# handles both `driver` and `executor` container roles as invoked by the
# Spark-on-Kubernetes scheduler via spark-submit --master k8s://...
