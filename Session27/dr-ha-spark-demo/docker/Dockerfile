# =====================================================================
# Stage 1: Build — compile + shade the fat jar with Maven + Java 17
# =====================================================================
FROM maven:3.9.6-eclipse-temurin-17 AS build
WORKDIR /build

# Leverage Docker layer caching: resolve dependencies before copying source
COPY pom.xml .
RUN mvn -B dependency:go-offline

COPY src ./src
RUN mvn -B clean package -DskipTests

# =====================================================================
# Stage 2: Runtime — Spark-on-Kubernetes base image + our shaded jar only
# =====================================================================
# apache/spark:3.5.1-java17 ships Spark 3.5.1, Java 17, and the K8s entrypoint.sh
# used by spark-submit --master k8s://... in cluster mode.
FROM apache/spark:3.5.1-java17

USER root

ENV SPARK_HOME=/opt/spark
ENV APP_HOME=/opt/dr-ha-demo

RUN mkdir -p ${APP_HOME}/jars ${APP_HOME}/reports ${APP_HOME}/backups && \
    useradd -u 1001 -m -d /home/sparkuser sparkuser && \
    chown -R 1001:1001 ${APP_HOME}

COPY --from=build /build/target/cross-region-failover-simulation.jar ${APP_HOME}/jars/app.jar

# Delta Lake + Azure connectors needed at runtime but excluded from the shaded jar
# (kept as separate jars for AKS driver/executor --jars or --packages resolution
#  when spark-submit runs from a CI pipeline with network access; for a fully
#  offline image, `RUN mvn dependency:copy-dependencies` here instead).
COPY docker/entrypoint-extra-jars.txt ${APP_HOME}/jars/README-EXTRA-JARS.txt

WORKDIR ${APP_HOME}
USER 1001

# spark-submit (see k8s/spark-submit-command.sh) sets --class/--master/--conf;
# this ENTRYPOINT is inherited unmodified from the base image and dispatches
# correctly whether the container is started as driver or executor by the
# Spark-on-Kubernetes scheduler.
ENTRYPOINT ["/opt/entrypoint.sh"]
