# ---------- Stage 1: Build ----------
FROM eclipse-temurin:17-jdk-jammy AS build
WORKDIR /workspace
COPY pom.xml .
COPY src ./src
# Dependency layer caching: resolve deps before copying source changes
RUN apt-get update && apt-get install -y maven --no-install-recommends \
    && mvn -q -B dependency:go-offline \
    && mvn -q -B clean package -DskipTests

# ---------- Stage 2: Runtime (official Spark base, Java 17) ----------
FROM apache/spark:3.5.1-java17
USER root

ENV SPARK_HOME=/opt/spark
WORKDIR /opt/spark/work-dir

COPY --from=build /workspace/target/spark-aks-fraud-demo.jar /opt/spark/jars/spark-aks-fraud-demo.jar

# Non-root execution — required by most AKS Pod Security Admission baseline/restricted policies
RUN useradd -u 1001 -m sparkuser && chown -R sparkuser:sparkuser /opt/spark
USER 1001

ENTRYPOINT ["/opt/spark/bin/spark-submit"]
