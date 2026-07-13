# =============================================================================
# Stage 1 — Build with Maven + Temurin 17
# =============================================================================
FROM maven:3.9.6-eclipse-temurin-17 AS builder
WORKDIR /build
COPY pom.xml .
RUN mvn -B dependency:go-offline
COPY src ./src
RUN mvn -B clean package -DskipTests

# =============================================================================
# Stage 2 — Minimal JRE runtime (no Spark dependency at all for this service)
# =============================================================================
FROM eclipse-temurin:17-jre-jammy
WORKDIR /app
COPY --from=builder /build/target/streaming-health-dashboard.jar /app/streaming-health-dashboard.jar

EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/streaming-health-dashboard.jar"]
