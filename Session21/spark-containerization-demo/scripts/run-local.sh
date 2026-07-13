#!/bin/bash
# Builds the fat jar and runs it directly (no Docker) - use this to demo the
# BASELINE ("uncontainerized") behaviour before introducing Docker at all.
# Usage: ./scripts/run-local.sh [diagnostics|job|stress-memory|stress-cpu|all]
set -e
cd "$(dirname "$0")/.."

echo "[run-local.sh] Building fat jar with Maven ..."
mvn -q clean package -DskipTests

MODE="${1:-all}"

# These --add-opens flags are ONLY needed for "java -cp ... MainClass" or when
# IntelliJ launches the main() method directly, because that path does NOT read
# the jar's manifest. Running the fat jar itself (java -jar ...) picks up the
# equivalent Add-Opens entries automatically from MANIFEST.MF (see pom.xml).
JAVA_OPTS="--add-opens=java.base/java.lang=ALL-UNNAMED \
--add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
--add-opens=java.base/java.io=ALL-UNNAMED \
--add-opens=java.base/java.net=ALL-UNNAMED \
--add-opens=java.base/java.nio=ALL-UNNAMED \
--add-opens=java.base/java.util=ALL-UNNAMED \
--add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
--add-opens=java.base/sun.nio.cs=ALL-UNNAMED \
--add-opens=java.base/sun.security.action=ALL-UNNAMED \
--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"

echo "[run-local.sh] Running mode = $MODE"
java $JAVA_OPTS -jar target/spark-containerization-demo-1.0.0.jar "$MODE"
