#!/bin/bash
# Builds the fat jar and runs a given mode. No Docker required.
# Usage: ./scripts/run-local.sh [ci|cd|pipeline|version|job|bluegreen-demo|canary-demo|reset]
set -e
cd "$(dirname "$0")/.."

echo "[run-local.sh] Building fat jar with Maven ..."
mvn -q clean package -DskipTests

MODE="${1:-pipeline}"

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
java $JAVA_OPTS -jar target/spark-cicd-gitops-demo-1.0.0.jar "$MODE"
