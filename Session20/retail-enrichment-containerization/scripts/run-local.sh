#!/usr/bin/env bash
# Build the fat jar with Maven and run it locally (no Docker) on Java 17.
# Useful to sanity-check the job before containerizing it.
set -euo pipefail

cd "$(dirname "$0")/.."

echo ">> Building fat jar with Maven..."
mvn -B clean package -DskipTests

JAVA_ADD_OPENS="--add-opens=java.base/java.lang=ALL-UNNAMED \
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

echo ">> Running job locally..."
java $JAVA_ADD_OPENS -jar target/retail-enrichment.jar data output

echo ">> Done. Check the 'output/' directory."
