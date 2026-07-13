#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

echo ">> Building fat jar with Maven..."
mvn -B clean package

ADD_OPENS="--add-opens=java.base/java.lang=ALL-UNNAMED \
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

export APP_VERSION="1.0.0-local"
export GIT_COMMIT="$(git rev-parse --short HEAD 2>/dev/null || echo nogit)"
export APP_ENV="local"
export DEPLOY_SLOT="local"
export PORT="8080"

echo ">> Starting Fraud Scoring Service on http://localhost:8080 ..."
java $ADD_OPENS -jar target/fraud-app.jar
