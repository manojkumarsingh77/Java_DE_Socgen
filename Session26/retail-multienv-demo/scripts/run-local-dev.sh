#!/usr/bin/env bash
# =====================================================================================
# run-local-dev.sh — macOS (Apple Silicon M1 Max) / Linux
# Runs the packaged jar locally against the 'dev' environment config.
# Mirrors exactly what the IntelliJ Run Configuration does — use this if you'd
# rather run from a terminal.
# =====================================================================================
set -euo pipefail

cd "$(dirname "$0")/.."

# dev uses local-env secrets; this dummy value is only needed if your dev config
# ever points at an abfss:// path. The default application-dev.conf uses a local
# filesystem path so this is not required to run — kept here for completeness.
export RETAIL_SECRET_ADLS_RETAILPLATFORMDEVSA_ACCOUNT_KEY="not-needed-for-local-filesystem-output"

mvn -B clean package -DskipTests

java \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
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
  --add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
  --add-opens=java.base/java.util.concurrent.locks=ALL-UNNAMED \
  --add-opens=java.base/java.nio.charset=ALL-UNNAMED \
  --add-opens=java.base/javax.security.auth=ALL-UNNAMED \
  -Djdk.reflect.useDirectMethodHandle=false \
  -Dretail.env=dev \
  -Dio.netty.tryReflectionSetAccessible=true \
  -Xms1g -Xmx4g \
  -cp target/retail-multienv-demo.jar \
  com.retailbank.dataplatform.Main
