#!/bin/sh
# Resolves the mode from: first CLI arg > DEMO_MODE env var > default "diagnostics"
set -e
MODE="${1:-${DEMO_MODE:-diagnostics}}"
echo "[entrypoint.sh] JAVA_OPTS = ${JAVA_OPTS}"
echo "[entrypoint.sh] mode      = ${MODE}"
exec java ${JAVA_OPTS} -jar app.jar "${MODE}"
