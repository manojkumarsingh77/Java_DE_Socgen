#!/bin/sh
set -e
MODE="${1:-pipeline}"
echo "[entrypoint.sh] JAVA_OPTS = ${JAVA_OPTS}"
echo "[entrypoint.sh] mode      = ${MODE}"
exec java ${JAVA_OPTS} -jar app.jar "${MODE}"
