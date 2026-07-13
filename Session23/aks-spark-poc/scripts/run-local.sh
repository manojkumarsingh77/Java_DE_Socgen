#!/usr/bin/env bash
# =============================================================================
# run-local.sh - Build and run the POC locally on macOS (M1 Max) or Linux
# Usage: ./scripts/run-local.sh [dev|staging|prod]
# =============================================================================
set -euo pipefail

ENV_NAME="${1:-dev}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

if [ -f "$PROJECT_ROOT/run-configs/.env" ]; then
  echo "Loading environment variables from run-configs/.env"
  set -a
  # shellcheck disable=SC1091
  source "$PROJECT_ROOT/run-configs/.env"
  set +a
fi

export APP_ENV="$ENV_NAME"
export POD_NAMESPACE="${POD_NAMESPACE:-banking-$ENV_NAME}"
export AZURE_MODE="${AZURE_MODE:-false}"

echo "=============================================================="
echo " Building fat jar via Maven Shade Plugin..."
echo "=============================================================="
cd "$PROJECT_ROOT"
mvn -q clean package -DskipTests

echo "=============================================================="
echo " Running POC | APP_ENV=$ENV_NAME | AZURE_MODE=$AZURE_MODE"
echo "=============================================================="

JAVA_OPTS=$(cat <<'EOF'
--add-opens=java.base/java.lang=ALL-UNNAMED
--add-opens=java.base/java.lang.invoke=ALL-UNNAMED
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED
--add-opens=java.base/java.io=ALL-UNNAMED
--add-opens=java.base/java.net=ALL-UNNAMED
--add-opens=java.base/java.nio=ALL-UNNAMED
--add-opens=java.base/java.util=ALL-UNNAMED
--add-opens=java.base/java.util.concurrent=ALL-UNNAMED
--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED
--add-opens=java.base/sun.nio.cs=ALL-UNNAMED
--add-opens=java.base/sun.security.action=ALL-UNNAMED
--add-opens=java.base/sun.util.calendar=ALL-UNNAMED
--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED
EOF
)

# shellcheck disable=SC2086
java -Xms2g -Xmx6g $JAVA_OPTS \
  -DAPP_ENV="$ENV_NAME" \
  -jar target/aks-spark-poc-shaded.jar

echo "=============================================================="
echo " Done. Check ./data/$ENV_NAME/raw and ./data/$ENV_NAME/curated"
echo "=============================================================="
