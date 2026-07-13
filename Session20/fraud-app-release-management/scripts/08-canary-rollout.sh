#!/usr/bin/env bash
# Shifts the percentage of live traffic routed to "green" (the release
# candidate) by rewriting nginx/conf.d/upstream.conf and reloading Nginx
# (a graceful reload - zero dropped connections).
#
# Usage: scripts/08-canary-rollout.sh <0|10|25|50|100>
set -euo pipefail
cd "$(dirname "$0")/.."

PCT="${1:-10}"
CONF="nginx/conf.d/upstream.conf"

case "${PCT}" in
  0)
    BLUE_LINE="server fraud-app-blue:8080;"
    GREEN_LINE="server fraud-app-green:8080 down;"
    ;;
  10)
    BLUE_LINE="server fraud-app-blue:8080 weight=9;"
    GREEN_LINE="server fraud-app-green:8080 weight=1;"
    ;;
  25)
    BLUE_LINE="server fraud-app-blue:8080 weight=3;"
    GREEN_LINE="server fraud-app-green:8080 weight=1;"
    ;;
  50)
    BLUE_LINE="server fraud-app-blue:8080 weight=1;"
    GREEN_LINE="server fraud-app-green:8080 weight=1;"
    ;;
  100)
    BLUE_LINE="server fraud-app-blue:8080 down;"
    GREEN_LINE="server fraud-app-green:8080;"
    ;;
  *)
    echo "Usage: $0 [0|10|25|50|100]"
    exit 1
    ;;
esac

cat > "${CONF}" <<EOF
# Rewritten by scripts/08-canary-rollout.sh at $(date -u +%Y-%m-%dT%H:%M:%SZ)
# Canary weight: ${PCT}% of traffic on green
upstream fraud_backend {
    ${BLUE_LINE}
    ${GREEN_LINE}
}
EOF

echo ">> Canary set to ${PCT}% green traffic. Reloading Nginx (graceful, zero downtime)..."
docker exec fraud-nginx nginx -s reload

echo ">> Done. Verify the traffic split with:"
echo "   for i in \$(seq 1 20); do curl -s http://localhost:8080/version | grep -o '\"deploySlot\":\"[a-z]*\"'; done | sort | uniq -c"
