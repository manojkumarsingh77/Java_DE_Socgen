#!/usr/bin/env bash
# Instant rollback: send 100% of traffic back to blue (the last known-good
# version). This is the entire point of Blue/Green - rollback is a
# config/reload operation, not a redeploy, so it completes in seconds.
set -euo pipefail
cd "$(dirname "$0")/.."

echo ">> ROLLBACK: forcing 100% traffic back to blue..."
bash scripts/08-canary-rollout.sh 0
echo ">> Rollback complete. green is now fully idle again; investigate before retrying."
