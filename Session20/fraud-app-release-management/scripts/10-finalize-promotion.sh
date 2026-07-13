#!/usr/bin/env bash
# Run this AFTER a canary has been sitting at 100% green with good metrics
# for a while. It retags prod-green as the new prod-blue in the registry,
# recreates the blue container from that image, and resets the router back
# to a clean 100%-blue / 0%-green baseline - ready for the NEXT release.
set -euo pipefail
cd "$(dirname "$0")/.."

REGISTRY="localhost:5000"

echo ">> Finalizing promotion: green becomes the new stable blue..."
docker pull "${REGISTRY}/fraud-app:prod-green"
docker tag "${REGISTRY}/fraud-app:prod-green" "${REGISTRY}/fraud-app:prod-blue"
docker push "${REGISTRY}/fraud-app:prod-blue"

echo ">> Recreating the blue container from the new image..."
docker compose -f docker-compose.prod.yml pull fraud-app-blue
docker compose -f docker-compose.prod.yml up -d --force-recreate fraud-app-blue

echo ">> Resetting router to 100% blue / 0% green (clean baseline for next release)..."
bash scripts/08-canary-rollout.sh 0

echo ">> Promotion finalized. Deploy the NEXT release candidate to prod-green when ready."
