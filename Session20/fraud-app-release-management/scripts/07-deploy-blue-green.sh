#!/usr/bin/env bash
# Stands up the Blue/Green pair + Nginx and resets traffic to 100% blue /
# 0% green (the safe starting point before any canary ramp-up). Then
# smoke-tests the green (candidate) container DIRECTLY, bypassing Nginx
# entirely, using a disposable curl container - proving it's healthy
# BEFORE it ever sees a single unit of real production traffic.
set -euo pipefail
cd "$(dirname "$0")/.."

echo ">> Bringing up prod Blue/Green stack (fraud-app-blue, fraud-app-green, nginx)..."
docker compose -f docker-compose.prod.yml up -d

echo ">> Resetting canary to 0% (100% blue) - safe baseline before rollout..."
bash scripts/08-canary-rollout.sh 0

echo ">> Smoke-testing green (candidate) directly, bypassing the load balancer..."
docker run --rm --network fraudnet curlimages/curl:8.8.0 \
  curl -sf http://fraud-app-green:8080/health && echo " -> green /health OK"
docker run --rm --network fraudnet curlimages/curl:8.8.0 \
  curl -s http://fraud-app-green:8080/version

echo ""
echo ">> Blue/Green stack is up. All live traffic still on blue."
echo ">> Next: bash scripts/08-canary-rollout.sh 10   (start the canary ramp)"
