#!/usr/bin/env bash
# Starts a local Docker Registry container as a stand-in for Azure Container
# Registry (ACR) so the whole promotion flow (dev -> stage -> prod tags) is
# runnable on a laptop with no cloud account. See README section 7 for the
# real `az acr` command equivalents.
set -euo pipefail

if docker ps --format '{{.Names}}' | grep -q '^local-registry$'; then
  echo ">> local-registry already running on port 5000"
  exit 0
fi

if docker ps -a --format '{{.Names}}' | grep -q '^local-registry$'; then
  echo ">> starting existing local-registry container"
  docker start local-registry
  exit 0
fi

echo ">> starting a fresh local-registry container (ACR stand-in) on port 5000"
docker run -d -p 5000:5000 --restart=always --name local-registry registry:2
echo ">> Registry ready at localhost:5000"
